from cnslibs.cns import cns_baseclass
from cnslibs.common import heketi_ops
from cnslibs.common.openshift_ops import (
    get_gluster_vol_info_by_pvc_name,
    oc_create_pvc,
    oc_create_sc,
    oc_create_secret,
    oc_create_tiny_pod_with_volume,
    oc_delete,
    verify_pvc_status_is_bound,
    wait_for_pod_be_ready,
    wait_for_resource_absence,
)


class TestArbiterVolumeCreateExpandDelete(cns_baseclass.CnsBaseClass):

    def setUp(self):
        super(TestArbiterVolumeCreateExpandDelete, self).setUp()

        # Skip test if it is not CNS deployment
        if self.deployment_type != "cns":
            raise self.skipTest("This test can run only on CNS deployment.")
        self.node = self.ocp_master_node[0]

        # Mark one of the Heketi nodes as arbiter-supported if none of
        # existent nodes or devices already enabled to support it.
        heketi_server_url = self.cns_storage_class['storage_class1']['resturl']
        arbiter_tags = ('required', 'supported')
        arbiter_already_supported = False

        self.node_id_list = heketi_ops.heketi_node_list(
            self.heketi_client_node, heketi_server_url)

        for node_id in self.node_id_list[::-1]:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, heketi_server_url, node_id, json=True)
            if node_info.get('tags', {}).get('arbiter') in arbiter_tags:
                arbiter_already_supported = True
                break
            for device in node_info['devices'][::-1]:
                if device.get('tags', {}).get('arbiter') in arbiter_tags:
                    arbiter_already_supported = True
                    break
            else:
                continue
            break
        if not arbiter_already_supported:
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, heketi_server_url,
                'node', self.node_id_list[0], 'supported')

    def _set_arbiter_tag_with_further_revert(self, node, server_url,
                                             source, source_id, tag_value,
                                             revert_to=None):
        heketi_ops.set_arbiter_tag(
            node, server_url, source, source_id, tag_value)
        if revert_to is None:
            self.addCleanup(
                heketi_ops.rm_arbiter_tag, node, server_url, source, source_id)
        else:
            self.addCleanup(
                heketi_ops.set_arbiter_tag,
                node, server_url, source, source_id, tag_value)

    def _create_storage_class(self):
        sc = self.cns_storage_class['storage_class1']
        secret = self.cns_secret['secret1']

        # Create secret file for usage in storage class
        self.secret_name = oc_create_secret(
            self.node, namespace=secret['namespace'],
            data_key=self.heketi_cli_key, secret_type=secret['type'])
        self.addCleanup(
            oc_delete, self.node, 'secret', self.secret_name)

        # Create storage class
        self.sc_name = oc_create_sc(
            self.node, resturl=sc['resturl'],
            restuser=sc['restuser'], secretnamespace=sc['secretnamespace'],
            secretname=self.secret_name,
            volumeoptions="user.heketi.arbiter true",
        )
        self.addCleanup(oc_delete, self.node, 'sc', self.sc_name)

    def _create_and_wait_for_pvc(self, pvc_size=1):
        # Create PVC
        self.pvc_name = oc_create_pvc(
            self.node, self.sc_name, pvc_name_prefix='arbiter-pvc',
            pvc_size=pvc_size)
        self.addCleanup(
            wait_for_resource_absence, self.node, 'pvc', self.pvc_name)
        self.addCleanup(oc_delete, self.node, 'pvc', self.pvc_name)

        # Wait for PVC to be in bound state
        verify_pvc_status_is_bound(self.node, self.pvc_name)

    def test_arbiter_pvc_create(self):
        """Test case CNS-944"""

        # Create sc with gluster arbiter info
        self._create_storage_class()

        # Create PVC and wait for it to be in 'Bound' state
        self._create_and_wait_for_pvc()

        # Get vol info
        vol_info = get_gluster_vol_info_by_pvc_name(self.node, self.pvc_name)

        # Verify amount proportion of data and arbiter bricks
        bricks = vol_info['bricks']['brick']
        arbiter_brick_amount = sum([int(b['isArbiter']) for b in bricks])
        data_brick_amount = len(bricks) - arbiter_brick_amount
        self.assertGreater(
            data_brick_amount, 0,
            "Data brick amount is expected to be bigger than 0. "
            "Actual amount is '%s'." % arbiter_brick_amount)
        self.assertGreater(
            arbiter_brick_amount, 0,
            "Arbiter brick amount is expected to be bigger than 0. "
            "Actual amount is '%s'." % arbiter_brick_amount)
        self.assertEqual(
            data_brick_amount,
            (arbiter_brick_amount * 2),
            "Expected 1 arbiter brick per 2 data bricks. "
            "Arbiter brick amount is '%s', Data brick amount is '%s'." % (
                arbiter_brick_amount, data_brick_amount)
        )

    def test_arbiter_pvc_mount_on_pod(self):
        """Test case CNS-945"""

        # Create sc with gluster arbiter info
        self._create_storage_class()

        # Create PVC and wait for it to be in 'Bound' state
        self._create_and_wait_for_pvc()

        # Create POD with attached volume
        mount_path = "/mnt"
        pod_name = oc_create_tiny_pod_with_volume(
            self.node, self.pvc_name, "test-arbiter-pvc-mount-on-app-pod",
            mount_path=mount_path)
        self.addCleanup(oc_delete, self.node, 'pod', pod_name)

        # Wait for POD be up and running
        wait_for_pod_be_ready(self.node, pod_name, timeout=60, wait_step=2)

        # Get volume ID
        vol_info = get_gluster_vol_info_by_pvc_name(self.node, self.pvc_name)
        vol_id = vol_info["gluster_vol_id"]

        # Verify that POD has volume mounted on it
        cmd = "oc exec {0} -- df -PT {1} | grep {1}".format(
            pod_name, mount_path)
        out = self.cmd_run(cmd)
        err_msg = ("Failed to get info about mounted '%s' volume. "
                   "Output is empty." % vol_id)
        self.assertTrue(out, err_msg)

        # Verify volume data on POD
        # Filesystem  Type           Size    Used  Avail   Cap Mounted on
        # IP:vol_id   fuse.glusterfs 1038336 33408 1004928  3% /mnt
        data = [s for s in out.strip().split(' ') if s]
        actual_vol_id = data[0].split(':')[-1]
        self.assertEqual(
            vol_id, actual_vol_id,
            "Volume ID does not match: expected is "
            "'%s' and actual is '%s'." % (vol_id, actual_vol_id))
        self.assertIn(
            "gluster", data[1],
            "Filesystem type is expected to be of 'glusterfs' type. "
            "Actual value is '%s'." % data[1])
        self.assertEqual(
            mount_path, data[6],
            "Unexpected mount path. Expected is '%s' and actual is '%s'." % (
                mount_path, data[6]))
        max_size = 1024 ** 2
        total_size = int(data[2])
        self.assertLessEqual(
            total_size, max_size,
            "Volume has bigger size '%s' than expected - '%s'." % (
                total_size, max_size))
        min_available_size = int(max_size * 0.95)
        available_size = int(data[4])
        self.assertLessEqual(
            min_available_size, available_size,
            "Minimum available size (%s) not satisfied. Actual is '%s'." % (
                min_available_size, available_size))

        # Write data on mounted volume
        write_data_cmd = (
            "dd if=/dev/zero of=%s/file$i bs=%s count=1; " % (
                mount_path, available_size))
        self.cmd_run(write_data_cmd)

    def test_create_arbiter_vol_with_more_than_one_brick_set(self):
        """Test case CNS-942"""

        # Set arbiter:disabled tag to the data devices and get their info
        heketi_server_url = self.cns_storage_class['storage_class1']['resturl']
        data_nodes = []
        for node_id in self.node_id_list[0:2]:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, heketi_server_url, node_id, json=True)

            if len(node_info['devices']) < 2:
                self.skipTest(
                    "Nodes are expected to have at least 2 devices")
            if not all([int(d['storage']['free']) > (3 * 1024**2)
                        for d in node_info['devices'][0:2]]):
                self.skipTest(
                    "Devices are expected to have more than 3Gb of free space")
            for device in node_info['devices']:
                self._set_arbiter_tag_with_further_revert(
                    self.heketi_client_node, heketi_server_url,
                    'device', device['id'], 'disabled',
                    device.get('tags', {}).get('arbiter'))
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, heketi_server_url,
                'node', node_id, 'disabled',
                node_info.get('tags', {}).get('arbiter'))

            data_nodes.append(node_info)

        # Set arbiter:required tag to all other nodes and their devices
        for node_id in self.node_id_list[2:]:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, heketi_server_url, node_id, json=True)
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, heketi_server_url,
                'node', node_id, 'required',
                node_info.get('tags', {}).get('arbiter'))
            for device in node_info['devices']:
                self._set_arbiter_tag_with_further_revert(
                    self.heketi_client_node, heketi_server_url,
                    'device', device['id'], 'required',
                    device.get('tags', {}).get('arbiter'))

        # Get second big volume between 2 data nodes and use it
        # for target vol calculation.
        for i, node_info in enumerate(data_nodes):
            biggest_disk_free_space = 0
            for device in node_info['devices'][0:2]:
                free = int(device['storage']['free'])
                if free > biggest_disk_free_space:
                    biggest_disk_free_space = free
            data_nodes[i]['biggest_free_space'] = biggest_disk_free_space
        target_vol_size_kb = 1 + min([
            n['biggest_free_space'] for n in data_nodes])

        # Check that all the data devices have, at least, half of required size
        all_big_enough = True
        for node_info in data_nodes:
            for device in node_info['devices'][0:2]:
                if float(device['storage']['free']) < (target_vol_size_kb / 2):
                    all_big_enough = False
                    break

        # Create sc with gluster arbiter info
        self._create_storage_class()

        # Create helper arbiter vol if not all the data devices have
        # half of required free space.
        if not all_big_enough:
            helper_vol_size_kb, target_vol_size_kb = 0, 0
            smaller_device_id = None
            for node_info in data_nodes:
                devices = node_info['devices']
                if ((devices[0]['storage']['free']) > (
                        devices[1]['storage']['free'])):
                    smaller_device_id = devices[1]['id']
                    smaller_device = devices[1]['storage']['free']
                    bigger_device = devices[0]['storage']['free']
                else:
                    smaller_device_id = devices[0]['id']
                    smaller_device = devices[0]['storage']['free']
                    bigger_device = devices[1]['storage']['free']
                diff = bigger_device - (2 * smaller_device) + 1
                if diff > helper_vol_size_kb:
                    helper_vol_size_kb = diff
                    target_vol_size_kb = bigger_device - diff

            # Disable smaller device and create helper vol on bigger one
            # to reduce its size, then enable smaller device back.
            try:
                out = heketi_ops.heketi_device_disable(
                    self.heketi_client_node, heketi_server_url,
                    smaller_device_id)
                self.assertTrue(out)
                self._create_and_wait_for_pvc(
                    int(helper_vol_size_kb / 1024.0**2) + 1)
            finally:
                out = heketi_ops.heketi_device_enable(
                    self.heketi_client_node, heketi_server_url,
                    smaller_device_id)
                self.assertTrue(out)

        # Create target arbiter volume
        self._create_and_wait_for_pvc(int(target_vol_size_kb / 1024.0**2))

        # Get gluster volume info
        vol_info = get_gluster_vol_info_by_pvc_name(self.node, self.pvc_name)

        # Check amount of bricks
        bricks = vol_info['bricks']['brick']
        arbiter_brick_amount = sum([int(b['isArbiter']) for b in bricks])
        data_brick_amount = len(bricks) - arbiter_brick_amount
        self.assertEqual(
            data_brick_amount,
            (arbiter_brick_amount * 2),
            "Expected 1 arbiter brick per 2 data bricks. "
            "Arbiter brick amount is '%s', Data brick amount is '%s'." % (
                arbiter_brick_amount, data_brick_amount))
        self.assertGreater(
            data_brick_amount, 3,
            "Data brick amount is expected to be bigger than 3. "
            "Actual amount is '%s'." % data_brick_amount)
        self.assertGreater(
            arbiter_brick_amount, 1,
            "Arbiter brick amount is expected to be bigger than 1. "
            "Actual amount is '%s'." % arbiter_brick_amount)
