import ddt

from cnslibs.cns import cns_baseclass
from cnslibs.common import heketi_ops
from cnslibs.common.openshift_ops import (
    get_gluster_vol_info_by_pvc_name,
    get_ocp_gluster_pod_names,
    oc_create_pvc,
    oc_create_sc,
    oc_create_secret,
    oc_create_tiny_pod_with_volume,
    oc_delete,
    verify_pvc_status_is_bound,
    wait_for_pod_be_ready,
    wait_for_resource_absence,
)


@ddt.ddt
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
        if tag_value is None:
            # Remove arbiter tag logic
            heketi_ops.rm_arbiter_tag(node, server_url, source, source_id)
            if revert_to is not None:
                self.addCleanup(heketi_ops.set_arbiter_tag,
                                node, server_url, source, source_id, revert_to)
        else:
            # Add arbiter tag logic
            heketi_ops.set_arbiter_tag(
                node, server_url, source, source_id, tag_value)
            if revert_to is not None:
                self.addCleanup(heketi_ops.set_arbiter_tag,
                                node, server_url, source, source_id, revert_to)
            else:
                self.addCleanup(heketi_ops.rm_arbiter_tag,
                                node, server_url, source, source_id)

    def _create_storage_class(self, avg_file_size=None):
        sc = self.cns_storage_class['storage_class1']
        secret = self.cns_secret['secret1']

        # Create secret file for usage in storage class
        self.secret_name = oc_create_secret(
            self.node, namespace=secret['namespace'],
            data_key=self.heketi_cli_key, secret_type=secret['type'])
        self.addCleanup(
            oc_delete, self.node, 'secret', self.secret_name)

        vol_options = "user.heketi.arbiter true"
        if avg_file_size:
            vol_options += ",user.heketi.average-file-size %s" % avg_file_size

        # Create storage class
        self.sc_name = oc_create_sc(
            self.node, resturl=sc['resturl'],
            restuser=sc['restuser'], secretnamespace=sc['secretnamespace'],
            secretname=self.secret_name,
            volumeoptions=vol_options,
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
        min_available_size = int(max_size * 0.93)
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

    # NOTE(vponomar): do not create big volumes setting value less than 64
    # for 'avg_file_size'. It will cause creation of very huge amount of files
    # making one test run very loooooooong.
    @ddt.data(
        (2, 0), # noqa: equivalent of 64KB of avg size
        (1, 4),
        (2, 64),
        (3, 128),
        (3, 256),
        (5, 512),
        (5, 1024),
        (5, 10240),
        (10, 1024000),
    )
    @ddt.unpack
    def test_verify_arbiter_brick_able_to_contain_expected_amount_of_files(
            self, pvc_size_gb, avg_file_size):
        """Test cases CNS-1182-1190"""

        # Create sc with gluster arbiter info
        self._create_storage_class(avg_file_size)

        # Create PVC and wait for it to be in 'Bound' state
        self._create_and_wait_for_pvc(pvc_size_gb)

        # Get volume info
        vol_info = get_gluster_vol_info_by_pvc_name(self.node, self.pvc_name)
        bricks = vol_info['bricks']['brick']
        arbiter_bricks = []
        data_bricks = []
        for brick in bricks:
            if int(brick['isArbiter']) == 1:
                arbiter_bricks.append(brick)
            else:
                data_bricks.append(brick)

        # Verify proportion of data and arbiter bricks
        arbiter_brick_amount = len(arbiter_bricks)
        data_brick_amount = len(data_bricks)
        expected_file_amount = pvc_size_gb * 1024**2 / (avg_file_size or 64)
        expected_file_amount = expected_file_amount / arbiter_brick_amount
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

        # Try to create expected amount of files on arbiter brick mount
        passed_arbiter_bricks = []
        not_found = "Mount Not Found"
        gluster_pods = get_ocp_gluster_pod_names(self.node)
        for brick in arbiter_bricks:
            for gluster_pod in gluster_pods:
                # "brick path" looks like following:
                # ip_addr:/path/to/vg/brick_unique_name/brick
                # So, we remove "ip_addr" and "/brick" parts to have mount path
                brick_path = brick["name"].split(":")[-1]
                cmd = "oc exec %s -- mount | grep %s || echo '%s'" % (
                    gluster_pod, brick_path[0:-6], not_found)
                out = self.cmd_run(cmd)
                if out != not_found:
                    cmd = (
                        "oc exec %s -- python -c \"["
                        "    open('%s/foo_file{0}'.format(i), 'a').close()"
                        "    for i in range(%s)"
                        "]\"" % (gluster_pod, brick_path, expected_file_amount)
                    )
                    out = self.cmd_run(cmd)
                    passed_arbiter_bricks.append(brick_path)
                    break

        # Make sure all the arbiter bricks were checked
        for brick in arbiter_bricks:
            self.assertIn(
                brick["name"].split(":")[-1], passed_arbiter_bricks,
                "Arbiter brick '%s' was not verified. Looks like it was "
                "not found on any of gluster nodes." % brick_path)

    @ddt.data(True, False)
    def test_aribiter_required_tag_on_node_or_devices_other_disabled(
            self, node_with_tag):
        """Test cases CNS-989 and CNS-997"""

        pvc_amount = 3

        # Get Heketi nodes info
        heketi_server_url = self.cns_storage_class['storage_class1']['resturl']
        node_id_list = heketi_ops.heketi_node_list(
            self.heketi_client_node, heketi_server_url)

        # Set arbiter:required tags
        arbiter_node = heketi_ops.heketi_node_info(
            self.heketi_client_node, heketi_server_url, node_id_list[0],
            json=True)
        arbiter_nodes_ip_addresses = arbiter_node['hostnames']['storage']
        self._set_arbiter_tag_with_further_revert(
            self.heketi_client_node, heketi_server_url, 'node',
            node_id_list[0], ('required' if node_with_tag else None),
            revert_to=arbiter_node.get('tags', {}).get('arbiter'))
        for device in arbiter_node['devices']:
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, heketi_server_url, 'device',
                device['id'], (None if node_with_tag else 'required'),
                revert_to=device.get('tags', {}).get('arbiter'))

        # Set arbiter:disabled tags
        data_nodes, data_nodes_ip_addresses = [], []
        for node_id in node_id_list[1:]:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, heketi_server_url, node_id, json=True)
            if not any([int(d['storage']['free']) > (pvc_amount * 1024**2)
                        for d in node_info['devices']]):
                self.skipTest(
                    "Devices are expected to have more than "
                    "%sGb of free space" % pvc_amount)
            data_nodes_ip_addresses.extend(node_info['hostnames']['storage'])
            for device in node_info['devices']:
                self._set_arbiter_tag_with_further_revert(
                    self.heketi_client_node, heketi_server_url, 'device',
                    device['id'], (None if node_with_tag else 'disabled'),
                    revert_to=device.get('tags', {}).get('arbiter'))
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, heketi_server_url, 'node',
                node_id, ('disabled' if node_with_tag else None),
                revert_to=node_info.get('tags', {}).get('arbiter'))
            data_nodes.append(node_info)

        # Create PVCs and check that their bricks are correctly located
        self._create_storage_class()
        for i in range(pvc_amount):
            self._create_and_wait_for_pvc(1)

            # Get gluster volume info
            vol_info = get_gluster_vol_info_by_pvc_name(
                self.node, self.pvc_name)
            arbiter_bricks, data_bricks = [], []
            for brick in vol_info['bricks']['brick']:
                if int(brick["isArbiter"]) == 1:
                    arbiter_bricks.append(brick["name"])
                else:
                    data_bricks.append(brick["name"])

            # Verify that all the arbiter bricks are located on
            # arbiter:required node and data bricks on all other nodes only.
            for arbiter_brick in arbiter_bricks:
                self.assertIn(
                    arbiter_brick.split(':')[0], arbiter_nodes_ip_addresses)
            for data_brick in data_bricks:
                self.assertIn(
                    data_brick.split(':')[0], data_nodes_ip_addresses)

    def test_create_delete_pvcs_to_make_gluster_reuse_released_space(self):
        """Test case CNS-1265"""
        min_storage_gb = 10

        # Set arbiter:disabled tags to the first 2 nodes
        data_nodes = []
        biggest_disks = []
        heketi_server_url = self.cns_storage_class['storage_class1']['resturl']
        self.assertGreater(len(self.node_id_list), 2)
        for node_id in self.node_id_list[0:2]:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, heketi_server_url, node_id, json=True)
            biggest_disk_free_space = 0
            for device in node_info['devices']:
                disk_free_space = int(device['storage']['free'])
                if disk_free_space < (min_storage_gb * 1024**2):
                    self.skipTest(
                        "Devices are expected to have more than "
                        "%sGb of free space" % min_storage_gb)
                if disk_free_space > biggest_disk_free_space:
                    biggest_disk_free_space = disk_free_space
                self._set_arbiter_tag_with_further_revert(
                    self.heketi_client_node, heketi_server_url, 'device',
                    device['id'], 'disabled',
                    revert_to=device.get('tags', {}).get('arbiter'))
            biggest_disks.append(biggest_disk_free_space)
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, heketi_server_url, 'node',
                node_id, 'disabled',
                revert_to=node_info.get('tags', {}).get('arbiter'))
            data_nodes.append(node_info)

        # Set arbiter:required tag to all other nodes and their devices
        arbiter_nodes = []
        for node_id in self.node_id_list[2:]:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, heketi_server_url, node_id, json=True)
            for device in node_info['devices']:
                self._set_arbiter_tag_with_further_revert(
                    self.heketi_client_node, heketi_server_url, 'device',
                    device['id'], 'required',
                    revert_to=device.get('tags', {}).get('arbiter'))
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, heketi_server_url, 'node',
                node_id, 'required',
                revert_to=node_info.get('tags', {}).get('arbiter'))
            arbiter_nodes.append(node_info)

        # Calculate size and amount of volumes to be created
        pvc_size = int(min(biggest_disks) / 1024**2)
        pvc_amount = max([len(n['devices']) for n in data_nodes]) + 1

        # Create sc with gluster arbiter info
        self._create_storage_class()

        # Create and delete 3 small volumes concurrently
        pvc_names = []
        for i in range(3):
            pvc_name = oc_create_pvc(
                self.node, self.sc_name, pvc_name_prefix='arbiter-pvc',
                pvc_size=int(pvc_size / 3))
            pvc_names.append(pvc_name)
        exception_exists = False
        for pvc_name in pvc_names:
            try:
                verify_pvc_status_is_bound(self.node, pvc_name)
            except Exception:
                for pvc_name in pvc_names:
                    self.addCleanup(
                        wait_for_resource_absence, self.node, 'pvc', pvc_name)
                for pvc_name in pvc_names:
                    self.addCleanup(oc_delete, self.node, 'pvc', pvc_name)
                exception_exists = True
        if exception_exists:
            raise
        for pvc_name in pvc_names:
            oc_delete(self.node, 'pvc', pvc_name)
        for pvc_name in pvc_names:
            wait_for_resource_absence(self.node, 'pvc', pvc_name)

        # Create and delete big volumes in a loop
        for i in range(pvc_amount):
            pvc_name = oc_create_pvc(
                self.node, self.sc_name, pvc_name_prefix='arbiter-pvc',
                pvc_size=pvc_size)
            try:
                verify_pvc_status_is_bound(self.node, pvc_name)
            except Exception:
                self.addCleanup(
                    wait_for_resource_absence, self.node, 'pvc', pvc_name)
                self.addCleanup(oc_delete, self.node, 'pvc', pvc_name)
                raise
            oc_delete(self.node, 'pvc', pvc_name)
            wait_for_resource_absence(self.node, 'pvc', pvc_name)
