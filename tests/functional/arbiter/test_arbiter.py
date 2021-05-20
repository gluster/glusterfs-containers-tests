import re

import ddt
from glusto.core import Glusto as g
from glustolibs.gluster import volume_ops
import pytest

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import gluster_ops
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import heketi_version
from openshiftstoragelibs import node_ops
from openshiftstoragelibs import openshift_ops
from openshiftstoragelibs import openshift_version
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import utils
from openshiftstoragelibs import waiter

BRICK_REGEX = r"^(.*):\/var\/lib\/heketi\/mounts\/(.*)\/brick$"
HEKETI_VOLS = re.compile(r"Id:(\S+)\s+Cluster:(\S+)\s+Name:(\S+)")
DEVICE_REGEX = re.compile(r"vg_\w*")


@ddt.ddt
class TestArbiterVolumeCreateExpandDelete(baseclass.BaseClass):

    def setUp(self):
        super(TestArbiterVolumeCreateExpandDelete, self).setUp()
        self.node = self.ocp_master_node[0]
        if openshift_version.get_openshift_version() < "3.9":
            self.skipTest("Arbiter feature cannot be used on OCP older "
                          "than 3.9, because 'volumeoptions' for Heketi "
                          "is not supported there.")
        version = heketi_version.get_heketi_version(self.heketi_client_node)
        if version < '6.0.0-11':
            self.skipTest("heketi-client package %s does not support arbiter "
                          "functionality" % version.v_str)

        # Mark one of the Heketi nodes as arbiter-supported if none of
        # existent nodes or devices already enabled to support it.
        self.heketi_server_url = self.sc.get('resturl')
        arbiter_tags = ('required', 'supported')
        arbiter_already_supported = False

        self.node_id_list = heketi_ops.heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)

        for node_id in self.node_id_list[::-1]:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
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
                self.heketi_client_node, self.heketi_server_url,
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

    def verify_amount_and_proportion_of_arbiter_and_data_bricks(
            self, vol_info, arbiter_bricks=1, data_bricks=2):
        # Verify amount and proportion of arbiter and data bricks
        bricks_list = vol_info['bricks']['brick']
        bricks = {
            'arbiter_list': [],
            'data_list': [],
            'arbiter_amount': 0,
            'data_amount': 0
        }

        for brick in bricks_list:
            if int(brick['isArbiter']) == 1:
                bricks['arbiter_list'].append(brick)
            else:
                bricks['data_list'].append(brick)

        bricks['arbiter_amount'] = len(bricks['arbiter_list'])
        bricks['data_amount'] = len(bricks['data_list'])

        self.assertGreaterEqual(
            bricks['arbiter_amount'], arbiter_bricks,
            "Arbiter brick amount is expected to be Greater or Equal to %s. "
            "Actual amount is '%s'." % (
                arbiter_bricks, bricks['arbiter_amount']))

        self.assertGreaterEqual(
            bricks['data_amount'], data_bricks,
            "Data brick amount is expected to be Greater or Equal to %s. "
            "Actual amount is '%s'." % (data_bricks, bricks['data_amount']))

        self.assertEqual(
            bricks['data_amount'],
            (bricks['arbiter_amount'] * 2),
            "Expected 1 arbiter brick per 2 data bricks. "
            "Arbiter brick amount is '%s', Data brick amount is '%s'." % (
                bricks['arbiter_amount'], bricks['data_amount'])
        )

        return bricks

    @pytest.mark.tier1
    def test_arbiter_pvc_create(self):
        """Validate dynamic provision of an arbiter volume"""

        # Create sc with gluster arbiter info
        self.create_storage_class(is_arbiter_vol=True)

        # Create PVC and wait for it to be in 'Bound' state
        self.create_and_wait_for_pvc()

        # Get vol info
        vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
            self.node, self.pvc_name)

        self.verify_amount_and_proportion_of_arbiter_and_data_bricks(vol_info)

    @pytest.mark.tier1
    def test_arbiter_pvc_mount_on_pod(self):
        """Validate new volume creation using app pod"""
        # Create sc with gluster arbiter info
        self.create_storage_class(is_arbiter_vol=True)

        # Create PVC and wait for it to be in 'Bound' state
        self.create_and_wait_for_pvc()

        # Create POD with attached volume
        mount_path = "/mnt"
        pod_name = openshift_ops.oc_create_tiny_pod_with_volume(
            self.node, self.pvc_name, "test-arbiter-pvc-mount-on-app-pod",
            mount_path=mount_path, image=self.io_container_image_cirros)
        self.addCleanup(openshift_ops.oc_delete, self.node, 'pod', pod_name)

        # Wait for POD be up and running
        openshift_ops.wait_for_pod_be_ready(
            self.node, pod_name, timeout=60, wait_step=2)

        # Get volume ID
        vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
            self.node, self.pvc_name)
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

    @pytest.mark.tier1
    def test_create_arbiter_vol_with_more_than_one_brick_set(self):
        """Validate volume creation using heketi for more than six brick set"""

        # Set arbiter:disabled tag to the data devices and get their info
        data_nodes = []
        for node_id in self.node_id_list[0:2]:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)

            if len(node_info['devices']) < 2:
                self.skipTest(
                    "Nodes are expected to have at least 2 devices")
            if not all([int(d['storage']['free']) > (3 * 1024**2)
                        for d in node_info['devices'][0:2]]):
                self.skipTest(
                    "Devices are expected to have more than 3Gb of free space")
            for device in node_info['devices']:
                self._set_arbiter_tag_with_further_revert(
                    self.heketi_client_node, self.heketi_server_url,
                    'device', device['id'], 'disabled',
                    device.get('tags', {}).get('arbiter'))
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, self.heketi_server_url,
                'node', node_id, 'disabled',
                node_info.get('tags', {}).get('arbiter'))

            data_nodes.append(node_info)

        # Set arbiter:required tag to all other nodes and their devices
        for node_id in self.node_id_list[2:]:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, self.heketi_server_url,
                'node', node_id, 'required',
                node_info.get('tags', {}).get('arbiter'))
            for device in node_info['devices']:
                self._set_arbiter_tag_with_further_revert(
                    self.heketi_client_node, self.heketi_server_url,
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
        self.create_storage_class(is_arbiter_vol=True)

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
                    self.heketi_client_node, self.heketi_server_url,
                    smaller_device_id)
                self.assertTrue(out)
                self.create_and_wait_for_pvc(
                    int(helper_vol_size_kb / 1024.0**2) + 1)
            finally:
                out = heketi_ops.heketi_device_enable(
                    self.heketi_client_node, self.heketi_server_url,
                    smaller_device_id)
                self.assertTrue(out)

        # Create target arbiter volume
        self.create_and_wait_for_pvc(int(target_vol_size_kb / 1024.0**2))

        # Get gluster volume info
        vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
            self.node, self.pvc_name)

        self.verify_amount_and_proportion_of_arbiter_and_data_bricks(
            vol_info, arbiter_bricks=2, data_bricks=4)

    @pytest.mark.tier2
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
        """Validate arbiter brick creation with different avg file size"""

        # Create sc with gluster arbiter info
        self.create_storage_class(
            is_arbiter_vol=True, arbiter_avg_file_size=avg_file_size)

        # Create PVC and wait for it to be in 'Bound' state
        self.create_and_wait_for_pvc(pvc_size_gb)

        # Get volume info
        vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
            self.node, self.pvc_name)

        # Verify proportion of data and arbiter bricks
        bricks_info = (
            self.verify_amount_and_proportion_of_arbiter_and_data_bricks(
                vol_info))

        expected_file_amount = pvc_size_gb * 1024**2 // (avg_file_size or 64)
        expected_file_amount = (
            expected_file_amount // bricks_info['arbiter_amount'])

        # Try to create expected amount of files on arbiter brick mount
        passed_arbiter_bricks = []
        not_found = "Mount Not Found"
        for brick in bricks_info['arbiter_list']:
            # "brick path" looks like following:
            # ip_addr:/path/to/vg/brick_unique_name/brick
            gluster_ip, brick_path = brick["name"].split(":")
            brick_path = brick_path[0:-6]

            cmd = "mount | grep %s || echo '%s'" % (brick_path, not_found)
            out = openshift_ops.cmd_run_on_gluster_pod_or_node(
                self.node, cmd, gluster_ip)
            if out != not_found:
                cmd = (
                    "python -c \"["
                    "    open('%s/foo_file{0}'.format(i), 'a').close()"
                    "    for i in range(%s)"
                    "]\"" % (brick_path, expected_file_amount)
                )
                openshift_ops.cmd_run_on_gluster_pod_or_node(
                    self.node, cmd, gluster_ip)
                passed_arbiter_bricks.append(brick["name"])

        # Make sure all the arbiter bricks were checked
        for brick in bricks_info['arbiter_list']:
            self.assertIn(
                brick["name"], passed_arbiter_bricks,
                "Arbiter brick '%s' was not verified. Looks like it was "
                "not found on any of gluster PODs/nodes." % brick["name"])

    @pytest.mark.tier3
    @ddt.data(
        (False, False, True, True),
        (True, True, False, False),
        (False, True, False, False),
        (True, False, False, False)
    )
    @ddt.unpack
    def test_arbiter_required_tag_on_node_or_devices_other_disabled(
            self, r_node_tag, d_node_tag, r_device_tag, d_device_tag):
        """Validate arbiter vol creation with node or device tag"""

        pvc_amount = 3

        # Get Heketi nodes info
        node_id_list = heketi_ops.heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)

        # Disable n-3 nodes
        for node_id in node_id_list[3:]:
            heketi_ops.heketi_node_disable(self.heketi_client_node,
                                           self.heketi_server_url, node_id)
            self.addCleanup(heketi_ops.heketi_node_enable,
                            self.heketi_client_node,
                            self.heketi_server_url,
                            node_id)

        # Set arbiter:required tags
        arbiter_node = heketi_ops.heketi_node_info(
            self.heketi_client_node, self.heketi_server_url, node_id_list[0],
            json=True)
        arbiter_nodes_ip_addresses = arbiter_node['hostnames']['storage']
        self._set_arbiter_tag_with_further_revert(
            self.heketi_client_node, self.heketi_server_url, 'node',
            node_id_list[0], ('required' if r_node_tag else None),
            revert_to=arbiter_node.get('tags', {}).get('arbiter'))
        for device in arbiter_node['devices']:
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, self.heketi_server_url, 'device',
                device['id'], ('required' if r_device_tag else None),
                revert_to=device.get('tags', {}).get('arbiter'))

        # Set arbiter:disabled tags
        data_nodes_ip_addresses = []
        for node_id in node_id_list[1:]:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            if not any([int(d['storage']['free']) > (pvc_amount * 1024**2)
                        for d in node_info['devices']]):
                self.skipTest(
                    "Devices are expected to have more than "
                    "%sGb of free space" % pvc_amount)
            data_nodes_ip_addresses.extend(node_info['hostnames']['storage'])
            for device in node_info['devices']:
                self._set_arbiter_tag_with_further_revert(
                    self.heketi_client_node, self.heketi_server_url, 'device',
                    device['id'], ('disabled' if d_device_tag else None),
                    revert_to=device.get('tags', {}).get('arbiter'))
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, self.heketi_server_url, 'node',
                node_id, ('disabled' if d_node_tag else None),
                revert_to=node_info.get('tags', {}).get('arbiter'))

        # Create PVCs and check that their bricks are correctly located
        self.create_storage_class(is_arbiter_vol=True)
        for i in range(pvc_amount):
            self.create_and_wait_for_pvc(1)

            # Get gluster volume info
            vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
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

    @pytest.mark.tier3
    def test_create_delete_pvcs_to_make_gluster_reuse_released_space(self):
        """Validate reuse of volume space after deletion of PVCs"""
        min_storage_gb = 10

        # Set arbiter:disabled tags to the first 2 nodes
        data_nodes = []
        biggest_disks = []
        self.assertGreater(len(self.node_id_list), 2)
        for node_id in self.node_id_list[0:2]:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
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
                    self.heketi_client_node, self.heketi_server_url, 'device',
                    device['id'], 'disabled',
                    revert_to=device.get('tags', {}).get('arbiter'))
            biggest_disks.append(biggest_disk_free_space)
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, self.heketi_server_url, 'node',
                node_id, 'disabled',
                revert_to=node_info.get('tags', {}).get('arbiter'))
            data_nodes.append(node_info)

        # Set arbiter:required tag to all other nodes and their devices
        arbiter_nodes = []
        for node_id in self.node_id_list[2:]:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            for device in node_info['devices']:
                self._set_arbiter_tag_with_further_revert(
                    self.heketi_client_node, self.heketi_server_url, 'device',
                    device['id'], 'required',
                    revert_to=device.get('tags', {}).get('arbiter'))
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, self.heketi_server_url, 'node',
                node_id, 'required',
                revert_to=node_info.get('tags', {}).get('arbiter'))
            arbiter_nodes.append(node_info)

        # Calculate size and amount of volumes to be created
        pvc_size = int(min(biggest_disks) / 1024**2)
        pvc_amount = max([len(n['devices']) for n in data_nodes]) + 1

        # Create sc with gluster arbiter info
        self.create_storage_class(is_arbiter_vol=True)

        # Create and delete 3 small volumes concurrently
        pvc_names = self.create_and_wait_for_pvcs(
            pvc_size=int(pvc_size / 3), pvc_name_prefix='arbiter-pvc',
            pvc_amount=3, sc_name=self.sc_name)

        for pvc_name in pvc_names:
            openshift_ops.oc_delete(self.node, 'pvc', pvc_name)
        for pvc_name in pvc_names:
            openshift_ops.wait_for_resource_absence(self.node, 'pvc', pvc_name)

        # Create and delete big volumes in a loop
        for i in range(pvc_amount):
            pvc_name = openshift_ops.oc_create_pvc(
                self.node, self.sc_name, pvc_name_prefix='arbiter-pvc',
                pvc_size=pvc_size)
            try:
                openshift_ops.verify_pvc_status_is_bound(
                    self.node, pvc_name, 300, 10)
            except Exception:
                self.addCleanup(
                    openshift_ops.wait_for_resource_absence,
                    self.node, 'pvc', pvc_name)
                self.addCleanup(
                    openshift_ops.oc_delete, self.node, 'pvc', pvc_name)
                raise
            openshift_ops.oc_delete(self.node, 'pvc', pvc_name)
            openshift_ops.wait_for_resource_absence(self.node, 'pvc', pvc_name)

    @pytest.mark.tier1
    def test_arbiter_volume_expand_using_pvc(self):
        """Validate arbiter volume expansion by PVC creation"""
        # Create sc with gluster arbiter info
        self.create_storage_class(
            is_arbiter_vol=True, allow_volume_expansion=True)

        # Create PVC and wait for it to be in 'Bound' state
        self.create_and_wait_for_pvc()

        # Get vol info
        vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
            self.node, self.pvc_name)

        self.verify_amount_and_proportion_of_arbiter_and_data_bricks(vol_info)

        pvc_size = 2
        openshift_ops.resize_pvc(self.node, self.pvc_name, pvc_size)
        openshift_ops.verify_pvc_size(self.node, self.pvc_name, pvc_size)

        vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
            self.node, self.pvc_name)

        self.verify_amount_and_proportion_of_arbiter_and_data_bricks(
            vol_info, arbiter_bricks=2, data_bricks=4)

    @pytest.mark.tier2
    @ddt.data(True, False)
    def test_expand_arbiter_volume_setting_tags_on_nodes_or_devices(
            self, node_tags):
        """Validate exapnsion of arbiter volume with defferent tags

           This test case is going to run two tests:
                1. If value is True it is going to set tags
                   on nodes and run test
                2. If value is False it is going to set tags
                   on devices and run test
        """

        data_nodes = []
        arbiter_nodes = []

        # set tags arbiter:disabled, arbiter:required
        for i, node_id in enumerate(self.node_id_list):
            if node_tags:
                self._set_arbiter_tag_with_further_revert(
                    self.heketi_client_node, self.heketi_server_url, 'node',
                    node_id, 'disabled' if i < 2 else 'required')

            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)

            if not node_tags:
                for device in node_info['devices']:
                    self._set_arbiter_tag_with_further_revert(
                        self.heketi_client_node, self.heketi_server_url,
                        'device', device['id'],
                        'disabled' if i < 2 else 'required')
                    device_info = heketi_ops.heketi_device_info(
                        self.heketi_client_node, self.heketi_server_url,
                        device['id'], json=True)
                    self.assertEqual(
                        device_info['tags']['arbiter'],
                        'disabled' if i < 2 else 'required')

            node = {
                'id': node_id, 'host': node_info['hostnames']['storage'][0]}
            if node_tags:
                self.assertEqual(
                    node_info['tags']['arbiter'],
                    'disabled' if i < 2 else 'required')
            data_nodes.append(node) if i < 2 else arbiter_nodes.append(
                node)

        # Create sc with gluster arbiter info
        self.create_storage_class(
            is_arbiter_vol=True, allow_volume_expansion=True)

        # Create PVC and wait for it to be in 'Bound' state
        self.create_and_wait_for_pvc()

        vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
            self.node, self.pvc_name)

        bricks = self.verify_amount_and_proportion_of_arbiter_and_data_bricks(
            vol_info)

        arbiter_hosts = [obj['host'] for obj in arbiter_nodes]
        data_hosts = [obj['host'] for obj in data_nodes]

        for brick in bricks['arbiter_list']:
            self.assertIn(brick['name'].split(':')[0], arbiter_hosts)

        for brick in bricks['data_list']:
            self.assertIn(brick['name'].split(':')[0], data_hosts)

        # Expand PVC and verify the size
        pvc_size = 2
        openshift_ops.resize_pvc(self.node, self.pvc_name, pvc_size)
        openshift_ops.verify_pvc_size(self.node, self.pvc_name, pvc_size)

        vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
            self.node, self.pvc_name)

        bricks = self.verify_amount_and_proportion_of_arbiter_and_data_bricks(
            vol_info, arbiter_bricks=2, data_bricks=4)

        for brick in bricks['arbiter_list']:
            self.assertIn(brick['name'].split(':')[0], arbiter_hosts)

        for brick in bricks['data_list']:
            self.assertIn(brick['name'].split(':')[0], data_hosts)

    @pytest.mark.tier2
    @ddt.data(
        (4, '250M', True),
        (8, '122M', True),
        (16, '58M', True),
        (32, '26M', True),
        (4, '250M', False),
        (8, '122M', False),
        (16, '58M', False),
        (32, '26M', False),
    )
    @ddt.unpack
    def test_expand_arbiter_volume_according_to_avg_file_size(
            self, avg_file_size, expected_brick_size, vol_expand=True):
        """Validate expansion of arbiter volume with diff avg file size"""
        data_hosts = []
        arbiter_hosts = []

        # set tags arbiter:disabled, arbiter:required
        for i, node_id in enumerate(self.node_id_list):
            self._set_arbiter_tag_with_further_revert(
                self.heketi_client_node, self.heketi_server_url, 'node',
                node_id, 'disabled' if i < 2 else 'required')

            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            (data_hosts.append(node_info['hostnames']['storage'][0])
                if i < 2 else
                arbiter_hosts.append(node_info['hostnames']['storage'][0]))
            self.assertEqual(
                node_info['tags']['arbiter'],
                'disabled' if i < 2 else 'required')

        # Create sc with gluster arbiter info
        self.create_storage_class(
            is_arbiter_vol=True, allow_volume_expansion=True,
            arbiter_avg_file_size=avg_file_size)

        # Create PVC and wait for it to be in 'Bound' state
        self.create_and_wait_for_pvc()

        vol_expanded = False

        for i in range(2):
            vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
                self.node, self.pvc_name)
            bricks = (
                self.verify_amount_and_proportion_of_arbiter_and_data_bricks(
                    vol_info,
                    arbiter_bricks=(2 if vol_expanded else 1),
                    data_bricks=(4 if vol_expanded else 2)
                )
            )

            # verify arbiter bricks lies on arbiter hosts
            for brick in bricks['arbiter_list']:
                ip, brick_name = brick['name'].split(':')
                self.assertIn(ip, arbiter_hosts)
                # verify the size of arbiter brick
                cmd = "df -h %s --output=size | tail -1" % brick_name
                out = openshift_ops.cmd_run_on_gluster_pod_or_node(
                    self.node, cmd, ip)
                self.assertEqual(out, expected_brick_size)
            # verify that data bricks lies on data hosts
            for brick in bricks['data_list']:
                self.assertIn(brick['name'].split(':')[0], data_hosts)

            if vol_expanded or not vol_expand:
                break
            # Expand PVC and verify the size
            pvc_size = 2
            openshift_ops.resize_pvc(self.node, self.pvc_name, pvc_size)
            openshift_ops.verify_pvc_size(self.node, self.pvc_name, pvc_size)
            vol_expanded = True

    @pytest.mark.tier1
    @podcmd.GlustoPod()
    def test_arbiter_volume_delete_using_pvc(self):
        """Test Arbiter volume delete using pvc when volume is not mounted
           on app pod
        """
        prefix = "autotest-%s" % utils.get_random_str()

        # Create sc with gluster arbiter info
        sc_name = self.create_storage_class(
            vol_name_prefix=prefix, is_arbiter_vol=True)

        # Create PVC and wait for it to be in 'Bound' state
        pvc_name = self.create_and_wait_for_pvc(
            pvc_name_prefix=prefix, sc_name=sc_name)

        # Get vol info
        gluster_vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
            self.node, pvc_name)

        # Verify arbiter volume properties
        self.verify_amount_and_proportion_of_arbiter_and_data_bricks(
            gluster_vol_info)

        # Get volume ID
        gluster_vol_id = gluster_vol_info["gluster_vol_id"]

        # Delete the pvc
        openshift_ops.oc_delete(self.node, 'pvc', pvc_name)
        openshift_ops.wait_for_resource_absence(self.node, 'pvc', pvc_name)

        # Check the heketi volume list if pvc is deleted
        g.log.info("List heketi volumes")
        heketi_volumes = heketi_ops.heketi_volume_list(
            self.heketi_client_node, self.heketi_server_url)

        err_msg = "Failed to delete heketi volume by prefix %s" % prefix
        self.assertNotIn(prefix, heketi_volumes, err_msg)

        # Check presence for the gluster volume
        get_gluster_vol_info = volume_ops.get_volume_info(
            "auto_get_gluster_endpoint", gluster_vol_id)
        err_msg = "Failed to delete gluster volume %s" % gluster_vol_id
        self.assertFalse(get_gluster_vol_info, err_msg)

        # Check presence of bricks and lvs
        for brick in gluster_vol_info['bricks']['brick']:
            gluster_node_ip, brick_name = brick["name"].split(":")

            with self.assertRaises(exceptions.ExecutionError):
                cmd = "df %s" % brick_name
                openshift_ops.cmd_run_on_gluster_pod_or_node(
                    self.node, cmd, gluster_node_ip)

            with self.assertRaises(exceptions.ExecutionError):
                lv_match = re.search(BRICK_REGEX, brick["name"])
                if lv_match:
                    cmd = "lvs %s" % lv_match.group(2).strip()
                    openshift_ops.cmd_run_on_gluster_pod_or_node(
                        self.node, cmd, gluster_node_ip)

    @pytest.mark.tier4
    def test_arbiter_scaled_heketi_and_gluster_volume_mapping(self):
        """Test to validate PVC, Heketi & gluster volume mapping
        for large no of PVC's
        """
        prefix = "autotest-{}".format(utils.get_random_str())

        sc_name = self.create_storage_class(
            vol_name_prefix=prefix, is_arbiter_vol=True)
        for count in range(5):
            self.create_and_wait_for_pvcs(
                pvc_name_prefix=prefix, pvc_amount=20,
                sc_name=sc_name, timeout=300, wait_step=5)
        openshift_ops.match_pvc_and_pv(self.heketi_client_node, prefix)

        h_vol_list = heketi_ops.heketi_volume_list_by_name_prefix(
            self.heketi_client_node, self.heketi_server_url, prefix, json=True)
        heketi_volume_ids = sorted([v[0] for v in h_vol_list])
        openshift_ops.match_pv_and_heketi_volumes(
            self.heketi_client_node, heketi_volume_ids, prefix)

        heketi_volume_names = sorted([
            v[2].replace("{}_".format(prefix), "") for v in h_vol_list])
        gluster_ops.match_heketi_and_gluster_volumes_by_prefix(
            heketi_volume_names, "{}_".format(prefix))

    @pytest.mark.tier2
    @podcmd.GlustoPod()
    def test_arbiter_volume_node_tag_removal(self):
        """Test remove tags from nodes and check if arbiter volume is
        created randomly.
        """
        h_volume_size, bricks_list = 1, []
        h_client, h_url = self.heketi_client_node, self.heketi_server_url

        node_ids = heketi_ops.heketi_node_list(h_client, h_url)
        self.assertTrue(node_ids, "Failed to get heketi node list")

        # Remove all the tag from the nodes
        for node_id in node_ids:
            node_info = heketi_ops.heketi_node_info(
                h_client, h_url, node_id, json=True)
            self._set_arbiter_tag_with_further_revert(
                h_client, h_url, 'node', node_id, None,
                node_info.get('tags', {}).get('arbiter'))
            h_info = heketi_ops.heketi_node_info(
                h_client, h_url, node_id, json=True)

            # Make sure that all tags are removed
            err_msg = "Failed to remove tags from node id {}".format(node_id)
            self.assertEqual(h_info.get("tags"), None, err_msg)

        # Creating 10 volumes for verificaton
        for count in range(10):
            h_volume = heketi_ops.heketi_volume_create(
                h_client, h_url, h_volume_size,
                gluster_volume_options='user.heketi.arbiter true', json=True)
            h_vol_id = h_volume["id"]
            h_vol_name = h_volume["name"]

            self.addCleanup(
                heketi_ops.heketi_volume_delete, h_client, h_url, h_vol_id,
                h_vol_name, gluster_volume_options='user.heketi.arbiter true')

            g_vol_list = volume_ops.get_volume_list(
                "auto_get_gluster_endpoint")

            err_msg = ("Failed to find heketi volume name {} in gluster volume"
                       "list {}".format(h_vol_name, g_vol_list))
            self.assertIn(h_vol_name, g_vol_list, err_msg)

            g_vol_info = volume_ops.get_volume_info(
                'auto_get_gluster_endpoint', h_vol_name)

            err_msg = "Brick details are empty for{}"
            for brick_details in g_vol_info[h_vol_name]["bricks"]["brick"]:
                if brick_details['isArbiter'][0] == '1':
                    brick_ip_name = brick_details["name"]
                    self.assertTrue(brick_ip_name, err_msg.format(h_vol_name))
                    bricks_list.append(brick_ip_name)

        arbiter_brick_ip = {
            brick.strip().split(":")[0] for brick in bricks_list}
        self.assertTrue(
            arbiter_brick_ip, "Brick IP not found in {}".format(bricks_list))

        err_msg = ("Expecting more than one IP but received "
                   "{}".format(arbiter_brick_ip))
        self.assertGreaterEqual(len(arbiter_brick_ip), 1, err_msg)

    @pytest.mark.tier3
    @podcmd.GlustoPod()
    def test_arbiter_volume_delete_using_pvc_mounted_on_app_pod(self):
        """Test Arbiter volume delete using a pvc when volume is mounted
           on app pod
        """
        if openshift_version.get_openshift_version() <= "3.9":
            self.skipTest(
                "PVC deletion while pod is running is not supported"
                " in OCP older than 3.10")

        prefix = "autotest-{}".format(utils.get_random_str())

        # Create sc with gluster arbiter info
        sc_name = self.create_storage_class(
            vol_name_prefix=prefix, is_arbiter_vol=True)

        # Create PVC and corresponding App pod
        pvc_name = self.create_and_wait_for_pvc(
            pvc_name_prefix=prefix, sc_name=sc_name)
        dc_name, pod_name = self.create_dc_with_pvc(pvc_name)

        # Get vol info
        g_vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
            self.node, pvc_name)

        # Verify arbiter volume properties
        self.verify_amount_and_proportion_of_arbiter_and_data_bricks(
            g_vol_info)

        # Get volume ID
        g_vol_id = g_vol_info["gluster_vol_id"]

        # Delete the pvc
        openshift_ops.oc_delete(self.node, 'pvc', pvc_name)
        with self.assertRaises(exceptions.ExecutionError):
            openshift_ops.wait_for_resource_absence(
                self.node, 'pvc', pvc_name, interval=10, timeout=60)

        # Verify if IO is running on the app pod
        filepath = "/mnt/file_for_testing_volume.log"
        cmd = "dd if=/dev/urandom of={} bs=1K count=100".format(filepath)
        err_msg = "Failed to execute command {} on pod {}".format(
            cmd, pod_name)
        ret, out, err = openshift_ops.oc_rsh(self.node, pod_name, cmd)
        self.assertFalse(ret, err_msg)

        # Delete the app pod
        openshift_ops.oc_delete(self.node, 'pod', pod_name)
        openshift_ops.wait_for_resource_absence(self.node, 'pod', pod_name)

        # Check the status of the newly spinned app pod
        pod_status = openshift_ops.oc_get_custom_resource(
            self.node, 'pod', custom=':.status.phase',
            selector='deploymentconfig={}'.format(dc_name))[0]
        self.assertEqual(pod_status[0], 'Pending')

        # Check if the PVC is deleted
        openshift_ops.wait_for_resource_absence(self.node, 'pvc', pvc_name)

        # Check the heketi volume list if volume is deleted
        heketi_volumes = heketi_ops.heketi_volume_list(
            self.heketi_client_node, self.heketi_server_url)
        err_msg = "Failed to delete heketi volume by prefix {}".format(prefix)
        self.assertNotIn(prefix, heketi_volumes, err_msg)

        # Check for absence of the gluster volume
        get_gluster_vol_info = volume_ops.get_volume_info(
            "auto_get_gluster_endpoint", g_vol_id)
        err_msg = ("Failed to delete gluster volume {} for PVC {}".format(
            g_vol_id, pvc_name))
        self.assertFalse(get_gluster_vol_info, err_msg)

        # Check for absence of bricks and lvs
        for brick in g_vol_info['bricks']['brick']:
            gluster_node_ip, brick_name = brick["name"].split(":")

            err_msg = "Brick not found"
            cmd = "df {} || echo {} ".format(brick_name, err_msg)
            out = openshift_ops.cmd_run_on_gluster_pod_or_node(
                self.node, cmd, gluster_node_ip)
            self.assertEqual(
                out, err_msg, "Brick {} still present".format(brick_name))

            err_msg = "LV not found"
            lv_match = re.search(BRICK_REGEX, brick["name"])
            if lv_match:
                lv = lv_match.group(2).strip()
                cmd = "lvs {} || echo {}".format(lv, err_msg)
                out = openshift_ops.cmd_run_on_gluster_pod_or_node(
                    self.node, cmd, gluster_node_ip)
                self.assertEqual(
                    out, err_msg, "LV {} still present".format(lv))

    @pytest.mark.tier1
    @podcmd.GlustoPod()
    def test_arbiter_volume_create_device_size_greater_than_volume_size(self):
        """Validate creation of arbiter volume through heketi"""
        size = 5
        vol_create_info = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, size,
            gluster_volume_options="user.heketi.arbiter true", json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol_create_info["id"])
        vol_name = vol_create_info['name']

        # calculating size of data and arbiter bricks
        size_list = [size * 1024 * 1024, size * 1024 * 16]
        for brick_size in vol_create_info['bricks']:
            self.assertIn(
                brick_size['size'], size_list, "Failed to check brick size")

        # Get gluster volume info
        gluster_vol = volume_ops.get_volume_info(
            'auto_get_gluster_endpoint', volname=vol_name)
        self.assertTrue(
            gluster_vol, "Failed to get volume {} info".format(vol_name))
        volume_name = gluster_vol[vol_name]
        self.assertEqual(
            volume_name['arbiterCount'], "1",
            "arbiter count {} is different from actual count: 1".format(
                volume_name['arbiterCount']))
        self.assertEqual(volume_name['replicaCount'], "3", (
            "replica count is different for volume {} Actual:{} "
            "Expected : 3".format(
                volume_name, volume_name['replicaCount'])))

    def _arbiter_volume_node_tag_operations(self, node_tags):
        """Verify arbiter volume creation with different arbiter tags on
        different nodes
        """
        h_volume_size, vol_amount, arbiter_tag_node_ip = 1, 5, None
        bricks_list = []
        h_client, h_url = self.heketi_client_node, self.heketi_server_url

        h_nodes = heketi_ops.heketi_node_list(h_client, h_url)
        self.assertTrue(h_nodes, "Failed to get heketi node list")

        for i, node_id in enumerate(h_nodes):
            node_info = heketi_ops.heketi_node_info(
                h_client, h_url, node_id, json=True)
            self.assertTrue(
                node_info,
                "Failed to get the heketi node info for node id"
                " {}".format(node_id))

            # Add tags on 3 nodes and disable remaining
            node_tag = node_tags[i] if i < 3 else 'disabled'
            revert_to_tag = node_info.get('tags', {}).get('arbiter')
            self._set_arbiter_tag_with_further_revert(
                h_client, h_url, 'node', node_id, node_tag, revert_to_tag)

            h_info = heketi_ops.heketi_node_info(
                h_client, h_url, node_id, json=True)
            self.assertTrue(
                h_info,
                "Failed to get the heketi node info for node id"
                " {}".format(node_id))
            err_msg = "Failed to fetch {} from the heketi node info {}"
            h_node_id = h_info['id']
            self.assertTrue(h_node_id, err_msg.format('Node id', h_node_id))
            h_node_ip = h_info['hostnames']['storage']
            self.assertTrue(h_node_ip, err_msg.format('ip', h_info))
            h_node_tag = h_info.get('tags', {}).get('arbiter')

            # Make sure that the tags are removed and new tags are added
            err_msg = "Failed to remove and add {} tags to node id {}"
            if node_tags.count(None) > 1:
                self.assertEqual(
                    h_node_tag, node_tag, err_msg.format(node_tag, node_id))
            else:
                if h_node_tag == 'required':
                    # To have a count of node having arbiter tag
                    arbiter_tag_node_ip = h_node_ip

                    # To have count of node id having arbiter tag
                    arbiter_tag_node_id = h_node_id
                self.assertEqual(
                    h_node_tag, node_tag, err_msg.format(node_tag, node_id))

        # Create 5 volumes for verification
        for count in range(vol_amount):
            try:
                g_vol_option = 'user.heketi.arbiter true'
                h_volume = heketi_ops.heketi_volume_create(
                    h_client, h_url, h_volume_size,
                    gluster_volume_options=g_vol_option, json=True)

                # Verify volume creation with the expected result for the
                # given tags on nodes
                if not node_tags[3]:
                    raise AssertionError(
                        "Expecting volume should not be created as tags on"
                        " nodes are {}".format(node_tags))

                err_msg = "Failed fetch {} from {}"
                h_vol_id = h_volume["id"]
                self.assertTrue(h_vol_id, err_msg.format('id', h_volume))
                h_vol_name = h_volume["name"]
                self.assertTrue(h_vol_id, err_msg.format('name', h_volume))
                self.addCleanup(
                    heketi_ops.heketi_volume_delete, h_client, h_url,
                    h_vol_id, h_vol_name, gluster_volume_options=g_vol_option)

                g_vol_list = volume_ops.get_volume_list(
                    "auto_get_gluster_endpoint")
                self.assertIn(
                    h_vol_name, g_vol_list,
                    "Failed to find heketi volume name {} in gluster"
                    " volume list {}".format(h_vol_name, g_vol_list))

                g_vol_info = volume_ops.get_volume_info(
                    'auto_get_gluster_endpoint', h_vol_name)
                self.assertTrue(
                    g_vol_info,
                    "Failed to get the details of the volume "
                    "{}".format(h_vol_name))

                for brick_details in g_vol_info[h_vol_name]["bricks"]["brick"]:
                    self.assertTrue(
                        brick_details['isArbiter'],
                        "Expecting 0 or 1 to identify arbiter brick")

                    # Get the bricks details where arbiter volume is present
                    if brick_details['isArbiter'][0] == '1':
                        self.assertTrue(
                            brick_details["name"],
                            "Brick details are empty for "
                            "{}".format(h_vol_name))
                        brick_ip_name = brick_details["name"]
                        bricks_list.append(brick_ip_name)
            except AssertionError:
                # Verify volume creation with the expected result for the
                # given tags on nodes
                if node_tags[3]:
                    raise
                return

        # Get the bricks ip on which arbiter volume is created
        arbiter_brick_ip = {
            brick.strip().split(":")[0] for brick in bricks_list}
        self.assertTrue(
            arbiter_brick_ip,
            "Brick ip not found in {}".format(bricks_list))

        # Verify heketi topology info for tags and ip
        h_topology_info = heketi_ops.heketi_topology_info(
            h_client, h_url, raise_on_error=True, json=True)
        self.assertTrue(
            h_topology_info,
            "Failed to fetch heketi topology info")
        er_msg = "Failed to find the {} from the heketi topology info {}"
        for i in range(len(h_nodes)):
            h_node_info = h_topology_info['clusters'][0]['nodes'][i]
            self.assertTrue(
                h_node_info,
                er_msg.format('node details', h_topology_info))
            h_node_id = h_node_info['id']
            self.assertTrue(
                h_node_id,
                er_msg.format('node id', h_topology_info))
            h_node_ip = h_node_info['hostnames']['storage']
            self.assertTrue(
                h_node_ip,
                er_msg.format('ip', h_topology_info))
            h_node_tag = h_node_info.get('tags', {}).get('arbiter')

            # Verify tags from node info and topology info
            node_tag = node_tags[i] if i < 3 else 'disabled'
            err_msg = ("Expecting {} {} from node info and {} from topology"
                       " info should be same")
            self.assertEqual(
                node_tag, h_node_tag,
                err_msg.format('tags', node_tag, h_node_tag))

            if h_node_tag == 'required':
                # Verify the ip on which arbiter tag is required
                self.assertEqual(
                    h_node_ip, arbiter_tag_node_ip,
                    err_msg.format('ip', h_node_ip, arbiter_tag_node_ip))

                # Verify the node id on which arbiter tag is required
                self.assertEqual(
                    h_node_id, arbiter_tag_node_id,
                    err_msg.format('node id', h_node_id, arbiter_tag_node_id))

        # Verify if the count of no arbiter tag on the node is more
        # than one out of 3 nodes then ip will be on more than 1 node
        if node_tags.count(None) > 1:
            self.assertGreaterEqual(
                len(arbiter_brick_ip), 1,
                "Expecting more than one IP but received "
                "{}".format(arbiter_brick_ip))
        else:
            self.assertEqual(
                len(arbiter_brick_ip), 1,
                "Expecting one IP but received "
                "{}".format(arbiter_brick_ip))
            self.assertEqual(
                len(arbiter_brick_ip), len(arbiter_tag_node_ip),
                "Expecting the count of bricks were the volume is"
                " created {} and number of nodes having the arbiter"
                " tag {} ".format(arbiter_brick_ip, arbiter_tag_node_ip))
            self.assertEqual(
                arbiter_brick_ip.pop(), arbiter_tag_node_ip[0],
                "Did not match ip of the bricks were the arbiter volume "
                "is created {} and and number of nodes having the arbiter"
                " tag {}".format(arbiter_brick_ip, arbiter_tag_node_ip))

    @pytest.mark.tier2
    @ddt.data(
        ((None, None, None, True),
         ('required', 'disabled', 'disabled', True)),
        (('disabled', 'disabled', 'required', True),
         ('required', 'disabled', 'disabled', True)),
        ('required', 'required', 'disabled', False),
        ('disabled', 'disabled', 'disabled', False),
        ('required', 'required', 'required', False),
        ('disabled', None, None, True),
        (None, 'required', 'required', False))
    @podcmd.GlustoPod()
    def test_arbiter_volume_with_different_tags_on_nodes(self, node_tags):
        """
        Validate of arbiter volume creation with different tags
        """
        if len(node_tags) == 2:
            self._arbiter_volume_node_tag_operations(node_tags[0])
            self._arbiter_volume_node_tag_operations(node_tags[1])
        else:
            self._arbiter_volume_node_tag_operations(node_tags)

    @pytest.mark.tier2
    @podcmd.GlustoPod()
    def test_create_arbiter_volume_with_avg_file_size_and_expand(self):
        """
        Test to verfiy creation of arbiter volume with avg file size
        and expand it
        """
        h_client, h_url = self.heketi_client_node, self.heketi_server_url
        device_tags = ['disabled', 'disabled', 'required']
        bricks_list, heketi_vg_path, bricks_list_after = [], [], []
        device_list_arbiter, g_arbiter_vg = [], []
        arbiter_brick_size_before, arbiter_brick_size_after = 0, 0
        h_volume_size, expand_size = 5, 10

        # Fetch the available size for each brick
        cmd = "df -h {} | awk '{{print $4}}' | tail -1"

        node_ids = heketi_ops.heketi_node_list(h_client, h_url)
        self.assertTrue(node_ids, "Failed to get heketi node list")
        err_msg = "Failed to add {} tags from device id{}"
        for i, node_id in enumerate(node_ids):
            node_info = heketi_ops.heketi_node_info(
                h_client, h_url, node_id, json=True)
            for device in node_info['devices']:
                device_info = heketi_ops.heketi_device_info(
                    h_client, h_url, device['id'], json=True)
                self._set_arbiter_tag_with_further_revert(
                    h_client, h_url, 'device', device['id'],
                    device_tags[i] if i < 3 else 'disabled',
                    device_info.get('tags', {}).get('arbiter'))

                device_info = heketi_ops.heketi_device_info(
                    h_client, h_url, device['id'], json=True)
                h_node_ip = node_info['hostnames']['storage']

                # Fetch the ip of nodes having tag arbiter: requied
                if device_info.get('tags', {}).get('arbiter') == 'required':
                    arbiter_tag_node_ip = h_node_ip

                    # Fetch the device name were arbiter: requied
                    device_list_arbiter.append(device)

                # Make sure that all tags are placed properly
                err_msg = "Failed to add {} tags for device {} of node {}"
                device_tag = device_tags[i] if i < 3 else 'disabled'
                self.assertEqual(
                    device_info['tags']['arbiter'], device_tag,
                    err_msg.format(device_tag, device['id'], node_id))

        v_option = 'user.heketi.arbiter true, user.heketi.average-file-size 4'
        h_volume = heketi_ops.heketi_volume_create(
            h_client, h_url, h_volume_size,
            gluster_volume_options=v_option, json=True)
        h_vol_id = h_volume["id"]
        h_vol_name = h_volume["name"]
        self.addCleanup(
            heketi_ops.heketi_volume_delete, h_client, h_url,
            h_vol_id, h_vol_name, gluster_volume_options=v_option)

        g_vol_list = volume_ops.get_volume_list("auto_get_gluster_endpoint")
        self.assertIn(
            h_vol_name, g_vol_list,
            "Failed to find heketi volume name {} in gluster"
            " volume list {}".format(h_vol_name, g_vol_list))

        # Get the device info only if arbiter: requied
        for device in device_list_arbiter:
            device_info = heketi_ops.heketi_device_info(
                h_client, h_url, device['id'], json=True)

            # Fetch vg from device info were arbiter tag is required
            if device_info['bricks']:
                vg = device_info['bricks'][0]['path']
                self.assertTrue(
                    vg, "Failed to find the details of the bricks")
                device_vg = DEVICE_REGEX.findall(vg)
                self.assertTrue(
                    device_vg,
                    "Failed to fetch the device vg from {}".format(vg))
                heketi_vg_path.append(device_vg[0])

        g_vol_info = volume_ops.get_volume_info(
            'auto_get_gluster_endpoint', h_vol_name)
        self.assertTrue(
            g_vol_info,
            "Failed to get the details of the volume {}".format(h_vol_name))

        # Fetch brick count before expansion
        brick_count_before = g_vol_info[h_vol_name]['brickCount']

        # Fetch the details of arbiter bricks
        for brick_details in g_vol_info[h_vol_name]["bricks"]["brick"]:
            arbiter_brick = brick_details['isArbiter']
            self.assertTrue(
                arbiter_brick,
                "Expecting 0 or 1 to identify arbiter brick but found "
                "{}".format(arbiter_brick))

            # Get the bricks details where arbiter volume is present
            if arbiter_brick[0] == '1':
                self.assertTrue(
                    brick_details["name"],
                    "Brick details are empty for {}".format(h_vol_name))
                bricks_list.append(brick_details["name"])

        # Get the bricks ip on which arbiter volume is created
        arbiter_brick_ip = {
            brick.strip().split(":")[0] for brick in bricks_list}
        self.assertTrue(
            arbiter_brick_ip, "Brick ip not found in {}".format(bricks_list))

        # Fetch vg from the bricks where arbiter volume is present
        for brick in bricks_list:
            device_vg = DEVICE_REGEX.findall(brick)
            self.assertTrue(
                device_vg,
                "Failed to fetch the device vg from {}".format(brick))
            if device_vg[0] not in g_arbiter_vg:
                g_arbiter_vg.append(device_vg[0])
        self.assertTrue(
            g_arbiter_vg, "Failed to find the device with aribter tag")

        # verify if vg match from gluster and and heketi
        for device in g_arbiter_vg:
            self.assertIn(
                device, heketi_vg_path,
                "Failed to match vg {} from gluster  with vg {} from heketi "
                "side".format(device, heketi_vg_path))

        # Verify heketi topology info for tags
        h_topology_info = heketi_ops.heketi_topology_info(
            h_client, h_url, raise_on_error=True, json=True)
        self.assertTrue(
            h_topology_info, "Failed to fetch heketi topology info")
        er_msg = "Failed to find the {} from the heketi topology info {}"
        for i, node in enumerate(range(len(
                h_topology_info['clusters'][0]['nodes']))):
            node_details = h_topology_info['clusters'][0]['nodes'][node]
            for device in range(len(node_details['devices'])):
                h_device_info = node_details['devices'][device]
                self.assertTrue(
                    h_device_info, er_msg.format('device info', device))
                h_device_tag = h_device_info.get('tags', {}).get('arbiter')
                self.assertTrue(h_device_tag, er_msg.format('tag', device))
                device_tag = device_tags[i] if i < 3 else 'disabled'
                err_msg = ("Expecting {} {} from device info and {} from "
                           "topology info should be same")
                self.assertEqual(
                    device_tag, h_device_tag,
                    err_msg.format('tags', device_tag, h_device_tag))

                if h_device_tag == 'required':
                    if h_device_info['bricks']:
                        # verfiy the vg on which arbiter tag is required
                        vg = (h_device_info['bricks'][0]['path'])
                        h_device_vg = DEVICE_REGEX.findall(vg)
                        self.assertTrue(
                            h_device_vg,
                            "Failed to fetch the device vg from device info "
                            "{}".format(vg))
                        self.assertIn(
                            h_device_vg[0], heketi_vg_path,
                            err_msg.format('Vg', h_device_vg, heketi_vg_path))

        # Check if only one arbiter brick ip is present
        self.assertEqual(
            len(arbiter_brick_ip), 1,
            "Expecting one IP but received {}".format(arbiter_brick_ip))
        self.assertEqual(
            len(arbiter_brick_ip), len(arbiter_tag_node_ip),
            "Expecting the count of bricks were the volume is"
            " created {} and number of nodes having the arbiter"
            " tag {} ".format(arbiter_brick_ip, arbiter_tag_node_ip))
        self.assertEqual(
            arbiter_brick_ip.pop(), arbiter_tag_node_ip[0],
            "Did not match ip of the bricks were the arbiter volume "
            "is created {} and and number of nodes having the arbiter"
            " tag {}".format(arbiter_brick_ip, arbiter_tag_node_ip))

        for i, brick in enumerate(bricks_list):
            arbiter_brick_ip = brick.strip().split(":")[0]
            self.assertTrue(
                arbiter_brick_ip, "Brick ip not found in {}".format(brick))
            arbiter_bricks_mount_path = brick.strip().split(":")[1]
            self.assertTrue(
                arbiter_bricks_mount_path,
                "Arbiter brick mount path not found in {}".format(brick))

            arbiter_brick_size_before += int(
                openshift_ops.cmd_run_on_gluster_pod_or_node(
                    self.node, cmd.format(
                        arbiter_bricks_mount_path), arbiter_brick_ip)[:-1])
        # Expand volume
        heketi_ops.heketi_volume_expand(
            h_client, h_url, h_vol_id, expand_size, json=True)

        g_vol_info = volume_ops.get_volume_info(
            'auto_get_gluster_endpoint', h_vol_name)

        # Fetch the bricks count after vol expansion
        brick_count_after = g_vol_info[h_vol_name]['brickCount']
        self.assertGreater(
            brick_count_after, brick_count_before,
            "Failed to expand the volume {}".format(h_vol_name))
        for brick_details in g_vol_info[h_vol_name]["bricks"]["brick"]:
            arbiter_brick = brick_details['isArbiter']
            self.assertTrue(
                arbiter_brick,
                "Expecting 0 or 1 to identify arbiter brick but found "
                "{}".format(arbiter_brick))

            # Get the bricks details where arbiter volume is present
            if arbiter_brick[0] == '1':
                self.assertTrue(
                    brick_details["name"],
                    "Brick details are empty for {}".format(h_vol_name))
                bricks_list_after.append(brick_details["name"])

        # Get the bricks ip on which arbiter volume is created
        arbiter_brick_ip = {
            brick.strip().split(":")[0] for brick in bricks_list_after}
        self.assertTrue(
            arbiter_brick_ip,
            "Brick ip not found in {}".format(bricks_list_after))

        # Fetch arbiter brick ip and arbiter bricks mount path length

        for i, brick in enumerate(bricks_list_after):
            arbiter_brick_ip = brick.strip().split(":")[0]
            self.assertTrue(
                arbiter_brick_ip, "Brick ip not found in {}".format(brick))
            arbiter_bricks_mount_path = brick.strip().split(":")[1]
            self.assertTrue(
                arbiter_bricks_mount_path,
                "Arbiter brick mount path not found in {}".format(brick))

            # Fetching the size of arbiter bricks after volume expansion
            arbiter_brick_size_after += int(
                openshift_ops.cmd_run_on_gluster_pod_or_node(
                    self.node, cmd.format(
                        arbiter_bricks_mount_path), arbiter_brick_ip)[:-1])

        # Verfiy if the size of arbiter brick increase after expansion
        self.assertGreater(
            arbiter_brick_size_after,
            arbiter_brick_size_before,
            "Expecting arbiter brick size {} after volume expansion must be "
            "greater than  arbiter brick size {} before volume "
            "expansion".format(
                arbiter_brick_size_after, arbiter_brick_size_before))

    @pytest.mark.tier4
    def test_poweroff_gluster_nodes_after_filling_inodes_arbiter_brick(self):
        """Validate io after filling up the arbiter brick and node poweroff"""

        # Create sc with gluster arbiter info
        sc_name = self.create_storage_class(is_arbiter_vol=True)

        if self.is_containerized_gluster():
            # Get list of all gluster nodes and mark them unschedulable
            g_nodes = openshift_ops.oc_get_custom_resource(
                self.node, 'pod', ':.spec.nodeName',
                selector='glusterfs-node=pod')
            g_nodes = [node[0] for node in g_nodes]
            openshift_ops.oc_adm_manage_node(
                self.node, '--schedulable=false', nodes=g_nodes)
            self.addCleanup(openshift_ops.oc_adm_manage_node,
                            self.node, '--schedulable=true', nodes=g_nodes)

        # Create PVC and corresponding App pod
        self.create_and_wait_for_pvc(sc_name=sc_name)
        dc_name, pod_name = self.create_dc_with_pvc(
            self.pvc_name, image=self.io_container_image_busybox)

        # Get vol info
        vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
            self.node, self.pvc_name)
        vol_name = vol_info['gluster_vol_id']
        bricks_list = (
            self.verify_amount_and_proportion_of_arbiter_and_data_bricks(
                vol_info))
        arbiter_brick = bricks_list['arbiter_list'][0]['name'].split(":")[1]

        # Fetch the host ip of the arbiter brick and free inodes data
        hosts_with_inodes_info = (
            gluster_ops.get_gluster_vol_free_inodes_with_hosts_of_bricks(
                vol_name))
        for node_ip, inodes_info in hosts_with_inodes_info.items():
            for brick, inodes in inodes_info.items():
                if arbiter_brick == brick:
                    arb_free_inodes = int(inodes) - 450
                    break

        # Completely  fill free inodes of arbiter brick
        while(arb_free_inodes > 0):

            # Create a randome master file at the mount point
            path = "/mnt/{}/".format(utils.get_random_str())
            filename = "masterfile"
            dd_cmd = (
                "oc exec {} -- /bin/sh -c 'mkdir {};"
                "dd if=/dev/urandom of={}{} bs=1 count=4000'".format(
                    pod_name, path, path, filename))
            self.cmd_run(dd_cmd)

            # Split masterfile into multiple smaller files of 1 byte each
            split_cmd = (
                "oc exec {} -- /bin/sh -c 'cd {}; split -b 1 -a 10 {}'".format(
                    pod_name, path, filename))
            self.cmd_run(split_cmd)
            arb_free_inodes -= 4000

            # Check if there pending heals then break the loop,
            # because inodes of arbiter brick are filled
            try:
                gluster_ops.wait_to_heal_complete(
                    timeout=60, vol_name=vol_name)
            except AssertionError:
                break

        # Poweroff the one of the node with data brick
        target_ip = bricks_list['data_list'][0]['name'].split(":")[0]
        target_vm_name = node_ops.find_vm_name_by_ip_or_hostname(target_ip)
        for ip, info in self.gluster_servers_info.items():
            if ip == target_ip:
                target_hostname = info['manage']
                break
        self.power_off_gluster_node_vm(target_vm_name, target_hostname)

        # Create a file with text test and check if message is "no space left"
        try:
            file_cmd = ("oc exec {} -- /bin/sh -c \"echo 'test' > "
                        "/mnt/".format(pod_name))
            self.cmd_run(file_cmd + '{}"'.format(utils.get_random_str()))
        except AssertionError as err:
            msg = "No space left on device"
            if msg not in str(err):
                raise

        # Power on gluster node
        self.power_on_gluster_node_vm(target_vm_name, target_hostname)

        # Try to create a file
        for w in waiter.Waiter(120, 5):
            try:
                self.cmd_run(file_cmd + '{}"'.format(utils.get_random_str()))
                break
            except AssertionError:
                continue
        if w.expired:
            raise

    def _arbiter_volume_device_tag_operations(
            self, device_tag_count, vol_creation):
        """Verify arbiter volume creation with different arbiter tags on
        different devices
        """
        h_volume_size, bricks_list, h_devices_with_tag = 1, [], []
        device_list_arbiter, vol_count = [], 5
        h_client, h_url = self.heketi_client_node, self.heketi_server_url
        g_vol_option = 'user.heketi.arbiter true'

        node_ids = heketi_ops.heketi_node_list(h_client, h_url)
        self.assertTrue(node_ids, "Failed to get heketi node list")

        for i, node_id in enumerate(node_ids):
            node_info = heketi_ops.heketi_node_info(
                h_client, h_url, node_id, json=True)

            # Except 3 disable all the remaining nodes
            if i > 2:
                heketi_ops.heketi_node_disable(h_client, h_url, node_id)
                self.addCleanup(
                    heketi_ops.heketi_node_enable, h_client, h_url, node_id)
                continue

            # Verify if 3 devices are present
            if len(node_info['devices']) < 3:
                # Add device if 3 devices are not found
                storage_host_info = g.config.get("gluster_servers")
                if not storage_host_info:
                    self.skipTest(
                        "Skip test case as 'gluster_servers' option is "
                        "not provided in config file")

                # Fetch additional_devices details
                storage_host_info = list(storage_host_info.values())[0]
                storage_device = storage_host_info["additional_devices"][0]

                # Add device to the node
                heketi_ops.heketi_device_add(
                    h_client, h_url, storage_device, node_id)

                # Fetch device info after device add
                node_info = heketi_ops.heketi_node_info(
                    h_client, h_url, node_id, json=True)
                device_id = None
                for device in node_info["devices"]:
                    if device["name"] == storage_device:
                        device_id = device["id"]
                        break
                self.assertTrue(
                    device_id,
                    "Failed to add device {} on node"
                    " {}".format(storage_device, node_id))

                self.addCleanup(
                    heketi_ops.heketi_device_delete,
                    h_client, h_url, device_id)
                self.addCleanup(
                    heketi_ops.heketi_device_remove,
                    h_client, h_url, device_id)
                self.addCleanup(
                    heketi_ops.heketi_device_disable,
                    h_client, h_url, device_id)

                # Set arbiter tag on all 3 devices of particular nodes
                # and disabled on other devices
                for device in node_info['devices']:
                    device_info = heketi_ops.heketi_device_info(
                        h_client, h_url, device['id'], json=True)
                    device_tag = (
                        None if not device_tag_count else 'disabled'
                        if i < device_tag_count else 'required')
                    revert_to_tag = device_info.get('tags', {}).get('arbiter')
                    self._set_arbiter_tag_with_further_revert(
                        h_client, h_url, 'device', device['id'],
                        device_tag, revert_to_tag)
                    device_info = heketi_ops.heketi_device_info(
                        h_client, h_url, device['id'], json=True)
                    h_node_ip = node_info['hostnames']['storage']
                    h_device_tag = device_info.get('tags', {}).get('arbiter')

                    # Make sure that the tags are updated
                    err_msg = "Failed to update {} tags to device id {}"
                    if not device_tag_count:
                        self.assertEqual(
                            device_tag, h_device_tag,
                            err_msg.format(device_tag, device['id']))
                    else:
                        # Fetch the ip and device name were arbiter: requied
                        if h_device_tag == 'required':
                            arbiter_tag_node_ip = h_node_ip
                            device_list_arbiter.append(device)

                        # Make sure that all tags are placed properly
                        self.assertEqual(
                            device_tag, h_device_tag,
                            "Failed to add {} tags for device {} of node "
                            "{}".format(device_tag, device['id'], node_id))
        # Create 5 volumes for verificaton
        for count in range(vol_count):
            try:
                h_volume = heketi_ops.heketi_volume_create(
                    h_client, h_url, h_volume_size,
                    gluster_volume_options=g_vol_option, json=True)
                h_vol_id = h_volume["id"]
                h_vol_name = h_volume["name"]
                self.addCleanup(
                    heketi_ops.heketi_volume_delete, h_client, h_url, h_vol_id,
                    h_vol_name, gluster_volume_options=g_vol_option)

                # Verify if volume creation with the expected result for the
                # given tags on devices
                if not vol_creation:
                    raise exceptions.ExecutionError(
                        "Expecting volume should not be created as tags on"
                        " devices {} required tags".format(device_tag_count))

                # Get the device info only if arbiter: requied
                for device in device_list_arbiter:
                    device_info = heketi_ops.heketi_device_info(
                        h_client, h_url, device['id'], json=True)

                    # Fetch vg from device info were arbiter tag is required
                    if device_info['bricks']:
                        vg = device_info['bricks'][0]['path']
                        self.assertTrue(
                            vg, "Failed to find the details of the bricks")
                        device_vg = DEVICE_REGEX.findall(vg)[0]
                        self.assertTrue(
                            device_vg,
                            "Failed to fetch the device vg from {}".format(vg))
                        h_devices_with_tag.append(device_vg)

                # Fetch gluster volume list
                g_vol_list = volume_ops.get_volume_list(
                    "auto_get_gluster_endpoint")
                self.assertIn(
                    h_vol_name, g_vol_list,
                    "Failed to find heketi volume name {} in gluster volume"
                    " list {}".format(h_vol_name, g_vol_list))

                g_vol_info = volume_ops.get_volume_info(
                    'auto_get_gluster_endpoint', h_vol_name)

                # Fetch the device id on which arbiter volume is present
                for brick_details in g_vol_info[h_vol_name]["bricks"]["brick"]:
                    if brick_details['isArbiter'][0] == '1':
                        brick_ip_name = brick_details["name"]
                        self.assertTrue(
                            brick_ip_name,
                            "Brick details are empty for{}".format(h_vol_name))
                        bricks_list.append(brick_ip_name)

            except AssertionError:
                # Verify volume creation with the expected result for the
                # given tags on nodes
                if vol_creation:
                    raise
                return

            # Verify heketi topology info for tags
            h_topology_info = heketi_ops.heketi_topology_info(
                h_client, h_url, raise_on_error=True, json=True)
            self.assertTrue(
                h_topology_info, "Failed to fetch heketi topology info")
            for i, node in enumerate(range(len(
                    h_topology_info['clusters'][0]['nodes']))):
                node_details = h_topology_info['clusters'][0]['nodes'][node]

                # Select the 3 nodes as remaining are disable
                if i < 3:
                    for device in range(len(node_details['devices'])):
                        h_device_info = node_details['devices'][device]
                        self.assertTrue(
                            h_device_info,
                            "Failed to find the {} from the heketi topology "
                            "info {}".format('device info', device))
                        h_device_tag = h_device_info.get(
                            'tags', {}).get('arbiter')
                        device_tag = (
                            None if not device_tag_count else 'disabled'
                            if i < device_tag_count else 'required')
                        err_msg = ("Expecting {} {} from device info and {} "
                                   "from topology info should be same")
                        self.assertEqual(
                            device_tag, h_device_tag,
                            err_msg.format('tags', device_tag, h_device_tag))

                        if h_device_tag == 'required':
                            if h_device_info['bricks']:
                                # verfiy the vg where arbiter tag is required
                                vg = (h_device_info['bricks'][0]['path'])
                                h_device_vg = DEVICE_REGEX.findall(vg)
                                self.assertTrue(
                                    h_device_vg,
                                    "Failed to fetch the device vg from device"
                                    " info {}".format(vg))
                                self.assertIn(
                                    h_device_vg[0], h_devices_with_tag,
                                    err_msg.format(
                                        'Vg', h_device_vg, h_devices_with_tag))

            # Get the bricks ip on which arbiter volume is created
            arbiter_brick_ip = {
                brick.strip().split(":")[0] for brick in bricks_list}
            self.assertTrue(
                arbiter_brick_ip,
                "Brick ip not found in {}".format(bricks_list))

            # Get vg were the arbiter volume is stored
            g_arbiter_vg = {
                DEVICE_REGEX.findall(brick)[0] for brick in bricks_list}
            self.assertTrue(
                g_arbiter_vg, "Failed to find the device with aribter tag")

            # Verify if the count of no arbiter tag on the node is more
            # than one out of 3 nodes then ip will be on more than 1 node
            if not device_tag_count:
                self.assertGreaterEqual(
                    len(arbiter_brick_ip), 1,
                    "Expecting more than one ip but received "
                    "{}".format(arbiter_brick_ip))
            else:
                self.assertEqual(
                    len(arbiter_brick_ip), 1,
                    "Expecting one ip but received "
                    "{}".format(arbiter_brick_ip))
                self.assertEqual(
                    len(arbiter_brick_ip), len(arbiter_tag_node_ip),
                    "Expecting the count of bricks where the volume is"
                    " created {} and number of nodes having the arbiter"
                    " tag {} ".format(arbiter_brick_ip, arbiter_tag_node_ip))
                self.assertEqual(
                    arbiter_brick_ip.pop(), arbiter_tag_node_ip[0],
                    "Did not match ip of the bricks where the arbiter volume "
                    "is created {} and and number of nodes having the arbiter"
                    " tag {}".format(arbiter_brick_ip, arbiter_tag_node_ip))

                # Verify if vg match from gluster and heketi side
                for device in list(g_arbiter_vg):
                    self.assertIn(
                        device, h_devices_with_tag,
                        "Failed to match vg {} from gluster side with vg {} "
                        "from heketi side".format(device, h_devices_with_tag))

    @pytest.mark.tier2
    @ddt.data(
        (1, False),
        (2, True),
        (0, True))
    @ddt.unpack
    @podcmd.GlustoPod()
    def test_arbiter_volume_with_different_tags_on_devices(
            self, device_tag_count, vol_creation):
        """Test remove tags from nodes and check if arbiter volume is
        created randomly"""
        self._arbiter_volume_device_tag_operations(
            device_tag_count, vol_creation)
