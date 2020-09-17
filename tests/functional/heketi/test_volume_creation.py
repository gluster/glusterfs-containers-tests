from glusto.core import Glusto as g
from glustolibs.gluster import snap_ops
from glustolibs.gluster import volume_ops
import pytest
import six

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import node_ops
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import utils


class TestVolumeCreationTestCases(BaseClass):
    """
    Class for volume creation related test cases
    """

    def setUp(self):
        super(TestVolumeCreationTestCases, self).setUp()
        self.node = self.ocp_master_node[0]

    @pytest.mark.tier1
    @podcmd.GlustoPod()
    def test_create_heketi_volume(self):
        """Test heketi volume creation and background gluster validation"""

        hosts = []
        gluster_servers = []
        brick_info = []

        output_dict = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 10, json=True)

        self.assertNotEqual(output_dict, False,
                            "Volume could not be created")

        volume_name = output_dict["name"]
        volume_id = output_dict["id"]

        self.addCleanup(
            heketi_ops.heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, volume_id)

        self.assertEqual(output_dict["durability"]
                         ["replicate"]["replica"], 3,
                         "Volume %s is not replica 3" % volume_id)

        self.assertEqual(output_dict["size"], 10,
                         "Volume %s is not of intended size"
                         % volume_id)

        mount_node = (output_dict["mount"]["glusterfs"]
                      ["device"].strip().split(":")[0])
        hosts.append(mount_node)

        for backup_volfile_server in (output_dict["mount"]["glusterfs"]
                                      ["options"]["backup-volfile-servers"]
                                      .strip().split(",")):
            hosts.append(backup_volfile_server)

        for gluster_server in self.gluster_servers:
            gluster_servers.append(g.config["gluster_servers"]
                                   [gluster_server]["storage"])

        self.assertEqual(set(hosts), set(gluster_servers),
                         "Hosts and gluster servers not matching for %s"
                         % volume_id)

        volume_info = volume_ops.get_volume_info(
            'auto_get_gluster_endpoint', volume_name)
        self.assertIsNotNone(volume_info, "get_volume_info returned None")

        volume_status = volume_ops.get_volume_status(
            'auto_get_gluster_endpoint', volume_name)
        self.assertIsNotNone(
            volume_status, "get_volume_status returned None")

        self.assertEqual(int(volume_info[volume_name]["status"]), 1,
                         "Volume %s status down" % volume_id)
        for brick_details in volume_info[volume_name]["bricks"]["brick"]:
            brick_info.append(brick_details["name"])

        self.assertNotEqual(
            brick_info, [], "Brick details are empty for %s" % volume_name)

        for brick in brick_info:
            brick_data = brick.strip().split(":")
            brick_ip = brick_data[0]
            brick_name = brick_data[1]
            self.assertEqual(int(volume_status
                             [volume_name][brick_ip]
                             [brick_name]["status"]), 1,
                             "Brick %s is not up" % brick_name)

    @pytest.mark.tier1
    def test_volume_creation_no_free_devices(self):
        """Validate heketi error is returned when no free devices available"""
        node, server_url = self.heketi_client_node, self.heketi_server_url

        # Get nodes info
        node_id_list = heketi_ops.heketi_node_list(node, server_url)
        node_info_list = []
        for node_id in node_id_list[0:3]:
            node_info = heketi_ops.heketi_node_info(
                node, server_url, node_id, json=True)
            node_info_list.append(node_info)

        # Disable 4th and other nodes
        for node_id in node_id_list[3:]:
            heketi_ops.heketi_node_disable(node, server_url, node_id)
            self.addCleanup(
                heketi_ops.heketi_node_enable, node, server_url, node_id)

        # Disable second and other devices on the first 3 nodes
        for node_info in node_info_list[0:3]:
            devices = node_info["devices"]
            self.assertTrue(
                devices, "Node '%s' does not have devices." % node_info["id"])
            if devices[0]["state"].strip().lower() != "online":
                self.skipTest("Test expects first device to be enabled.")
            if len(devices) < 2:
                continue
            for device in node_info["devices"][1:]:
                out = heketi_ops.heketi_device_disable(
                    node, server_url, device["id"])
                self.assertTrue(
                    out, "Failed to disable the device %s" % device["id"])
                self.addCleanup(
                    heketi_ops.heketi_device_enable,
                    node, server_url, device["id"])

        # Calculate common available space
        available_spaces = [
            int(node_info["devices"][0]["storage"]["free"])
            for n in node_info_list[0:3]]
        min_space_gb = int(min(available_spaces) / 1024**2)
        self.assertGreater(min_space_gb, 3, "Not enough available free space.")

        # Create first small volume
        vol = heketi_ops.heketi_volume_create(node, server_url, 1, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol["id"])

        # Try to create second volume getting "no free space" error
        try:
            vol_fail = heketi_ops.heketi_volume_create(
                node, server_url, min_space_gb, json=True)
        except AssertionError:
            g.log.info("Volume was not created as expected.")
        else:
            self.addCleanup(
                heketi_ops.heketi_volume_delete, self.heketi_client_node,
                self.heketi_server_url, vol_fail["bricks"][0]["volume"])
            self.assertFalse(
                vol_fail,
                "Volume should have not been created. Out: %s" % vol_fail)

    @pytest.mark.tier2
    @podcmd.GlustoPod()
    def test_volume_create_replica_2(self):
        """Validate creation of a replica 2 volume"""
        vol_create_info = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 1,
            replica=2, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol_create_info["id"],
            raise_on_error=True)
        actual_replica = int(
            vol_create_info["durability"]["replicate"]["replica"])
        self.assertEqual(
            actual_replica, 2,
            "Volume '%s' has '%s' as value for replica,"
            " expected 2." % (vol_create_info["id"], actual_replica))
        vol_name = vol_create_info['name']

        # Get gluster volume info
        gluster_vol = volume_ops.get_volume_info(
            'auto_get_gluster_endpoint', volname=vol_name)
        self.assertTrue(
            gluster_vol, "Failed to get volume '%s' info" % vol_name)

        # Check amount of bricks
        brick_amount = len(gluster_vol[vol_name]['bricks']['brick'])
        self.assertEqual(brick_amount, 2,
                         "Brick amount is expected to be 2. "
                         "Actual amount is '%s'" % brick_amount)

    @pytest.mark.tier2
    @podcmd.GlustoPod()
    def test_volume_create_snapshot_enabled(self):
        """Validate volume creation with snapshot enabled"""
        factor = 1.5
        vol_create_info = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 1,
            snapshot_factor=factor, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol_create_info["id"])
        snap_factor_count = vol_create_info["snapshot"]["factor"]
        self.assertEqual(
            snap_factor_count, factor,
            "snapshot factor %s is not same as %s" % (
                snap_factor_count, factor))

        vol_name, snap_name = vol_create_info["name"], "snap1"
        try:
            ret, out, err = snap_ops.snap_create(
                'auto_get_gluster_endpoint', vol_name,
                snap_name, timestamp=False)
            self.assertEqual(
                ret, 0, "Failed to create snapshot %s" % snap_name)

            # Get gluster volume info
            gluster_vol = volume_ops.get_volume_info(
                'auto_get_gluster_endpoint', volname=vol_name)
            self.assertTrue(
                gluster_vol, "Failed to get volume '%s' info" % vol_name)
            self.assertEqual(
                gluster_vol[vol_name]['snapshotCount'], "1",
                "Failed to get snapshot count for volume %s" % vol_name)
        finally:
            ret, out, err = snap_ops.snap_delete(
                'auto_get_gluster_endpoint', snap_name)
            self.assertEqual(
                ret, 0, "Failed to delete snapshot %s" % snap_name)

    @podcmd.GlustoPod()
    def get_gluster_vol_info(self, file_vol):
        """Get Gluster vol info.

        Args:
            ocp_client (str): Node to execute OCP commands.
            file_vol (str): file volume name.

        Returns:
            dict: Info of the given gluster vol.
        """
        g_vol_info = volume_ops.get_volume_info(
            "auto_get_gluster_endpoint", file_vol)

        if not g_vol_info:
            raise AssertionError("Failed to get volume info for gluster "
                                 "volume '%s'" % file_vol)
        if file_vol in g_vol_info:
            g_vol_info = g_vol_info.get(file_vol)
        return g_vol_info

    @pytest.mark.tier1
    def test_volume_creation_of_size_greater_than_the_device_size(self):
        """Validate creation of a volume of size greater than the size of a
        device.
        """
        h_node, h_url = self.heketi_client_node, self.heketi_server_url

        # Remove existing BHV to calculate freespace
        bhv_list = heketi_ops.get_block_hosting_volume_list(h_node, h_url)
        if bhv_list:
            for bhv in bhv_list:
                bhv_info = heketi_ops.heketi_volume_info(
                    h_node, h_url, bhv, json=True)
                if bhv_info['blockinfo'].get('blockvolume') is None:
                    heketi_ops.heketi_volume_delete(h_node, h_url, bhv)

        topology = heketi_ops.heketi_topology_info(h_node, h_url, json=True)
        nodes_free_space, nodes_ips = [], []
        selected_nodes, selected_devices = [], []
        cluster = topology['clusters'][0]
        node_count = len(cluster['nodes'])
        msg = ("At least 3 Nodes are required in cluster. "
               "But only %s Nodes are present." % node_count)
        if node_count < 3:
            self.skipTest(msg)

        online_nodes_count = 0
        for node in cluster['nodes']:
            nodes_ips.append(node['hostnames']['storage'][0])

            if node['state'] != 'online':
                continue

            online_nodes_count += 1

            # Disable nodes after 3rd online nodes
            if online_nodes_count > 3:
                heketi_ops.heketi_node_disable(h_node, h_url, node['id'])
                self.addCleanup(
                    heketi_ops.heketi_node_enable, h_node, h_url, node['id'])
                continue

            selected_nodes.append(node['id'])

            device_count = len(node['devices'])
            msg = ("At least 2 Devices are required on each Node."
                   "But only %s Devices are present." % device_count)
            if device_count < 2:
                self.skipTest(msg)

            sel_devices, online_devices_count, free_space = [], 0, 0
            for device in node['devices']:
                if device['state'] != 'online':
                    continue

                online_devices_count += 1

                # Disable devices after 2nd online devices
                if online_devices_count > 2:
                    heketi_ops.heketi_device_disable(
                        h_node, h_url, device['id'])
                    self.addCleanup(
                        heketi_ops.heketi_device_enable, h_node, h_url,
                        device['id'])
                    continue

                sel_devices.append(device['id'])
                free_space += int(device['storage']['free'] / (1024**2))

            selected_devices.append(sel_devices)
            nodes_free_space.append(free_space)

            msg = ("At least 2 online Devices are required on each Node. "
                   "But only %s Devices are online on Node: %s." % (
                       online_devices_count, node['id']))
            if online_devices_count < 2:
                self.skipTest(msg)

        msg = ("At least 3 online Nodes are required in cluster. "
               "But only %s Nodes are online in Cluster: %s." % (
                   online_nodes_count, cluster['id']))
        if online_nodes_count < 3:
            self.skipTest(msg)

        # Select node with minimum free space
        min_free_size = min(nodes_free_space)
        index = nodes_free_space.index(min_free_size)

        # Get max device size from selected node
        device_size = 0
        for device in selected_devices[index]:
            device_info = heketi_ops.heketi_device_info(
                h_node, h_url, device, json=True)
            device_size = max(device_size, (
                int(device_info['storage']['total'] / (1024**2))))

        vol_size = device_size + 1

        if vol_size >= min_free_size:
            self.skipTest('Required free space %s is not available' % vol_size)

        # Create heketi volume with device size + 1
        vol_info = self.create_heketi_volume_with_name_and_wait(
            name="volume_size_greater_than_device_size", size=vol_size,
            json=True)

        # Get gluster server IP's from heketi volume info
        glusterfs_servers = heketi_ops.get_vol_file_servers_and_hosts(
            h_node, h_url, vol_info['id'])

        # Verify gluster server IP's in heketi volume info
        msg = ("gluster IP's '%s' does not match with IP's '%s' found in "
               "heketi volume info" % (
                   nodes_ips, glusterfs_servers['vol_servers']))
        self.assertEqual(
            set(glusterfs_servers['vol_servers']), set(nodes_ips), msg)

        vol_name = vol_info['name']
        gluster_v_info = self.get_gluster_vol_info(vol_name)

        # Verify replica count in gluster v info
        msg = "Volume %s is replica %s instead of replica 3" % (
            vol_name, gluster_v_info['replicaCount'])
        self.assertEqual('3', gluster_v_info['replicaCount'])

        # Verify distCount in gluster v info
        msg = "Volume %s distCount is %s instead of distCount as 3" % (
            vol_name, int(gluster_v_info['distCount']))
        self.assertEqual(
            int(gluster_v_info['brickCount']) // 3,
            int(gluster_v_info['distCount']), msg)

        # Verify bricks count in gluster v info
        msg = ("Volume %s does not have bricks count multiple of 3. It has %s"
               % (vol_name, gluster_v_info['brickCount']))
        self.assertFalse(int(gluster_v_info['brickCount']) % 3, msg)

    @pytest.mark.tier2
    def test_create_volume_with_same_name(self):
        """Test create two volumes with the same name and verify that 2nd one
        is failing with the appropriate error.
        """
        h_node, h_url = self.heketi_client_node, self.heketi_server_url
        vol_name = "autovol-%s" % utils.get_random_str()

        # Create volume for the first time
        vol_info = heketi_ops.heketi_volume_create(
            h_node, h_url, size=1, name=vol_name, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete, h_node, h_url, vol_info['id'])

        vol_info_new = None
        try:
            # Try to create volume for 2nd time
            vol_info_new = heketi_ops.heketi_volume_create(
                h_node, h_url, size=1, name=vol_name, json=True)
            self.addCleanup(
                heketi_ops.heketi_volume_delete,
                h_node, h_url, vol_info_new['id'])
        except AssertionError as err:
            # Verify msg in error
            msg = "Volume name '%s' already in use" % vol_name
            if msg not in six.text_type(err):
                raise

        # Raise exception if there is no error raised by heketi
        msg = ('Volume %s and %s got created two times with the same name '
               'unexpectedly.' % (vol_info, vol_info_new))
        self.assertFalse(vol_info_new, msg)

    @pytest.mark.tier2
    def test_heketi_volume_provision_after_node_reboot(self):
        """Provision volume before and after node reboot"""
        # Skip test if not able to connect to Cloud Provider
        try:
            node_ops.find_vm_name_by_ip_or_hostname(self.node)
        except (NotImplementedError, exceptions.ConfigError) as e:
            self.skipTest(e)

        h_client, h_server = self.heketi_client_node, self.heketi_server_url
        g_nodes = [
            g_node["manage"]
            for g_node in self.gluster_servers_info.values()][2:]

        # Create heketi volume
        vol_info = heketi_ops.heketi_volume_create(
            h_client, h_server, 1, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete,
            h_client, h_server, vol_info['id'])

        # Power off gluster server nodes
        for g_node in g_nodes:
            vm_name = node_ops.find_vm_name_by_ip_or_hostname(g_node)
            self.power_off_gluster_node_vm(vm_name, g_node)

        # Create heketi volume when gluster nodes are down
        with self.assertRaises(AssertionError):
            vol_info = heketi_ops.heketi_volume_create(
                h_client, h_server, 1, json=True)
            self.addCleanup(
                heketi_ops.heketi_volume_delete,
                h_client, h_server, vol_info['id'])

        # Power on gluster server nodes
        for g_node in g_nodes:
            vm_name = node_ops.find_vm_name_by_ip_or_hostname(g_node)
            self.power_on_gluster_node_vm(vm_name, g_node)

        # Try to create heketi volume after reboot
        vol_info = heketi_ops.heketi_volume_create(
            h_client, h_server, 1, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete,
            h_client, h_server, vol_info['id'])
