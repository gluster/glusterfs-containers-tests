from glusto.core import Glusto as g
from glustolibs.gluster import volume_ops

from cnslibs.common import exceptions
from cnslibs.common import heketi_libs
from cnslibs.common import heketi_ops
from cnslibs.common import openshift_ops
from cnslibs.common import podcmd


class TestVolumeCreationTestCases(heketi_libs.HeketiBaseClass):
    """
    Class for volume creation related test cases
    """

    @podcmd.GlustoPod()
    def test_create_heketi_volume(self):
        """
        Method to test heketi volume creation and
        background gluster validation
        """

        hosts = []
        gluster_servers = []
        brick_info = []

        output_dict = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 10, json=True)

        self.assertNotEqual(output_dict, False,
                            "Volume could not be created")

        volume_name = output_dict["name"]
        volume_id = output_dict["id"]

        self.addCleanup(self.delete_volumes, volume_id)

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

        if self.deployment_type == "cns":
            gluster_pod = openshift_ops.get_ocp_gluster_pod_names(
                self.heketi_client_node)[1]

            p = podcmd.Pod(self.heketi_client_node, gluster_pod)

            volume_info = volume_ops.get_volume_info(p, volume_name)
            volume_status = volume_ops.get_volume_status(p, volume_name)

        elif self.deployment_type == "crs":
            volume_info = volume_ops.get_volume_info(
                self.heketi_client_node, volume_name)
            volume_status = volume_ops.get_volume_status(
                self.heketi_client_node, volume_name)

        self.assertNotEqual(volume_info, None,
                            "get_volume_info returned None")
        self.assertNotEqual(volume_status, None,
                            "get_volume_status returned None")

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

    def test_volume_creation_no_free_devices(self):
        """Test case CNS-804"""
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
        self.addCleanup(self.delete_volumes, vol["id"])

        # Try to create second volume getting "no free space" error
        try:
            vol_fail = heketi_ops.heketi_volume_create(
                node, server_url, min_space_gb, json=True)
        except exceptions.ExecutionError:
            g.log.info("Volume was not created as expected.")
        else:
            self.addCleanup(
                self.delete_volumes, vol_fail["bricks"][0]["volume"])
            self.assertFalse(
                vol_fail,
                "Volume should have not been created. Out: %s" % vol_fail)
