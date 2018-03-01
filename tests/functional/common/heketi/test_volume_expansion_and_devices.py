from __future__ import division
import json
import math
import unittest

from glusto.core import Glusto as g
from glustolibs.gluster import volume_ops, rebalance_ops

from cnslibs.common.exceptions import ExecutionError, ConfigError
from cnslibs.common.heketi_libs import HeketiClientSetupBaseClass
from cnslibs.common.openshift_ops import get_ocp_gluster_pod_names
from cnslibs.common import heketi_ops, podcmd


class TestVolumeExpansionAndDevicesTestCases(HeketiClientSetupBaseClass):
    """
    Class for volume expansion and devices addition related test cases
    """

    @podcmd.GlustoPod()
    def get_num_of_bricks(self, volume_name):
        """
        Method to determine number of
        bricks at present in the volume
        """
        brick_info = []

        if self.deployment_type == "cns":

            gluster_pod = get_ocp_gluster_pod_names(
                self.heketi_client_node)[1]

            p = podcmd.Pod(self.heketi_client_node, gluster_pod)

            volume_info_before_expansion = volume_ops.get_volume_info(
                p, volume_name)

        elif self.deployment_type == "crs":
            volume_info_before_expansion = volume_ops.get_volume_info(
                self.heketi_client_node, volume_name)

        self.assertIsNotNone(
            volume_info_before_expansion,
            "Volume info is None")

        for brick_details in (volume_info_before_expansion
                              [volume_name]["bricks"]["brick"]):

            brick_info.append(brick_details["name"])

        num_of_bricks = len(brick_info)

        return num_of_bricks

    @podcmd.GlustoPod()
    def get_rebalance_status(self, volume_name):
        """
        Rebalance status after expansion
        """
        if self.deployment_type == "cns":
            gluster_pod = get_ocp_gluster_pod_names(
                self.heketi_client_node)[1]

            p = podcmd.Pod(self.heketi_client_node, gluster_pod)

            wait_reb = rebalance_ops.wait_for_rebalance_to_complete(
                p, volume_name)
            self.assertTrue(wait_reb, "Rebalance not complete")

            reb_status = rebalance_ops.get_rebalance_status(
                p, volume_name)

        elif self.deployment_type == "crs":
            wait_reb = rebalance_ops.wait_for_rebalance_to_complete(
                self.heketi_client_node, volume_name)
            self.assertTrue(wait_reb, "Rebalance not complete")

            reb_status = rebalance_ops.get_rebalance_status(
                self.heketi_client_node, volume_name)

        self.assertEqual(reb_status["aggregate"]["statusStr"],
                         "completed", "Rebalance not yet completed")

    @podcmd.GlustoPod()
    def get_brick_and_volume_status(self, volume_name):
        """
        Status of each brick in a volume
        for background validation
        """
        brick_info = []

        if self.deployment_type == "cns":
            gluster_pod = get_ocp_gluster_pod_names(
                self.heketi_client_node)[1]

            p = podcmd.Pod(self.heketi_client_node, gluster_pod)

            volume_info = volume_ops.get_volume_info(p, volume_name)
            volume_status = volume_ops.get_volume_status(p, volume_name)

        elif self.deployment_type == "crs":
            volume_info = volume_ops.get_volume_info(
                self.heketi_client_node, volume_name)
            volume_status = volume_ops.get_volume_status(
                self.heketi_client_node, volume_name)

        self.assertIsNotNone(volume_info, "Volume info is empty")
        self.assertIsNotNone(volume_status, "Volume status is empty")

        self.assertEqual(int(volume_info[volume_name]["status"]), 1,
                         "Volume not up")
        for brick_details in volume_info[volume_name]["bricks"]["brick"]:
            brick_info.append(brick_details["name"])

        if brick_info == []:
            raise ExecutionError("Brick details empty for %s" % volume_name)

        for brick in brick_info:
            brick_data = brick.strip().split(":")
            brick_ip = brick_data[0]
            brick_name = brick_data[1]
            self.assertEqual(int(volume_status[volume_name][brick_ip]
                             [brick_name]["status"]), 1,
                             "Brick %s not up" % brick_name)

    def enable_disable_devices(self, additional_devices_attached, enable=True):
        """
        Method to enable and disable devices
        """
        op = 'enable' if enable else 'disable'
        for node_id in additional_devices_attached.keys():
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)

            if not enable:
                self.assertNotEqual(node_info, False,
                                    "Node info for node %s failed" % node_id)

            for device in node_info["devices"]:
                if device["name"] == additional_devices_attached[node_id]:
                    out = getattr(heketi_ops, 'heketi_device_%s' % op)(
                        self.heketi_client_node,
                        self.heketi_server_url,
                        device["id"],
                        json=True)
                    if out is False:
                        g.log.info("Device %s could not be %sd"
                                   % (device["id"], op))
                    else:
                        g.log.info("Device %s %sd" % (device["id"], op))

    def enable_devices(self, additional_devices_attached):
        """
        Method to call enable_disable_devices to enable devices
        """
        return self.enable_disable_devices(additional_devices_attached, True)

    def disable_devices(self, additional_devices_attached):
        """
        Method to call enable_disable_devices to disable devices
        """
        return self.enable_disable_devices(additional_devices_attached, False)

    def get_devices_summary_free_space(self):
        """
        Calculates minimum free space per device and
        returns total free space across all devices
        """

        heketi_node_id_list = []
        free_spaces = []

        heketi_node_list_string = heketi_ops.heketi_node_list(
            self.heketi_client_node,
            self.heketi_server_url, mode="cli", json=True)

        self.assertNotEqual(
            heketi_node_list_string, False,
            "Heketi node list empty")

        for line in heketi_node_list_string.strip().split("\n"):
            heketi_node_id_list.append(line.strip().split(
                "Cluster")[0].strip().split(":")[1])

        for node_id in heketi_node_id_list:
            node_info_dict = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            total_free_space = 0
            for device in node_info_dict["devices"]:
                total_free_space += device["storage"]["free"]
            free_spaces.append(total_free_space)

        total_free_space = sum(free_spaces)/(1024 ** 2)
        total_free_space = int(math.floor(total_free_space))

        return total_free_space

    def detach_devices_attached(self, device_id_list):
        """
        All the devices attached are gracefully
        detached in this function
        """
        for device_id in device_id_list:
            device_disable = heketi_ops.heketi_device_disable(
                self.heketi_client_node, self.heketi_server_url, device_id)
            self.assertNotEqual(
                device_disable, False,
                "Device %s could not be disabled" % device_id)
            device_remove = heketi_ops.heketi_device_remove(
                self.heketi_client_node, self.heketi_server_url, device_id)
            self.assertNotEqual(
                device_remove, False,
                "Device %s could not be removed" % device_id)
            device_delete = heketi_ops.heketi_device_delete(
                self.heketi_client_node, self.heketi_server_url, device_id)
            self.assertNotEqual(
                device_delete, False,
                "Device %s could not be deleted" % device_id)

    @podcmd.GlustoPod()
    def test_add_device_heketi_cli(self):
        """
        Method to test heketi device addition with background
        gluster validation
        """
        node_id_list = []
        device_id_list = []
        hosts = []
        gluster_servers = []

        node_list_info = heketi_ops.heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)

        self.assertNotEqual(node_list_info, False,
                            "heketi node list command failed")

        lines = node_list_info.strip().split("\n")

        for line in lines:
            node_id_list.append(line.strip().split("Cluster")
                                [0].strip().split(":")[1])

        creation_info = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 100, json=True)

        self.assertNotEqual(creation_info, False,
                            "Volume creation failed")

        self.addCleanup(self.delete_volumes, creation_info["id"])

        ret, out, err = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 620, json=True,
            raw_cli_output=True)

        self.assertEqual(ret, 255, "Volume creation did not fail ret- %s "
                         "out- %s err= %s" % (ret, out, err))
        g.log.info("Volume creation failed as expected, err- %s" % err)

        if ret == 0:
            out_json = json.loads(out)
            self.addCleanup(self.delete_volumes, out_json["id"])

        for node_id in node_id_list:
            device_present = False
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)

            self.assertNotEqual(
                node_info, False,
                "Heketi node info on node %s failed" % node_id)

            node_ip = node_info["hostnames"]["storage"][0]

            for gluster_server in g.config["gluster_servers"].keys():
                gluster_server_ip = (g.config["gluster_servers"]
                                     [gluster_server]["storage"])
                if gluster_server_ip == node_ip:
                    device_name = (g.config["gluster_servers"][gluster_server]
                                   ["additional_devices"][0])
                    break
            device_addition_info = heketi_ops.heketi_device_add(
                self.heketi_client_node, self.heketi_server_url,
                device_name, node_id, json=True)

            self.assertNotEqual(device_addition_info, False,
                                "Device %s addition failed" % device_name)

            node_info_after_addition = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            for device in node_info_after_addition["devices"]:
                if device["name"] == device_name:
                    device_present = True
                    device_id_list.append(device["id"])

            self.assertEqual(device_present, True,
                             "device %s not present" % device["id"])

        self.addCleanup(self.detach_devices_attached, device_id_list)

        output_dict = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url,
            620, json=True)

        self.assertNotEqual(output_dict, False, "Volume creation failed")
        self.addCleanup(self.delete_volumes, output_dict["id"])

        self.assertEqual(output_dict["durability"]["replicate"]["replica"], 3)
        self.assertEqual(output_dict["size"], 620)
        mount_node = (output_dict["mount"]["glusterfs"]
                      ["device"].strip().split(":")[0])

        hosts.append(mount_node)
        backup_volfile_server_list = (
            output_dict["mount"]["glusterfs"]["options"]
            ["backup-volfile-servers"].strip().split(","))

        for backup_volfile_server in backup_volfile_server_list:
                hosts.append(backup_volfile_server)
        for gluster_server in g.config["gluster_servers"].keys():
                gluster_servers.append(g.config["gluster_servers"]
                                       [gluster_server]["storage"])
        self.assertEqual(
            set(hosts), set(gluster_servers),
            "Hosts do not match gluster servers for %s" % output_dict["id"])

        volume_name = output_dict["name"]

        self.get_brick_and_volume_status(volume_name)

    @unittest.skip("Failure of this test messes up the test system "
                   "for other tests to pass. So, this test is "
                   "skipped temporarily until failure case is "
                   "handled.")
    def test_volume_expansion_expanded_volume(self):
        """
        To test volume expansion with brick and rebalance
        validation
        """
        creation_info = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 10, json=True)

        self.assertNotEqual(creation_info, False, "Volume creation failed")

        volume_name = creation_info["name"]
        volume_id = creation_info["id"]

        free_space_after_creation = self.get_devices_summary_free_space()

        volume_info_before_expansion = heketi_ops.heketi_volume_info(
            self.heketi_client_node,
            self.heketi_server_url,
            volume_id, json=True)

        self.assertNotEqual(
            volume_info_before_expansion, False,
            "Heketi volume info for %s failed" % volume_id)

        heketi_vol_info_size_before_expansion = (
            volume_info_before_expansion["size"])

        num_of_bricks_before_expansion = self.get_num_of_bricks(volume_name)

        self.get_brick_and_volume_status(volume_name)

        expansion_info = heketi_ops.heketi_volume_expand(
            self.heketi_client_node,
            self.heketi_server_url,
            volume_id, 3)

        self.assertNotEqual(expansion_info, False,
                            "Volume %s expansion failed" % volume_id)

        free_space_after_expansion = self.get_devices_summary_free_space()

        self.assertTrue(
            free_space_after_creation > free_space_after_expansion,
            "Expansion of %s did not consume free space" % volume_id)

        num_of_bricks_after_expansion = self.get_num_of_bricks(volume_name)

        self.get_brick_and_volume_status(volume_name)
        self.get_rebalance_status(volume_name)

        volume_info_after_expansion = heketi_ops.heketi_volume_info(
            self.heketi_client_node,
            self.heketi_server_url,
            volume_id, json=True)

        self.assertNotEqual(
            volume_info_after_expansion, False,
            "Heketi volume info for %s command failed" % volume_id)

        heketi_vol_info_size_after_expansion = (
            volume_info_after_expansion["size"])

        difference_size_after_expansion = (
            heketi_vol_info_size_after_expansion -
            heketi_vol_info_size_before_expansion)

        self.assertTrue(
            difference_size_after_expansion > 0,
            "Volume expansion for %s did not consume free space" % volume_id)

        num_of_bricks_added_after_expansion = (num_of_bricks_after_expansion -
                                               num_of_bricks_before_expansion)

        self.assertEqual(
            num_of_bricks_added_after_expansion, 3,
            "Number of bricks added in %s after expansion is not 3"
            % volume_name)

        further_expansion_info = heketi_ops.heketi_volume_expand(
            self.heketi_client_node,
            self.heketi_server_url,
            volume_id, 3)

        self.assertNotEqual(further_expansion_info, False,
                            "Volume expansion failed for %s" % volume_id)

        free_space_after_further_expansion = (
            self.get_devices_summary_free_space())
        self.assertTrue(
            free_space_after_expansion > free_space_after_further_expansion,
            "Further expansion of %s did not consume free space" % volume_id)

        num_of_bricks_after_further_expansion = (
            self.get_num_of_bricks(volume_name))

        self.get_brick_and_volume_status(volume_name)

        self.get_rebalance_status(volume_name)

        volume_info_after_further_expansion = heketi_ops.heketi_volume_info(
            self.heketi_client_node,
            self.heketi_server_url,
            volume_id, json=True)

        self.assertNotEqual(
            volume_info_after_further_expansion, False,
            "Heketi volume info for %s failed" % volume_id)

        heketi_vol_info_size_after_further_expansion = (
            volume_info_after_further_expansion["size"])

        difference_size_after_further_expansion = (
            heketi_vol_info_size_after_further_expansion -
            heketi_vol_info_size_after_expansion)

        self.assertTrue(
            difference_size_after_further_expansion > 0,
            "Size of volume %s did not increase" % volume_id)

        num_of_bricks_added_after_further_expansion = (
            num_of_bricks_after_further_expansion -
            num_of_bricks_after_expansion)

        self.assertEqual(
            num_of_bricks_added_after_further_expansion, 3,
            "Number of bricks added is not 3 for %s" % volume_id)

        free_space_before_deletion = self.get_devices_summary_free_space()

        volume_delete = heketi_ops.heketi_volume_delete(
            self.heketi_client_node, self.heketi_server_url,
            volume_id, json=True)

        self.assertNotEqual(volume_delete, False, "Deletion of %s failed"
                            % volume_id)

        free_space_after_deletion = self.get_devices_summary_free_space()

        self.assertTrue(free_space_after_deletion > free_space_before_deletion,
                        "Free space not reclaimed after deletion of %s"
                        % volume_id)

    def test_volume_expansion_no_free_space(self):
        """
        To test volume expansion when there is no free
        space
        """

        heketi_node_id_list = []
        additional_devices_attached = {}
        heketi_node_list_string = heketi_ops.heketi_node_list(
            self.heketi_client_node,
            self.heketi_server_url, mode="cli", json=True)

        self.assertNotEqual(heketi_node_list_string, False,
                            "Heketi node list command failed")

        for line in heketi_node_list_string.strip().split("\n"):
            heketi_node_id_list.append(line.strip().split(
                "Cluster")[0].strip().split(":")[1])

        for node_id in heketi_node_id_list:
            node_info_dict = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            self.assertNotEqual(node_info_dict, False,
                                "Heketi node info for %s failed" % node_id)
            for gluster_server in self.gluster_servers:
                gluster_server_ip = (
                    self.gluster_servers_info[gluster_server]["storage"])
                node_ip = node_info_dict["hostnames"]["storage"][0]

                if gluster_server_ip == node_ip:
                    addition_status = (
                        heketi_ops.heketi_device_add(
                            self.heketi_client_node,
                            self.heketi_server_url,
                            self.gluster_servers_info[gluster_server]
                            ["additional_devices"][0], node_id))

                    self.assertNotEqual(addition_status, False,
                                        "Addition of device %s failed"
                                        % self.gluster_servers_info
                                        [gluster_server]
                                        ["additional_devices"][0])

                additional_devices_attached.update({node_id:
                                                   self.gluster_servers_info
                                                   [gluster_server]
                                                   ["additional_devices"][0]})

        additional_devices_ids = []
        for node_id in additional_devices_attached.keys():
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)

            for device in node_info["devices"]:
                if device["name"] == additional_devices_attached[node_id]:
                    additional_devices_ids.append(device["id"])

        self.addCleanup(self.detach_devices_attached,
                        additional_devices_ids)

        for node_id in additional_devices_attached.keys():
            flag_device_added = False
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            for device in node_info["devices"]:
                if device["name"] == additional_devices_attached[node_id]:
                    flag_device_added = True

            self.assertTrue(flag_device_added)

        self.disable_devices(additional_devices_attached)

        creation_info = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 675, json=True)

        self.assertNotEqual(creation_info, False, "Volume creation failed")

        volume_name = creation_info["name"]
        volume_id = creation_info["id"]

        volume_info_before_expansion = heketi_ops.heketi_volume_info(
            self.heketi_client_node,
            self.heketi_server_url,
            volume_id, json=True)

        heketi_vol_info_size_before_expansion = (
            volume_info_before_expansion["size"])

        num_of_bricks_before_expansion = self.get_num_of_bricks(volume_name)

        self.get_brick_and_volume_status(volume_name)

        free_space_after_creation = self.get_devices_summary_free_space()

        ret, out, err = heketi_ops.heketi_volume_expand(
            self.heketi_client_node, self.heketi_server_url,
            volume_id, 50, raw_cli_output=True)

        self.assertEqual(ret, 255, "volume expansion did not fail ret- %s "
                         "out- %s err= %s" % (ret, out, err))
        g.log.info("Volume expansion failed as expected, err- %s" % err)

        if ret == 0:
            out_json = json.loads(out)
            self.addCleanup(self.delete_volumes, out_json["id"])

        self.enable_devices(additional_devices_attached)

        expansion_info = heketi_ops.heketi_volume_expand(
            self.heketi_client_node, self.heketi_server_url,
            volume_id, 50, json=True)

        self.assertNotEqual(expansion_info, False,
                            "Volume %s could not be expanded" % volume_id)

        free_space_after_expansion = self.get_devices_summary_free_space()

        self.assertTrue(
            free_space_after_creation > free_space_after_expansion,
            "Free space not consumed after expansion of %s" % volume_id)

        num_of_bricks_after_expansion = self.get_num_of_bricks(volume_name)

        self.get_brick_and_volume_status(volume_name)

        volume_info_after_expansion = heketi_ops.heketi_volume_info(
            self.heketi_client_node,
            self.heketi_server_url,
            volume_id, json=True)

        self.assertNotEqual(
            volume_info_after_expansion, False,
            "Heketi volume info for %s failed" % volume_id)

        heketi_vol_info_size_after_expansion = (
            volume_info_after_expansion["size"])

        difference_size_after_expansion = (
                           heketi_vol_info_size_after_expansion -
                           heketi_vol_info_size_before_expansion)

        self.assertTrue(difference_size_after_expansion > 0,
                        "Size of %s not increased" % volume_id)

        num_of_bricks_added_after_expansion = (num_of_bricks_after_expansion -
                                               num_of_bricks_before_expansion)

        self.assertEqual(num_of_bricks_added_after_expansion, 3)

        deletion_info = heketi_ops.heketi_volume_delete(
            self.heketi_client_node, self.heketi_server_url, volume_id,
            json=True)

        self.assertNotEqual(deletion_info, False,
                            "Deletion of %s not successful" % volume_id)

        free_space_after_deletion = self.get_devices_summary_free_space()

        self.assertTrue(
            free_space_after_deletion > free_space_after_expansion,
            "Free space not reclaimed after deletion of volume %s" % volume_id)

    @unittest.skip("Failure of this test messes up the test system "
                   "for other tests to pass. So, this test is "
                   "skipped temporarily until failure case is "
                   "handled.")
    @podcmd.GlustoPod()
    def test_volume_expansion_rebalance_brick(self):
        """
        To test volume expansion with brick and rebalance
        validation
        """
        creation_info = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 10, json=True)

        self.assertNotEqual(creation_info, False, "Volume creation failed")

        volume_name = creation_info["name"]
        volume_id = creation_info["id"]

        free_space_after_creation = self.get_devices_summary_free_space()

        volume_info_before_expansion = heketi_ops.heketi_volume_info(
            self.heketi_client_node,
            self.heketi_server_url,
            volume_id, json=True)

        self.assertNotEqual(volume_info_before_expansion, False,
                            "Volume info for %s failed" % volume_id)

        heketi_vol_info_size_before_expansion = (
            volume_info_before_expansion["size"])

        self.get_brick_and_volume_status(volume_name)
        num_of_bricks_before_expansion = self.get_num_of_bricks(volume_name)

        expansion_info = heketi_ops.heketi_volume_expand(
            self.heketi_client_node,
            self.heketi_server_url,
            volume_id, 5)

        self.assertNotEqual(expansion_info, False,
                            "Volume expansion of %s failed" % volume_id)

        free_space_after_expansion = self.get_devices_summary_free_space()
        self.assertTrue(
            free_space_after_creation > free_space_after_expansion,
            "Free space not consumed after expansion of %s" % volume_id)

        volume_info_after_expansion = heketi_ops.heketi_volume_info(
            self.heketi_client_node,
            self.heketi_server_url,
            volume_id, json=True)

        self.assertNotEqual(volume_info_after_expansion, False,
                            "Volume info failed for %s" % volume_id)

        heketi_vol_info_size_after_expansion = (
            volume_info_after_expansion["size"])

        difference_size = (heketi_vol_info_size_after_expansion -
                           heketi_vol_info_size_before_expansion)

        self.assertTrue(
            difference_size > 0,
            "Size not increased after expansion of %s" % volume_id)

        self.get_brick_and_volume_status(volume_name)
        num_of_bricks_after_expansion = self.get_num_of_bricks(volume_name)

        num_of_bricks_added = (num_of_bricks_after_expansion -
                               num_of_bricks_before_expansion)

        self.assertEqual(
            num_of_bricks_added, 3,
            "Number of bricks added is not 3 for %s" % volume_id)

        self.get_rebalance_status(volume_name)

        deletion_info = heketi_ops.heketi_volume_delete(
            self.heketi_client_node, self.heketi_server_url,
            volume_id, json=True)

        self.assertNotEqual(deletion_info, False,
                            "Deletion of volume %s failed" % volume_id)

        free_space_after_deletion = self.get_devices_summary_free_space()

        self.assertTrue(
            free_space_after_deletion > free_space_after_expansion,
            "Free space is not reclaimed after volume deletion of %s"
            % volume_id)

