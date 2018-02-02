from __future__ import division
import json
import math
import unittest

from glusto.core import Glusto as g
from glustolibs.gluster import volume_ops

from cnslibs.common.exceptions import ExecutionError, ConfigError
from cnslibs.common.heketi_libs import HeketiClientSetupBaseClass
from cnslibs.common.openshift_ops import get_ocp_gluster_pod_names
from cnslibs.common import heketi_ops, podcmd


class TestVolumeCreationTestCases(HeketiClientSetupBaseClass):
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

        self.assertNotEqual(volume_info, None,
                            "get_volume_info returned None")
        self.assertNotEqual(volume_status, None,
                            "get_volume_status returned None")

        self.assertEqual(int(volume_info[volume_name]["status"]), 1,
                         "Volume %s status down" % volume_id)
        for brick_details in volume_info[volume_name]["bricks"]["brick"]:
            brick_info.append(brick_details["name"])

        if brick_info == []:
            raise ExecutionError("Brick details empty for %s" % volume_name)

        for brick in brick_info:
            brick_data = brick.strip().split(":")
            brick_ip = brick_data[0]
            brick_name = brick_data[1]
            self.assertEqual(int(volume_status
                             [volume_name][brick_ip]
                             [brick_name]["status"]), 1,
                             "Brick %s is not up" % brick_name)

    def test_volume_creation_no_free_devices(self):
        """
        To test volume creation when there are no free devices
        """

        large_volume = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url,
            595, json=True)

        self.assertNotEqual(large_volume, False, "Volume creation failed")
        self.addCleanup(self.delete_volumes, large_volume["id"])

        small_volume = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url,
            90, json=True)

        self.assertNotEqual(small_volume, False, "Volume creation failed")
        self.addCleanup(self.delete_volumes, small_volume["id"])

        ret, out, err = heketi_ops.heketi_volume_create(
                self.heketi_client_node, self.heketi_server_url,
                50, raw_cli_output=True)

        self.assertEqual(err.strip(), "Error: No space",
                         "Volume creation failed with invalid reason")

        if ret == 0:
            out_json = json.loads(out)
            self.addCleanup(self.delete_volumes, out_json["id"])

