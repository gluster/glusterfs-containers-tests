"""Test cases to enable device in heketi."""
import json

from cnslibs.common.heketi_libs import HeketiBaseClass
from cnslibs.common.heketi_ops import (heketi_node_enable,
                                       heketi_node_info,
                                       heketi_node_disable,
                                       heketi_node_list,
                                       heketi_volume_create,
                                       heketi_device_disable,
                                       heketi_device_info,
                                       heketi_device_enable)
from glusto.core import Glusto as g


class TestHeketiDeviceEnable(HeketiBaseClass):
    """Test device enable functionality from heketi-cli."""

    def enable_node(self, node_id):
        """
        Enable node through heketi-cli.

        :param node_id: str node ID
        """
        if node_id is None:
            return
        out = heketi_node_enable(self.heketi_client_node,
                                 self.heketi_server_url,
                                 node_id)
        self.assertNotEqual(out, False,
                            "Failed to enable node of"
                            " id %s" % node_id)

    def disable_node(self, node_id):
        """
        Disable node through heketi-cli.

        :param node_id: str node ID
        """
        if node_id is None:
            return
        out = heketi_node_disable(self.heketi_client_node,
                                  self.heketi_server_url,
                                  node_id)
        self.assertNotEqual(out, False,
                            "Failed to disable node of"
                            " id %s" % node_id)

    def get_node_info(self, node_id):
        """
        Get node information from node_id.

        :param node_id: str node ID
        :return node_info: list node information
        """
        if node_id is None:
            return
        node_info = heketi_node_info(
            self.heketi_client_node, self.heketi_server_url,
            node_id, json=True)
        self.assertNotEqual(node_info, False,
                            "Node info on %s failed" % node_id)
        return node_info

    def get_online_nodes(self, node_list):
        """
        Get online nodes information from node_list.

        :param node_list: list of node ID's
        :return: list node information of online  nodes
        """
        online_hosts_info = []

        for node in node_list:
            node_info = self.get_node_info(node)
            if node_info["state"] == "online":
                online_hosts_info.append(node_info)

        return online_hosts_info

    def disable_device(self, device_id):
        """
        Disable device  from heketi.

        :param device_id: str device ID to  be disabled
        """
        if device_id is None:
            return
        out = heketi_device_disable(
            self.heketi_client_node, self.heketi_server_url,
            device_id)

        self.assertNotEqual(out, False, "Failed to disable device of"
                                        " id %s" % device_id)

    def enable_device(self, device_id):
        """
        Enable device  from heketi.

        :param device_id: str device ID to  be enabled
        """
        if device_id is None:
            return
        out = heketi_device_enable(
            self.heketi_client_node, self.heketi_server_url,
            device_id)
        self.assertNotEqual(out, False, "Failed to enable device of"
                                        " id %s" % device_id)

    def get_device_info(self, device_id):
        """
        Get device information from heketi.

        :param device_id: str device ID to  fetch information
        :return device_info: dict device information
        """
        if device_id is None:
            return
        device_info = heketi_device_info(self.heketi_client_node,
                                         self.heketi_server_url,
                                         device_id,
                                         json=True)
        self.assertNotEqual(device_info, False,
                            "Device info on %s failed" % device_id)

        return device_info

    def check_any_of_bricks_present_in_device(self, bricks, device_id):
        """
        Check any of the bricks present in the device.

        :param bricks: list bricks of volume
        :param device_id: device ID
        :return True: bool if bricks are  present on device
        :return False: bool if bricks are not present on device
        """
        if device_id is None:
            return False
        device_info = self.get_device_info(device_id)
        for brick in bricks:
            if brick['device'] != device_id:
                continue
            for brick_info in device_info['bricks']:
                if brick_info['path'] == brick['path']:
                    return True
        return False

    def test_device_enable(self):
        """Test case CNS-764: Test device enable functionality."""
        g.log.info("Disable and Enable device in heketi")
        node_list = heketi_node_list(self.heketi_client_node,
                                     self.heketi_server_url)
        self.assertTrue(node_list, "Failed to list heketi nodes")
        g.log.info("Successfully got the list of nodes")

        # Fetch online nodes  from node list
        online_hosts = self.get_online_nodes(node_list)

        # skip test if online node count is less  than 3, to  create  replicate
        # volume we need at least 3 nodes  to  be online
        if len(online_hosts) < 3:
            raise self.skipTest(
                "This test can run only if online hosts are more than 2")

        # if we have n nodes, disable n-3 nodes
        for node_info in online_hosts[3:]:
            node_id = node_info["id"]
            g.log.info("going to disable node id %s", node_id)
            self.disable_node(node_id)
            self.addCleanup(self.enable_node, node_id)

        for host in online_hosts[1:3]:
            found_online = False
            for device in host["devices"]:
                if device["state"].strip().lower() == "online":
                    found_online = True
                    break
            if not found_online:
                self.skipTest(("no device online on node %s" % host["id"]))

        # on the first node, disable all but one device:
        online_device_id = ""
        for device in online_hosts[0]["devices"]:
            if device["state"].strip().lower() != "online":
                continue
            device_id = device["id"]
            if online_device_id == "":
                online_device_id = device_id
            else:
                g.log.info("going to disable device %s", device_id)
                self.disable_device(device_id)
                self.addCleanup(self.enable_device, device_id)
        if online_device_id == "":
            self.skipTest(
                ("no device online on node %s" % online_hosts[0]["id"]))

        # create volume when 1 device is online
        vol_size = 1
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url, vol_size,
                                        json=True)
        self.assertTrue(vol_info, (
                "Failed to create heketi volume of size %d" % vol_size))
        self.addCleanup(self.delete_volumes, vol_info['id'])

        # check created  volume  brick is present on the device
        present = self.check_any_of_bricks_present_in_device(
            vol_info['bricks'],
            online_device_id)
        self.assertTrue(present, "bricks is present on this  device")

        g.log.info("going to disable device id %s", online_device_id)

        self.disable_device(online_device_id)
        self.addCleanup(self.enable_device, online_device_id)

        ret, out, err = heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url,
            vol_size, json=True, raw_cli_output=True)
        if ret == 0:
            out_json = json.loads(out)
            self.addCleanup(self.delete_volumes, out_json["id"])
        self.assertNotEqual(ret, 0,
                            ("Volume creation did not fail ret- %s "
                             "out- %s err- %s" % (ret, out, err)))

        g.log.info("Volume creation failed as expected, err- %s", err)

        # enable back  the device which was previously disabled
        g.log.info("going to enable device id %s", online_device_id)
        self.enable_device(online_device_id)

        # create volume when device is enabled
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url, vol_size,
                                        json=True)
        self.assertTrue(vol_info, (
                "Failed to create heketi volume of size %d" % vol_size))
        self.addCleanup(self.delete_volumes, vol_info['id'])

        # check created  volume  brick is present on the device
        present = self.check_any_of_bricks_present_in_device(
            vol_info['bricks'],
            online_device_id)
        self.assertTrue(present, "brick is present on this  device")
