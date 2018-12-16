import json

from glusto.core import Glusto as g

from cnslibs.common.heketi_libs import HeketiBaseClass
from cnslibs.common.heketi_ops import (heketi_node_enable,
                                       heketi_node_info,
                                       heketi_node_disable,
                                       heketi_node_list,
                                       heketi_volume_create,
                                       heketi_device_add,
                                       heketi_device_delete,
                                       heketi_device_disable,
                                       heketi_device_remove,
                                       heketi_device_info,
                                       heketi_device_enable,
                                       heketi_topology_info)


class TestHeketiDeviceOperations(HeketiBaseClass):
    """Test Heketi device enable/disable and remove functionality."""

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
        device_info = heketi_device_info(self.heketi_client_node,
                                         self.heketi_server_url,
                                         device_id,
                                         json=True)
        self.assertNotEqual(device_info, False,
                            "Device info on %s failed" % device_id)
        for brick in bricks:
            if brick['device'] != device_id:
                continue
            for brick_info in device_info['bricks']:
                if brick_info['path'] == brick['path']:
                    return True
        return False

    def get_online_nodes_disable_redundant(self):
        """
        Find online nodes and disable n-3 nodes and return
        list of online nodes
        """
        node_list = heketi_node_list(self.heketi_client_node,
                                     self.heketi_server_url)
        self.assertTrue(node_list, "Failed to list heketi nodes")
        g.log.info("Successfully got the list of nodes")
        # Fetch online nodes  from node list
        online_hosts = []

        for node in node_list:
            node_info = heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node, json=True)
            if node_info["state"] == "online":
                online_hosts.append(node_info)

        # Skip test if online node count is less than 3i
        if len(online_hosts) < 3:
            raise self.skipTest(
                "This test can run only if online hosts are more than 2")
        # if we have n nodes, disable n-3 nodes
        for node_info in online_hosts[3:]:
            node_id = node_info["id"]
            g.log.info("going to disable node id %s", node_id)
            heketi_node_disable(self.heketi_client_node,
                                self.heketi_server_url,
                                node_id)
            self.addCleanup(heketi_node_enable,
                            self.heketi_client_node,
                            self.heketi_server_url,
                            node_id)

        for host in online_hosts[1:3]:
            found_online = False
            for device in host["devices"]:
                if device["state"].strip().lower() == "online":
                    found_online = True
                    break
            if not found_online:
                self.skipTest(("no device online on node %s" % host["id"]))

        return online_hosts

    def test_device_enable_disable(self):
        """Test case CNS-764. Test device enable and disable functionality."""

        # Disable all but one device on the first online node
        online_hosts = self.get_online_nodes_disable_redundant()
        online_device_id = ""
        for device in online_hosts[0]["devices"]:
            if device["state"].strip().lower() != "online":
                continue
            device_id = device["id"]
            if online_device_id == "":
                online_device_id = device_id
            else:
                g.log.info("going to disable device %s", device_id)
                heketi_device_disable(
                    self.heketi_client_node, self.heketi_server_url, device_id)
                self.addCleanup(
                    heketi_device_enable,
                    self.heketi_client_node, self.heketi_server_url, device_id)
        if online_device_id == "":
            self.skipTest(
                "No device online on node %s" % online_hosts[0]["id"])

        # Create volume when only 1 device is online
        vol_size = 1
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url, vol_size,
                                        json=True)
        self.assertTrue(vol_info, (
            "Failed to create heketi volume of size %d" % vol_size))
        self.addCleanup(self.delete_volumes, vol_info['id'])

        # Check that one of volume's bricks is present on the device
        present = self.check_any_of_bricks_present_in_device(
            vol_info['bricks'], online_device_id)
        self.assertTrue(
            present,
            "None of '%s' volume bricks is present on the '%s' device." % (
                vol_info['id'], online_device_id))

        g.log.info("Going to disable device id %s", online_device_id)
        heketi_device_disable(
            self.heketi_client_node, self.heketi_server_url, online_device_id)
        self.addCleanup(heketi_device_enable, self.heketi_client_node,
                        self.heketi_server_url, online_device_id)

        ret, out, err = heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url,
            vol_size, json=True, raw_cli_output=True)
        if ret == 0:
            self.addCleanup(self.delete_volumes, json.loads(out)["id"])
        self.assertNotEqual(ret, 0,
                            ("Volume creation did not fail. ret- %s "
                             "out- %s err- %s" % (ret, out, err)))
        g.log.info("Volume creation failed as expected, err- %s", err)

        # Enable back the device which was previously disabled
        g.log.info("Going to enable device id %s", online_device_id)
        heketi_device_enable(
            self.heketi_client_node, self.heketi_server_url, online_device_id)

        # Create volume when device is enabled
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url, vol_size,
                                        json=True)
        self.assertTrue(vol_info, (
            "Failed to create heketi volume of size %d" % vol_size))
        self.addCleanup(self.delete_volumes, vol_info['id'])

        # Check that one of volume's bricks is present on the device
        present = self.check_any_of_bricks_present_in_device(
            vol_info['bricks'], online_device_id)
        self.assertTrue(
            present,
            "None of '%s' volume bricks is present on the '%s' device." % (
                vol_info['id'], online_device_id))

    def test_device_remove_operation(self):
        """Test case CNS-766. Test device remove functionality."""
        gluster_server_0 = g.config["gluster_servers"].values()[0]
        try:
            device_name = gluster_server_0["additional_devices"][0]
        except IndexError:
            self.skipTest("Additional disk is not specified for node.")
        manage_hostname = gluster_server_0["manage"]

        # Get node ID of the Gluster hostname
        topo_info = heketi_topology_info(self.heketi_client_node,
                                         self.heketi_server_url, json=True)
        self.assertTrue(
            topo_info["clusters"][0]["nodes"],
            "Cluster info command returned empty list of nodes.")

        node_id = None
        for node in topo_info["clusters"][0]["nodes"]:
            if manage_hostname == node['hostnames']["manage"][0]:
                node_id = node["id"]
                break
        self.assertNotEqual(
            node_id, None,
            "No information about node_id for %s" % manage_hostname)

        # Iterate chosen node devices and pick the smallest online one.
        lowest_device_size = lowest_device_id = None
        online_hosts = self.get_online_nodes_disable_redundant()
        for host in online_hosts[0:3]:
            if node_id != host["id"]:
                continue
            for device in host["devices"]:
                if device["state"].strip().lower() != "online":
                    continue
                if (lowest_device_size is None or
                        device["storage"]["total"] < lowest_device_size):
                    lowest_device_size = device["storage"]["total"]
                    lowest_device_id = device["id"]
        if lowest_device_id is None:
            self.skipTest(
                "Didn't find suitable device for disablement on '%s' node." % (
                    node_id))

        # Create volume
        vol_size = 1
        vol_info = heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, vol_size,
            json=True)
        self.assertTrue(vol_info, (
            "Failed to create heketi volume of size %d" % vol_size))
        self.addCleanup(self.delete_volumes, vol_info['id'])

        # Add extra device, then remember it's ID and size
        heketi_device_add(self.heketi_client_node, self.heketi_server_url,
                          device_name, node_id)
        node_info_after_addition = heketi_node_info(
            self.heketi_client_node, self.heketi_server_url, node_id,
            json=True)
        for device in node_info_after_addition["devices"]:
            if device["name"] != device_name:
                continue
            device_id_new = device["id"]
            device_size_new = device["storage"]["total"]
        self.addCleanup(heketi_device_delete, self.heketi_client_node,
                        self.heketi_server_url, device_id_new)
        self.addCleanup(heketi_device_remove, self.heketi_client_node,
                        self.heketi_server_url, device_id_new)
        self.addCleanup(heketi_device_disable, self.heketi_client_node,
                        self.heketi_server_url, device_id_new)

        if lowest_device_size > device_size_new:
            skip_msg = ("Skip test case, because newly added disk %s is "
                        "smaller than device which we want to remove %s." % (
                            device_size_new, lowest_device_size))
            self.skipTest(skip_msg)

        g.log.info("Removing device id %s" % lowest_device_id)
        ret, out, err = heketi_device_remove(
            self.heketi_client_node, self.heketi_server_url,
            lowest_device_id, raw_cli_output=True)
        if ret == 0:
            self.addCleanup(heketi_device_enable, self.heketi_client_node,
                            self.heketi_server_url, lowest_device_id)
            self.addCleanup(heketi_device_disable, self.heketi_client_node,
                            self.heketi_server_url, lowest_device_id)
        self.assertNotEqual(ret, 0, (
            "Device removal did not fail. ret: %s, out: %s, err: %s." % (
                ret, out, err)))
        g.log.info("Device removal failed as expected, err- %s", err)

        # Need to disable device before removing
        heketi_device_disable(
            self.heketi_client_node, self.heketi_server_url, lowest_device_id)
        self.addCleanup(heketi_device_enable, self.heketi_client_node,
                        self.heketi_server_url, lowest_device_id)

        # Remove device from Heketi
        heketi_device_remove(
            self.heketi_client_node, self.heketi_server_url, lowest_device_id)
        self.addCleanup(heketi_device_disable, self.heketi_client_node,
                        self.heketi_server_url, lowest_device_id)

        # Create volume
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url, vol_size,
                                        json=True)
        self.assertTrue(vol_info, (
                "Failed to create heketi volume of size %d" % vol_size))
        self.addCleanup(self.delete_volumes, vol_info['id'])

        # Check that none of volume's bricks is present on the device
        present = self.check_any_of_bricks_present_in_device(
            vol_info['bricks'], lowest_device_id)
        self.assertFalse(
            present,
            "Some of the '%s' volume bricks is present of the removed "
            "'%s' device." % (vol_info['id'], lowest_device_id))
