import json

import ddt
from glusto.core import Glusto as g

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.exceptions import ExecutionError
from openshiftstoragelibs.heketi_ops import (
    heketi_device_add,
    heketi_device_delete,
    heketi_device_disable,
    heketi_device_enable,
    heketi_device_info,
    heketi_device_remove,
    heketi_node_disable,
    heketi_node_enable,
    heketi_node_info,
    heketi_node_list,
    heketi_topology_info,
    heketi_volume_create,
    heketi_volume_delete,
)


@ddt.ddt
class TestHeketiDeviceOperations(BaseClass):
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
        """Validate device enable and disable functionality"""

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
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol_info['id'])

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
            self.addCleanup(
                heketi_volume_delete, self.heketi_client_node,
                self.heketi_server_url, json.loads(out)["id"])
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
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol_info['id'])

        # Check that one of volume's bricks is present on the device
        present = self.check_any_of_bricks_present_in_device(
            vol_info['bricks'], online_device_id)
        self.assertTrue(
            present,
            "None of '%s' volume bricks is present on the '%s' device." % (
                vol_info['id'], online_device_id))

    @ddt.data(True, False)
    def test_device_remove_operation(self, delete_device):
        """Validate remove/delete device using heketi-cli"""

        gluster_server_0 = g.config["gluster_servers"].values()[0]
        try:
            device_name = gluster_server_0["additional_devices"][0]
        except (KeyError, IndexError):
            self.skipTest(
                "Additional disk is not specified for node with following "
                "hostnames and IP addresses: %s, %s." % (
                    gluster_server_0.get('manage', '?'),
                    gluster_server_0.get('storage', '?')))
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
                    lowest_device_name = device["name"]
        if lowest_device_id is None:
            self.skipTest(
                "Didn't find suitable device for disablement on '%s' node." % (
                    node_id))

        # Create volume
        vol_size = 1
        vol_info = heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, vol_size,
            json=True)
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol_info['id'])

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
            self.heketi_client_node, self.heketi_server_url,
            lowest_device_id)
        if not delete_device:
            self.addCleanup(heketi_device_enable, self.heketi_client_node,
                            self.heketi_server_url, lowest_device_id)

        # Remove device from Heketi
        try:
            heketi_device_remove(
                self.heketi_client_node, self.heketi_server_url,
                lowest_device_id)
        except Exception:
            if delete_device:
                self.addCleanup(heketi_device_enable, self.heketi_client_node,
                                self.heketi_server_url, lowest_device_id)
            raise
        if not delete_device:
            self.addCleanup(heketi_device_disable, self.heketi_client_node,
                            self.heketi_server_url, lowest_device_id)

        if delete_device:
            try:
                heketi_device_delete(
                    self.heketi_client_node, self.heketi_server_url,
                    lowest_device_id)
            except Exception:
                self.addCleanup(heketi_device_enable, self.heketi_client_node,
                                self.heketi_server_url, lowest_device_id)
                self.addCleanup(heketi_device_disable, self.heketi_client_node,
                                self.heketi_server_url, lowest_device_id)
                raise
            self.addCleanup(
                heketi_device_add,
                self.heketi_client_node, self.heketi_server_url,
                lowest_device_name, node_id)

        # Create volume
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url, vol_size,
                                        json=True)
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol_info['id'])

        if delete_device:
            return

        # Check that none of volume's bricks is present on the device
        present = self.check_any_of_bricks_present_in_device(
            vol_info['bricks'], lowest_device_id)
        self.assertFalse(
            present,
            "Some of the '%s' volume bricks is present of the removed "
            "'%s' device." % (vol_info['id'], lowest_device_id))

    def test_heketi_with_device_removal_insuff_space(self):
        """Validate heketi with device removal insufficient space"""

        # Disable 4+ nodes and 3+ devices on the first 3 nodes
        min_free_space_gb = 5
        min_free_space = min_free_space_gb * 1024**2
        heketi_url = self.heketi_server_url
        heketi_node = self.heketi_client_node
        nodes = {}

        node_ids = heketi_node_list(heketi_node, heketi_url)
        self.assertTrue(node_ids)
        for node_id in node_ids:
            node_info = heketi_node_info(
                heketi_node, heketi_url, node_id, json=True)
            if (node_info["state"].lower() != "online" or
                    not node_info["devices"]):
                continue
            if len(nodes) > 2:
                heketi_node_disable(heketi_node, heketi_url, node_id)
                self.addCleanup(
                    heketi_node_enable, heketi_node, heketi_url, node_id)
                continue
            for device in node_info["devices"]:
                if device["state"].lower() != "online":
                    continue
                free_space = device["storage"]["free"]
                if node_id not in nodes:
                    nodes[node_id] = []
                if (free_space < min_free_space or len(nodes[node_id]) > 1):
                    heketi_device_disable(
                        heketi_node, heketi_url, device["id"])
                    self.addCleanup(
                        heketi_device_enable,
                        heketi_node, heketi_url, device["id"])
                    continue
                nodes[node_id].append({
                    "device_id": device["id"], "free": free_space})

        # Skip test if nodes requirements are not met
        if (len(nodes) < 3 or
                not all(map((lambda _list: len(_list) > 1), nodes.values()))):
            raise self.skipTest(
                "Could not find 3 online nodes with 2 online devices "
                "having free space bigger than %dGb." % min_free_space_gb)

        # Calculate size of a potential distributed vol
        if nodes[node_ids[0]][0]["free"] > nodes[node_ids[0]][1]["free"]:
            index = 0
        else:
            index = 1
        vol_size_gb = int(nodes[node_ids[0]][index]["free"] / (1024 ** 2)) + 1
        device_id = nodes[node_ids[0]][index]["device_id"]

        # Create volume with such size that we consume space more than
        # size of smaller disks
        try:
            heketi_vol = heketi_volume_create(
                heketi_node, heketi_url, vol_size_gb, json=True)
        except Exception as e:
            g.log.warning(
                "Got following error trying to create '%s'Gb vol: %s" % (
                    vol_size_gb, e))
            vol_size_gb -= 1
            heketi_vol = heketi_volume_create(
                heketi_node, heketi_url, vol_size_gb, json=True)
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, heketi_vol["bricks"][0]["volume"])

        # Try to 'remove' bigger Heketi disk expecting error,
        # because there is no space on smaller disk to relocate bricks to
        heketi_device_disable(heketi_node, heketi_url, device_id)
        self.addCleanup(
            heketi_device_enable, heketi_node, heketi_url, device_id)
        try:
            self.assertRaises(
                ExecutionError, heketi_device_remove,
                heketi_node, heketi_url, device_id)
        except Exception:
            self.addCleanup(
                heketi_device_disable, heketi_node, heketi_url, device_id)
            raise
