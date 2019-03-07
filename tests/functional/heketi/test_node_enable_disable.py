"""Test cases to disable and enable node in heketi."""
import json

from glusto.core import Glusto as g

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.heketi_ops import (
    heketi_node_disable,
    heketi_node_enable,
    heketi_node_info,
    heketi_node_list,
    heketi_volume_create,
    heketi_volume_delete,
)


class TestHeketiNodeState(BaseClass):
    """Test node enable and disable functionality."""

    def enable_node(self, node_id):
        """
        Enable node through heketi-cli.

        :param node_id: str node ID
        """
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

    def test_node_state(self):
        """
        Test node enable and disable functionality.

        If we have 4 gluster servers, if we disable 1/4 nodes from heketi
        and create a volume, the volume creation should be successful.

        If we disable 2/4 nodes from heketi-cli and create a volume
        the volume creation should fail.

        If we enable back one gluster server and create a volume
        the volume creation should be successful.
        """
        g.log.info("Disable node in heketi")
        node_list = heketi_node_list(self.heketi_client_node,
                                     self.heketi_server_url)
        self.assertTrue(node_list, "Failed to list heketi nodes")
        g.log.info("Successfully got the list of nodes")
        online_hosts = self.get_online_nodes(node_list)

        if len(online_hosts) < 3:
            raise self.skipTest(
                "This test can run only if online hosts are more "
                "than 2")
        # if we have n nodes, disable n-3 nodes
        for node_info in online_hosts[3:]:
            node_id = node_info["id"]
            g.log.info("going to disable node id %s", node_id)
            self.disable_node(node_id)
            self.addCleanup(self.enable_node, node_id)

        vol_size = 1
        # create volume when 3 nodes are online
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url, vol_size,
                                        json=True)
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol_info['id'])

        node_id = online_hosts[0]['id']
        g.log.info("going to disable node id %s", node_id)
        self.disable_node(node_id)
        self.addCleanup(self.enable_node, node_id)

        # try to create a volume, volume creation should fail
        ret, out, err = heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url,
            vol_size, raw_cli_output=True)
        if ret == 0:
            out_json = json.loads(out)
            self.addCleanup(
                heketi_volume_delete, self.heketi_client_node,
                self.heketi_server_url, out_json["id"])
        self.assertNotEqual(ret, 0,
                            ("Volume creation did not fail ret- %s "
                             "out- %s err- %s" % (ret, out, err)))

        g.log.info("Volume creation failed as expected, err- %s", err)
        # enable node
        self.enable_node(node_id)

        # create volume when node is enabled
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url, vol_size,
                                        json=True)
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol_info['id'])
