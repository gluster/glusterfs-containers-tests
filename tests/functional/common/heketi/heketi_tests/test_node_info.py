from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.peer_ops import get_pool_list

from cnslibs.common.baseclass import BaseClass
from cnslibs.common import heketi_ops, podcmd


class TestHeketiVolume(BaseClass):
    """
    Class to test heketi volume create
    """

    @podcmd.GlustoPod()
    def test_to_get_list_of_nodes(self):
        """
        Listing all nodes and compare the
        node listed in previous step
        """

        # List all list
        ip = []
        g.log.info("Listing the node id")
        heketi_node_id_list = heketi_ops.heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)

        g.log.info("Successfully listed the node")

        if (len(heketi_node_id_list) == 0):
            raise ExecutionError("Node list empty")

        for node_id in heketi_node_id_list:
            g.log.info("Retrieve the node info")
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            self.assertTrue(node_info, ("Failed to "
                            "retrieve the node info"))
            g.log.info("Successfully retrieved the node info %s" % node_id)
            ip.append(node_info["hostnames"]["storage"])

        # Compare the node listed in previous step
        hostname = []

        g.log.info("Get the pool list")
        list_of_pools = get_pool_list('auto_get_gluster_endpoint')
        self.assertTrue(list_of_pools, ("Failed to get the "
                        "pool list from gluster pods/nodes"))
        g.log.info("Successfully got the pool list from gluster pods/nodes")
        for pool in list_of_pools:
            hostname.append(pool["hostname"])

        if (len(heketi_node_id_list) != len(list_of_pools)):
            raise ExecutionError(
                "Heketi volume list %s is not equal "
                "to gluster volume list %s" % ((ip), (hostname)))
        g.log.info("The node IP's from node info and list"
                   " is : %s/n and pool list from gluster"
                   " pods/nodes is %s" % ((ip), (hostname)))

    def test_to_retrieve_node_info(self):
        """
        List and retrieve node related info
        """

        # List all list
        g.log.info("Listing the node id")
        heketi_node_id_list = heketi_ops.heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)
        self.assertTrue(heketi_node_id_list, ("Node Id list is empty."))
        g.log.info("Successfully listed the node")

        for node_id in heketi_node_id_list:
            g.log.info("Retrieve the node info")
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            self.assertTrue(node_info, ("Failed to "
                            "retrieve the node info"))
            g.log.info("Successfully retrieved the node info %s" % node_id)
