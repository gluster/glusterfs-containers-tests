from glusto.core import Glusto as g
from glustolibs.gluster import peer_ops

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import podcmd


class TestHeketiNodeOperations(baseclass.BaseClass):
    """Class to test heketi node operations
    """

    @podcmd.GlustoPod()
    def test_heketi_node_list(self):
        """Test node list operation
        """
        h_client, h_server = self.heketi_client_node, self.heketi_server_url

        # List heketi nodes
        node_ips = []
        heketi_node_id_list = heketi_ops.heketi_node_list(h_client, h_server)

        for node_id in heketi_node_id_list:
            node_info = heketi_ops.heketi_node_info(
                h_client, h_server, node_id, json=True)
            node_ips.append(node_info["hostnames"]["storage"])

        # Compare the node listed in previous step
        hostnames = []
        list_of_pools = peer_ops.get_pool_list('auto_get_gluster_endpoint')
        self.assertTrue(
            list_of_pools,
            "Failed to get the pool list from gluster pods/nodes")
        for pool in list_of_pools:
            hostnames.append(pool["hostname"])
        self.assertEqual(
            len(heketi_node_id_list), len(list_of_pools),
            "Heketi volume list %s is not equal to gluster volume list %s"
            % (node_ips, hostnames))

    def test_heketi_node_info(self):
        """Test heketi node info operation
        """
        h_client, h_server = self.heketi_client_node, self.heketi_server_url

        # List heketi node
        heketi_node_id_list = heketi_ops.heketi_node_list(h_client, h_server)
        self.assertTrue(heketi_node_id_list, "Node Id list is empty.")

        for node_id in heketi_node_id_list:
            node_info = heketi_ops.heketi_node_info(
                h_client, h_server, node_id, json=True)
            self.assertTrue(node_info, "Failed to retrieve the node info")
            self.assertEqual(
                node_info["id"], node_id,
                "Failed to match node ID. Exp: %s, Act: %s" % (
                    node_id, node_info["id"]))

    def test_heketi_node_states_enable_disable(self):
        """Test node enable and disable functionality
        """
        h_client, h_server = self.heketi_client_node, self.heketi_server_url

        node_list = heketi_ops.heketi_node_list(h_client, h_server)
        online_hosts = []
        for node_id in node_list:
            node_info = heketi_ops.heketi_node_info(
                h_client, h_server, node_id, json=True)
            if node_info["state"] == "online":
                online_hosts.append(node_info)

        if len(online_hosts) < 3:
            raise self.skipTest(
                "This test can run only if online hosts are more than 2")

        #  Disable n-3 nodes, in case we have n nodes
        for node_info in online_hosts[3:]:
            node_id = node_info["id"]
            heketi_ops.heketi_node_disable(h_client, h_server, node_id)
            self.addCleanup(
                heketi_ops.heketi_node_enable, h_client, h_server, node_id)

        # Create volume when 3 nodes are online
        vol_size = 1
        vol_info = heketi_ops.heketi_volume_create(
            h_client, h_server, vol_size, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete,
            h_client, h_server, vol_info['id'])

        node_id = online_hosts[0]['id']
        try:
            heketi_ops.heketi_node_disable(h_client, h_server, node_id)

            # Try to create a volume, volume creation should fail
            with self.assertRaises(AssertionError):
                heketi_volume = heketi_ops.heketi_volume_create(
                    h_client, h_server, vol_size)
                self.addCleanup(
                    heketi_ops.heketi_volume_delete,
                    h_client, h_server, heketi_volume["id"])
        finally:
            # Enable heketi node
            heketi_ops.heketi_node_enable(h_client, h_server, node_id)

        # Create volume when heketi node is enabled
        vol_info = heketi_ops.heketi_volume_create(
            h_client, h_server, vol_size, json=True)
        heketi_ops.heketi_volume_delete(h_client, h_server, vol_info['id'])

    def test_heketi_node_add_with_valid_cluster(self):
        """Test heketi node add operation with valid cluster id"""
        storage_host_info = g.config.get("additional_gluster_servers")
        if not storage_host_info:
            self.skipTest(
                "Skip test case as 'additional_gluster_servers' option is "
                "not provided in config file")

        h_client, h_server = self.heketi_client_node, self.heketi_server_url

        storage_host_info = list(storage_host_info.values())[0]
        storage_host_manage = storage_host_info["manage"]
        storage_host_name = storage_host_info["storage"]
        storage_device = storage_host_info["additional_devices"][0]
        storage_zone = 1

        cluster_info = heketi_ops.heketi_cluster_list(
            h_client, h_server, json=True)
        cluster_id = cluster_info["clusters"][0]

        if self.is_containerized_gluster():
            self.configure_node_to_run_gluster_pod(storage_host_manage)
        else:
            self.configure_node_to_run_gluster_node(storage_host_manage)

        heketi_node_info = heketi_ops.heketi_node_add(
            h_client, h_server, storage_zone, cluster_id,
            storage_host_manage, storage_host_name, json=True)
        heketi_node_id = heketi_node_info["id"]
        self.addCleanup(
            heketi_ops.heketi_node_delete, h_client, h_server, heketi_node_id)
        self.addCleanup(
            heketi_ops.heketi_node_remove, h_client, h_server, heketi_node_id)
        self.addCleanup(
            heketi_ops.heketi_node_disable, h_client, h_server, heketi_node_id)
        self.assertEqual(
            heketi_node_info["cluster"], cluster_id,
            "Node got added in unexpected cluster exp: %s, act: %s" % (
                cluster_id, heketi_node_info["cluster"]))

        heketi_ops.heketi_device_add(
            h_client, h_server, storage_device, heketi_node_id)
        heketi_node_info = heketi_ops.heketi_node_info(
            h_client, h_server, heketi_node_id, json=True)
        device_id = None
        for device in heketi_node_info["devices"]:
            if device["name"] == storage_device:
                device_id = device["id"]
                break
        err_msg = ("Failed to add device %s on node %s" % (
            storage_device, heketi_node_id))
        self.assertTrue(device_id, err_msg)

        self.addCleanup(
            heketi_ops.heketi_device_delete, h_client, h_server, device_id)
        self.addCleanup(
            heketi_ops.heketi_device_remove, h_client, h_server, device_id)
        self.addCleanup(
            heketi_ops.heketi_device_disable, h_client, h_server, device_id)

        cluster_info = heketi_ops.heketi_cluster_info(
            h_client, h_server, cluster_id, json=True)
        self.assertIn(
            heketi_node_info["id"], cluster_info["nodes"],
            "Newly added node %s not found in cluster %s, cluster info %s" % (
                heketi_node_info["id"], cluster_id, cluster_info))
