import ddt
from glusto.core import Glusto as g
import pytest

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs.waiter import Waiter


@ddt.ddt
class TestClusterOperationsTestCases(baseclass.BaseClass):
    """Class for heketi cluster creation related test cases"""

    @pytest.mark.tier1
    @ddt.data("", "block", "file")
    def test_heketi_cluster_create(self, disable_volume_type):
        """Test heketi cluster creation"""
        kwargs = {"json": True}
        if disable_volume_type:
            kwargs.update({disable_volume_type: False})

        # Create heketi cluster
        cluster_info = heketi_ops.heketi_cluster_create(
            self.heketi_client_node, self.heketi_server_url, **kwargs)
        self.addCleanup(
            heketi_ops.heketi_cluster_delete, self.heketi_client_node,
            self.heketi_server_url, cluster_info["id"])

        # Validate block and file options
        err_msg = "Cluster with %s option, unexpectedly set to %s"
        if disable_volume_type:
            self.assertFalse(
                cluster_info[disable_volume_type],
                err_msg % (disable_volume_type, "True"))
        else:
            self.assertTrue(
                cluster_info["block"], err_msg % ("block", "False"))
            self.assertTrue(cluster_info["file"], err_msg % ("file", "False"))

    @pytest.mark.tier1
    def test_heketi_cluster_list(self):
        """Test and validateheketi cluster list operation"""
        # Create heketi cluster
        cluster_info = heketi_ops.heketi_cluster_create(
            self.heketi_client_node, self.heketi_server_url, json=True)
        self.addCleanup(
            heketi_ops.heketi_cluster_delete, self.heketi_client_node,
            self.heketi_server_url, cluster_info["id"])

        # Get heketi cluster list and validate presence of newly
        # created cluster
        cluster_list = heketi_ops.heketi_cluster_list(
            self.heketi_client_node, self.heketi_server_url, json=True)
        err_msg = (
            "Cluster id %s not found in cluster list %s"
            % (cluster_info["id"], cluster_list["clusters"]))
        self.assertIn(cluster_info["id"], cluster_list["clusters"], err_msg)

    @pytest.mark.tier1
    def test_heketi_cluster_info(self):
        """Test and validateheketi cluster info operation"""
        # Create heketi cluster
        cluster_info = heketi_ops.heketi_cluster_create(
            self.heketi_client_node, self.heketi_server_url, json=True)
        self.addCleanup(
            heketi_ops.heketi_cluster_delete, self.heketi_client_node,
            self.heketi_server_url, cluster_info["id"])

        # Get newly created heketi cluster info
        get_cluster_info = heketi_ops.heketi_cluster_info(
            self.heketi_client_node, self.heketi_server_url,
            cluster_info["id"], json=True)

        # Validate newly created heketi cluster info
        params = (
            ("id", cluster_info["id"]),
            ("block", True),
            ("file", True),
            ("blockvolumes", []),
            ("volumes", []),
            ("nodes", []))
        for param, value in params:
            self.assertEqual(get_cluster_info[param], value)

    @pytest.mark.tier1
    def test_heketi_cluster_delete(self):
        """Test and validateheketi cluster delete operation"""
        # Create heketi cluster
        cluster_info = heketi_ops.heketi_cluster_create(
            self.heketi_client_node, self.heketi_server_url, json=True)

        # Delete newly created cluster
        heketi_ops.heketi_cluster_delete(
            self.heketi_client_node, self.heketi_server_url,
            cluster_info["id"])

        # Get heketi cluster list and check for absence of deleted cluster
        cluster_list = heketi_ops.heketi_cluster_list(
            self.heketi_client_node, self.heketi_server_url, json=True)
        err_msg = (
            "Cluster id %s was not expected in cluster list %s"
            % (cluster_info["id"], cluster_list["clusters"]))
        self.assertNotIn(cluster_info["id"], cluster_list["clusters"], err_msg)

    @pytest.mark.tier1
    def test_create_heketi_cluster_and_add_node(self):
        """Test heketi node add to a newly created cluster"""
        storage_host_info = g.config.get("additional_gluster_servers")
        if not storage_host_info:
            self.skipTest(
                "Skip test case as 'additional_gluster_servers' option is "
                "not provided in config file")

        storage_host_info = list(storage_host_info.values())[0]
        try:
            storage_hostname = storage_host_info["manage"]
            storage_ip = storage_host_info["storage"]
            storage_device = storage_host_info["devices"][0]
        except KeyError:
            msg = ("Config options 'additional_gluster_servers.manage' "
                   "'additional_gluster_servers.storage' and "
                   "'additional_gluster_servers.devices' "
                   "must be set.")
            g.log.error(msg)
            raise exceptions.ConfigError(msg)

        h_client, h_server = self.heketi_client_node, self.heketi_server_url
        storage_zone = 1

        cluster_id = heketi_ops.heketi_cluster_create(
            self.heketi_client_node, self.heketi_server_url, json=True)["id"]
        self.addCleanup(
            heketi_ops.heketi_cluster_delete, self.heketi_client_node,
            self.heketi_server_url, cluster_id)

        self.configure_node_to_run_gluster(storage_hostname)

        heketi_node_info = heketi_ops.heketi_node_add(
            h_client, h_server, storage_zone, cluster_id,
            storage_hostname, storage_ip, json=True)
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

        topology_info = heketi_ops.heketi_topology_info(
            h_client, h_server, json=True)

        cluster_details = [
            cluster for cluster in topology_info["clusters"]
            if cluster["id"] == cluster_id]
        err_msg = "Cluster details for id '%s' not found" % cluster_id
        self.assertTrue(cluster_details, err_msg)
        err_msg = ("Multiple clusters with same id '%s' found %s" % (
                   cluster_id, cluster_details))
        self.assertEqual(len(cluster_details), 1, err_msg)

        node_details = [
            node for node in cluster_details[0]["nodes"]
            if node["id"] == heketi_node_id]
        err_msg = "Node details for id '%s' not found" % heketi_node_id
        self.assertTrue(node_details, err_msg)
        err_msg = ("Multiple nodes with same id '%s' found %s" % (
                   heketi_node_id, node_details))
        self.assertEqual(len(node_details), 1, err_msg)

        err_msg = "Unexpected %s found '%s', expected '%s'"
        exp_storage_hostname = node_details[0]["hostnames"]["manage"][0]
        self.assertEqual(
            exp_storage_hostname, storage_hostname,
            err_msg % ("hostname", exp_storage_hostname, storage_hostname,))
        exp_storage_ip = node_details[0]["hostnames"]["storage"][0]
        self.assertEqual(
            exp_storage_ip, storage_ip,
            err_msg % ("IP address", exp_storage_ip, storage_ip))
        zone = node_details[0]["zone"]
        self.assertEqual(
            zone, storage_zone, err_msg % ("zone", zone, storage_zone))

    @pytest.mark.tier1
    def test_heketi_server_operations_cleanup_on_idle_setup(self):
        """Run heketi db clean up on an idle setup"""
        h_node, h_url = self.heketi_client_node, self.heketi_server_url
        err_msg = "There should not be any pending operations list {}"

        # Verify the server operations
        for waiter_add in Waiter(300, 20):
            initial_ops = heketi_ops.heketi_server_operations_list(
                h_node, h_url)
            if not initial_ops:
                break
        if waiter_add.expired:
            self.assertFalse(initial_ops, err_msg.format(initial_ops))

        # Run cleanup
        cleanup = heketi_ops.heketi_server_operation_cleanup(h_node, h_url)
        self.assertFalse(
            cleanup, "Cleanup command failed with message {}".format(cleanup))

        # Verify the server operations
        final_ops = heketi_ops.heketi_server_operations_list(h_node, h_url)
        self.assertFalse(final_ops, err_msg.format(final_ops))
