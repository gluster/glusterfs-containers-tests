from glusto.core import Glusto as g
from glustolibs.gluster import peer_ops
import six

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import openshift_ops
from openshiftstoragelibs import openshift_storage_version
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import utils


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

    def add_heketi_node_to_cluster(self, cluster_id):
        """Add new node to a cluster"""
        storage_host_info = g.config.get("additional_gluster_servers")
        if not storage_host_info:
            self.skipTest(
                "Skip test case as 'additional_gluster_servers' option is "
                "not provided in config file")

        storage_host_info = list(storage_host_info.values())[0]
        try:
            storage_hostname = storage_host_info["manage"]
            storage_ip = storage_host_info["storage"]
        except KeyError:
            msg = ("Config options 'additional_gluster_servers.manage' "
                   "and 'additional_gluster_servers.storage' must be set.")
            g.log.error(msg)
            raise exceptions.ConfigError(msg)

        h_client, h_server = self.heketi_client_node, self.heketi_server_url
        storage_zone = 1

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

        return storage_hostname, storage_ip

    @podcmd.GlustoPod()
    def test_heketi_node_add_with_valid_cluster(self):
        """Test heketi node add operation with valid cluster id"""
        if (openshift_storage_version.get_openshift_storage_version()
                < "3.11.4"):
            self.skipTest(
                "This test case is not supported for < OCS 3.11.4 builds due "
                "to bug BZ-1732831")

        h_client, h_server = self.heketi_client_node, self.heketi_server_url
        ocp_node = self.ocp_master_node[0]

        # Get heketi endpoints before adding node
        h_volume_ids = heketi_ops.heketi_volume_list(
            h_client, h_server, json=True)
        h_endpoints_before_new_node = heketi_ops.heketi_volume_endpoint_patch(
            h_client, h_server, h_volume_ids["volumes"][0])

        cluster_info = heketi_ops.heketi_cluster_list(
            h_client, h_server, json=True)
        storage_hostname, storage_ip = self.add_heketi_node_to_cluster(
            cluster_info["clusters"][0])

        # Get heketi nodes and validate for newly added node
        h_node_ids = heketi_ops.heketi_node_list(h_client, h_server, json=True)
        for h_node_id in h_node_ids:
            node_hostname = heketi_ops.heketi_node_info(
                h_client, h_server, h_node_id, json=True)
            if node_hostname["hostnames"]["manage"][0] == storage_hostname:
                break
            node_hostname = None

        err_msg = ("Newly added heketi node %s not found in heketi node "
                   "list %s" % (storage_hostname, h_node_ids))
        self.assertTrue(node_hostname, err_msg)

        # Check gluster peer status for newly added node
        if self.is_containerized_gluster():
            gluster_pods = openshift_ops.get_ocp_gluster_pod_details(ocp_node)
            gluster_pod = [
                gluster_pod["pod_name"]
                for gluster_pod in gluster_pods
                if gluster_pod["pod_hostname"] == storage_hostname][0]

            gluster_peer_status = peer_ops.get_peer_status(
                podcmd.Pod(ocp_node, gluster_pod))
        else:
            gluster_peer_status = peer_ops.get_peer_status(
                storage_hostname)
        self.assertEqual(
            len(gluster_peer_status), len(self.gluster_servers))

        err_msg = "Expected peer status is 1 and actual is %s"
        for peer in gluster_peer_status:
            peer_status = int(peer["connected"])
            self.assertEqual(peer_status, 1, err_msg % peer_status)

        # Get heketi endpoints after adding node
        h_endpoints_after_new_node = heketi_ops.heketi_volume_endpoint_patch(
            h_client, h_server, h_volume_ids["volumes"][0])

        # Get openshift openshift endpoints and patch with heketi endpoints
        heketi_db_endpoint = openshift_ops.oc_get_custom_resource(
            ocp_node, "dc", name=self.heketi_dc_name,
            custom=".:spec.template.spec.volumes[*].glusterfs.endpoints")[0]
        openshift_ops.oc_patch(
            ocp_node, "ep", heketi_db_endpoint, h_endpoints_after_new_node)
        self.addCleanup(
            openshift_ops.oc_patch, ocp_node, "ep", heketi_db_endpoint,
            h_endpoints_before_new_node)
        ep_addresses = openshift_ops.oc_get_custom_resource(
            ocp_node, "ep", name=heketi_db_endpoint,
            custom=".:subsets[*].addresses[*].ip")[0].split(",")

        err_msg = "Hostname %s not present in endpoints %s" % (
            storage_ip, ep_addresses)
        self.assertIn(storage_ip, ep_addresses, err_msg)

    def test_heketi_node_add_with_invalid_cluster(self):
        """Test heketi node add operation with invalid cluster id"""
        storage_hostname, cluster_id = None, utils.get_random_str(size=33)
        try:
            storage_hostname, _ = self.add_heketi_node_to_cluster(cluster_id)
        except AssertionError as e:
            err_msg = ("validation failed: cluster: %s is not a valid UUID" %
                       cluster_id)
            if err_msg not in six.text_type(e):
                raise

        err_msg = ("Unexpectedly node %s got added to cluster %s" % (
            storage_hostname, cluster_id))
        self.assertFalse(storage_hostname, err_msg)
