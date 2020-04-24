import ddt
from glusto.core import Glusto as g
from glustolibs.gluster import peer_ops
import pytest
import six

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import gluster_ops
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import openshift_ops
from openshiftstoragelibs import openshift_storage_version
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import utils


@ddt.ddt
class TestHeketiNodeOperations(baseclass.BaseClass):
    """Class to test heketi node operations
    """

    def setUp(self):
        super(TestHeketiNodeOperations, self).setUp()
        self.node = self.ocp_master_node[0]
        self.h_node = self.heketi_client_node
        self.h_url = self.heketi_server_url

    @pytest.mark.tier0
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

    @pytest.mark.tier1
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

    @pytest.mark.tier0
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
    def heketi_node_add_with_valid_cluster(self):
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

    @pytest.mark.tier0
    def test_heketi_node_add_with_valid_cluster(self):
        """Test heketi node add operation with valid cluster id"""
        if not self.is_containerized_gluster():
            self.skipTest(
                "Skipping this test case as CRS version check "
                "is not implemented")

        if (openshift_storage_version.get_openshift_storage_version()
                < "3.11.4"):
            self.skipTest(
                "This test case is not supported for < OCS 3.11.4 builds due "
                "to bug BZ-1732831")

        # Add node to valid cluster id
        self.heketi_node_add_with_valid_cluster()

    @pytest.mark.tier1
    def test_validate_heketi_node_add_with_db_check(self):
        """Test heketi db check after node add operation"""
        if not self.is_containerized_gluster():
            self.skipTest(
                "Skipping this test case as CRS version check "
                "is not implemented")

        if (openshift_storage_version.get_openshift_storage_version()
                < "3.11.4"):
            self.skipTest(
                "This test case is not supported for < OCS 3.11.4 builds due "
                "to bug BZ-1732831")

        # Get the total number of nodes in heketi db
        intial_db_info = heketi_ops.heketi_db_check(
            self.heketi_client_node, self.heketi_server_url, json=True)
        initial_node_count = intial_db_info['nodes']['total']

        # Add node to valid cluster id
        self.heketi_node_add_with_valid_cluster()

        # Verify the addition of node in heketi db
        final_db_info = heketi_ops.heketi_db_check(
            self.heketi_client_node, self.heketi_server_url, json=True)
        final_node_count = final_db_info['nodes']['total']
        msg = (
            "Initial node count {} and final node count {} in heketi db is"
            " not as expected".format(initial_node_count, final_node_count))
        self.assertEqual(initial_node_count + 1, final_node_count, msg)

    @pytest.mark.tier1
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

    def get_node_to_be_added(self):
        try:
            # Initializes additional gluster nodes variables
            self.additional_gluster_servers = list(
                g.config['additional_gluster_servers'].keys())
            self.additional_gluster_servers_info = (
                g.config['additional_gluster_servers'])
            return list(self.additional_gluster_servers_info.values())[0]
        except (KeyError, AttributeError, IndexError):
            self.skipTest("Required 'additional_gluster_servers' option is "
                          "not set in the config file.")

    def get_vol_ids_by_pvc_names(self, pvc_names):
        vol_ids = []
        custom = (r':.metadata.annotations."gluster\.kubernetes\.io\/'
                  'heketi-volume-id"')
        for pvc in pvc_names:
            pv = openshift_ops.get_pv_name_from_pvc(self.node, pvc)
            vol_id = openshift_ops.oc_get_custom_resource(
                self.node, 'pv', custom, pv)
            vol_ids.append(vol_id[0])
        return vol_ids

    def get_vol_names_from_vol_ids(self, vol_ids):
        vol_names = []
        for vol_id in vol_ids:
            vol_info = heketi_ops.heketi_volume_info(
                self.h_node, self.h_url, vol_id, json=True)
            vol_names.append(vol_info['name'])
        return vol_names

    def verify_gluster_peer_status(
            self, gluster_node, new_node_manage, new_node_storage,
            state='present'):

        # Verify gluster peer status
        peer_status = openshift_ops.cmd_run_on_gluster_pod_or_node(
            self.node, 'gluster peer status', gluster_node)
        found = (new_node_manage in peer_status
                 or new_node_storage in peer_status)
        if state == 'present':
            msg = ('Node %s did not get attached in gluster peer status %s'
                   % (new_node_manage, peer_status))
            self.assertTrue(found, msg)
        elif state == 'absent':
            msg = ('Node %s did not get deattached in gluster peer status %s'
                   % (new_node_manage, peer_status))
            self.assertFalse(found, msg)
        else:
            msg = "State %s is other than present, absent" % state
            raise AssertionError(msg)

    def verify_node_is_present_or_not_in_heketi(
            self, node_id, manage_hostname, storage_ip, state='present'):

        topology = heketi_ops.heketi_topology_info(
            self.h_node, self.h_url, json=True)
        if state == 'present':
            present = False
            for node in topology['clusters'][0]['nodes']:
                if node_id == node['id']:
                    self.assertEqual(
                        manage_hostname, node['hostnames']['manage'][0])
                    self.assertEqual(
                        storage_ip, node['hostnames']['storage'][0])
                    present = True
                    break
            self.assertTrue(present, 'Node %s not found in heketi' % node_id)

        elif state == 'absent':
            for node in topology['clusters'][0]['nodes']:
                self.assertNotEqual(
                    manage_hostname, node['hostnames']['manage'][0])
                self.assertNotEqual(
                    storage_ip, node['hostnames']['storage'][0])
                self.assertNotEqual(node_id, node['id'])
        else:
            msg = "State %s is other than present, absent" % state
            raise AssertionError(msg)

    def verify_gluster_server_present_in_heketi_vol_info_or_not(
            self, vol_ids, gluster_server, state='present'):

        # Verify gluster servers in vol info
        for vol_id in vol_ids:
            g_servers = heketi_ops.get_vol_file_servers_and_hosts(
                self.h_node, self.h_url, vol_id)
            g_servers = (g_servers['vol_servers'] + g_servers['vol_hosts'])
            if state == 'present':
                self.assertIn(gluster_server, g_servers)
            elif state == 'absent':
                self.assertNotIn(gluster_server, g_servers)
            else:
                msg = "State %s is other than present, absent" % state
                raise AssertionError(msg)

    def verify_volume_bricks_are_present_or_not_on_heketi_node(
            self, vol_ids, node_id, state='present'):

        for vol_id in vol_ids:
            vol_info = heketi_ops.heketi_volume_info(
                self.h_node, self.h_url, vol_id, json=True)
            bricks = vol_info['bricks']
            self.assertFalse((len(bricks) % 3))
            if state == 'present':
                found = False
                for brick in bricks:
                    if brick['node'] == node_id:
                        found = True
                        break
                self.assertTrue(
                    found, 'Bricks of vol %s does not present on node %s'
                    % (vol_id, node_id))
            elif state == 'absent':
                for brick in bricks:
                    self.assertNotEqual(
                        brick['node'], node_id, 'Bricks of vol %s is present '
                        'on node %s' % (vol_id, node_id))
            else:
                msg = "State %s is other than present, absent" % state
                raise AssertionError(msg)

    def get_ready_for_node_add(self, hostname):
        self.configure_node_to_run_gluster(hostname)

        h_nodes = heketi_ops.heketi_node_list(self.h_node, self.h_url)

        # Disable nodes except first two nodes
        for node_id in h_nodes[2:]:
            heketi_ops.heketi_node_disable(self.h_node, self.h_url, node_id)
            self.addCleanup(
                heketi_ops.heketi_node_enable, self.h_node, self.h_url,
                node_id)

    def add_device_on_heketi_node(self, node_id, device_name):

        # Add Devices on heketi node
        heketi_ops.heketi_device_add(
            self.h_node, self.h_url, device_name, node_id)

        # Get device id of newly added device
        node_info = heketi_ops.heketi_node_info(
            self.h_node, self.h_url, node_id, json=True)
        for device in node_info['devices']:
            if device['name'] == device_name:
                return device['id']
        raise AssertionError('Device %s did not found on node %s' % (
            device_name, node_id))

    def delete_node_and_devices_on_it(self, node_id):

        heketi_ops.heketi_node_disable(self.h_node, self.h_url, node_id)
        heketi_ops.heketi_node_remove(self.h_node, self.h_url, node_id)
        node_info = heketi_ops.heketi_node_info(
            self.h_node, self.h_url, node_id, json=True)
        for device in node_info['devices']:
            heketi_ops.heketi_device_delete(
                self.h_node, self.h_url, device['id'])
        heketi_ops.heketi_node_delete(self.h_node, self.h_url, node_id)

    @pytest.mark.tier0
    @ddt.data('remove', 'delete')
    def test_heketi_node_remove_or_delete(self, operation='delete'):
        """Test node remove and delete functionality of heketi and validate
        gluster peer status and heketi topology.
        """
        # Get node info to be added in heketi cluster
        new_node = self.get_node_to_be_added()
        new_node_manage = new_node['manage']
        new_node_storage = new_node['storage']

        self.get_ready_for_node_add(new_node_manage)

        h, h_node, h_url = heketi_ops, self.h_node, self.h_url

        # Get cluster id where node needs to be added.
        cluster_id = h.heketi_cluster_list(h_node, h_url, json=True)
        cluster_id = cluster_id['clusters'][0]

        h_nodes_list = h.heketi_node_list(h_node, h_url)

        node_needs_cleanup = False
        try:
            # Add new node
            h_new_node = h.heketi_node_add(
                h_node, h_url, 1, cluster_id, new_node_manage,
                new_node_storage, json=True)['id']
            node_needs_cleanup = True

            # Get hostname of first gluster node
            g_node = h.heketi_node_info(
                h_node, h_url, h_nodes_list[0],
                json=True)['hostnames']['manage'][0]

            # Verify gluster peer status
            self.verify_gluster_peer_status(
                g_node, new_node_manage, new_node_storage)

            # Add Devices on newly added node
            device_id = self.add_device_on_heketi_node(
                h_new_node, new_node['devices'][0])

            # Create PVC's and DC's
            vol_count = 5
            pvcs = self.create_and_wait_for_pvcs(pvc_amount=vol_count)
            dcs = self.create_dcs_with_pvc(pvcs)

            # Get vol id's
            vol_ids = self.get_vol_ids_by_pvc_names(pvcs)

            # Get bricks count on newly added node
            bricks = h.get_bricks_on_heketi_node(
                h_node, h_url, h_new_node)
            self.assertGreaterEqual(len(bricks), vol_count)

            # Enable the nodes back, which were disabled earlier
            for node_id in h_nodes_list[2:]:
                h.heketi_node_enable(h_node, h_url, node_id)

            if operation == 'remove':
                # Remove the node
                h.heketi_node_disable(h_node, h_url, h_new_node)
                h.heketi_node_remove(h_node, h_url, h_new_node)
                # Delete the device
                h.heketi_device_delete(h_node, h_url, device_id)
            elif operation == 'delete':
                # Remove and delete device
                h.heketi_device_disable(h_node, h_url, device_id)
                h.heketi_device_remove(h_node, h_url, device_id)
                h.heketi_device_delete(h_node, h_url, device_id)
                # Remove node
                h.heketi_node_disable(h_node, h_url, h_new_node)
                h.heketi_node_remove(h_node, h_url, h_new_node)
            else:
                msg = "Operation %s is other than remove, delete." % operation
                raise AssertionError(msg)

            # Delete Node
            h.heketi_node_delete(h_node, h_url, h_new_node)
            node_needs_cleanup = False

            # Verify node is deleted from heketi
            self.verify_node_is_present_or_not_in_heketi(
                h_new_node, new_node_manage, new_node_storage, state='absent')

            # Verify gluster peer status
            self.verify_gluster_peer_status(
                g_node, new_node_manage, new_node_storage, state='absent')

            # Verify gluster servers are not present in vol info
            self.verify_gluster_server_present_in_heketi_vol_info_or_not(
                vol_ids, new_node_storage, state='absent')

            # Verify vol bricks are not present on deleted nodes
            self.verify_volume_bricks_are_present_or_not_on_heketi_node(
                vol_ids, new_node_storage, state='absent')

            # Wait for heal to complete
            gluster_ops.wait_to_heal_complete(g_node=g_node)

            for _, pod in dcs.values():
                openshift_ops.wait_for_pod_be_ready(self.node, pod, timeout=1)
        finally:
            # Cleanup newly added Node
            if node_needs_cleanup:
                self.addCleanup(self.delete_node_and_devices_on_it, h_new_node)

            # Enable the nodes back, which were disabled earlier
            for node_id in h_nodes_list[2:]:
                self.addCleanup(h.heketi_node_enable, h_node, h_url, node_id)

    @pytest.mark.tier2
    @ddt.data(
        ("volume", "create"),
        ("volume", "delete"),
        ("volume", "expand"),
        ("blockvolume", "create"),
        ("blockvolume", "delete"),
    )
    @ddt.unpack
    def test_volume_operations_while_node_removal_is_running(
            self, vol_type, vol_operation):
        """Test volume operations like create, delete and expand while node
        removal is running parallely at backend.
        """
        # Get node info to be added in heketi cluster
        new_node = self.get_node_to_be_added()
        new_node_manage = new_node['manage']
        new_node_storage = new_node['storage']

        self.get_ready_for_node_add(new_node_manage)

        h, h_node, h_url = heketi_ops, self.h_node, self.h_url

        # Get cluster id where node needs to be added.
        cluster_id = h.heketi_cluster_list(h_node, h_url, json=True)
        cluster_id = cluster_id['clusters'][0]

        h_nodes_list = h.heketi_node_list(h_node, h_url)

        node_needs_cleanup = False
        try:
            # Add new node
            h_new_node = h.heketi_node_add(
                h_node, h_url, 1, cluster_id, new_node_manage,
                new_node_storage, json=True)['id']
            node_needs_cleanup = True

            # Get hostname of first gluster node
            g_node = h.heketi_node_info(
                h_node, h_url, h_nodes_list[0],
                json=True)['hostnames']['manage'][0]

            # Verify gluster peer status
            self.verify_gluster_peer_status(
                g_node, new_node_manage, new_node_storage)

            # Add Devices on newly added node
            device_id = self.add_device_on_heketi_node(
                h_new_node, new_node['devices'][0])

            # Create Volumes
            vol_count = 5
            for i in range(vol_count):
                vol_info = h.heketi_volume_create(
                    h_node, h_url, 1, clusters=cluster_id, json=True)
                self.addCleanup(
                    h.heketi_volume_delete, h_node, h_url, vol_info['id'])

            # Get bricks count on newly added node
            bricks = h.get_bricks_on_heketi_node(
                h_node, h_url, h_new_node)
            self.assertGreaterEqual(len(bricks), vol_count)

            # Enable the nodes back, which were disabled earlier
            for node_id in h_nodes_list[2:]:
                h.heketi_node_enable(h_node, h_url, node_id)

            # Disable the node
            h.heketi_node_disable(h_node, h_url, h_new_node)

            vol_count = 2 if vol_type == 'volume' else 5
            if vol_operation in ('delete', 'expand'):
                # Create file/block volumes to delete or expand while node
                # removal is running
                vol_list = []
                for i in range(vol_count):
                    vol_info = getattr(h, "heketi_%s_create" % vol_type)(
                        h_node, h_url, 1, clusters=cluster_id, json=True)
                    self.addCleanup(
                        getattr(h, "heketi_%s_delete" % vol_type),
                        h_node, h_url, vol_info['id'], raise_on_error=False)
                    vol_list.append(vol_info)

            # Remove the node and devices on it
            cmd = 'heketi-cli node remove %s -s %s --user %s --secret %s' % (
                h_new_node, h_url, self.heketi_cli_user, self.heketi_cli_key)
            proc = g.run_async(h_node, cmd)

            if vol_operation == 'create':
                # Create file/block volume while node removal is running
                for i in range(vol_count):
                    vol_info = getattr(h, "heketi_%s_create" % vol_type)(
                        h_node, h_url, 1, clusters=cluster_id, json=True)
                    self.addCleanup(
                        getattr(h, "heketi_%s_delete" % vol_type),
                        h_node, h_url, vol_info['id'])

            elif vol_operation == 'delete':
                # Delete file/block volume while node removal is running
                for vol in vol_list:
                    getattr(h, "heketi_%s_delete" % vol_type)(
                        h_node, h_url, vol['id'])

            elif vol_operation == 'expand':
                # Expand file volume while node removal is running
                for vol in vol_list:
                    vol_info = h.heketi_volume_expand(
                        h_node, h_url, vol['id'], '1', json=True)
                    self.assertEquals(2, vol_info['size'])

            else:
                msg = "Invalid vol_operation %s" % vol_operation
                raise AssertionError(msg)

            # Get the status of node removal command
            retcode, stdout, stderr = proc.async_communicate()
            msg = 'Failed to remove node %s from heketi with error %s' % (
                h_new_node, stderr)
            self.assertFalse(retcode, msg)

            h.heketi_device_delete(h_node, h_url, device_id)
            h.heketi_node_delete(h_node, h_url, h_new_node)
            node_needs_cleanup = False

            # Verify node is deleted from heketi
            self.verify_node_is_present_or_not_in_heketi(
                h_new_node, new_node_manage, new_node_storage, state='absent')

            # Verify gluster peer status
            self.verify_gluster_peer_status(
                g_node, new_node_manage, new_node_storage, state='absent')
        finally:
            # Cleanup newly added Node
            if node_needs_cleanup:
                self.addCleanup(self.delete_node_and_devices_on_it, h_new_node)

            # Enable the nodes back, which were disabled earlier
            for node_id in h_nodes_list[2:]:
                self.addCleanup(h.heketi_node_enable, h_node, h_url, node_id)
