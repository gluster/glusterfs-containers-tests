import pytest

from glustolibs.gluster import volume_ops
import six

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import heketi_version
from openshiftstoragelibs import node_ops
from openshiftstoragelibs import openshift_ops
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import waiter


class TestHeketiBrickEvict(BaseClass):
    """Test Heketi brick evict functionality."""

    def setUp(self):
        super(TestHeketiBrickEvict, self).setUp()

        version = heketi_version.get_heketi_version(self.heketi_client_node)
        if version < '9.0.0-14':
            self.skipTest(
                "heketi-client package {} does not support brick evict".format(
                    version.v_str))

        self.ocp_client = self.ocp_master_node[0]

        node_list = heketi_ops.heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)

        if len(node_list) > 3:
            return

        for node_id in node_list:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url, node_id,
                json=True)
            if len(node_info["devices"]) < 2:
                self.skipTest("does not have extra device/node to evict brick")

    @podcmd.GlustoPod()
    def _get_gluster_vol_info(self, file_vol):
        """Get Gluster vol info.

        Args:
            ocp_client (str): Node to execute OCP commands.
            file_vol (str): file volume name.

        Returns:
            dict: Info of the given gluster vol.
        """
        g_vol_info = volume_ops.get_volume_info(
            "auto_get_gluster_endpoint", file_vol)

        if not g_vol_info:
            raise AssertionError("Failed to get volume info for gluster "
                                 "volume {}".format(file_vol))
        if file_vol in g_vol_info:
            g_vol_info = g_vol_info.get(file_vol)
        return g_vol_info

    @pytest.mark.tier1
    def test_heketi_brick_evict(self):
        """Test brick evict basic functionality and verify it replace a brick
        properly
        """
        h_node, h_server = self.heketi_client_node, self.heketi_server_url

        size = 1
        vol_info_old = heketi_ops.heketi_volume_create(
            h_node, h_server, size, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete, h_node, h_server,
            vol_info_old['id'])
        heketi_ops.heketi_brick_evict(
            h_node, h_server, vol_info_old["bricks"][0]['id'])

        vol_info_new = heketi_ops.heketi_volume_info(
            h_node, h_server, vol_info_old['id'], json=True)

        bricks_old = set({brick['path'] for brick in vol_info_old["bricks"]})
        bricks_new = set({brick['path'] for brick in vol_info_new["bricks"]})
        self.assertEqual(
            len(bricks_new - bricks_old), 1,
            "Brick was not replaced with brick evict for vol \n {}".format(
                vol_info_new))

        gvol_info = self._get_gluster_vol_info(vol_info_new['name'])
        gbricks = set(
            {brick['name'].split(":")[1]
                for brick in gvol_info["bricks"]["brick"]})
        self.assertEqual(
            bricks_new, gbricks, "gluster vol info and heketi vol info "
            "mismatched after brick evict {} \n {}".format(
                gvol_info, vol_info_new))

    def _wait_for_gluster_pod_after_node_reboot(self, node_hostname):
        """Wait for glusterfs pod to be ready after node reboot"""
        openshift_ops.wait_for_ocp_node_be_ready(
            self.ocp_client, node_hostname)
        gluster_pod = openshift_ops.get_gluster_pod_name_for_specific_node(
            self.ocp_client, node_hostname)
        openshift_ops.wait_for_pod_be_ready(self.ocp_client, gluster_pod)
        services = (
            ("glusterd", "running"), ("gluster-blockd", "running"),
            ("tcmu-runner", "running"), ("gluster-block-target", "exited"))
        for service, state in services:
            openshift_ops.check_service_status_on_pod(
                self.ocp_client, gluster_pod, service, "active", state)

    def _power_off_node_and_wait_node_to_be_not_ready(self, hostname):
        # Bring down the glusterfs node
        vm_name = node_ops.find_vm_name_by_ip_or_hostname(hostname)
        self.addCleanup(
            self._wait_for_gluster_pod_after_node_reboot, hostname)
        self.addCleanup(node_ops.power_on_vm_by_name, vm_name)
        node_ops.power_off_vm_by_name(vm_name)

        # Wait glusterfs node to become NotReady
        custom = r'":.status.conditions[?(@.type==\"Ready\")]".status'
        for w in waiter.Waiter(300, 20):
            status = openshift_ops.oc_get_custom_resource(
                self.ocp_client, 'node', custom, hostname)
            if status[0] in ['False', 'Unknown']:
                break
        if w.expired:
            raise exceptions.ExecutionError(
                "Failed to bring down node {}".format(hostname))

    @pytest.mark.tier4b
    def test_brick_evict_on_three_node_with_one_down(self):
        """Test brick evict basic functionality and verify brick evict
        will fail after node down if nodes are three"""

        h_node, h_server = self.heketi_client_node, self.heketi_server_url

        # Disable node if more than 3
        node_list = heketi_ops.heketi_node_list(h_node, h_server)
        if len(node_list) > 3:
            for node_id in node_list[3:]:
                heketi_ops.heketi_node_disable(h_node, h_server, node_id)
                self.addCleanup(
                    heketi_ops.heketi_node_enable, h_node, h_server, node_id)

        # Create heketi volume
        vol_info = heketi_ops.heketi_volume_create(
            h_node, h_server, 1, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete,
            h_node, h_server, vol_info.get('id'))

        # Get node on which heketi pod is scheduled
        heketi_pod = openshift_ops.get_pod_name_from_dc(
            self.ocp_client, self.heketi_dc_name)
        heketi_node = openshift_ops.oc_get_custom_resource(
            self.ocp_client, 'pod', '.:spec.nodeName', heketi_pod)[0]

        # Get list of hostname from node id
        host_list = []
        for node_id in node_list[3:]:
            node_info = heketi_ops.heketi_node_info(
                h_node, h_server, node_id, json=True)
            host_list.append(node_info.get('hostnames').get('manage')[0])

        # Get brick id and glusterfs node which is not heketi node
        for node in vol_info.get('bricks', {}):
            node_info = heketi_ops.heketi_node_info(
                h_node, h_server, node.get('node'), json=True)
            hostname = node_info.get('hostnames').get('manage')[0]
            if (hostname != heketi_node) and (hostname not in host_list):
                brick_id = node.get('id')
                break

        self._power_off_node_and_wait_node_to_be_not_ready(hostname)

        # Perform brick evict operation
        try:
            heketi_ops.heketi_brick_evict(h_node, h_server, brick_id)
        except AssertionError as e:
            if ('No Replacement was found' not in six.text_type(e)):
                raise

    @pytest.mark.tier4b
    def test_brick_evict_on_more_than_three_node_with_one_down(self):
        """Test brick evict basic functionality and verify brick evict
        will success after one node down out of more than three nodes"""

        h_node, h_server = self.heketi_client_node, self.heketi_server_url

        # Create heketi volume
        vol_info = heketi_ops.heketi_volume_create(
            h_node, h_server, 1, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete,
            h_node, h_server, vol_info.get('id'))

        # Get node on which heketi pod is scheduled
        heketi_pod = openshift_ops.get_pod_name_from_dc(
            self.ocp_client, self.heketi_dc_name)
        heketi_node = openshift_ops.oc_get_custom_resource(
            self.ocp_client, 'pod', '.:spec.nodeName', heketi_pod)[0]

        # Get brick id and glusterfs node which is not heketi node
        for node in vol_info.get('bricks', {}):
            node_info = heketi_ops.heketi_node_info(
                h_node, h_server, node.get('node'), json=True)
            hostname = node_info.get('hostnames').get('manage')[0]
            if hostname != heketi_node:
                brick_id = node.get('id')
                break

        self._power_off_node_and_wait_node_to_be_not_ready(hostname)

        # Perform brick evict operation
        heketi_ops.heketi_brick_evict(h_node, h_server, brick_id)

        # Get volume info after brick evict operation
        vol_info_new = heketi_ops.heketi_volume_info(
            h_node, h_server, vol_info.get('id'), json=True)

        # Get previous and new bricks from volume
        bricks_old = set(
            {brick.get('path') for brick in vol_info.get("bricks")})
        bricks_new = set(
            {brick.get('path') for brick in vol_info_new.get("bricks")})
        self.assertEqual(
            len(bricks_new - bricks_old), 1,
            "Brick was not replaced with brick evict for vol \n {}".format(
                vol_info_new))

        # Get gluster volume info
        g_vol_info = self._get_gluster_vol_info(vol_info_new.get('name'))

        # Validate bricks on gluster volume and heketi volume
        g_bricks = set(
            {brick.get('name').split(":")[1]
                for brick in g_vol_info.get("bricks", {}).get("brick")})
        self.assertEqual(
            bricks_new, g_bricks, "gluster vol info and heketi vol info "
            "mismatched after brick evict {} \n {}".format(
                g_bricks, g_vol_info))
