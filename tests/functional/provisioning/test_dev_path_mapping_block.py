import ddt
import pytest
from glusto.core import Glusto as g

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import node_ops
from openshiftstoragelibs import openshift_storage_libs
from openshiftstoragelibs import podcmd


@ddt.ddt
class TestDevPathMapping(baseclass.GlusterBlockBaseClass):
    '''Class that contain dev path mapping test cases for
       gluster file & block volumes
    '''

    def setUp(self):
        super(TestDevPathMapping, self).setUp()
        self.node = self.ocp_master_node[0]
        self.h_node, self.h_server = (
            self.heketi_client_node, self.heketi_server_url)
        h_nodes_list = heketi_ops.heketi_node_list(self.h_node, self.h_server)
        h_node_count = len(h_nodes_list)
        if h_node_count < 3:
            self.skipTest(
                "At least 3 nodes are required, found {}".format(
                    h_node_count))

        # Disable 4th and other nodes
        for node_id in h_nodes_list[3:]:
            heketi_ops.heketi_node_disable(
                self.h_node, self.h_server, node_id)
            self.addCleanup(
                heketi_ops.heketi_node_enable,
                self.h_node, self.h_server, node_id)

        h_info = heketi_ops.heketi_node_info(
            self.h_node, self.h_server, h_nodes_list[0], json=True)
        self.assertTrue(
            h_info, "Failed to get the heketi node info for node id"
            " {}".format(h_nodes_list[0]))

        self.node_ip = h_info['hostnames']['storage'][0]
        self.node_hostname = h_info["hostnames"]["manage"][0]
        self.vm_name = node_ops.find_vm_name_by_ip_or_hostname(
            self.node_hostname)
        self.devices_list = [device['name'] for device in h_info["devices"]]

        # Get list of additional devices for one of the Gluster nodes
        for gluster_server in list(g.config["gluster_servers"].values()):
            if gluster_server['storage'] == self.node_ip:
                additional_device = gluster_server.get("additional_devices")
                if additional_device:
                    self.devices_list.extend(additional_device)

        # sort the devices list
        self.devices_list.sort()

    @pytest.mark.tier2
    @podcmd.GlustoPod()
    def test_dev_path_block_volume_create(self):
        """Validate dev path mapping for block volumes"""

        pvc_size, pvc_amount = 2, 5
        pvs_info_before = openshift_storage_libs.get_pvs_info(
            self.node, self.node_ip, self.devices_list, raise_on_error=False)
        self.detach_and_attach_vmdk(
            self.vm_name, self.node_hostname, self.devices_list)
        pvs_info_after = openshift_storage_libs.get_pvs_info(
            self.node, self.node_ip, self.devices_list, raise_on_error=False)

        # Compare pvs info before and after
        for (path, uuid, vg_name), (_path, _uuid, _vg_name) in zip(
                pvs_info_before[:-1], pvs_info_after[1:]):
            self.assertEqual(
                uuid, _uuid, "pv_uuid check failed. Expected:{},"
                "Actual: {}".format(uuid, _uuid))
            self.assertEqual(
                vg_name, _vg_name, "vg_name check failed. Expected:"
                "{}, Actual:{}".format(vg_name, _vg_name))

        # Create block volumes
        pvcs = self.create_and_wait_for_pvcs(
            pvc_size=pvc_size, pvc_amount=pvc_amount)
        self.create_dcs_with_pvc(pvcs)
        self.validate_block_volumes_count(
            self.h_node, self.h_server, self.node_ip)
