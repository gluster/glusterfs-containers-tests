import ddt
import pytest
from glusto.core import Glusto as g
from glustolibs.gluster import block_libs

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import command
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import node_ops
from openshiftstoragelibs import openshift_ops
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

    def _cleanup_heketi_volumes(self, existing_volumes):
        """Cleanup created BHV and BV"""

        volumes = heketi_ops.heketi_volume_list(
            self.h_node, self.h_server, json=True).get("volumes")
        new_volumes = list(set(volumes) - set(existing_volumes))
        for volume in new_volumes:
            h_vol_info = heketi_ops.heketi_volume_info(
                self.h_node, self.h_server, volume, json=True)
            if h_vol_info.get("block"):
                for block_vol in (
                        h_vol_info.get("blockinfo").get("blockvolume")):
                    heketi_ops.heketi_blockvolume_delete(
                        self.h_node, self.h_server, block_vol,
                        raise_on_error=False)
            heketi_ops.heketi_volume_delete(
                self.h_node, self.h_server, volume, raise_on_error=False)

    @pytest.mark.tier4
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

    def _get_space_use_percent_in_app_pod(self, pod_name):
        """Check if IO's are running in the app pod"""

        use_percent = []
        cmd = "oc exec {} -- df -h /mnt | tail -1"

        # Run 10 times to track the percentage used
        for _ in range(10):
            out = command.cmd_run(cmd.format(pod_name), self.node).split()[3]
            self.assertTrue(
                out, "Failed to fetch mount point details from the pod "
                "{}".format(pod_name))
            use_percent.append(out[:-1])
        return use_percent

    def _create_app_pod_and_verify_pvs(self):
        """Create block volume with app pod and verify IO's. Compare path,
        uuid, vg_name.
        """
        pvc_size, pvc_amount = 2, 1

        # Space to use for io's in KB
        space_to_use = 104857600

        # Create block volumes
        pvc_name = self.create_and_wait_for_pvcs(
            pvc_size=pvc_size, pvc_amount=pvc_amount)

        #  Create dcs and app pods with I/O on it
        dc_name = self.create_dcs_with_pvc(pvc_name, space_to_use=space_to_use)

        # Pod names list
        pod_name = [pod_name for _, pod_name in list(dc_name.values())][0]
        self.assertTrue(
            pod_name, "Failed to get the pod name from {}".format(dc_name))

        # Fetch dc list
        dc_name = [pod for pod, _ in list(dc_name.values())][0]

        pvs_info_before = openshift_storage_libs.get_pvs_info(
            self.node, self.node_ip, self.devices_list, raise_on_error=False)

        # Check if IO's are running
        use_percent_before = self._get_space_use_percent_in_app_pod(pod_name)

        # Compare volumes
        self.validate_file_volumes_count(
            self.h_node, self.h_server, self.node_ip)
        self.detach_and_attach_vmdk(
            self.vm_name, self.node_hostname, self.devices_list)

        # Check if IO's are running
        use_percent_after = self._get_space_use_percent_in_app_pod(pod_name)
        self.assertNotEqual(
            use_percent_before, use_percent_after,
            "Failed to execute IO's in the app pod {}".format(
                pod_name))

        pvs_info_after = openshift_storage_libs.get_pvs_info(
            self.node, self.node_ip, self.devices_list, raise_on_error=False)

        # Compare pvs info before and after
        for (path, uuid, vg_name), (_path, _uuid, _vg_name) in zip(
                pvs_info_before[:-1], pvs_info_after[1:]):
            self.assertEqual(
                uuid, _uuid, "pv_uuid check failed. Expected: {},"
                " Actual: {}".format(uuid, _uuid))
            self.assertEqual(
                vg_name, _vg_name, "vg_name check failed. Expected: {},"
                " Actual:{}".format(vg_name, _vg_name))
        return pod_name, dc_name, use_percent_before

    @pytest.mark.tier4
    @podcmd.GlustoPod()
    def test_dev_path_mapping_app_pod_with_block_volume_reboot(self):
        """Validate dev path mapping for app pods with block volume after reboot
        """
        # Create block volume with app pod and verify IO's
        # and Compare path, uuid, vg_name
        pod_name, dc_name, use_percent = self._create_app_pod_and_verify_pvs()

        # Delete app pods
        openshift_ops.oc_delete(self.node, 'pod', pod_name)
        openshift_ops.wait_for_resource_absence(self.node, 'pod', pod_name)

        # Wait for the new app pod to come up
        self.assertTrue(
            dc_name, "Failed to get the dc name from {}".format(dc_name))
        pod_name = openshift_ops.get_pod_name_from_dc(self.node, dc_name)
        openshift_ops.wait_for_pod_be_ready(self.node, pod_name)

        # Check if IO's are running after respin of app pod
        use_percent_after = self._get_space_use_percent_in_app_pod(pod_name)
        self.assertNotEqual(
            use_percent, use_percent_after,
            "Failed to execute IO's in the app pod {} after respin".format(
                pod_name))

    @pytest.mark.tier4
    @podcmd.GlustoPod()
    def test_dev_path_block_volume_delete(self):
        """Validate device path name changes the deletion of
           already existing file volumes
        """

        pvc_size, pvc_amount = 2, 5
        pvc_names, gluster_block_list, vol_details = [], [], []

        # Fetch BHV list
        h_bhv_list_before = heketi_ops.get_block_hosting_volume_list(
            self.h_node, self.h_server).keys()

        # Create storage class
        sc_name = self.create_storage_class()

        # Delete created BHV and BV as cleanup during failures
        self.addCleanup(self._cleanup_heketi_volumes, h_bhv_list_before)

        # Create PVC's
        for i in range(0, pvc_amount):
            pvc_name = openshift_ops.oc_create_pvc(
                self.node, sc_name, pvc_size=pvc_size)
            pvc_names.append(pvc_name)
            self.addCleanup(
                openshift_ops.wait_for_resource_absence,
                self.node, 'pvc', pvc_name)
            self.addCleanup(
                openshift_ops.oc_delete, self.node, 'pvc', pvc_name,
                raise_on_absence=False)

        # Wait for PVC's to be bound
        openshift_ops.wait_for_pvc_be_bound(self.node, pvc_names)

        # Get volume name list
        for pvc_name in pvc_names:
            pv_name = openshift_ops.get_pv_name_from_pvc(self.node, pvc_name)
            volume_name = openshift_ops.get_vol_names_from_pv(
                self.node, pv_name, vol_type='block')
            vol_details.append(volume_name)

        # Get BHV list after BV creation
        h_bhv_list_after = heketi_ops.get_block_hosting_volume_list(
            self.h_node, self.h_server).keys()
        self.assertTrue(h_bhv_list_after, "Failed to get the BHV list")

        # Validate BV's count
        self.validate_block_volumes_count(
            self.h_node, self.h_server, self.node_ip)

        # Collect pvs info and detach disks and collect pvs info
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

        # Delete created PVC's
        for pvc_name in pvc_names:
            openshift_ops.oc_delete(self.node, 'pvc', pvc_name)

        # Wait for pvc to get deleted
        openshift_ops.wait_for_resources_absence(self.node, 'pvc', pvc_names)

        # Get existing BHV list
        for bhv_name in h_bhv_list_after:
            b_list = block_libs.get_block_list(self.node_ip, volname=bhv_name)
            self.assertIsNotNone(
                gluster_block_list, "Failed to get gluster block list")
            gluster_block_list.append(b_list)

        # Get list of block volumes using heketi
        h_blockvol_list = heketi_ops.heketi_blockvolume_list(
            self.h_node, self.h_server, json=True)

        # Validate volumes created are not present
        for vol in vol_details:
            self.assertNotIn(
                vol, gluster_block_list,
                "Failed to delete volume {}".format(vol))
            self.assertNotIn(
                vol, h_blockvol_list['blockvolumes'],
                "Failed to delete blockvolume '{}'".format(vol))
