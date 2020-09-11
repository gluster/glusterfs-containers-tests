import ddt
import pytest
from glusto.core import Glusto as g
from glustolibs.gluster import volume_ops

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import command
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import node_ops
from openshiftstoragelibs import openshift_ops
from openshiftstoragelibs import openshift_storage_libs
from openshiftstoragelibs import podcmd


@ddt.ddt
class TestDevPathMapping(baseclass.BaseClass):
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
    def test_dev_path_file_volume_create(self):
        """Validate dev path mapping for file volumes"""

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

        # Create file volumes
        pvcs = self.create_and_wait_for_pvcs(
            pvc_size=pvc_size, pvc_amount=pvc_amount)
        self.create_dcs_with_pvc(pvcs)
        self.validate_file_volumes_count(
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
        """Create file volume with app pod and verify IO's. Compare path,
        uuid, vg_name.
        """
        pvc_size, pvc_amount = 2, 1

        # Space to use for io's in KB
        space_to_use = 104857600

        # Create file volumes
        pvc_name = self.create_and_wait_for_pvcs(
            pvc_size=pvc_size, pvc_amount=pvc_amount)

        #  Create dcs and app pods with I/O on it
        dc_name = self.create_dcs_with_pvc(pvc_name, space_to_use=space_to_use)

        # Pod names list
        pod_name = [pod_name for _, pod_name in list(dc_name.values())][0]
        self.assertTrue(
            pod_name, "Failed to get the pod name from {}".format(dc_name))
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

    @pytest.mark.tier2
    @podcmd.GlustoPod()
    def test_dev_path_mapping_app_pod_with_file_volume_reboot(self):
        """Validate dev path mapping for app pods with file volume after reboot
        """
        # Create file volume with app pod and verify IO's
        # and Compare path, uuid, vg_name
        pod_name, dc_name, use_percent = self._create_app_pod_and_verify_pvs()

        # Delete app pods
        openshift_ops.oc_delete(self.node, 'pod', pod_name)
        openshift_ops.wait_for_resource_absence(self.node, 'pod', pod_name)

        # Wait for the new app pod to come up
        dc_name = [pod for pod, _ in list(dc_name.values())][0]
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

    @pytest.mark.tier2
    @podcmd.GlustoPod()
    def test_dev_path_file_volume_delete(self):
        """Validate device path name changes the deletion of
           already existing file volumes
        """

        pvc_size, pvc_amount = 2, 5
        vol_details, pvc_names = [], []

        # Create PVC's
        sc_name = self.create_storage_class()
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
        openshift_ops.wait_for_pvcs_be_bound(self.node, pvc_names)

        # Get Volumes name and validate volumes count
        for pvc_name in pvc_names:
            pv_name = openshift_ops.get_pv_name_from_pvc(self.node, pvc_name)
            volume_name = openshift_ops.get_vol_names_from_pv(
                self.node, pv_name)
            vol_details.append(volume_name)

        # Verify file volumes count
        self.validate_file_volumes_count(
            self.h_node, self.h_server, self.node_ip)

        # Collect pvs info and detach disks and get pvs info
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

        # Wait for resource absence and get volume list
        openshift_ops.wait_for_resources_absence(self.node, 'pvc', pvc_names)
        vol_list = volume_ops.get_volume_list(self.node_ip)
        self.assertIsNotNone(vol_list, "Failed to get volumes list")

        # Validate volumes created are not present
        for vol in vol_details:
            self.assertNotIn(
                vol, vol_list, "Failed to delete volume {}".format(vol))

    def _heketi_pod_delete_cleanup(self):
        """Cleanup for deletion of heketi pod using force delete"""
        try:
            # Fetch heketi pod after delete
            pod_name = openshift_ops.get_pod_name_from_dc(
                self.node, self.heketi_dc_name)
            openshift_ops.wait_for_pod_be_ready(
                self.node, pod_name, timeout=1)
        except exceptions.ExecutionError:

            # Force delete and wait for new pod to come up
            openshift_ops.oc_delete(
                self.node, 'pod', pod_name, is_force=True)
            openshift_ops.wait_for_resource_absence(
                self.node, 'pod', pod_name)
            new_pod_name = openshift_ops.get_pod_name_from_dc(
                self.node, self.heketi_dc_name)
            openshift_ops.wait_for_pod_be_ready(self.node, new_pod_name)

    @pytest.mark.tier2
    @podcmd.GlustoPod()
    def test_dev_path_mapping_heketi_pod_reboot(self):
        """Validate dev path mapping for heketi pod reboot
        """
        self.node = self.ocp_master_node[0]
        h_node, h_url = self.heketi_client_node, self.heketi_server_url

        # Create file volume with app pod and verify IO's
        # and Compare path, uuid, vg_name
        pod_name, dc_name, use_percent = self._create_app_pod_and_verify_pvs()

        # Fetch heketi-pod name
        heketi_pod_name = openshift_ops.get_pod_name_from_dc(
            self.node, self.heketi_dc_name)

        # Respin heketi-pod (it restarts the pod)
        openshift_ops.oc_delete(
            self.node, "pod", heketi_pod_name,
            collect_logs=self.heketi_logs_before_delete)
        self.addCleanup(self._heketi_pod_delete_cleanup)
        openshift_ops.wait_for_resource_absence(
            self.node, "pod", heketi_pod_name)

        # Fetch new heketi-pod name
        heketi_pod_name = openshift_ops.get_pod_name_from_dc(
            self.node, self.heketi_dc_name)
        openshift_ops.wait_for_pod_be_ready(self.node, heketi_pod_name)

        # Check heketi server is running
        self.assertTrue(
            heketi_ops.hello_heketi(h_node, h_url),
            "Heketi server {} is not alive".format(h_url))

        # Check if IO's are running after respin of heketi pod
        use_percent_after = self._get_space_use_percent_in_app_pod(pod_name)
        self.assertNotEqual(
            use_percent, use_percent_after,
            "Failed to execute IO's in the app pod {} after respin".format(
                pod_name))

    def _get_gluster_pod(self):
        """Fetch gluster pods"""
        # Fetch one gluster pod from its nodes
        g_hostname = list(self.gluster_servers_info.values())[0].get('manage')
        self.assertTrue(g_hostname, "Failed to fetch gluster hostname")
        g_pod = openshift_ops.get_gluster_pod_name_for_specific_node(
            self.node, g_hostname)
        return g_pod

    def _guster_pod_delete_cleanup(self):
        """Cleanup for deletion of gluster pod using force delete"""
        try:
            # Fetch gluster pod after delete
            pod_name = self._get_gluster_pod()

            # Check if gluster pod name is ready state
            openshift_ops.wait_for_pod_be_ready(self.node, pod_name, timeout=1)
        except exceptions.ExecutionError:
            # Force delete and wait for new pod to come up
            openshift_ops.oc_delete(self.node, 'pod', pod_name, is_force=True)
            openshift_ops.wait_for_resource_absence(self.node, 'pod', pod_name)

            # Fetch gluster pod after force delete
            g_new_pod = self._get_gluster_pod()
            openshift_ops.wait_for_pod_be_ready(self.node, g_new_pod)

    @pytest.mark.tier2
    @podcmd.GlustoPod()
    def test_dev_path_mapping_gluster_pod_reboot(self):
        """Validate dev path mapping for app pods with file volume after reboot
        """
        # Skip the tc for independent mode
        if not self.is_containerized_gluster():
            self.skipTest("Skip TC as it is not supported in independent mode")

        # Create file volume with app pod and verify IO's
        # and Compare path, uuid, vg_name
        pod_name, dc_name, use_percent = self._create_app_pod_and_verify_pvs()

        # Fetch the gluster pod name from node
        g_pod = self._get_gluster_pod()

        # Respin a gluster pod
        openshift_ops.oc_delete(self.node, "pod", g_pod)
        self.addCleanup(self._guster_pod_delete_cleanup)

        # Wait for pod to get absent
        openshift_ops.wait_for_resource_absence(self.node, "pod", g_pod)

        # Fetch gluster pod after delete
        g_pod = self._get_gluster_pod()
        openshift_ops.wait_for_pod_be_ready(self.node, g_pod)

        # Check if IO's are running after respin of gluster pod
        use_percent_after = self._get_space_use_percent_in_app_pod(pod_name)
        self.assertNotEqual(
            use_percent, use_percent_after,
            "Failed to execute IO's in the app pod {} after respin".format(
                pod_name))
