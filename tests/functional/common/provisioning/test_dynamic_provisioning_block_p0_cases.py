from unittest import skip

from cnslibs.cns.cns_baseclass import GlusterBlockBaseClass
from cnslibs.common.exceptions import ExecutionError
from cnslibs.common.openshift_ops import (
    get_gluster_pod_names_by_pvc_name,
    get_pod_name_from_dc,
    get_pv_name_from_pvc,
    oc_create_app_dc_with_io,
    oc_create_pvc,
    oc_delete,
    oc_get_custom_resource,
    oc_rsh,
    scale_dc_pod_amount_and_wait,
    verify_pvc_status_is_bound,
    wait_for_pod_be_ready,
    wait_for_resource_absence
    )
from cnslibs.common.heketi_ops import (
    heketi_blockvolume_delete,
    heketi_blockvolume_list
    )
from cnslibs.common.waiter import Waiter
from glusto.core import Glusto as g


class TestDynamicProvisioningBlockP0(GlusterBlockBaseClass):
    '''
     Class that contain P0 dynamic provisioning test cases
     for block volume
    '''

    def setUp(self):
        super(TestDynamicProvisioningBlockP0, self).setUp()
        self.node = self.ocp_master_node[0]

    def dynamic_provisioning_glusterblock(
            self, set_hacount, create_vol_name_prefix=False):
        datafile_path = '/mnt/fake_file_for_%s' % self.id()

        # Create DC with attached PVC
        sc_name = self.create_storage_class(
            set_hacount=set_hacount,
            create_vol_name_prefix=create_vol_name_prefix)
        pvc_name = self.create_and_wait_for_pvc(
            pvc_name_prefix='autotest-block', sc_name=sc_name)
        dc_name, pod_name = self.create_dc_with_pvc(pvc_name)

        # Check that we can write data
        for cmd in ("dd if=/dev/urandom of=%s bs=1K count=100",
                    "ls -lrt %s",
                    "rm -rf %s"):
            cmd = cmd % datafile_path
            ret, out, err = oc_rsh(self.node, pod_name, cmd)
            self.assertEqual(
                ret, 0,
                "Failed to execute '%s' command on '%s'." % (cmd, self.node))

    def test_dynamic_provisioning_glusterblock_hacount_true(self):
        """Validate dynamic provisioning for glusterblock
        """
        self.dynamic_provisioning_glusterblock(set_hacount=True)

    def test_dynamic_provisioning_glusterblock_hacount_false(self):
        """Validate storage-class mandatory parameters for block
        """
        self.dynamic_provisioning_glusterblock(set_hacount=False)

    def test_dynamic_provisioning_glusterblock_heketipod_failure(self):
        """Validate PVC with glusterblock creation when heketi pod is down"""
        datafile_path = '/mnt/fake_file_for_%s' % self.id()

        # Create DC with attached PVC
        sc_name = self.create_storage_class()
        app_1_pvc_name = self.create_and_wait_for_pvc(
            pvc_name_prefix='autotest-block', sc_name=sc_name)
        app_1_dc_name, app_1_pod_name = self.create_dc_with_pvc(app_1_pvc_name)

        # Write test data
        write_data_cmd = (
            "dd if=/dev/urandom of=%s bs=1K count=100" % datafile_path)
        ret, out, err = oc_rsh(self.node, app_1_pod_name, write_data_cmd)
        self.assertEqual(
            ret, 0,
            "Failed to execute command %s on %s" % (write_data_cmd, self.node))

        # Remove Heketi pod
        heketi_down_cmd = "oc scale --replicas=0 dc/%s --namespace %s" % (
            self.heketi_dc_name, self.storage_project_name)
        heketi_up_cmd = "oc scale --replicas=1 dc/%s --namespace %s" % (
            self.heketi_dc_name, self.storage_project_name)
        self.addCleanup(self.cmd_run, heketi_up_cmd)
        heketi_pod_name = get_pod_name_from_dc(
            self.node, self.heketi_dc_name, timeout=10, wait_step=3)
        self.cmd_run(heketi_down_cmd)
        wait_for_resource_absence(self.node, 'pod', heketi_pod_name)

        # Create second PVC
        app_2_pvc_name = oc_create_pvc(
            self.node, pvc_name_prefix='autotest-block2', sc_name=sc_name
        )
        self.addCleanup(
            wait_for_resource_absence, self.node, 'pvc', app_2_pvc_name)
        self.addCleanup(
            oc_delete, self.node, 'pvc', app_2_pvc_name
        )

        # Create second app POD
        app_2_dc_name = oc_create_app_dc_with_io(self.node, app_2_pvc_name)
        self.addCleanup(oc_delete, self.node, 'dc', app_2_dc_name)
        self.addCleanup(
            scale_dc_pod_amount_and_wait, self.node, app_2_dc_name, 0)
        app_2_pod_name = get_pod_name_from_dc(self.node, app_2_dc_name)

        # Bring Heketi pod back
        self.cmd_run(heketi_up_cmd)

        # Wait for Heketi POD be up and running
        new_heketi_pod_name = get_pod_name_from_dc(
            self.node, self.heketi_dc_name, timeout=10, wait_step=2)
        wait_for_pod_be_ready(
            self.node, new_heketi_pod_name, wait_step=5, timeout=120)

        # Wait for second PVC and app POD be ready
        verify_pvc_status_is_bound(self.node, app_2_pvc_name)
        wait_for_pod_be_ready(
            self.node, app_2_pod_name, timeout=150, wait_step=3)

        # Verify that we are able to write data
        ret, out, err = oc_rsh(self.node, app_2_pod_name, write_data_cmd)
        self.assertEqual(
            ret, 0,
            "Failed to execute command %s on %s" % (write_data_cmd, self.node))

    @skip("Blocked by BZ-1632873")
    def test_dynamic_provisioning_glusterblock_glusterpod_failure(self):
        """Create glusterblock PVC when gluster pod is down"""
        datafile_path = '/mnt/fake_file_for_%s' % self.id()

        # Create DC with attached PVC
        sc_name = self.create_storage_class()
        pvc_name = self.create_and_wait_for_pvc(
            pvc_name_prefix='autotest-block', sc_name=sc_name)
        dc_name, pod_name = self.create_dc_with_pvc(pvc_name)

        # Run IO in background
        io_cmd = "oc rsh %s dd if=/dev/urandom of=%s bs=1000K count=900" % (
            pod_name, datafile_path)
        async_io = g.run_async(self.node, io_cmd, "root")

        # Pick up one of the hosts which stores PV brick (4+ nodes case)
        gluster_pod_data = get_gluster_pod_names_by_pvc_name(
            self.node, pvc_name)[0]

        # Delete glusterfs POD from chosen host and wait for spawn of new one
        oc_delete(self.node, 'pod', gluster_pod_data["pod_name"])
        cmd = ("oc get pods -o wide | grep glusterfs | grep %s | "
               "grep -v Terminating | awk '{print $1}'") % (
                   gluster_pod_data["host_name"])
        for w in Waiter(600, 30):
            out = self.cmd_run(cmd)
            new_gluster_pod_name = out.strip().split("\n")[0].strip()
            if not new_gluster_pod_name:
                continue
            else:
                break
        if w.expired:
            error_msg = "exceeded timeout, new gluster pod not created"
            g.log.error(error_msg)
            raise ExecutionError(error_msg)
        new_gluster_pod_name = out.strip().split("\n")[0].strip()
        g.log.info("new gluster pod name is %s" % new_gluster_pod_name)
        wait_for_pod_be_ready(self.node, new_gluster_pod_name)

        # Check that async IO was not interrupted
        ret, out, err = async_io.async_communicate()
        self.assertEqual(ret, 0, "IO %s failed on %s" % (io_cmd, self.node))

    def test_glusterblock_logs_presence_verification(self):
        """Validate presence of glusterblock provisioner POD and it's status"""
        gb_prov_cmd = ("oc get pods --all-namespaces "
                       "-l glusterfs=block-%s-provisioner-pod "
                       "-o=custom-columns=:.metadata.name,:.status.phase" % (
                           self.storage_project_name))
        ret, out, err = g.run(self.ocp_client[0], gb_prov_cmd, "root")

        self.assertEqual(ret, 0, "Failed to get Glusterblock provisioner POD.")
        gb_prov_name, gb_prov_status = out.split()
        self.assertEqual(gb_prov_status, 'Running')

        # Create Secret, SC and PVC
        self.create_storage_class()
        self.create_and_wait_for_pvc()

        # Get list of Gluster PODs
        g_pod_list_cmd = (
            "oc get pods --all-namespaces -l glusterfs-node=pod "
            "-o=custom-columns=:.metadata.name,:.metadata.namespace")
        ret, out, err = g.run(self.ocp_client[0], g_pod_list_cmd, "root")

        self.assertEqual(ret, 0, "Failed to get list of Gluster PODs.")
        g_pod_data_list = out.split()
        g_pods_namespace = g_pod_data_list[1]
        g_pods = [pod for pod in out.split()[::2]]
        logs = ("gluster-block-configshell", "gluster-blockd")

        # Verify presence and not emptiness of logs on Gluster PODs
        self.assertGreater(len(g_pods), 0, "We expect some PODs:\n %s" % out)
        for g_pod in g_pods:
            for log in logs:
                cmd = (
                    "oc exec -n %s %s -- "
                    "tail -n 5 /var/log/glusterfs/gluster-block/%s.log" % (
                        g_pods_namespace, g_pod, log))
                ret, out, err = g.run(self.ocp_client[0], cmd, "root")

                self.assertFalse(err, "Error output is not empty: \n%s" % err)
                self.assertEqual(ret, 0, "Failed to exec '%s' command." % cmd)
                self.assertTrue(out, "Command '%s' output is empty." % cmd)

    def test_dynamic_provisioning_glusterblock_heketidown_pvc_delete(self):
        """Validate PVC deletion when heketi is down"""

        # Create Secret, SC and PVCs
        self.create_storage_class()
        self.pvc_name_list = self.create_and_wait_for_pvcs(
            1, 'pvc-heketi-down', 3)

        # remove heketi-pod
        scale_dc_pod_amount_and_wait(self.ocp_client[0],
                                     self.heketi_dc_name,
                                     0,
                                     self.storage_project_name)
        try:
            # delete pvc
            for pvc in self.pvc_name_list:
                oc_delete(self.ocp_client[0], 'pvc', pvc)
            for pvc in self.pvc_name_list:
                with self.assertRaises(ExecutionError):
                    wait_for_resource_absence(
                       self.ocp_client[0], 'pvc', pvc,
                       interval=3, timeout=30)
        finally:
            # bring back heketi-pod
            scale_dc_pod_amount_and_wait(self.ocp_client[0],
                                         self.heketi_dc_name,
                                         1,
                                         self.storage_project_name)

        # verify PVC's are deleted
        for pvc in self.pvc_name_list:
            wait_for_resource_absence(self.ocp_client[0], 'pvc',
                                      pvc,
                                      interval=1, timeout=120)

        # create a new PVC
        self.create_and_wait_for_pvc()

    def test_recreate_app_pod_with_attached_block_pv(self):
        """Validate app pod attached block device I/O after restart"""
        datafile_path = '/mnt/temporary_test_file'

        # Create DC with POD and attached PVC to it
        sc_name = self.create_storage_class()
        pvc_name = self.create_and_wait_for_pvc(
            pvc_name_prefix='autotest-block', sc_name=sc_name)
        dc_name, pod_name = self.create_dc_with_pvc(pvc_name)

        # Write data
        write_cmd = "oc exec %s -- dd if=/dev/urandom of=%s bs=4k count=10000"
        self.cmd_run(write_cmd % (pod_name, datafile_path))

        # Recreate app POD
        scale_dc_pod_amount_and_wait(self.node, dc_name, 0)
        scale_dc_pod_amount_and_wait(self.node, dc_name, 1)
        new_pod_name = get_pod_name_from_dc(self.node, dc_name)

        # Check presence of already written file
        check_existing_file_cmd = (
            "oc exec %s -- ls %s" % (new_pod_name, datafile_path))
        out = self.cmd_run(check_existing_file_cmd)
        self.assertIn(datafile_path, out)

        # Perform I/O on the new POD
        self.cmd_run(write_cmd % (new_pod_name, datafile_path))

    def test_volname_prefix_glusterblock(self):
        """Validate custom volname prefix blockvol"""

        self.dynamic_provisioning_glusterblock(
            set_hacount=False, create_vol_name_prefix=True)

        pv_name = get_pv_name_from_pvc(self.node, self.pvc_name)
        vol_name = oc_get_custom_resource(
                self.node, 'pv',
                ':.metadata.annotations.glusterBlockShare', pv_name)[0]

        block_vol_list = heketi_blockvolume_list(
                self.heketi_client_node, self.heketi_server_url)

        self.assertIn(vol_name, block_vol_list)

        self.assertTrue(vol_name.startswith(
            self.sc.get('volumenameprefix', 'autotest')))

    def test_dynamic_provisioning_glusterblock_reclaim_policy_retain(self):
        """Validate retain policy for gluster-block after PVC deletion"""

        self.create_storage_class(reclaim_policy='Retain')
        self.create_and_wait_for_pvc()

        dc_name = oc_create_app_dc_with_io(self.node, self.pvc_name)

        try:
            pod_name = get_pod_name_from_dc(self.node, dc_name)
            wait_for_pod_be_ready(self.node, pod_name)
        finally:
            scale_dc_pod_amount_and_wait(self.node, dc_name, pod_amount=0)
            oc_delete(self.node, 'dc', dc_name)

        # get the name of volume
        pv_name = get_pv_name_from_pvc(self.node, self.pvc_name)

        custom = [r':.metadata.annotations."gluster\.org\/volume\-id"',
                  r':.spec.persistentVolumeReclaimPolicy']
        vol_id, reclaim_policy = oc_get_custom_resource(
            self.node, 'pv', custom, pv_name)

        # checking the retainPolicy of pvc
        self.assertEqual(reclaim_policy, 'Retain')

        # delete the pvc
        oc_delete(self.node, 'pvc', self.pvc_name)

        # check if pv is also deleted or not
        with self.assertRaises(ExecutionError):
            wait_for_resource_absence(
                self.node, 'pvc', self.pvc_name, interval=3, timeout=30)

        # getting the blockvol list
        blocklist = heketi_blockvolume_list(self.heketi_client_node,
                                            self.heketi_server_url)
        self.assertIn(vol_id, blocklist)

        heketi_blockvolume_delete(self.heketi_client_node,
                                  self.heketi_server_url, vol_id)
        blocklist = heketi_blockvolume_list(self.heketi_client_node,
                                            self.heketi_server_url)
        self.assertNotIn(vol_id, blocklist)
        oc_delete(self.node, 'pv', pv_name)
        wait_for_resource_absence(self.node, 'pv', pv_name)
