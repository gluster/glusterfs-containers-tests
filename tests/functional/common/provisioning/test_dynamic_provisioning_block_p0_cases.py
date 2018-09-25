import time
from unittest import skip

from cnslibs.common.dynamic_provisioning import (
    get_pvc_status,
    get_pod_name_from_dc,
    wait_for_pod_be_ready,
    verify_pvc_status_is_bound)
from cnslibs.cns.cns_baseclass import CnsGlusterBlockBaseClass
from cnslibs.common.exceptions import ExecutionError
from cnslibs.common.openshift_ops import (
    get_gluster_pod_names_by_pvc_name,
    oc_create_secret,
    oc_create_sc,
    oc_create_pvc,
    oc_create_app_dc_with_io,
    oc_delete,
    oc_rsh,
    scale_dc_pod_amount_and_wait,
    wait_for_resource_absence)
from cnslibs.common.waiter import Waiter
from glusto.core import Glusto as g


class TestDynamicProvisioningBlockP0(CnsGlusterBlockBaseClass):
    '''
     Class that contain P0 dynamic provisioning test cases
     for block volume
    '''

    def setUp(self):
        super(TestDynamicProvisioningBlockP0, self).setUp()
        self.node = self.ocp_master_node[0]

    def _create_storage_class(self):
        sc = self.cns_storage_class['storage_class2']
        secret = self.cns_secret['secret2']

        # Create secret file
        self.secret_name = oc_create_secret(
            self.node, namespace=secret['namespace'],
            data_key=self.heketi_cli_key, secret_type=secret['type'])
        self.addCleanup(oc_delete, self.node, 'secret', self.secret_name)

        # Create storage class
        self.sc_name = oc_create_sc(
            self.ocp_master_node[0], provisioner="gluster.org/glusterblock",
            resturl=sc['resturl'], restuser=sc['restuser'],
            restsecretnamespace=sc['restsecretnamespace'],
            restsecretname=self.secret_name, hacount=sc['hacount'],
        )
        self.addCleanup(oc_delete, self.node, 'sc', self.sc_name)

        return self.sc_name

    def _create_and_wait_for_pvcs(self, pvc_size=1,
                                  pvc_name_prefix='autotests-block-pvc',
                                  pvc_amount=1):
        # Create PVCs
        pvc_names = []
        for i in range(pvc_amount):
            pvc_name = oc_create_pvc(
                self.node, self.sc_name, pvc_name_prefix=pvc_name_prefix,
                pvc_size=pvc_size)
            pvc_names.append(pvc_name)
            self.addCleanup(
                wait_for_resource_absence, self.node, 'pvc', pvc_name)
        for pvc_name in pvc_names:
            self.addCleanup(oc_delete, self.node, 'pvc', pvc_name)

        # Wait for PVCs to be in bound state
        for pvc_name in pvc_names:
            verify_pvc_status_is_bound(self.node, pvc_name)

        return pvc_names

    def _create_and_wait_for_pvc(self, pvc_size=1,
                                 pvc_name_prefix='autotests-block-pvc'):
        self.pvc_name = self._create_and_wait_for_pvcs(
            pvc_size=pvc_size, pvc_name_prefix=pvc_name_prefix)[0]
        return self.pvc_name

    def _create_dc_with_pvc(self):
        # Create storage class and secret objects
        self._create_storage_class()

        # Create PVC
        pvc_name = self._create_and_wait_for_pvc()

        # Create DC with POD and attached PVC to it
        dc_name = oc_create_app_dc_with_io(self.node, pvc_name)
        self.addCleanup(oc_delete, self.node, 'dc', dc_name)
        self.addCleanup(scale_dc_pod_amount_and_wait, self.node, dc_name, 0)
        pod_name = get_pod_name_from_dc(self.node, dc_name)
        wait_for_pod_be_ready(self.node, pod_name)

        return dc_name, pod_name, pvc_name

    def test_dynamic_provisioning_glusterblock(self):
        datafile_path = '/mnt/fake_file_for_%s' % self.id()

        # Create DC with attached PVC
        dc_name, pod_name, pvc_name = self._create_dc_with_pvc()

        # Check that we can write data
        for cmd in ("dd if=/dev/urandom of=%s bs=1K count=100",
                    "ls -lrt %s",
                    "rm -rf %s"):
            cmd = cmd % datafile_path
            ret, out, err = oc_rsh(self.node, pod_name, cmd)
            self.assertEqual(
                ret, 0,
                "Failed to execute '%s' command on '%s'." % (cmd, self.node))

    def test_dynamic_provisioning_glusterblock_heketipod_failure(self):
        datafile_path = '/mnt/fake_file_for_%s' % self.id()

        # Create DC with attached PVC
        app_1_dc_name, app_1_pod_name, app_1_pvc_name = (
            self._create_dc_with_pvc())

        # Write test data
        write_data_cmd = (
            "dd if=/dev/urandom of=%s bs=1K count=100" % datafile_path)
        ret, out, err = oc_rsh(self.node, app_1_pod_name, write_data_cmd)
        self.assertEqual(
            ret, 0,
            "Failed to execute command %s on %s" % (write_data_cmd, self.node))

        # Remove Heketi pod
        heketi_down_cmd = "oc scale --replicas=0 dc/%s --namespace %s" % (
            self.heketi_dc_name, self.cns_project_name)
        heketi_up_cmd = "oc scale --replicas=1 dc/%s --namespace %s" % (
            self.heketi_dc_name, self.cns_project_name)
        self.addCleanup(self.cmd_run, heketi_up_cmd)
        heketi_pod_name = get_pod_name_from_dc(
            self.node, self.heketi_dc_name, timeout=10, wait_step=3)
        self.cmd_run(heketi_down_cmd)
        wait_for_resource_absence(self.node, 'pod', heketi_pod_name)

        # Create second PVC
        app_2_pvc_name = oc_create_pvc(
            self.node, self.sc_name, pvc_name_prefix='autotests-block-pvc',
            pvc_size=1)
        self.addCleanup(
            wait_for_resource_absence, self.node, 'pvc', app_2_pvc_name)
        self.addCleanup(oc_delete, self.node, 'pvc', app_2_pvc_name)

        # Check status of the second PVC after small pause
        time.sleep(2)
        ret, status = get_pvc_status(self.node, app_2_pvc_name)
        self.assertTrue(ret, "Failed to get pvc status of %s" % app_2_pvc_name)
        self.assertEqual(
            status, "Pending",
            "PVC status of %s is not in Pending state" % app_2_pvc_name)

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
        datafile_path = '/mnt/fake_file_for_%s' % self.id()

        # Create DC with attached PVC
        dc_name, pod_name, pvc_name = self._create_dc_with_pvc()

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
        # Verify presence of glusterblock provisioner POD and its status
        gb_prov_cmd = ("oc get pods --all-namespaces "
                       "-l glusterfs=block-cns-provisioner-pod "
                       "-o=custom-columns=:.metadata.name,:.status.phase")
        ret, out, err = g.run(self.ocp_client[0], gb_prov_cmd, "root")

        self.assertEqual(ret, 0, "Failed to get Glusterblock provisioner POD.")
        gb_prov_name, gb_prov_status = out.split()
        self.assertEqual(gb_prov_status, 'Running')

        # Create storage class and secret objects
        self._create_storage_class()

        # Create PVC
        self._create_and_wait_for_pvc()

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
