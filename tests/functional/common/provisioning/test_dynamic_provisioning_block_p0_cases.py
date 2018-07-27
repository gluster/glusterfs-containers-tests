import time

from cnslibs.common.dynamic_provisioning import (
    create_mongodb_pod,
    create_secret_file,
    create_storage_class_file,
    get_pvc_status,
    get_pod_name_from_dc,
    wait_for_pod_be_ready,
    verify_pvc_status_is_bound)
from cnslibs.cns.cns_baseclass import CnsGlusterBlockBaseClass
from cnslibs.common.exceptions import ExecutionError
from cnslibs.common.heketi_ops import (
    export_heketi_cli_server)
from cnslibs.common.openshift_ops import (
    get_ocp_gluster_pod_names,
    oc_create,
    oc_create_secret,
    oc_create_sc,
    oc_create_pvc,
    oc_delete,
    oc_rsh,
    wait_for_resource_absence)
from cnslibs.common.waiter import Waiter
from glusto.core import Glusto as g


class TestDynamicProvisioningBlockP0(CnsGlusterBlockBaseClass):
    '''
     Class that contain P0 dynamic provisioning test cases
     for block volume
    '''

    def _create_storage_class(self):
        sc = self.cns_storage_class['storage_class2']
        secret = self.cns_secret['secret2']

        # Create secret file
        self.secret_name = oc_create_secret(
            self.ocp_master_node[0],
            namespace=secret['namespace'],
            data_key=self.heketi_cli_key,
            secret_type=secret['type'])
        self.addCleanup(
            oc_delete, self.ocp_master_node[0], 'secret', self.secret_name)

        # Create storage class
        self.sc_name = oc_create_sc(
            self.ocp_master_node[0], provisioner="gluster.org/glusterblock",
            resturl=sc['resturl'], restuser=sc['restuser'],
            restsecretnamespace=sc['restsecretnamespace'],
            restsecretname=self.secret_name, hacount=sc['hacount'],
        )
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'sc', self.sc_name)

        return self.sc_name

    def test_dynamic_provisioning_glusterblock(self):
        pvc_name = "mongodb1-block"
        mongodb_filepath = '/var/lib/mongodb/data/file'

        # Create storage class and secret objects
        self._create_storage_class()

        ret = create_mongodb_pod(
            self.ocp_master_node[0], pvc_name, 10, self.sc_name)
        self.assertTrue(ret, "creation of mongodb pod failed")
        self.addCleanup(
            oc_delete, self.ocp_master_node[0], 'service', pvc_name)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'pvc', pvc_name)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'dc', pvc_name)
        pod_name = get_pod_name_from_dc(self.ocp_master_node[0], pvc_name)
        ret = wait_for_pod_be_ready(self.ocp_master_node[0], pod_name)
        self.assertTrue(ret, "verify mongodb pod status as running failed")

        cmd = "dd if=/dev/urandom of=%s bs=1K count=100" % mongodb_filepath
        ret, out, err = oc_rsh(self.ocp_master_node[0], pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))

        cmd = "ls -lrt %s" % mongodb_filepath
        ret, out, err = oc_rsh(self.ocp_master_node[0], pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        cmd = "rm -rf %s" % mongodb_filepath
        ret, out, err = oc_rsh(self.ocp_master_node[0], pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                                cmd, self.ocp_master_node[0]))

    def test_dynamic_provisioning_glusterblock_heketipod_failure(self):
        g.log.info("test_dynamic_provisioning_glusterblock_Heketipod_Failure")
        pvc_name = "mongodb2-block"

        # Create storage class and secret objects
        sc_name = self._create_storage_class()

        # Create App pod #1 and write data to it
        ret = create_mongodb_pod(
            self.ocp_master_node[0], pvc_name, 10, sc_name)
        self.assertTrue(ret, "creation of mongodb pod failed")
        self.addCleanup(
            oc_delete, self.ocp_master_node[0], 'service', pvc_name)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'pvc', pvc_name)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'dc', pvc_name)
        pod_name = get_pod_name_from_dc(self.ocp_master_node[0], pvc_name)
        ret = wait_for_pod_be_ready(self.ocp_master_node[0], pod_name,
                                    wait_step=5, timeout=300)
        self.assertTrue(ret, "verify mongodb pod status as running failed")
        cmd = ("dd if=/dev/urandom of=/var/lib/mongodb/data/file "
               "bs=1K count=100")
        ret, out, err = oc_rsh(self.ocp_master_node[0], pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))

        # Remove Heketi pod
        heketi_down_cmd = "oc scale --replicas=0 dc/%s --namespace %s" % (
            self.heketi_dc_name, self.cns_project_name)
        heketi_up_cmd = "oc scale --replicas=1 dc/%s --namespace %s" % (
            self.heketi_dc_name, self.cns_project_name)
        self.addCleanup(g.run, self.ocp_master_node[0], heketi_up_cmd, "root")
        ret, out, err = g.run(self.ocp_master_node[0], heketi_down_cmd, "root")

        get_heketi_podname_cmd = (
            "oc get pods --all-namespaces -o=custom-columns=:.metadata.name "
            "--no-headers=true "
            "--selector deploymentconfig=%s" % self.heketi_dc_name)
        ret, out, err = g.run(self.ocp_master_node[0], get_heketi_podname_cmd)
        wait_for_resource_absence(self.ocp_master_node[0], 'pod', out.strip())

        # Create App pod #2
        pvc_name3 = "mongodb3-block"
        ret = create_mongodb_pod(self.ocp_master_node[0],
                                 pvc_name3, 10, sc_name)
        self.assertTrue(ret, "creation of mongodb pod failed")
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'service',
                        pvc_name3)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'pvc',
                        pvc_name3)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'dc',
                        pvc_name3)
        ret, status = get_pvc_status(self.ocp_master_node[0],
                                     pvc_name3)
        self.assertTrue(ret, "failed to get pvc status of %s" % pvc_name3)
        self.assertEqual(status, "Pending", "pvc status of "
                         "%s is not in Pending state" % pvc_name3)

        # Bring Heketi pod back
        ret, out, err = g.run(self.ocp_master_node[0], heketi_up_cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                         heketi_up_cmd, self.ocp_master_node[0]))

        # Wait small amount of time before newly scheduled Heketi POD appears
        time.sleep(2)

        # Wait for Heketi POD be up and running
        ret, out, err = g.run(self.ocp_master_node[0], get_heketi_podname_cmd)
        ret = wait_for_pod_be_ready(self.ocp_master_node[0], out.strip(),
                                    wait_step=5, timeout=120)
        self.assertTrue(ret, "verify heketi pod status as running failed")

        # Verify App pod #2
        for w in Waiter(600, 30):
            ret, status = get_pvc_status(self.ocp_master_node[0], pvc_name3)
            self.assertTrue(ret, "failed to get pvc status of %s" % (
                                pvc_name3))
            if status != "Bound":
                g.log.info("pvc status of %s is not in Bound state,"
                           " sleeping for 30 sec" % pvc_name3)
                continue
            else:
                break
        if w.expired:
            error_msg = ("exceeded timeout 600 sec, pvc %s not in"
                         " Bound state" % pvc_name3)
            g.log.error(error_msg)
            raise ExecutionError(error_msg)
        self.assertEqual(status, "Bound", "pvc status of %s "
                         "is not in Bound state, its state is %s" % (
                             pvc_name3, status))
        pod_name = get_pod_name_from_dc(self.ocp_master_node[0], pvc_name3)
        ret = wait_for_pod_be_ready(self.ocp_master_node[0], pod_name,
                                    wait_step=5, timeout=300)
        self.assertTrue(ret, "verify %s pod status as "
                        "running failed" % pvc_name3)
        cmd = ("dd if=/dev/urandom of=/var/lib/mongodb/data/file "
               "bs=1K count=100")
        ret, out, err = oc_rsh(self.ocp_master_node[0], pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))

    def test_dynamic_provisioning_glusterblock_glusterpod_failure(self):
        g.log.info("test_dynamic_provisioning_glusterblock_Glusterpod_Failure")
        storage_class = self.cns_storage_class['storage_class2']
        secret = self.cns_secret['secret2']
        cmd = ("oc get svc %s "
               "-o=custom-columns=:.spec.clusterIP" % self.heketi_service_name)
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        heketi_cluster_ip = out.lstrip().strip()
        resturl_block = "http://%s:8080" % heketi_cluster_ip
        if not export_heketi_cli_server(
                    self.heketi_client_node,
                    heketi_cli_server=resturl_block,
                    heketi_cli_user=self.heketi_cli_user,
                    heketi_cli_key=self.heketi_cli_key):
                raise ExecutionError("Failed to export heketi cli server on %s"
                                     % self.heketi_client_node)
        cmd = ("heketi-cli cluster list "
               "| grep Id | cut -d ':' -f 2 | cut -d '[' -f 1")
        ret, out, err = g.run(self.ocp_client[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_client[0]))
        cluster_id = out.strip().split("\n")[0]
        sc_name = storage_class['name']
        pvc_name4 = "mongodb-4-block"
        ret = create_storage_class_file(
            self.ocp_master_node[0],
            sc_name,
            resturl_block,
            storage_class['provisioner'],
            restuser=storage_class['restuser'],
            restsecretnamespace=storage_class['restsecretnamespace'],
            restsecretname=secret['secret_name'],
            hacount=storage_class['hacount'],
            clusterids=cluster_id)
        self.assertTrue(ret, "creation of storage-class file failed")
        provisioner_name = storage_class['provisioner'].split("/")
        file_path = "/%s-%s-storage-class.yaml" % (
                        sc_name, provisioner_name[1])
        oc_create(self.ocp_master_node[0], file_path)
        self.addCleanup(oc_delete, self.ocp_master_node[0],
                        'sc', sc_name)
        ret = create_secret_file(self.ocp_master_node[0],
                                 secret['secret_name'],
                                 secret['namespace'],
                                 self.secret_data_key,
                                 secret['type'])
        self.assertTrue(ret, "creation of heketi-secret file failed")
        oc_create(self.ocp_master_node[0],
                  "/%s.yaml" % secret['secret_name'])
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'secret',
                        secret['secret_name'])
        ret = create_mongodb_pod(self.ocp_master_node[0],
                                 pvc_name4, 30, sc_name)
        self.assertTrue(ret, "creation of mongodb pod failed")
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'service',
                        pvc_name4)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'pvc', pvc_name4)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'dc', pvc_name4)
        pod_name = get_pod_name_from_dc(self.ocp_master_node[0], pvc_name4)
        ret = wait_for_pod_be_ready(self.ocp_master_node[0], pod_name)
        self.assertTrue(ret, "verify mongodb pod status as running failed")
        io_cmd = ("oc rsh %s dd if=/dev/urandom of=/var/lib/mongodb/data/file "
                  "bs=1000K count=1000") % pod_name
        proc = g.run_async(self.ocp_master_node[0], io_cmd, "root")
        gluster_pod_list = get_ocp_gluster_pod_names(self.ocp_master_node[0])
        g.log.info("gluster_pod_list - %s" % gluster_pod_list)
        gluster_pod_name = gluster_pod_list[0]
        cmd = ("oc get pods -o wide | grep %s | grep -v deploy "
               "| awk '{print $7}'") % gluster_pod_name
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        gluster_pod_node_name = out.strip().split("\n")[0].strip()
        oc_delete(self.ocp_master_node[0], 'pod', gluster_pod_name)
        cmd = ("oc get pods -o wide | grep glusterfs | grep %s | "
               "grep -v Terminating | awk '{print $1}'") % (
                   gluster_pod_node_name)
        for w in Waiter(600, 30):
            ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
            new_gluster_pod_name = out.strip().split("\n")[0].strip()
            if ret == 0 and not new_gluster_pod_name:
                continue
            else:
                break
        if w.expired:
            error_msg = "exceeded timeout, new gluster pod not created"
            g.log.error(error_msg)
            raise ExecutionError(error_msg)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        new_gluster_pod_name = out.strip().split("\n")[0].strip()
        g.log.info("new gluster pod name is %s" % new_gluster_pod_name)
        ret = wait_for_pod_be_ready(self.ocp_master_node[0],
                                    new_gluster_pod_name)
        self.assertTrue(ret, "verify %s pod status as running "
                        "failed" % new_gluster_pod_name)
        ret, out, err = proc.async_communicate()
        self.assertEqual(ret, 0, "IO %s failed on %s" % (io_cmd,
                         self.ocp_master_node[0]))

    def test_glusterblock_logs_presence_verification(self):
        # Verify presence of glusterblock provisioner POD and its status
        gb_prov_cmd = ("oc get pods --all-namespaces "
                       "-l glusterfs=block-cns-provisioner-pod "
                       "-o=custom-columns=:.metadata.name,:.status.phase")
        ret, out, err = g.run(self.ocp_client[0], gb_prov_cmd, "root")

        self.assertEqual(ret, 0, "Failed to get Glusterblock provisioner POD.")
        gb_prov_name, gb_prov_status = out.split()
        self.assertEqual(gb_prov_status, 'Running')

        # Create PVC
        pvc_name = oc_create_pvc(
            self.ocp_client[0], self._create_storage_class(),
            pvc_name_prefix="glusterblock-logs-verification")
        self.addCleanup(oc_delete, self.ocp_client[0], 'pvc', pvc_name)

        # Wait for PVC to be in bound state
        verify_pvc_status_is_bound(self.ocp_client[0], pvc_name)

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
