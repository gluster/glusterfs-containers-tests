import time

from cnslibs.common.dynamic_provisioning import (
    create_mongodb_pod,
    create_secret_file,
    create_storage_class_file,
    get_pvc_status,
    get_pod_name_from_dc,
    verify_pvc_status_is_bound,
    wait_for_pod_be_ready)
from cnslibs.cns.cns_baseclass import CnsBaseClass
from cnslibs.common.exceptions import ExecutionError
from cnslibs.common.heketi_ops import (
    verify_volume_name_prefix)
from cnslibs.common.openshift_ops import (
    get_ocp_gluster_pod_names,
    oc_create,
    oc_create_pvc,
    oc_create_app_dc_with_io,
    oc_delete,
    oc_rsh,
    scale_dc_pod_amount_and_wait,
    wait_for_resource_absence)
from cnslibs.common.waiter import Waiter
from glusto.core import Glusto as g


class TestDynamicProvisioningP0(CnsBaseClass):
    '''
     Class that contain P0 dynamic provisioning test cases for
     glusterfile volume
    '''
    def dynamic_provisioning_glusterfile(self, heketi_volname_prefix=False):
        storage_class = self.cns_storage_class['storage_class1']
        secret = self.cns_secret['secret1']
        node = self.ocp_master_node[0]
        sc_name = storage_class['name']
        cmd = ("oc get svc %s "
               "-o=custom-columns=:.spec.clusterIP" % self.heketi_service_name)
        ret, out, err = g.run(node, cmd, "root")
        self.assertEqual(
            ret, 0, "failed to execute command %s on %s" % (cmd, node))
        heketi_cluster_ip = out.lstrip().strip()
        resturl = "http://%s:8080" % heketi_cluster_ip
        ret = create_storage_class_file(
            node,
            sc_name,
            resturl,
            storage_class['provisioner'],
            restuser=storage_class['restuser'],
            secretnamespace=storage_class['secretnamespace'],
            secretname=secret['secret_name'],
            **({"volumenameprefix": storage_class['volumenameprefix']}
                if heketi_volname_prefix else {}))
        self.assertTrue(ret, "creation of storage-class file failed")
        provisioner_name = storage_class['provisioner'].split("/")
        file_path = "/%s-%s-storage-class.yaml" % (
                        sc_name, provisioner_name[1])
        oc_create(node, file_path)
        self.addCleanup(oc_delete, node, 'sc', sc_name)
        ret = create_secret_file(node,
                                 secret['secret_name'],
                                 secret['namespace'],
                                 self.secret_data_key,
                                 secret['type'])
        self.assertTrue(ret, "creation of heketi-secret file failed")
        oc_create(node, "/%s.yaml" % secret['secret_name'])
        self.addCleanup(oc_delete, node, 'secret', secret['secret_name'])

        # Create PVC
        pvc_name = oc_create_pvc(node, sc_name)
        self.addCleanup(wait_for_resource_absence, node, 'pvc', pvc_name)
        self.addCleanup(oc_delete, node, 'pvc', pvc_name)
        verify_pvc_status_is_bound(node, pvc_name)

        # Create DC with POD and attached PVC to it.
        dc_name = oc_create_app_dc_with_io(self.ocp_master_node[0], pvc_name)
        self.addCleanup(oc_delete, node, 'dc', dc_name)
        self.addCleanup(scale_dc_pod_amount_and_wait, node, dc_name, 0)

        pod_name = get_pod_name_from_dc(node, dc_name)
        wait_for_pod_be_ready(node, pod_name)

        # Verify Heketi volume name for prefix presence if provided
        if heketi_volname_prefix:
            ret = verify_volume_name_prefix(self.ocp_master_node[0],
                                            storage_class['volumenameprefix'],
                                            storage_class['secretnamespace'],
                                            pvc_name, resturl)
            self.assertTrue(ret, "verify volnameprefix failed")

        # Make sure we are able to work with files on the mounted volume
        filepath = "/mnt/file_for_testing_io.log"
        cmd = "dd if=/dev/urandom of=%s bs=1K count=100" % filepath
        ret, out, err = oc_rsh(node, pod_name, cmd)
        self.assertEqual(
            ret, 0, "Failed to execute command %s on %s" % (cmd, node))

        cmd = "ls -lrt %s" % filepath
        ret, out, err = oc_rsh(node, pod_name, cmd)
        self.assertEqual(
            ret, 0, "Failed to execute command %s on %s" % (cmd, node))

        cmd = "rm -rf %s" % filepath
        ret, out, err = oc_rsh(node, pod_name, cmd)
        self.assertEqual(
            ret, 0, "Failed to execute command %s on %s" % (cmd, node))

    def test_dynamic_provisioning_glusterfile(self):
        g.log.info("test_dynamic_provisioning_glusterfile")
        self.dynamic_provisioning_glusterfile(False)

    def test_dynamic_provisioning_glusterfile_volname_prefix(self):
        g.log.info("test_dynamic_provisioning_glusterfile volname prefix")
        self.dynamic_provisioning_glusterfile(True)

    def test_dynamic_provisioning_glusterfile_heketipod_failure(self):
        g.log.info("test_dynamic_provisioning_glusterfile_Heketipod_Failure")
        storage_class = self.cns_storage_class['storage_class1']
        secret = self.cns_secret['secret1']
        sc_name = storage_class['name']
        pvc_name2 = "mongodb2"
        cmd = ("oc get svc %s "
               "-o=custom-columns=:.spec.clusterIP" % self.heketi_service_name)
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        heketi_cluster_ip = out.lstrip().strip()
        resturl = "http://%s:8080" % heketi_cluster_ip
        ret = create_storage_class_file(
            self.ocp_master_node[0],
            sc_name,
            resturl,
            storage_class['provisioner'],
            restuser=storage_class['restuser'],
            secretnamespace=storage_class['secretnamespace'],
            secretname=secret['secret_name'])
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

        # Create App pod #1 and write data to it
        ret = create_mongodb_pod(self.ocp_master_node[0], pvc_name2,
                                 10, sc_name)
        self.assertTrue(ret, "creation of mongodb pod failed")
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'service',
                        pvc_name2)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'pvc',
                        pvc_name2)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'dc',
                        pvc_name2)
        pod_name = get_pod_name_from_dc(self.ocp_master_node[0], pvc_name2)
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
        pvc_name3 = "mongodb3"
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
        cmd = ("oc get svc %s "
               "-o=custom-columns=:.spec.clusterIP" % self.heketi_service_name)
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        heketi_cluster_new_ip = out.lstrip().strip()
        if heketi_cluster_new_ip != heketi_cluster_ip:
            oc_delete(self.ocp_master_node[0], 'sc', sc_name)
            resturl = "http://%s:8080" % heketi_cluster_ip
            ret = create_storage_class_file(
                self.ocp_master_node[0],
                sc_name,
                resturl,
                storage_class['provisioner'],
                restuser=storage_class['restuser'],
                secretnamespace=storage_class['secretnamespace'],
                secretname=storage_class['secretname'])
            self.assertTrue(ret, "creation of storage-class file failed")
            provisioner_name = storage_class['provisioner'].split("/")
            file_path = "/%s-%s-storage-class.yaml" % (
                             sc_name, provisioner_name[1])
            oc_create(self.ocp_master_node[0], file_path)
        for w in Waiter(600, 30):
            ret, status = get_pvc_status(self.ocp_master_node[0],
                                         pvc_name3)
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
        self.assertTrue(ret, "verify %s pod status "
                        "as running failed" % pvc_name3)
        cmd = ("dd if=/dev/urandom of=/var/lib/mongodb/data/file "
               "bs=1K count=100")
        ret, out, err = oc_rsh(self.ocp_master_node[0], pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))

    def test_dynamic_provisioning_glusterfile_glusterpod_failure(self):
        g.log.info("test_dynamic_provisioning_glusterfile_Glusterpod_Failure")
        storage_class = self.cns_storage_class['storage_class1']
        secret = self.cns_secret['secret1']
        sc_name = storage_class['name']
        pvc_name4 = "mongodb4"
        cmd = ("oc get svc %s "
               "-o=custom-columns=:.spec.clusterIP" % self.heketi_service_name)
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        heketi_cluster_ip = out.lstrip().strip()
        resturl = "http://%s:8080" % heketi_cluster_ip
        ret = create_storage_class_file(
            self.ocp_master_node[0],
            sc_name,
            resturl,
            storage_class['provisioner'],
            restuser=storage_class['restuser'],
            secretnamespace=storage_class['secretnamespace'],
            secretname=secret['secret_name'])
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
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'pvc',
                        pvc_name4)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'dc',
                        pvc_name4)
        pod_name = get_pod_name_from_dc(self.ocp_master_node[0], pvc_name4)
        ret = wait_for_pod_be_ready(self.ocp_master_node[0], pod_name,
                                    wait_step=5, timeout=300)
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
