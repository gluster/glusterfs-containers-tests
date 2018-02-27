from cnslibs.common.dynamic_provisioning import (
    create_mongodb_pod,
    create_secret_file,
    create_storage_class_file,
    get_pvc_status,
    verify_pod_status_running)
from cnslibs.cns.cns_baseclass import CnsGlusterBlockBaseClass
from cnslibs.common.exceptions import (
    ConfigError,
    ExecutionError)
from cnslibs.common.heketi_ops import (
    export_heketi_cli_server)
from cnslibs.common.openshift_ops import (
    get_ocp_gluster_pod_names,
    oc_create,
    oc_delete,
    oc_rsh)
from cnslibs.common.waiter import Waiter
from glusto.core import Glusto as g
import unittest


class TestDynamicProvisioningBlockP0(CnsGlusterBlockBaseClass):
    '''
     Class that contain P0 dynamic provisioning test cases
     for block volume
    '''
    def test_dynamic_provisioning_glusterblock(self):
        g.log.info("test_dynamic_provisioning_glusterblock")
        storage_class = self.cns_storage_class['storage_class2']
        secret = self.cns_secret['secret2']
        cmd = ("oc get svc | grep heketi | grep -v endpoints "
               "| awk '{print $2}'")
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        heketi_cluster_ip = out.strip().split("\n")[0]
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
        pvc_name1 = "mongodb1-block"
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
                                 pvc_name1, 10, sc_name)
        self.assertTrue(ret, "creation of mongodb pod failed")
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'service',
                        pvc_name1)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'pvc',
                        pvc_name1)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'dc',
                        pvc_name1)
        ret = verify_pod_status_running(self.ocp_master_node[0],
                                        pvc_name1)
        self.assertTrue(ret, "verify mongodb pod status as running failed")
        cmd = ("oc get pods | grep %s | grep -v deploy "
               "| awk {'print $1'}") % pvc_name1
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        pod_name = out.strip().split("\n")[0]
        cmd = ("dd if=/dev/urandom of=/var/lib/mongodb/data/file "
               "bs=1K count=100")
        ret, out, err = oc_rsh(self.ocp_master_node[0], pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        oc_delete(self.ocp_master_node[0], 'pod',  pod_name)
        ret = verify_pod_status_running(self.ocp_master_node[0],
                                        pvc_name1)
        self.assertTrue(ret, "verify mongodb pod status as running failed")
        cmd = ("oc get pods | grep %s | grep -v deploy "
               "| awk {'print $1'}") % pvc_name1
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        pod_name = out.strip().split("\n")[0]
        cmd = ("dd if=/dev/urandom of=/var/lib/mongodb/data/file "
               "bs=1K count=100")
        ret, out, err = oc_rsh(self.ocp_master_node[0], pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        oc_delete(self.ocp_master_node[0], 'pod', pod_name)
        ret = verify_pod_status_running(self.ocp_master_node[0],
                                        pvc_name1)
        self.assertTrue(ret, "verify mongodb pod status as running failed")
        cmd = ("oc get pods | grep %s | grep -v deploy "
               "| awk {'print $1'}") % pvc_name1
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        pod_name = out.strip().split("\n")[0]
        cmd = "ls -lrt /var/lib/mongodb/data/file"
        ret, out, err = oc_rsh(self.ocp_master_node[0], pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        cmd = "rm -rf /var/lib/mongodb/data/file"
        ret, out, err = oc_rsh(self.ocp_master_node[0], pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                                cmd, self.ocp_master_node[0]))

    @unittest.skip("skiping heketi-pod failure testcase")
    def test_dynamic_provisioning_glusterblock_heketipod_failure(self):
        g.log.info("test_dynamic_provisioning_glusterblock_Heketipod_Failure")
        storage_class = self.cns_storage_class['storage_class2']
        secret = self.cns_secret['secret2']
        cmd = ("oc get svc | grep heketi | grep -v endpoints "
               "| awk '{print $2}'")
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        heketi_cluster_ip = out.strip().split("\n")[0]
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
        pvc_name2 = "mongodb2-block"
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
                                 pvc_name2, 10, sc_name)
        self.assertTrue(ret, "creation of mongodb pod failed")
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'service',
                        pvc_name2)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'pvc',
                        pvc_name2)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'dc',
                        pvc_name2)
        ret = verify_pod_status_running(self.ocp_master_node[0],
                                        pvc_name2)
        self.assertTrue(ret, "verify mongodb pod status as running failed")
        cmd = ("oc get pods | grep %s | grep -v deploy "
               "| awk {'print $1'}") % pvc_name2
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        pod_name = out.strip().split("\n")[0]
        cmd = ("dd if=/dev/urandom of=/var/lib/mongodb/data/file "
               "bs=1K count=100")
        ret, out, err = oc_rsh(self.ocp_master_node[0], pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        oc_delete(self.ocp_master_node[0], 'dc', "heketi")
        oc_delete(self.ocp_master_node[0], 'service', "heketi")
        oc_delete(self.ocp_master_node[0], 'route', "heketi")
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
        cmd = "oc process heketi | oc create -f -"
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        ret = verify_pod_status_running(self.ocp_master_node[0], "heketi")
        self.assertTrue(ret, "verify heketi pod status as running failed")
        cmd = ("oc get svc | grep heketi | grep -v endpoints "
               "| awk '{print $2}'")
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        heketi_cluster_new_ip = out.strip().split("\n")[0]
        if heketi_cluster_new_ip != heketi_cluster_ip:
            oc_delete(self.ocp_master_node[0], 'sc', sc_name)
            resturl_block = "http://%s:8080" % heketi_cluster_new_ip
            ret = create_storage_class_file(
                self.ocp_master_node[0],
                sc_name,
                resturl_block,
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
            error_msg = ("exceeded timeout 300 sec, pvc %s not in"
                        " Bound state" % pvc_name3)
            g.log.error(error_msg)
            raise ExecutionError(error_msg)
        self.assertEqual(status, "Bound", "pvc status of %s "
                         "is not in Bound state, its state is %s" % (
                             pvc_name3, status))
        ret = verify_pod_status_running(self.ocp_master_node[0],
                                        pvc_name3)
        self.assertTrue(ret, "verify %s pod status as "
                        "running failed" % pvc_name3)
        cmd = ("oc get pods | grep %s | grep -v deploy "
               "|awk {'print $1'}") % pvc_name3
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        pod_name = out.strip().split("\n")[0]
        cmd = ("dd if=/dev/urandom of=/var/lib/mongodb/data/file "
               "bs=1K count=100")
        ret, out, err = oc_rsh(self.ocp_master_node[0], pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))

    def test_dynamic_provisioning_glusterblock_glusterpod_failure(self):
        g.log.info("test_dynamic_provisioning_glusterblock_Glusterpod_Failure")
        storage_class = self.cns_storage_class['storage_class2']
        secret = self.cns_secret['secret2']
        cmd = ("oc get svc | grep heketi | grep -v endpoints "
               "| awk '{print $2}'")
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        heketi_cluster_ip = out.strip().split("\n")[0]
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
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'pvc',
                        pvc_name4)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'dc',
                        pvc_name4)
        ret = verify_pod_status_running(self.ocp_master_node[0],
                                        pvc_name4)
        self.assertTrue(ret, "verify mongodb pod status as running failed")
        cmd = ("oc get pods | grep %s | grep -v deploy "
               "| awk {'print $1'}") % pvc_name4
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
        pod_name = out.strip().split("\n")[0]
        io_cmd = ("oc rsh %s dd if=/dev/urandom of=/var/lib/mongodb/data/file "
                  "bs=1000K count=1000") % pod_name
        proc = g.run_async(self.ocp_master_node[0], io_cmd, "root")
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, self.ocp_master_node[0]))
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
        ret = verify_pod_status_running(self.ocp_master_node[0],
                                        new_gluster_pod_name)
        self.assertTrue(ret, "verify %s pod status as running "
                        "failed" % new_gluster_pod_name)
        ret, out, err = proc.async_communicate()
        self.assertEqual(ret, 0, "IO %s failed on %s" % (io_cmd,
                         self.ocp_master_node[0]))
