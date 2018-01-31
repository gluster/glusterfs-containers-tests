from cnslibs.cns.cns_baseclass import CnsSetupBaseClass
from cnslibs.common.dynamic_provisioning import (
    create_secret_file,
    create_storage_class_file,
    create_pvc_file,
    create_app_pod_file)
from cnslibs.common.openshift_ops import oc_create
from glusto.core import Glusto as g


class TestDynamicProvisioning(CnsSetupBaseClass):
    '''
     Class for basic dynamic provisioning
    '''
    @classmethod
    def setUpClass(cls):
        super(TestDynamicProvisioning, cls).setUpClass()
        super(TestDynamicProvisioning, cls).cns_deploy()

    def test_dynamic_provisioning(self):
        g.log.info("testcase to test basic dynamic provisioning")
        storage_class = self.cns_storage_class['storage_class1']
        sc_name = storage_class['name']
        ret = create_storage_class_file(
            self.ocp_master_node[0],
            sc_name,
            storage_class['resturl'],
            storage_class['provisioner'],
            restuser=storage_class['restuser'],
            secretnamespace=storage_class['secretnamespace'],
            secretname=storage_class['secretname'])
        self.assertTrue(ret, "creation of storage-class file failed")
        provisioner_name = storage_class['provisioner'].split("/")
        file_path = ("/%s-%s-storage-class.yaml" % (
                         sc_name, provisioner_name[1]))
        oc_create(self.ocp_master_node[0], file_path)
        secret = self.cns_secret['secret1']
        ret = create_secret_file(self.ocp_master_node[0],
                                 secret['secret_name'],
                                 secret['namespace'],
                                 secret['data_key'],
                                 secret['type'])
        self.assertTrue(ret, "creation of heketi-secret file failed")
        oc_create(self.ocp_master_node[0],
                  "/%s.yaml" % secret['secret_name'])
        count = self.start_count_for_pvc
        for size, pvc in self.cns_pvc_size_number_dict.items():
            for i in range(1, pvc + 1):
                pvc_name = "pvc-claim%d" % count
                g.log.info("starting creation of claim file "
                           "for %s", pvc_name)
                ret = create_pvc_file(self.ocp_master_node[0],
                                      pvc_name, sc_name, size)
                self.assertTrue(ret, "create pvc file - %s failed" % pvc_name)
                file_path = "/pvc-claim%d.json" % count
                g.log.info("starting to create claim %s", pvc_name)
                oc_create(self.ocp_master_node[0], file_path)
                count = count + 1
        cmd = 'oc get pvc | grep pvc-claim | awk \'{print $1}\''
        ret, out, err = g.run(self.ocp_master_node[0], cmd, "root")
        self.assertEqual(ret, 0, "failed to execute cmd %s on %s err %s" % (
            cmd, self.ocp_master_node[0], out))
        complete_pvc_list = out.strip().split("\n")
        complete_pvc_list = map(str.strip, complete_pvc_list)
        count = self.start_count_for_pvc
        exisisting_pvc_list = []
        for i in range(1, count):
            exisisting_pvc_list.append("pvc-claim%d" % i)
        pvc_list = list(set(complete_pvc_list) - set(exisisting_pvc_list))
        index = 0
        for key, value in self.app_pvc_count_dict.items():
            for i in range(1, value + 1):
                claim_name = pvc_list[index]
                app_name = key + str(count)
                sample_app_name = key
                g.log.info("starting to create app_pod_file for %s", app_name)
                ret = create_app_pod_file(
                    self.ocp_master_node[0], claim_name,
                    app_name, sample_app_name)
                self.assertTrue(
                    ret, "creating app-pod file - %s failed" % app_name)
                file_path = "/%s.yaml" % app_name
                g.log.info("starting to create app_pod_%s", app_name)
                oc_create(self.ocp_master_node[0], file_path)
                index = index + 1
                count = count + 1
