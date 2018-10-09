import ddt
from glusto.core import Glusto as g

from cnslibs.cns import cns_baseclass
from cnslibs.common.openshift_ops import (
    oc_create_pvc,
    oc_create_sc,
    oc_create_secret,
    oc_delete,
    verify_pvc_status_is_bound,
    wait_for_resource_absence,
)


@ddt.ddt
class TestStorageClassCases(cns_baseclass.CnsBaseClass):

    @ddt.data(
        {"volumetype": "dist-rep:3"},
        {"resturl": "http://10.0.0.1:8080"},
        {"secretname": "fakesecretname"},
        {"secretnamespace": "fakenamespace"},
        {"restuser": "fakeuser"},
        )
    def test_sc_glusterfile_incorrect_parameter(
            self, parameter={}):
        # Polarion testcase id- CNS-708,709,713,714,715

        sc = self.cns_storage_class['storage_class1']
        secret = self.cns_secret['secret1']
        # Create secret file for usage in storage class
        self.secret_name = oc_create_secret(
            self.ocp_master_node[0], namespace=secret['namespace'],
            data_key=self.heketi_cli_key, secret_type=secret['type'])
        self.addCleanup(
            oc_delete, self.ocp_master_node[0], 'secret', self.secret_name)

        sc_parameter = {
            "resturl": sc['resturl'],
            "secretnamespace": sc['secretnamespace'],
            "secretname": self.secret_name,
            "restuser": sc['restuser'],
            "volumetype": "replicate:3"
        }
        sc_parameter.update(parameter)

        # Create storage class
        self.sc_name = oc_create_sc(
            self.ocp_master_node[0], **sc_parameter)
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'sc', self.sc_name)

        # Create PVC
        self.pvc_name = oc_create_pvc(self.ocp_client[0], self.sc_name)
        self.addCleanup(
            wait_for_resource_absence, self.ocp_master_node[0],
            'pvc', self.pvc_name)
        self.addCleanup(oc_delete, self.ocp_master_node[0],
                        'pvc', self.pvc_name)

        try:
            verify_pvc_status_is_bound(self.ocp_master_node[0], self.pvc_name)
        except AssertionError:
            g.log.info("pvc %s is not in bound state as expected" % (
                       self.pvc_name))
        else:
            err_msg = ("pvc %s in bound state, was expected to fail" % (
                       self.pvc_name))
            g.log.error(err_msg)
            raise AssertionError(err_msg)
