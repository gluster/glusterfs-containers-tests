import ddt
from glusto.core import Glusto as g

from cnslibs.cns import cns_baseclass
from cnslibs.common.openshift_ops import (
    oc_create_pvc,
    oc_create_sc,
    oc_create_secret,
    oc_delete,
    wait_for_events,
    wait_for_resource_absence,
)


@ddt.ddt
class TestStorageClassCases(cns_baseclass.CnsBaseClass):

    def sc_incorrect_parameter(self, vol_type, parameter={}):
        if vol_type == "glusterfile":
            sc = self.cns_storage_class['storage_class1']
            secret = self.cns_secret['secret1']
            # Create secret file for usage in storage class
            self.secret_name = oc_create_secret(
                self.ocp_master_node[0], namespace=secret['namespace'],
                data_key=self.heketi_cli_key, secret_type=secret['type'])
            self.addCleanup(
                oc_delete, self.ocp_master_node[0], 'secret', self.secret_name)
            sc_parameter = {
                "secretnamespace": sc['secretnamespace'],
                "secretname": self.secret_name,
                "volumetype": "replicate:3"
            }
        elif vol_type == "glusterblock":
            sc = self.cns_storage_class['storage_class2']
            secret = self.cns_secret['secret2']
            # Create secret file for usage in storage class
            self.secret_name = oc_create_secret(
                self.ocp_master_node[0], namespace=secret['namespace'],
                data_key=self.heketi_cli_key, secret_type=secret['type'])
            self.addCleanup(
                oc_delete, self.ocp_master_node[0], 'secret', self.secret_name)
            sc_parameter = {
                "provisioner": "gluster.org/glusterblock",
                "restsecretnamespace": sc['restsecretnamespace'],
                "restsecretname": self.secret_name,
                "hacount": sc['hacount']
            }
        else:
            err_msg = "invalid vol_type %s" % vol_type
            g.log.error(err_msg)
            raise AssertionError(err_msg)
        sc_parameter['resturl'] = sc['resturl']
        sc_parameter['restuser'] = sc['restuser']
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

        # Wait for event with error
        wait_for_events(self.ocp_master_node[0],
                        obj_name=self.pvc_name,
                        obj_type='PersistentVolumeClaim',
                        event_reason='ProvisioningFailed')

    @ddt.data(
        {"volumetype": "dist-rep:3"},
        {"resturl": "http://10.0.0.1:8080"},
        {"secretname": "fakesecretname"},
        {"secretnamespace": "fakenamespace"},
        {"restuser": "fakeuser"},
        {"volumenameprefix": "dept_qe"},
        )
    def test_sc_glusterfile_incorrect_parameter(self, parameter={}):
        """Polarion testcase id- CNS-708,709,713,714,715,921"""
        self.sc_incorrect_parameter("glusterfile", parameter)

    @ddt.data(
        {"resturl": "http://10.0.0.1:8080"},
        {"restsecretname": "fakerestsecretname",
         "restsecretnamespace": "fakerestnamespace"},
        {"restuser": "fakeuser"},
        )
    def test_sc_glusterblock_incorrect_parameter(self, parameter={}):
        """ Polarion testcase id- CNS-727,725,728"""
        self.sc_incorrect_parameter("glusterblock", parameter)
