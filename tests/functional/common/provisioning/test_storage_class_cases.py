from unittest import skip

import ddt
from glusto.core import Glusto as g

from cnslibs.cns import cns_baseclass
from cnslibs.common.cns_libs import validate_multipath_pod
from cnslibs.common.openshift_ops import (
    get_amount_of_gluster_nodes,
    get_gluster_blockvol_info_by_pvc_name,
    get_pod_name_from_dc,
    oc_create_app_dc_with_io,
    oc_create_pvc,
    oc_create_sc,
    oc_create_secret,
    oc_delete,
    scale_dc_pod_amount_and_wait,
    wait_for_events,
    wait_for_pod_be_ready,
    wait_for_resource_absence
)


@ddt.ddt
class TestStorageClassCases(cns_baseclass.BaseClass):

    def create_sc_with_parameter(self, vol_type, success=False, parameter={}):
        """creates storage class, pvc and validates event

        Args:
            vol_type (str): storage type either gluster file or block
            success (bool): if True check for successfull else failure
                            for pvc creation event
            parameter (dict): dictionary with storage class parameters
        """
        if vol_type == "glusterfile":
            sc = self.storage_classes.get(
                'storage_class1',
                self.storage_classes.get('file_storage_class'))

            # Create secret file for usage in storage class
            self.secret_name = oc_create_secret(
                self.ocp_master_node[0],
                namespace=sc.get('secretnamespace', 'default'),
                data_key=self.heketi_cli_key,
                secret_type=sc.get('provisioner', 'kubernetes.io/glusterfs'))
            self.addCleanup(
                oc_delete, self.ocp_master_node[0], 'secret', self.secret_name)
            sc_parameter = {
                "secretnamespace": sc['secretnamespace'],
                "secretname": self.secret_name,
                "volumetype": "replicate:3"
            }
        elif vol_type == "glusterblock":
            sc = self.storage_classes.get(
                'storage_class2',
                self.storage_classes.get('block_storage_class'))

            # Create secret file for usage in storage class
            self.secret_name = oc_create_secret(
                self.ocp_master_node[0],
                namespace=sc.get('restsecretnamespace', 'default'),
                data_key=self.heketi_cli_key,
                secret_type=sc.get('provisioner', 'gluster.org/glusterblock'))
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
        event_reason = 'ProvisioningFailed'
        if success:
            event_reason = 'ProvisioningSucceeded'
        wait_for_events(self.ocp_master_node[0],
                        obj_name=self.pvc_name,
                        obj_type='PersistentVolumeClaim',
                        event_reason=event_reason)

    def validate_gluster_block_volume_info(self, assertion_method, key, value):
        """Validates block volume info paramters value

        Args:
            assertion_method (func): assert method to be asserted
            key (str): block volume parameter to be asserted with value
            value (str): block volume parameter value to be asserted
        """
        # get block hosting volume of pvc created above
        gluster_blockvol_info = get_gluster_blockvol_info_by_pvc_name(
            self.ocp_master_node[0], self.heketi_server_url, self.pvc_name
        )

        # asserts value and keys
        assertion_method(gluster_blockvol_info[key], value)

    def validate_multipath_info(self, hacount):
        """validates multipath command on the pod node

        Args:
            hacount (int): hacount for which multipath to be checked
        """
        # create pod using pvc created
        dc_name = oc_create_app_dc_with_io(
            self.ocp_master_node[0], self.pvc_name
        )
        pod_name = get_pod_name_from_dc(self.ocp_master_node[0], dc_name)
        self.addCleanup(oc_delete, self.ocp_master_node[0], "dc", dc_name)
        self.addCleanup(
            scale_dc_pod_amount_and_wait, self.ocp_master_node[0], dc_name, 0
        )

        wait_for_pod_be_ready(
            self.ocp_master_node[0], pod_name, timeout=120, wait_step=3
        )

        # validates multipath for pod created with hacount
        self.assertTrue(
            validate_multipath_pod(self.ocp_master_node[0], pod_name, hacount),
            "multipath validation failed"
        )

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
        self.create_sc_with_parameter("glusterfile", parameter=parameter)

    @ddt.data(
        {"resturl": "http://10.0.0.1:8080"},
        {"restsecretname": "fakerestsecretname",
         "restsecretnamespace": "fakerestnamespace"},
        {"restuser": "fakeuser"},
        )
    def test_sc_glusterblock_incorrect_parameter(self, parameter={}):
        """ Polarion testcase id- CNS-727,725,728"""
        self.create_sc_with_parameter("glusterblock", parameter=parameter)

    @skip("Blocked by BZ-1609703")
    @ddt.data(1, 2)
    def test_gluster_block_provisioning_with_valid_ha_count(self, hacount):
        '''[CNS-544][CNS-1453] gluster-block provisioning with different valid
           'hacount' values
        '''
        # create storage class and pvc with given parameters
        self.create_sc_with_parameter(
            'glusterblock', success=True, parameter={'hacount': str(hacount)}
        )

        # validate HA parameter with gluster block volume
        self.validate_gluster_block_volume_info(
            self.assertEqual, 'HA', hacount
        )

        # TODO: need more info on hacount=1 for multipath validation hence
        #       skipping multipath validation
        if hacount > 1:
            self.validate_multipath_info(hacount)

    def test_gluster_block_provisioning_with_ha_count_as_glusterpod(self):
        '''[CNS-1443] gluster-block provisioning with "hacount" value equal to
           gluster pods count
        '''
        # get hacount as no of gluster pods the pvc creation
        hacount = get_amount_of_gluster_nodes(self.ocp_master_node[0])

        # create storage class and pvc with given parameters
        self.create_sc_with_parameter(
            'glusterblock', success=True, parameter={'hacount': str(hacount)}
        )

        # validate HA parameter with gluster block volume
        self.validate_gluster_block_volume_info(
            self.assertEqual, 'HA', hacount
        )
        self.validate_multipath_info(hacount)

    @skip("Blocked by BZ-1644685")
    def test_gluster_block_provisioning_with_invalid_ha_count(self):
        '''[CNS-1444] gluster-block provisioning with any invalid 'hacount'
           value
        '''
        # get hacount as no of gluster pods + 1 to fail the pvc creation
        hacount = get_amount_of_gluster_nodes(self.ocp_master_node[0]) + 1

        # create storage class and pvc with given parameters
        self.create_sc_with_parameter(
            'glusterblock', parameter={'hacount': str(hacount)}
        )

    @ddt.data('true', 'false', '')
    def test_gluster_block_chapauthenabled_parameter(self, chapauthenabled):
        '''[CNS-545][CNS-1445][CNS-1446] gluster-block provisioning with
           different 'chapauthenabled' values
        '''
        parameter = {}
        if chapauthenabled:
            parameter = {"chapauthenabled": chapauthenabled}

        # create storage class and pvc with given parameters
        self.create_sc_with_parameter(
            "glusterblock", success=True, parameter=parameter
        )

        if chapauthenabled == 'true' or chapauthenabled == '':
            # validate if password is set in gluster block volume info
            self.validate_gluster_block_volume_info(
                self.assertNotEqual, 'PASSWORD', ''
            )
        elif chapauthenabled == 'false':
            # validate if password is not set in gluster block volume info
            self.validate_gluster_block_volume_info(
                self.assertEqual, 'PASSWORD', ''
            )
        else:
            raise AssertionError(
                "Invalid chapauthenabled value '%s'" % chapauthenabled
            )
