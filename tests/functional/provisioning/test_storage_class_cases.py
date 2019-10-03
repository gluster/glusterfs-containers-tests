import ddt
from glusto.core import Glusto as g

from openshiftstoragelibs.exceptions import ExecutionError
from openshiftstoragelibs.heketi_ops import (
    heketi_blockvolume_info,
    heketi_cluster_list,
    heketi_volume_info,
    verify_volume_name_prefix,
)
from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.openshift_storage_libs import (
    get_iscsi_block_devices_by_path,
    get_mpath_name_from_device_name,
    validate_multipath_pod,
)
from openshiftstoragelibs.openshift_ops import (
    get_amount_of_gluster_nodes,
    get_gluster_blockvol_info_by_pvc_name,
    get_pod_name_from_dc,
    get_pv_name_from_pvc,
    oc_create_app_dc_with_io,
    oc_create_pvc,
    oc_create_sc,
    oc_create_secret,
    oc_delete,
    oc_get_custom_resource,
    oc_get_pods,
    scale_dc_pod_amount_and_wait,
    verify_pvc_status_is_bound,
    wait_for_events,
    wait_for_pod_be_ready,
    wait_for_resource_absence,
)
from openshiftstoragelibs.openshift_storage_version import (
    get_openshift_storage_version
)
from openshiftstoragelibs.openshift_version import get_openshift_version
from openshiftstoragelibs import utils


@ddt.ddt
class TestStorageClassCases(BaseClass):

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

        # Get pod info
        pod_info = oc_get_pods(
            self.ocp_master_node[0], selector='deploymentconfig=%s' % dc_name)
        node = pod_info[pod_name]['node']

        # Find iqn from volume info
        pv_name = get_pv_name_from_pvc(self.ocp_master_node[0], self.pvc_name)
        custom = [r':.metadata.annotations."gluster\.org\/volume\-id"']
        vol_id = oc_get_custom_resource(
            self.ocp_master_node[0], 'pv', custom, pv_name)[0]
        vol_info = heketi_blockvolume_info(
            self.heketi_client_node, self.heketi_server_url, vol_id, json=True)
        iqn = vol_info['blockvolume']['iqn']

        # Get the paths info from the node
        devices = get_iscsi_block_devices_by_path(node, iqn).keys()
        self.assertEqual(hacount, len(devices))

        # Validate mpath
        mpaths = set()
        for device in devices:
            mpaths.add(get_mpath_name_from_device_name(node, device))
        self.assertEqual(1, len(mpaths))
        validate_multipath_pod(
            self.ocp_master_node[0], pod_name, hacount, list(mpaths)[0])

    @ddt.data(
        {"volumetype": "dist-rep:3"},
        {"resturl": "http://10.0.0.1:8080"},
        {"secretname": "fakesecretname"},
        {"secretnamespace": "fakenamespace"},
        {"restuser": "fakeuser"},
        {"volumenameprefix": "dept_qe"},
        {"clusterids": "123456789abcdefg"},
    )
    def test_sc_glusterfile_incorrect_parameter(self, parameter={}):
        """Validate glusterfile storage with different incorrect parameters"""
        self.create_sc_with_parameter("glusterfile", parameter=parameter)

    @ddt.data(
        {"resturl": "http://10.0.0.1:8080"},
        {"restsecretname": "fakerestsecretname",
         "restsecretnamespace": "fakerestnamespace"},
        {"restuser": "fakeuser"},
        {"clusterids": "123456789abcdefg"},
    )
    def test_sc_glusterblock_incorrect_parameter(self, parameter={}):
        """Validate glusterblock storage with different incorrect parameters"""
        self.create_sc_with_parameter("glusterblock", parameter=parameter)

    @ddt.data(1, 2)
    def test_gluster_block_provisioning_with_valid_ha_count(self, hacount):
        """Validate gluster-block provisioning with different valid 'hacount'
           values
        """
        # TODO(vamahaja): Add check for CRS version
        if not self.is_containerized_gluster():
            self.skipTest(
                "Skipping this test case as CRS version check "
                "is not implemented")

        if hacount > 1 and get_openshift_storage_version() <= "3.9":
            self.skipTest(
                "Skipping this test case as multipath validation "
                "is not supported in OCS 3.9")

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
        """Validate gluster-block provisioning with "hacount" value equal
           to gluster pods count
        """
        # TODO(vamahaja): Add check for CRS version
        if not self.is_containerized_gluster():
            self.skipTest(
                "Skipping this test case as CRS version check "
                "is not implemented")

        if get_openshift_storage_version() <= "3.9":
            self.skipTest(
                "Skipping this test case as multipath validation "
                "is not supported in OCS 3.9")

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

    def test_gluster_block_provisioning_with_invalid_ha_count(self):
        """Validate gluster-block provisioning with any invalid 'hacount'
           value
        """
        # TODO(vamahaja): Add check for CRS version
        if not self.is_containerized_gluster():
            self.skipTest(
                "Skipping this test case as CRS version check "
                "is not implemented")

        if get_openshift_storage_version() <= "3.9":
            self.skipTest(
                "Skipping this test case as multipath validation "
                "is not supported in OCS 3.9")

        # get hacount as no of gluster pods + 1 to fail the pvc creation
        gluster_pod_count = get_amount_of_gluster_nodes(
            self.ocp_master_node[0])
        hacount = gluster_pod_count + 1

        # create storage class and pvc with given parameters
        self.create_sc_with_parameter(
            'glusterblock', success=True, parameter={'hacount': str(hacount)}
        )

        # validate HA parameter with gluster block volume
        self.validate_gluster_block_volume_info(
            self.assertEqual, 'HA', gluster_pod_count
        )
        self.validate_multipath_info(gluster_pod_count)

    @ddt.data('true', 'false', '')
    def test_gluster_block_chapauthenabled_parameter(self, chapauthenabled):
        """Validate gluster-block provisioning with different
           'chapauthenabled' values
        """
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

    def test_create_and_verify_pvc_with_volume_name_prefix(self):
        """create and verify pvc with volname prefix on an app pod"""
        if get_openshift_version() < "3.9":
            self.skipTest(
                "'volumenameprefix' option for Heketi is not supported"
                " in OCP older than 3.9")

        sc_name = self.create_storage_class(create_vol_name_prefix=True)
        pvc_name = self.create_and_wait_for_pvc(sc_name=sc_name)
        namespace = (self.sc.get(
            'secretnamespace',
            self.sc.get('restsecretnamespace', 'default')))
        verify_volume_name_prefix(
            self.heketi_client_node,
            self.sc.get("volumenameprefix", "autotest"),
            namespace, pvc_name, self.heketi_server_url)
        self.create_dc_with_pvc(pvc_name)
        pv_name = get_pv_name_from_pvc(self.ocp_master_node[0], pvc_name)
        endpoint = oc_get_custom_resource(
            self.ocp_master_node[0], "pv", ":spec.glusterfs.endpoints",
            name=pv_name)
        self.assertTrue(
            endpoint,
            "Failed to read Endpoints of %s on  %s " % (
                pv_name, self.ocp_master_node[0]))

    def test_try_to_create_sc_with_duplicated_name(self):
        """Verify SC creation fails with duplicate name"""
        sc_name = "test-sc-duplicated-name-" + utils.get_random_str()
        self.create_storage_class(sc_name=sc_name)
        self.create_and_wait_for_pvc(sc_name=sc_name)
        with self.assertRaises(AssertionError):
            self.create_storage_class(sc_name=sc_name)

    @ddt.data('secretName', 'secretNamespace', None)
    def test_sc_glusterfile_missing_parameter(self, parameter):
        """Validate glusterfile storage with missing parameters"""
        node, sc = self.ocp_master_node[0], self.sc
        secret_name = self.create_secret()

        parameters = {'resturl': sc['resturl'], 'restuser': sc['restuser']}
        if parameter == 'secretName':
            parameters['secretName'] = secret_name
        elif parameter == 'secretNamespace':
            parameters['secretNamespace'] = sc['secretnamespace']

        sc_name = oc_create_sc(node, **parameters)
        self.addCleanup(oc_delete, node, 'sc', sc_name)

        # Create PVC
        pvc_name = oc_create_pvc(node, sc_name)
        self.addCleanup(wait_for_resource_absence, node, 'pvc', pvc_name)
        self.addCleanup(oc_delete, node, 'pvc', pvc_name)

        # Wait for event with error
        wait_for_events(
            node, obj_name=pvc_name, obj_type='PersistentVolumeClaim',
            event_reason='ProvisioningFailed')

        # Verify PVC did not get bound
        with self.assertRaises(ExecutionError):
            verify_pvc_status_is_bound(node, pvc_name, timeout=1)

    def test_sc_create_with_clusterid(self):
        """Create storage class with 'cluster id'"""
        h_cluster_list = heketi_cluster_list(
            self.heketi_client_node, self.heketi_server_url, json=True)
        self.assertTrue(h_cluster_list, "Failed to list heketi cluster")
        cluster_id = h_cluster_list["clusters"][0]
        sc = self.create_storage_class(clusterid=cluster_id)
        pvc_name = self.create_and_wait_for_pvc(sc_name=sc)

        # Validate if cluster id is correct in the heketi volume info
        pv_name = get_pv_name_from_pvc(self.ocp_master_node[0], pvc_name)
        volume_id = oc_get_custom_resource(
            self.ocp_master_node[0], 'pv',
            r':metadata.annotations."gluster\.kubernetes\.io'
            r'\/heketi-volume-id"', name=pv_name)[0]
        volume_info = heketi_volume_info(
            self.heketi_client_node, self.heketi_server_url, volume_id,
            json=True)

        self.assertEqual(cluster_id, volume_info["cluster"],
                         "Cluster ID %s has NOT been used to"
                         "create the PVC %s. Found %s" %
                         (cluster_id, pvc_name, volume_info["cluster"]))
