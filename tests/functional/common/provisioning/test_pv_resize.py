import ddt
from cnslibs.common.cns_libs import (
    enable_pvc_resize)
from cnslibs.common.heketi_ops import (
    verify_volume_name_prefix)
from cnslibs.common.openshift_ops import (
    resize_pvc,
    get_pod_name_from_dc,
    get_pv_name_from_pvc,
    oc_create_app_dc_with_io,
    oc_create_pvc,
    oc_create_secret,
    oc_create_sc,
    oc_delete,
    oc_rsh,
    oc_version,
    scale_dc_pod_amount_and_wait,
    verify_pv_size,
    verify_pvc_size,
    verify_pvc_status_is_bound,
    wait_for_pod_be_ready,
    wait_for_resource_absence)
from cnslibs.cns.cns_baseclass import CnsBaseClass
from glusto.core import Glusto as g


@ddt.ddt
class TestPvResizeClass(CnsBaseClass):
    '''
     Class that contain test cases for
     pv resize
    '''
    @classmethod
    def setUpClass(cls):
        super(TestPvResizeClass, cls).setUpClass()
        version = oc_version(cls.ocp_master_node[0])
        if any(v in version for v in ("3.6", "3.7", "3.8")):
            return
        enable_pvc_resize(cls.ocp_master_node[0])

    def setUp(self):
        super(TestPvResizeClass, self).setUp()
        version = oc_version(self.ocp_master_node[0])
        if any(v in version for v in ("3.6", "3.7", "3.8")):
            msg = ("pv resize is not available in openshift "
                   "version %s " % version)
            g.log.error(msg)
            raise self.skipTest(msg)

    def _create_storage_class(self, volname_prefix=False):
        sc = self.cns_storage_class['storage_class1']
        secret = self.cns_secret['secret1']

        # create secret
        self.secret_name = oc_create_secret(
            self.ocp_master_node[0],
            namespace=secret['namespace'],
            data_key=self.heketi_cli_key,
            secret_type=secret['type'])
        self.addCleanup(
            oc_delete, self.ocp_master_node[0], 'secret', self.secret_name)

        # create storageclass
        self.sc_name = oc_create_sc(
            self.ocp_master_node[0], provisioner='kubernetes.io/glusterfs',
            resturl=sc['resturl'], restuser=sc['restuser'],
            secretnamespace=sc['secretnamespace'],
            secretname=self.secret_name,
            allow_volume_expansion=True,
            **({"volumenameprefix": sc['volumenameprefix']}
                if volname_prefix else {})
        )
        self.addCleanup(oc_delete, self.ocp_master_node[0], 'sc', self.sc_name)

        return self.sc_name

    @ddt.data(False, True)
    def test_pv_resize_with_prefix_for_name(self, volname_prefix=False):
        """testcases CNS-1037 and CNS-1038 """
        dir_path = "/mnt/"
        self._create_storage_class(volname_prefix)
        node = self.ocp_master_node[0]

        # Create PVC
        pvc_name = oc_create_pvc(node, self.sc_name, pvc_size=1)
        self.addCleanup(wait_for_resource_absence,
                        node, 'pvc', pvc_name)
        self.addCleanup(oc_delete, node, 'pvc', pvc_name)
        verify_pvc_status_is_bound(node, pvc_name)

        # Create DC with POD and attached PVC to it.
        dc_name = oc_create_app_dc_with_io(node, pvc_name)
        self.addCleanup(oc_delete, node, 'dc', dc_name)
        self.addCleanup(scale_dc_pod_amount_and_wait,
                        node, dc_name, 0)

        pod_name = get_pod_name_from_dc(node, dc_name)
        wait_for_pod_be_ready(node, pod_name)
        if volname_prefix:
            storage_class = self.cns_storage_class['storage_class1']
            ret = verify_volume_name_prefix(node,
                                            storage_class['volumenameprefix'],
                                            storage_class['secretnamespace'],
                                            pvc_name, self.heketi_server_url)
            self.assertTrue(ret, "verify volnameprefix failed")
        cmd = ("dd if=/dev/urandom of=%sfile "
               "bs=100K count=1000") % dir_path
        ret, out, err = oc_rsh(node, pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, node))
        cmd = ("dd if=/dev/urandom of=%sfile2 "
               "bs=100K count=10000") % dir_path
        ret, out, err = oc_rsh(node, pod_name, cmd)
        self.assertNotEqual(ret, 0, " This IO did not fail as expected "
                            "command %s on %s" % (cmd, node))
        pvc_size = 2
        resize_pvc(node, pvc_name, pvc_size)
        verify_pvc_size(node, pvc_name, pvc_size)
        pv_name = get_pv_name_from_pvc(node, pvc_name)
        verify_pv_size(node, pv_name, pvc_size)
        oc_delete(node, 'pod', pod_name)
        wait_for_resource_absence(node, 'pod', pod_name)
        pod_name = get_pod_name_from_dc(node, dc_name)
        wait_for_pod_be_ready(node, pod_name)
        cmd = ("dd if=/dev/urandom of=%sfile_new "
               "bs=50K count=10000") % dir_path
        ret, out, err = oc_rsh(node, pod_name, cmd)
        self.assertEqual(ret, 0, "failed to execute command %s on %s" % (
                             cmd, node))
