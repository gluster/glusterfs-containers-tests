
from glusto.core import Glusto as g
from cnslibs.common import exceptions
from cnslibs.cns.cns_baseclass import CnsBaseClass
from cnslibs.common.openshift_ops import (
    wait_for_resource_absence,
    oc_create_app_dc_with_io,
    oc_create_sc,
    oc_create_pvc,
    oc_delete,
    oc_get_yaml
)
from cnslibs.common.dynamic_provisioning import (
    verify_pvc_status_is_bound,
    wait_for_pod_be_ready,
    get_pod_name_from_dc
)

BLOCK_PROVISIONER = "gluster.org/glusterblock"
SECRET_NAME = "heketi-storage-admin-secret-block"
SECRET_NAMESPACE = "glusterfs"
REST_USER = "admin"
HEKETI_STORAGE_REOUTE = "heketi-storage"
PVC_SIZE = 10


class GlustorStabilityTestSetup(CnsBaseClass):
    """class for gluster stability (restarts different servces) testcases
       TC No's: CNS-1393, CNS-1394, CNS-1395
    """

    def setUp(self):
        """ Deploys, Verifies and adds resources required for testcases
            in cleanup method
        """
        self.oc_node = self.ocp_master_node[0]

        # step 1: Create a PVC using the gluster block storage class of
        #         size 10GB
        # step 6: Create an app pod that uses the PVC
        # step 10: Run a loop to run continuos I/O on the app pod
        self.sc_name, self.pvc_name, self.dc_name = self.deploy_resouces()

        # Aditional step to verify storage class
        oc_get_yaml(self.oc_node, "sc", self.sc_name)

        # step 7: To verify pod creation
        self.pod_name = get_pod_name_from_dc(
            self.oc_node,
            self.dc_name
        )
        wait_for_pod_be_ready(self.oc_node, self.pod_name)

        # step 2: To verify pvc creation
        # step 3: Verify creation of PV
        # step 4: Validate creation of endpoints and services
        # step 5: To verify volume creation
        verify_pvc_status_is_bound(self.oc_node, self.pvc_name)

        # step 16: Stop the I/O and delete the app pod alongwith the
        #          deployment config
        # step 17: Delete all the PVCs
        # step 18: Verify deletion of PVC
        # step 19: Verify whether the PV, and blockvolume from heketi
        #           and gluster have been deleted
        self.addCleanup(
            self.cleanup_resources,
            self.sc_name,
            self.pvc_name,
            self.dc_name,
            self.pod_name
        )

        # step 11: Run a loop to create 10 block PVCs
        self.pvc_list = []
        for pvc in range(10):
            test_pvc_name = oc_create_pvc(
                self.oc_node,
                self.sc_name,
                pvc_size=PVC_SIZE
            )
            self.pvc_list.append(test_pvc_name)
            self.addCleanup(oc_delete, self.oc_node, "pvc", test_pvc_name)

    def deploy_resouces(self):
        """Deploys required resources storage class, pvc and cirros app
        Returns:
            sc_name (str): deployed storage class name
            pvc_name (str): deployed persistent volume claim name
            dc_name (str): deployed deployment config name
        """
        rest_url = oc_get_yaml(self.oc_node, "route", HEKETI_STORAGE_REOUTE)
        sc_name = oc_create_sc(
            self.oc_node,
            provisioner=BLOCK_PROVISIONER,
            resturl="http://%s" % rest_url["spec"]["host"],
            restuser=REST_USER,
            restsecretnamespace=SECRET_NAMESPACE,
            restsecretname=SECRET_NAME
        )

        pvc_name = oc_create_pvc(
            self.oc_node,
            sc_name,
            pvc_size=PVC_SIZE
        )

        dc_name = oc_create_app_dc_with_io(
            self.oc_node,
            pvc_name
        )

        return sc_name, pvc_name, dc_name

    def cleanup_resources(self, sc_name, pvc_name, dc_name, pod_name):
        """cleanup resources deployed for tests
        Args:
            sc_name (str): deployed storage class name needs to be deleted
            pvc_name (str): deployed persistent volume claim name needs to
                            be deleted
            dc_name (str): deployed deployment config name needs to be deleted
            pod_name (str): deployed pod name to be deleted
        Raises:
            Exception if failed to clean any resource
        """
        try:
            oc_delete(self.oc_node, "dc", dc_name)
            wait_for_resource_absence(self.oc_node, "pod", pod_name)
        except exceptions.ExecutionError:
            g.log.error("failed to delete pod %s" % pod_name)

        try:
            oc_delete(self.oc_node, "pvc", pvc_name)
            wait_for_resource_absence(self.oc_node, "pvc", pvc_name)
        except AssertionError:
            g.log.info("pvc %s not present" % pvc_name)
        except exceptions.ExecutionError:
            g.log.error("failed to delete pvc %s" % pvc_name)

        try:
            oc_delete(self.oc_node, "sc", sc_name)
        except AssertionError:
            g.log.info("storage class %s not present" % sc_name)

    def test_restart_blockd_service_provision_volume_and_run_io(self):
        """
        """

    def test_restart_tcmu_service_provision_volume_and_run_io(self):
        """
        """

    def test_restart_blocktargetd_service_provision_volume_and_run_io(self):
        """
        """
