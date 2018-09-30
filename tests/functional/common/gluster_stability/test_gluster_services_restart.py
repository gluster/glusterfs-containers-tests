
import re
import json

from glusto.core import Glusto as g
from glustolibs.gluster.block_ops import block_list
from glustolibs.gluster.volume_ops import get_volume_list

from cnslibs.cns.cns_baseclass import CnsBaseClass
from cnslibs.common.heketi_ops import heketi_blockvolume_list
from cnslibs.common.dynamic_provisioning import (
    verify_pvc_status_is_bound,
    wait_for_pod_be_ready,
    get_pod_name_from_dc
)
from cnslibs.common.openshift_ops import (
    get_ocp_gluster_pod_names,
    wait_for_resource_absence,
    oc_create_app_dc_with_io,
    oc_get_custom_resource,
    oc_create_pvc,
    oc_create_sc,
    oc_get_yaml,
    oc_delete,
    oc_rsh
)
from cnslibs.common import exceptions, podcmd
from cnslibs.common.waiter import Waiter

SERVICE_STATUS_REGEX = "Active: active \((.*)\) since .*;.*"
HEKETI_BLOCK_VOLUME_REGEX = "^Id:(.*).Cluster:(.*).Name:blockvol_(.*)$"

SERVICE_TARGET = "gluster-block-target"
SERVICE_BLOCKD = "gluster-blockd"
SERVICE_TCMU = "tcmu-runner"
SERVICE_RESTART = "systemctl restart %s"
SERVICE_STATUS = "systemctl status %s"

BLOCK_PROVISIONER = "gluster.org/glusterblock"
SECRET_NAME = "heketi-storage-admin-secret-block"
SECRET_NAMESPACE = "glusterfs"
REST_USER = "admin"
HEKETI_STORAGE_REOUTE = "heketi-storage"
PVC_SIZE = 10
PREFIX = "autotests-pvc-"


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

    def check_glusterfs_service_status(self, pod_name, service_name,
                                       timeout=180, wait_step=10):
        """ Checks provided gluster service to be in "Running" status
            for given timeout on given pod_name
        Args:
            pod_name (str): Pod name on which service needs to be restarted
            service_name (str): Service which needs to be restarted
            timeout (int): Seconds for which it needs to be wait to be
                           service in "Running" state
            wait_step (int): Interval after every seconds service needs to
                             be checked
        """
        cmd_error_msg = "failed to check service status %s" % service_name
        exp_error_msg = ("exceeded timeout %s for waiting for service %s "
                         "to be in Running status" % (timeout, service_name))

        for w in Waiter(timeout, wait_step):
            ret, stdout, stderr = oc_rsh(
                self.oc_node,
                pod_name,
                SERVICE_STATUS % service_name
            )
            if ret != 0:
                g.log.error(cmd_error_msg)
                AssertionError(cmd_error_msg)

            for line in stdout.splitlines():
                status = re.search(SERVICE_STATUS_REGEX, line)
                if status and status.group(1) == "running":
                    return True

        if w.expired:
            g.log.error(exp_error_msg)
            raise exceptions.ExecutionError(exp_error_msg)

    def restart_glusterfs_service(self, pod_name, service_name):
        """ Restarts given service on geven pod_name
        Args:
            pod_name (str): Pod name on which service needs to be restarted
            service_name (str): Service which needs to be restarted
        """
        error_msg = "failed to restart service %s" % service_name

        ret, stdout, stderr = oc_rsh(
            self.oc_node,
            pod_name,
            SERVICE_RESTART % service_name
        )
        if ret != 0:
            g.log.error(error_msg)
            AssertionError(error_msg)

    def validate_pvc_and_pv(self):
        """ Validates volumes generated by pvc and pv
        """
        all_pvc_list = [
            pvc
            for pvc in oc_get_custom_resource(
                            self.oc_node,
                            "pvc",
                            ".metadata.name"
                        )
            if pvc.startswith(PREFIX)
        ]

        all_pv_list = [
            pv
            for pv in oc_get_custom_resource(
                        self.oc_node,
                        "pv",
                        ".spec.claimRef.name"
                      )
            if pv.startswith(PREFIX)
        ]

        self.assertItemsEqual(
            all_pvc_list,
            all_pv_list,
            "pvc and pv list match failed"
        )

    def get_heketi_block_vol(self):
        """ Lists heketi block volumes
        Returns:
            list : List of ids of heketi block volumes
        """
        heketi_cmd_out = heketi_blockvolume_list(
            self.heketi_client_node,
            self.heketi_server_url,
            secret=self.heketi_cli_key,
            user=self.heketi_cli_user
        )

        heketi_block_vol_list = []
        for block_vol in heketi_cmd_out.split("\n"):
            heketi_vol_match = re.search(
                HEKETI_BLOCK_VOLUME_REGEX,
                block_vol.strip()
            )
            if heketi_vol_match:
                heketi_block_vol_list.append(heketi_vol_match.group(3))

        return heketi_block_vol_list

    def validate_pv_and_heketi_block_volumes(self, heketi_block_vol_list):
        """ Validates block volumes generated by pv and heketi
        Args:
            heketi_block_vol_list (list): List of heketi block volumes
        """
        pv_block_list = [
            pv["metadata"]["annotations"]["gluster.org/volume-id"]
            for pv in oc_get_yaml(self.oc_node, "pv")["items"]
            if (pv["spec"]["claimRef"]["name"].startswith(PREFIX)
                and
                pv["metadata"]["annotations"]
                ["pv.kubernetes.io/provisioned-by"] == BLOCK_PROVISIONER)
        ]

        self.assertListEqual(
            pv_block_list,
            heketi_block_vol_list,
            "pv and heketi block list match failed"
        )

    @podcmd.GlustoPod()
    def validate_heketi_and_gluster_volume(self, gluster_pod,
                                           heketi_block_vol_list):
        """ Validates block volumes from heketi and and gluster
        Args:
            gluster_pod (str): Gluster pod name on which services needs
                               to be validated
            heketi_block_vol_list (list): List of heketi block volumes
        """

        p = podcmd.Pod(self.heketi_client_node, gluster_pod)
        gluster_vol_list = get_volume_list(p)

        gluster_vol_block_list = []
        for gluster_vol in gluster_vol_list[1:]:
            ret, out, err = block_list(p, gluster_vol)
            gluster_vol_block_list.extend([
                block_vol
                for block_vol in json.loads(out)["blocks"]
                if block_vol.startswith("blockvol_")
            ])

        self.assertListEqual(
            gluster_vol_block_list,
            heketi_block_vol_list,
            "gluster and heketi block vol list failed"
        )

    def validate_glusterfs_services(self, gluster_pod, services):
        """ Validates the gluster service status to be in "Running"
        Args:
            gluster_pod (str): Gluster pod name on which services needs
                               to be validated
            services (list): List of services needs to be validated
        """
        service_err_msg = "service %s is not running"

        for service in services:
            if not self.check_glusterfs_service_status(
                        gluster_pod,
                        service
                    ):
                AssertionError(service_err_msg % service)

    def validate_volumes_and_blocks(self, gluster_pod):
        """ Validates PVC volumes and block volumes generated through
            heketi and OCS
        Args:
            gluster_pod (str): Gluster pod name on which services needs
                               to be validated
        """
        # Verify pvc status is in "Bound" for all the pvc
        for pvc in self.pvc_list:
            verify_pvc_status_is_bound(
                self.oc_node,
                pvc,
                timeout=600,
                wait_step=10
            )

        # Validate pvcs and pvs created on OCS
        self.validate_pvc_and_pv()

        # Get list of block volumes using heketi
        heketi_block_vol_list = self.get_heketi_block_vol()

        # Validate block volumes listed by heketi and pvs
        self.validate_pv_and_heketi_block_volumes(heketi_block_vol_list)

        # Validate block volumes listed by heketi and gluster
        self.validate_heketi_and_gluster_volume(
            gluster_pod,
            heketi_block_vol_list
        )

    def test_restart_blockd_service_provision_volume_and_run_io(self):
        """ TC CNS-1393: Restart blockd service and validate services,
            volumes and blocks
        """
        gluster_pod = get_ocp_gluster_pod_names(self.oc_node)[0]

        # step 12: While the I/O is running and the provisioning requests
        #          are being serviced, on one of the gluster pods, restart
        #          gluster-blockd service
        self.restart_glusterfs_service(gluster_pod, SERVICE_BLOCKD)

        # step 13: Check the I/O on the app pod and the provisioning status
        #          while the gluster-blockd service is in the process of
        #          getting restarted
        # step 15: Check the status of I/O on the app pod and the
        #          provisioning status
        wait_for_pod_be_ready(
            self.oc_node,
            self.pod_name,
            timeout=60,
            wait_step=5
        )

        # step 14: Check the status of all the three services on the
        #          gluster pod
        self.validate_glusterfs_services(
            gluster_pod,
            [SERVICE_BLOCKD, SERVICE_TCMU, SERVICE_TARGET]
        )

        # Additional steps to validate pvc, pv, heketi block and gluster
        # block count
        self.validate_volumes_and_blocks(gluster_pod)

    def test_restart_tcmu_service_provision_volume_and_run_io(self):
        """ TC CNS-1394: Restart tcmu service and validate services,
            volumes and blocks
        """
        gluster_pod = get_ocp_gluster_pod_names(self.oc_node)[0]

        # step 12: While the I/O is running and the provisioning requests
        #          are being serviced, on one of the gluster pods, restart
        #          gluster-blockd service
        self.restart_glusterfs_service(gluster_pod, SERVICE_TCMU)

        # step 13: Check the I/O on the app pod and the provisioning status
        #          while the gluster-blockd service is in the process of
        #          getting restarted
        # step 15: Check the status of I/O on the app pod and the
        #          provisioning status
        wait_for_pod_be_ready(
            self.oc_node,
            self.pod_name,
            timeout=60,
            wait_step=5
        )

        # step 14: Check the status of all the three services on the
        #          gluster pod
        self.validate_glusterfs_services(
            gluster_pod,
            [SERVICE_TCMU, SERVICE_BLOCKD, SERVICE_TARGET]
        )

        # Additional steps to validate pvc, pv, heketi block and gluster
        # block count
        self.validate_volumes_and_blocks(gluster_pod)

    def test_restart_blocktarget_service_provision_volume_and_run_io(self):
        """ TC CNS-1395: Restart blocktarget service and validate services,
            volumes and blocks
        """
        gluster_pod = get_ocp_gluster_pod_names(self.oc_node)[0]

        # step 12: While the I/O is running and the provisioning requests
        #          are being serviced, on one of the gluster pods, restart
        #          gluster-blockd service
        self.restart_glusterfs_service(gluster_pod, SERVICE_TARGET)

        # step 13: Check the I/O on the app pod and the provisioning status
        #          while the gluster-blockd service is in the process of
        #          getting restarted
        # step 15: Check the status of I/O on the app pod and the
        #          provisioning status
        wait_for_pod_be_ready(
            self.oc_node,
            self.pod_name,
            timeout=60,
            wait_step=5
        )

        # step 14: Check the status of all the three services on the
        #          gluster pod
        self.validate_glusterfs_services(
            gluster_pod,
            [SERVICE_TARGET, SERVICE_TCMU, SERVICE_BLOCKD]
        )

        # Additional steps to validate pvc, pv, heketi block and gluster
        # block count
        self.validate_volumes_and_blocks(gluster_pod)
