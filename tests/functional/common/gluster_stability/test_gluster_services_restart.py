
import ddt
import re

from cnslibs.common.heketi_ops import (
    heketi_blockvolume_list,
    match_heketi_and_gluster_block_volumes
)
from cnslibs.common.openshift_ops import (
    check_service_status,
    get_ocp_gluster_pod_names,
    get_pod_name_from_dc,
    match_pv_and_heketi_block_volumes,
    match_pvc_and_pv,
    oc_create_app_dc_with_io,
    oc_create_pvc,
    oc_create_sc,
    oc_create_secret,
    oc_delete,
    oc_get_yaml,
    restart_service_on_pod,
    scale_dc_pod_amount_and_wait,
    verify_pvc_status_is_bound,
    wait_for_pod_be_ready,
    wait_for_resource_absence
)
from cnslibs.cns.cns_baseclass import CnsBaseClass
from cnslibs.common import podcmd

HEKETI_BLOCK_VOLUME_REGEX = "^Id:(.*).Cluster:(.*).Name:%s_(.*)$"

SERVICE_TARGET = "gluster-block-target"
SERVICE_BLOCKD = "gluster-blockd"
SERVICE_TCMU = "tcmu-runner"


@ddt.ddt
class GlusterStabilityTestSetup(CnsBaseClass):
    """class for gluster stability (restarts different servces) testcases
       TC No's: CNS-1393, CNS-1394, CNS-1395
    """

    def setUp(self):
        """Deploys, Verifies and adds resources required for testcases
           in cleanup method
        """
        self.oc_node = self.ocp_master_node[0]
        self.gluster_pod = get_ocp_gluster_pod_names(self.oc_node)[0]

        # prefix used to create resources, generating using glusto_test_id
        # which uses time and date of test case
        self.prefix = "autotest-%s" % (self.glustotest_run_id.replace("_", ""))

        _cns_storage_class = self.cns_storage_class.get(
            'storage_class2',
            self.cns_storage_class.get('block_storage_class'))
        self.provisioner = _cns_storage_class["provisioner"]
        self.restsecretnamespace = _cns_storage_class["restsecretnamespace"]
        self.restuser = _cns_storage_class["restuser"]
        self.resturl = _cns_storage_class["resturl"]

        # using pvc size count as 1 by default
        self.pvcsize = 1

        # using pvc count as 10 by default
        self.pvccount = 10

        # create gluster block storage class, PVC and user app pod
        self.sc_name, self.pvc_name, self.dc_name, self.secret_name = (
            self.deploy_resouces()
        )

        # verify storage class
        oc_get_yaml(self.oc_node, "sc", self.sc_name)

        # verify pod creation, it's state and get the pod name
        self.pod_name = get_pod_name_from_dc(
            self.oc_node, self.dc_name, timeout=180, wait_step=3
        )
        wait_for_pod_be_ready(
            self.oc_node, self.pod_name, timeout=180, wait_step=3
        )
        verify_pvc_status_is_bound(self.oc_node, self.pvc_name)

        # create pvc's to test
        self.pvc_list = []
        for pvc in range(self.pvccount):
            test_pvc_name = oc_create_pvc(
                self.oc_node, self.sc_name,
                pvc_name_prefix=self.prefix, pvc_size=self.pvcsize
            )
            self.pvc_list.append(test_pvc_name)
            self.addCleanup(
                wait_for_resource_absence, self.oc_node, "pvc", test_pvc_name,
                timeout=600, interval=10
            )

        for pvc_name in self.pvc_list:
            self.addCleanup(oc_delete, self.oc_node, "pvc", pvc_name)

    def deploy_resouces(self):
        """Deploys required resources storage class, pvc and user app
           with continous I/O runnig

        Returns:
            sc_name (str): deployed storage class name
            pvc_name (str): deployed persistent volume claim name
            dc_name (str): deployed deployment config name
            secretname (str): created secret file name
        """
        secretname = oc_create_secret(
            self.oc_node, namespace=self.restsecretnamespace,
            data_key=self.heketi_cli_key, secret_type=self.provisioner)
        self.addCleanup(oc_delete, self.oc_node, 'secret', secretname)

        sc_name = oc_create_sc(
            self.oc_node,
            sc_name_prefix=self.prefix, provisioner=self.provisioner,
            resturl=self.resturl, restuser=self.restuser,
            restsecretnamespace=self.restsecretnamespace,
            restsecretname=secretname, volumenameprefix=self.prefix
        )
        self.addCleanup(oc_delete, self.oc_node, "sc", sc_name)

        pvc_name = oc_create_pvc(
            self.oc_node, sc_name,
            pvc_name_prefix=self.prefix, pvc_size=self.pvcsize
        )
        self.addCleanup(
            wait_for_resource_absence, self.oc_node, "pvc", pvc_name,
            timeout=120, interval=5
        )
        self.addCleanup(oc_delete, self.oc_node, "pvc", pvc_name)

        dc_name = oc_create_app_dc_with_io(
            self.oc_node, pvc_name, dc_name_prefix=self.prefix
        )
        self.addCleanup(oc_delete, self.oc_node, "dc", dc_name)
        self.addCleanup(scale_dc_pod_amount_and_wait, self.oc_node, dc_name, 0)

        return sc_name, pvc_name, dc_name, secretname

    def get_heketi_block_volumes(self):
        """lists heketi block volumes

        Returns:
            list : list of ids of heketi block volumes
        """
        heketi_cmd_out = heketi_blockvolume_list(
            self.heketi_client_node,
            self.heketi_server_url,
            secret=self.heketi_cli_key,
            user=self.heketi_cli_user
        )

        self.assertTrue(heketi_cmd_out, "failed to get block volume list")

        heketi_block_volume_ids = []
        heketi_block_volume_names = []
        for block_vol in heketi_cmd_out.split("\n"):
            heketi_vol_match = re.search(
                HEKETI_BLOCK_VOLUME_REGEX % self.prefix, block_vol.strip()
            )
            if heketi_vol_match:
                heketi_block_volume_ids.append(
                    (heketi_vol_match.group(1)).strip()
                )
                heketi_block_volume_names.append(
                    (heketi_vol_match.group(3)).strip()
                )

        return (sorted(heketi_block_volume_ids), sorted(
            heketi_block_volume_names)
        )

    def validate_volumes_and_blocks(self):
        """Validates PVC and block volumes generated through heketi and OCS
        """

        # verify pvc status is in "Bound" for all the pvc
        for pvc in self.pvc_list:
            verify_pvc_status_is_bound(
                self.oc_node, pvc, timeout=300, wait_step=10
            )

        # validate pvcs and pvs created on OCS
        match_pvc_and_pv(self.oc_node, self.prefix)

        # get list of block volumes using heketi
        heketi_block_volume_ids, heketi_block_volume_names = (
            self.get_heketi_block_volumes()
        )

        # validate block volumes listed by heketi and pvs
        match_pv_and_heketi_block_volumes(
            self.oc_node, heketi_block_volume_ids, self.prefix
        )

        # validate block volumes listed by heketi and gluster
        gluster_pod_obj = podcmd.Pod(self.heketi_client_node, self.gluster_pod)
        match_heketi_and_gluster_block_volumes(
            gluster_pod_obj, heketi_block_volume_names, "%s_" % self.prefix
        )

    @ddt.data(SERVICE_BLOCKD, SERVICE_TCMU, SERVICE_TARGET)
    def test_restart_services_provision_volume_and_run_io(self, service):
        """[CNS-1393-1395] Restart gluster service then validate volumes
        """
        # restarts glusterfs service
        restart_service_on_pod(self.oc_node, self.gluster_pod, service)

        # wait for deployed user pod to be in Running state after restarting
        # service
        wait_for_pod_be_ready(
            self.oc_node, self.pod_name, timeout=60, wait_step=5
        )

        # checks if all glusterfs services are in running state
        for service in (SERVICE_BLOCKD, SERVICE_TCMU, SERVICE_TARGET):
            status = "exited" if service == SERVICE_TARGET else "running"
            self.assertTrue(
                check_service_status(
                    self.oc_node, self.gluster_pod, service, status
                ),
                "service %s is not in %s state" % (service, status)
            )

        # validates pvc, pv, heketi block and gluster block count after
        # service restarts
        self.validate_volumes_and_blocks()
