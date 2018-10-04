from unittest import skip

import ddt
import re
import time

from datetime import datetime
from glusto.core import Glusto as g
from cnslibs.common.heketi_ops import heketi_blockvolume_list
from cnslibs.common.openshift_ops import (
    check_service_status,
    oc_get_custom_resource,
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
    oc_rsh,
    restart_service_on_pod,
    scale_dc_pod_amount_and_wait,
    verify_pvc_status_is_bound,
    wait_for_pod_be_ready,
    wait_for_resource_absence
)
from cnslibs.common.gluster_ops import (
    get_block_hosting_volume_name,
    match_heketi_and_gluster_block_volumes_by_prefix,
    restart_block_hosting_volume,
    restart_brick_process,
    wait_to_heal_complete
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
        self.gluster_pod_obj = podcmd.Pod(self.oc_node, self.gluster_pod)

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

    def get_block_hosting_volume_by_pvc_name(self, pvc_name):
        """Get block hosting volume of pvc name given

        Args:
            pvc_name (str): pvc name of which host name is need
                            to be returned
        """
        pv_name = oc_get_custom_resource(
            self.oc_node, 'pvc', ':.spec.volumeName', name=pvc_name
        )[0]

        block_volume = oc_get_custom_resource(
            self.oc_node, 'pv',
            r':.metadata.annotations."gluster\.org\/volume\-id"',
            name=pv_name
        )[0]

        # get block hosting volume from pvc name
        block_hosting_vol = get_block_hosting_volume_name(
            self.heketi_client_node, self.heketi_server_url,
            block_volume, self.gluster_pod, self.oc_node
        )

        return block_hosting_vol

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
        match_heketi_and_gluster_block_volumes_by_prefix(
            self.gluster_pod_obj, heketi_block_volume_names,
            "%s_" % self.prefix
        )

    def get_io_time(self):
        """Gets last io time of io pod by listing log file directory
           /mnt on pod
        """
        ret, stdout, stderr = oc_rsh(
            self.oc_node, self.pod_name, "ls -l /mnt/ | awk '{print $8}'"
        )
        if ret != 0:
            err_msg = "failed to get io time for pod %s" % self.pod_name
            g.log.error(err_msg)
            raise AssertionError(err_msg)

        get_time = None
        try:
            get_time = datetime.strptime(stdout.strip(), "%H:%M")
        except Exception:
            g.log.error("invalid time format ret %s, stout: %s, "
                        "stderr: %s" % (ret, stdout, stderr))
            raise

        return get_time

    def restart_block_hosting_volume_wait_for_heal(self, block_hosting_vol):
        """restarts block hosting volume and wait for heal to complete

        Args:
            block_hosting_vol (str): block hosting volume which need to
                                     restart
        """
        start_io_time = self.get_io_time()

        restart_block_hosting_volume(self.gluster_pod_obj, block_hosting_vol)

        # Explicit wait to start ios on pvc after volume start
        time.sleep(5)
        resume_io_time = self.get_io_time()

        self.assertGreater(resume_io_time, start_io_time, "IO has not stopped")

        wait_to_heal_complete(self.gluster_pod_obj)

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

    @skip("Blocked by BZ-1634745, BZ-1635736, BZ-1636477")
    def test_target_side_failures_brick_failure_on_block_hosting_volume(self):
        """[CNS-1285] Target side failures - Brick failure on block
           hosting volume
        """
        # get block hosting volume from pvc name
        block_hosting_vol = self.get_block_hosting_volume_by_pvc_name(
            self.pvc_name
        )

        # restarts brick 2 process of block hosting volume
        restart_brick_process(
            self.oc_node, self.gluster_pod_obj, block_hosting_vol
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

    @skip("Blocked by BZ-1634745, BZ-1635736, BZ-1636477")
    def test_start_stop_block_volume_service(self):
        """[CNS-1314] Block hosting volume - stop/start block hosting
           volume when IO's and provisioning are going on
        """
        # get block hosting volume from pvc name
        block_hosting_vol = self.get_block_hosting_volume_by_pvc_name(
            self.pvc_name
        )

        # restarts one of the block hosting volume and checks heal
        self.restart_block_hosting_volume_wait_for_heal(block_hosting_vol)

        # validates pvc, pv, heketi block and gluster block count after
        # service restarts
        self.validate_volumes_and_blocks()
