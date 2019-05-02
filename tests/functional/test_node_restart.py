from unittest import skip

from glusto.core import Glusto as g

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.exceptions import ExecutionError
from openshiftstoragelibs.node_ops import node_reboot_by_command
from openshiftstoragelibs.openshift_ops import (
    check_service_status_on_pod,
    get_ocp_gluster_pod_details,
    oc_rsh,
    wait_for_pod_be_ready,
)


class TestNodeRestart(BaseClass):

    def setUp(self):
        super(TestNodeRestart, self).setUp()
        self.oc_node = self.ocp_master_node[0]

        self.gluster_pod_list = [
            pod["pod_name"]
            for pod in get_ocp_gluster_pod_details(self.oc_node)]
        if not self.gluster_pod_list:
            self.skipTest("Standalone Gluster is not supported by this test.")
        self.gluster_pod_name = self.gluster_pod_list[0]

        self.sc_name = self.create_storage_class()

        self.pvc_names = self._create_volumes_with_io(3)

    def _create_volumes_with_io(self, pvc_cnt, timeout=120, wait_step=3):
        pvc_names = self.create_and_wait_for_pvcs(
            pvc_amount=pvc_cnt, sc_name=self.sc_name,
            timeout=timeout, wait_step=wait_step
        )
        err_msg = "failed to execute command %s on pod %s with error: %s"
        for pvc_name in pvc_names:
            dc_name, pod_name = self.create_dc_with_pvc(pvc_name)

            # Make sure we are able to work with files
            # on the mounted volume
            filepath = "/mnt/file_for_testing_io.log"
            cmd = "dd if=/dev/urandom of=%s bs=1K count=100" % filepath
            ret, out, err = oc_rsh(self.oc_node, pod_name, cmd)
            self.assertEqual(ret, 0, err_msg % (cmd, pod_name, err))

            cmd = "ls -lrt %s" % filepath
            ret, out, err = oc_rsh(self.oc_node, pod_name, cmd)
            self.assertEqual(ret, 0, err_msg % (cmd, pod_name, err))

        return pvc_names

    def _check_fstab_and_df_entries(self, first_cmd, second_cmd):
        # matches output of "df --out=target" and entries in fstab
        # and vice-versa as per commands given in first_cmd and
        # second_cmd
        err_msg = "failed to execute command: %s with error: %s"

        ret, out, err = oc_rsh(self.oc_node, self.gluster_pod_name, first_cmd)
        self.assertEqual(ret, 0, err_msg % (first_cmd, err))

        for mnt_path in (out.strip()).split("\n"):
            ret, out, err = oc_rsh(
                self.oc_node, self.gluster_pod_name, second_cmd % mnt_path
            )
            self.assertEqual(ret, 0, err_msg % (second_cmd, err))

    def reboot_gluster_node_and_wait_for_services(self):
        gluster_node_ip = (
            g.config["gluster_servers"][self.gluster_servers[0]]["storage"])
        gluster_pod = filter(
            lambda pod: (pod["pod_host_ip"] == gluster_node_ip),
            get_ocp_gluster_pod_details(self.oc_node))
        if not gluster_pod:
            raise ExecutionError(
                "Gluster pod Host IP '%s' not matched." % gluster_node_ip)
        gluster_pod = gluster_pod[0]["pod_name"]
        self.addCleanup(
            wait_for_pod_be_ready, self.oc_node, gluster_pod)
        node_reboot_by_command(gluster_node_ip, timeout=600, wait_step=10)

        # wait for the gluster pod to be in 'Running' state
        wait_for_pod_be_ready(self.oc_node, gluster_pod)

        # glusterd and gluster-blockd service should be up and running
        services = (
            ("glusterd", "running"), ("gluster-blockd", "running"),
            ("tcmu-runner", "running"), ("gluster-block-target", "exited"))
        for service, state in services:
            check_service_status_on_pod(
                self.oc_node, gluster_pod, service, "active", state)

    @skip("Blocked by BZ-1652913")
    def test_node_restart_check_volume(self):
        df_cmd = "df --out=target | sed 1d | grep /var/lib/heketi"
        fstab_cmd = "grep '%s' /var/lib/heketi/fstab"
        self._check_fstab_and_df_entries(df_cmd, fstab_cmd)

        # reboot gluster node
        self.reboot_gluster_node_and_wait_for_services()

        fstab_cmd = ("grep '/var/lib/heketi' /var/lib/heketi/fstab "
                     "| cut -f2 -d ' '")
        df_cmd = "df --out=target | sed 1d | grep '%s'"
        self._check_fstab_and_df_entries(fstab_cmd, df_cmd)

        self._create_volumes_with_io(pvc_cnt=1, timeout=300, wait_step=10)
