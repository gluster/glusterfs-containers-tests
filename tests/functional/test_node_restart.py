import time
from unittest import skip

from glusto.core import Glusto as g

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.exceptions import ExecutionError
from openshiftstoragelibs.openshift_ops import (
    check_service_status_on_pod,
    get_ocp_gluster_pod_names,
    oc_rsh,
    wait_for_pod_be_ready,
)
from openshiftstoragelibs.waiter import Waiter


class TestNodeRestart(BaseClass):

    def setUp(self):
        super(TestNodeRestart, self).setUp()
        self.oc_node = self.ocp_master_node[0]

        self.gluster_pod_list = get_ocp_gluster_pod_names(self.oc_node)
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

    def _wait_for_gluster_pod_to_be_ready(self):
        for gluster_pod in self.gluster_pod_list:
            for w in Waiter(timeout=600, interval=10):
                try:
                    success = wait_for_pod_be_ready(
                        self.oc_node, gluster_pod, timeout=1, wait_step=1
                    )
                    if success:
                        break
                except ExecutionError as e:
                    g.log.info("exception %s while validating gluster "
                               "pod %s" % (e, gluster_pod))

            if w.expired:
                error_msg = ("exceeded timeout 600 sec, pod '%s' is "
                             "not in 'running' state" % gluster_pod)
                g.log.error(error_msg)
                raise ExecutionError(error_msg)

    def _node_reboot(self):
        storage_hostname = (g.config["gluster_servers"]
                            [self.gluster_servers[0]]["storage"])

        cmd = "sleep 3; /sbin/shutdown -r now 'Reboot triggered by Glusto'"
        ret, out, err = g.run(storage_hostname, cmd)

        self.addCleanup(self._wait_for_gluster_pod_to_be_ready)

        if ret != 255:
            err_msg = "failed to reboot host %s error: %s" % (
                storage_hostname, err)
            g.log.error(err_msg)
            raise AssertionError(err_msg)

        try:
            g.ssh_close_connection(storage_hostname)
        except Exception as e:
            g.log.error("failed to close connection with host %s"
                        " with error: %s" % (storage_hostname, e))
            raise

        # added sleep as node will restart after 3 sec
        time.sleep(3)

        for w in Waiter(timeout=600, interval=10):
            try:
                if g.rpyc_get_connection(storage_hostname, user="root"):
                    g.rpyc_close_connection(storage_hostname, user="root")
                    break
            except Exception as err:
                g.log.info("exception while getting connection: '%s'" % err)

        if w.expired:
            error_msg = ("exceeded timeout 600 sec, node '%s' is "
                         "not reachable" % storage_hostname)
            g.log.error(error_msg)
            raise ExecutionError(error_msg)

        # wait for the gluster pod to be in 'Running' state
        self._wait_for_gluster_pod_to_be_ready()

        # glusterd and gluster-blockd service should be up and running
        service_names = ("glusterd", "gluster-blockd", "tcmu-runner")
        for gluster_pod in self.gluster_pod_list:
            for service in service_names:
                g.log.info("gluster_pod - '%s' : gluster_service '%s'" % (
                    gluster_pod, service))
                check_service_status_on_pod(
                    self.oc_node, gluster_pod, service, "active", "running"
                )

    @skip("Blocked by BZ-1652913")
    def test_node_restart_check_volume(self):
        df_cmd = "df --out=target | sed 1d | grep /var/lib/heketi"
        fstab_cmd = "grep '%s' /var/lib/heketi/fstab"
        self._check_fstab_and_df_entries(df_cmd, fstab_cmd)

        self._node_reboot()

        fstab_cmd = ("grep '/var/lib/heketi' /var/lib/heketi/fstab "
                     "| cut -f2 -d ' '")
        df_cmd = "df --out=target | sed 1d | grep '%s'"
        self._check_fstab_and_df_entries(fstab_cmd, df_cmd)

        self._create_volumes_with_io(pvc_cnt=1, timeout=300, wait_step=10)
