import ddt

import pytest

from openshiftstoragelibs import command
from openshiftstoragelibs import baseclass
from openshiftstoragelibs import heketi_version
from openshiftstoragelibs import openshift_ops
from openshiftstoragelibs import openshift_version

# The script exec-on-host prevents from executing LVM commands on pod.
# It has been introduced as LVM wrapper in heketi v9.0.0-9
ENV_NAME = "HEKETI_LVM_WRAPPER"
ENV_VALUE = "/usr/sbin/exec-on-host"


@ddt.ddt
class TestHeketiLvmWrapper(baseclass.BaseClass):
    """Class to validate heketi LVM wrapper functionality"""

    def setUp(self):
        super(TestHeketiLvmWrapper, self).setUp()

        self.oc_node = self.ocp_master_node[0]
        self.pod_name = openshift_ops.get_ocp_gluster_pod_details(self.oc_node)
        self.h_pod_name = openshift_ops.get_pod_name_from_dc(
            self.oc_node, self.heketi_dc_name)

        ocp_version = openshift_version.get_openshift_version()
        if ocp_version < "3.11.170":
            self.skipTest("Heketi LVM Wrapper functionality does not "
                          "support on OCP {}".format(ocp_version.v_str))
        h_version = heketi_version.get_heketi_version(self.heketi_client_node)
        if h_version < '9.0.0-9':
            self.skipTest("heketi-client package {} does not support Heketi "
                          "LVM Wrapper functionality".format(h_version.v_str))

    @pytest.mark.tier0
    def test_lvm_script_and_wrapper_environments(self):
        """Validate lvm script present on glusterfs pods
           lvm wrapper environment is present on heketi pod"""

        # Check script /usr/sbin/exec-on-host is present in pod
        if self.is_containerized_gluster():
            cmd = "ls -lrt {}".format(ENV_VALUE)
            ret, _, err = openshift_ops.oc_rsh(
                self.oc_node, self.pod_name[0]['pod_name'], cmd)
            self.assertFalse(
                ret, "failed to execute command {} on pod {} with error:"
                " {}".format(cmd, self.pod_name[0]['pod_name'], err))

        # Get a value associated with HEKETI_LVM_WRAPPER
        custom = (r'":spec.containers[*].env[?(@.name==\"{}\")]'
                  r'.value"'.format(ENV_NAME))
        env_var_value = openshift_ops.oc_get_custom_resource(
            self.oc_node, "pod", custom, self.h_pod_name)

        # Check value /usr/sbin/exec-on-host is present in converged mode
        # and absent in independent mode deployment
        err_msg = "Heketi LVM environment {} match failed".format(ENV_VALUE)
        if self.is_containerized_gluster():
            self.assertEqual(env_var_value[0], ENV_VALUE, err_msg)
        else:
            self.assertIsNotNone(env_var_value[0], err_msg)

    @pytest.mark.tier0
    def test_lvm_script_executable_on_host(self):
        """Validate lvm script is executable on host instead
           of container"""

        # Skip the TC if independent mode deployment
        if not self.is_containerized_gluster():
            self.skipTest(
                "Skipping this test as LVM script is not available in "
                "independent mode deployment")

        pod_name = self.pod_name[0]['pod_name']
        gluster_pod_label = "glusterfs=storage-pod"

        # Remove LVM banaries to validate /usr/sbin/exec-on-host script
        # is execute LVM commands on host instead on pod
        cmd = "rm /usr/sbin/lvm"
        ret, _, err = openshift_ops.oc_rsh(self.oc_node, pod_name, cmd)
        self.addCleanup(
            openshift_ops.wait_for_pods_be_ready, self.oc_node,
            len(self.gluster_servers), gluster_pod_label)
        self.addCleanup(
            openshift_ops.wait_for_resource_absence, self.oc_node, "pod",
            pod_name)
        self.addCleanup(
            openshift_ops.oc_delete, self.oc_node, "pod", pod_name)
        err_msg = (
            "failed to execute command {} on pod {} with error: {}"
            "".format(cmd, pod_name, err))
        self.assertFalse(ret, err_msg)

        # Validate LVM command is not executable in pod
        cmd = "oc rsh {} lvs".format(pod_name)
        stdout = command.cmd_run(cmd, self.oc_node, raise_on_error=False)
        self.assertIn(
            'exec: \\"lvs\\": executable file not found in $PATH', stdout)

        # Run LVM command with /usr/sbin/exec-on-host
        cmd = "{} lvs".format(ENV_VALUE)
        ret, out, err = openshift_ops.oc_rsh(self.oc_node, pod_name, cmd)
        err_msg = (
            "failed to execute command {} on pod {} with error: {}"
            "".format(cmd, pod_name, err))
        self.assertFalse(ret, err_msg)
        self.assertIn("VG", out)
