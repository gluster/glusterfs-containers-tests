import re

import ddt
from glusto.core import Glusto as g
import pytest


from openshiftstoragelibs import baseclass
from openshiftstoragelibs import command
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import heketi_version
from openshiftstoragelibs import node_ops
from openshiftstoragelibs import openshift_ops
from openshiftstoragelibs import openshift_version
from openshiftstoragelibs import waiter

# The script exec-on-host prevents from executing LVM commands on pod.
# It has been introduced as LVM wrapper in heketi v9.0.0-9
ENV_NAME = "HEKETI_LVM_WRAPPER"
ENV_VALUE = "/usr/sbin/exec-on-host"
ENV_FALSE_VALUE = "/usr/bin/false"
DOCKER_SERVICE = "systemctl {} docker"
SERVICE_STATUS_REGEX = r"Active: (.*) \((.*)\)"


@ddt.ddt
class TestHeketiLvmWrapper(baseclass.BaseClass):
    """Class to validate heketi LVM wrapper functionality"""

    def setUp(self):
        super(TestHeketiLvmWrapper, self).setUp()

        self.oc_node = self.ocp_master_node[0]
        self.pod_name = openshift_ops.get_ocp_gluster_pod_details(self.oc_node)
        self.h_pod_name = openshift_ops.get_pod_name_from_dc(
            self.oc_node, self.heketi_dc_name)
        self.volume_size = 2

        ocp_version = openshift_version.get_openshift_version()
        if ocp_version < "3.11.170":
            self.skipTest("Heketi LVM Wrapper functionality does not "
                          "support on OCP {}".format(ocp_version.v_str))
        h_version = heketi_version.get_heketi_version(self.heketi_client_node)
        if h_version < '9.0.0-9':
            self.skipTest("heketi-client package {} does not support Heketi "
                          "LVM Wrapper functionality".format(h_version.v_str))

    def _check_heketi_pod_to_come_up_after_changing_env(self):
        heketi_pod = openshift_ops.get_pod_names_from_dc(
            self.oc_node, self.heketi_dc_name)[0]
        openshift_ops.wait_for_resource_absence(
            self.oc_node, "pod", heketi_pod)
        new_heketi_pod = openshift_ops.get_pod_names_from_dc(
            self.oc_node, self.heketi_dc_name)[0]
        openshift_ops.wait_for_pod_be_ready(
            self.oc_node, new_heketi_pod, wait_step=20)

    def _wait_for_docker_service_status(self, pod_host_ip, status, state):
        for w in waiter.Waiter(30, 3):
            out = command.cmd_run(DOCKER_SERVICE.format("status"), pod_host_ip)
            for line in out.splitlines():
                status_match = re.search(SERVICE_STATUS_REGEX, line)
                if (status_match and status_match.group(1) == status
                        and status_match.group(2) == state):
                    return True

    def _check_docker_status_is_active(self, pod_host_ip):
        try:
            command.cmd_run(DOCKER_SERVICE.format("is-active"), pod_host_ip)
        except Exception as err:
            if "inactive" in err:
                command.cmd_run(DOCKER_SERVICE.format("start"), pod_host_ip)
                self._wait_for_docker_service_status(
                    pod_host_ip, "active", "running")

    @pytest.mark.tier1
    def test_lvm_script_and_wrapper_environments(self):
        """Validate lvm script present on glusterfs pods
           lvm wrapper environment is present on heketi pod"""

        # Check script /usr/sbin/exec-on-host is present in pod
        if self.is_containerized_gluster():
            cmd = "ls -lrt {}".format(ENV_VALUE)
            ret, out, err = openshift_ops.oc_rsh(
                self.oc_node, self.pod_name[0]['pod_name'], cmd)
            self.assertFalse(
                ret, "failed to execute command {} on pod {} with error:"
                " {}".format(cmd, self.pod_name[0]['pod_name'], err))
            self.assertIn(ENV_VALUE, out)

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

    @pytest.mark.tier1
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

    @pytest.mark.tier1
    @ddt.data(ENV_FALSE_VALUE, ENV_VALUE, "")
    def test_lvm_script_with_wrapper_environment_value(self, env_var_value):
        """Validate the creation, deletion, etc operations when
        HEKETI_LVM_WRAPPER has different values assigned"""

        # Skip the TC if independent mode deployment
        if not self.is_containerized_gluster():
            self.skipTest(
                "Skipping this test as LVM script is not available in "
                "independent mode deployment")

        h_client, h_url = self.heketi_client_node, self.heketi_server_url

        # Set different values to HEKETI_LVM_WRAPPER
        if env_var_value != ENV_VALUE:
            cmd = 'oc set env dc/{} {}={}'
            command.cmd_run(
                cmd.format(self.heketi_dc_name, ENV_NAME, env_var_value),
                self.oc_node)
            self.addCleanup(
                self._check_heketi_pod_to_come_up_after_changing_env)
            self.addCleanup(
                command.cmd_run,
                cmd.format(self.heketi_dc_name, ENV_NAME, ENV_VALUE),
                self.oc_node)
            self._check_heketi_pod_to_come_up_after_changing_env()

        # Get new value associated with HEKETI_LVM_WRAPPER
        heketi_pod = openshift_ops.get_pod_names_from_dc(
            self.oc_node, self.heketi_dc_name)[0]
        custom = (
            "{{.spec.containers[*].env[?(@.name==\"{}\")].value}}".format(
                ENV_NAME))
        cmd = ("oc get pod {} -o=jsonpath='{}'".format(heketi_pod, custom))
        get_env_value = command.cmd_run(cmd, self.oc_node)

        # Validate new value assigned to heketi pod
        err_msg = "Failed to assign new value {} to {}".format(
            env_var_value, heketi_pod)
        self.assertEqual(get_env_value, env_var_value, err_msg)

        # Get the date before creating heketi volume
        cmd_date = "date -u '+%Y-%m-%d %T'"
        _date, _time = command.cmd_run(cmd_date, self.oc_node).split(" ")

        if env_var_value == ENV_FALSE_VALUE:
            # Heketi volume creation should fail when HEKETI_LVM_WRAPPER
            # assigned to /usr/bin/false
            err_msg = "Unexpectedly: volume has been created"
            with self.assertRaises(AssertionError, msg=err_msg):
                vol_info = heketi_ops.heketi_volume_create(
                    h_client, h_url, self.volume_size, json=True)
                self.addCleanup(
                    heketi_ops.heketi_volume_delete, h_client,
                    h_url, vol_info["bricks"][0]["volume"])
        else:
            # Heketi volume creation should succeed when HEKETI_LVM_WRAPPER
            # assigned value other than /usr/bin/false
            vol_info = heketi_ops.heketi_volume_create(
                h_client, h_url, self.volume_size, json=True)
            self.addCleanup(
                heketi_ops.heketi_volume_delete,
                h_client, h_url, vol_info["bricks"][0]["volume"])
            self.assertTrue(vol_info, ("Failed to create heketi "
                            "volume of size {}".format(self.volume_size)))

        # Get heketi logs with specific time
        cmd_logs = "oc logs {} --since-time {}T{}Z | grep {}".format(
            heketi_pod, _date, _time, "/usr/sbin/lvm")

        # Validate assigned value of HEKETI_LVM_WRAPPER is present in
        # heketi log
        for w in waiter.Waiter(60, 10):
            logs = command.cmd_run(cmd_logs, self.oc_node)
            status_match = re.search(env_var_value, logs)
            if status_match:
                break
        err_msg = "Heketi unable to execute LVM commands with {}".format(
            env_var_value)
        self.assertTrue(status_match, err_msg)

    @pytest.mark.tier2
    def test_docker_service_restart(self):
        """Validate docker service should not fail after restart"""

        # Skip the TC if independent mode deployment
        if not self.is_containerized_gluster():
            self.skipTest(
                "Skipping this test case as LVM script is not available in "
                "independent mode deployment")

        # Skip the TC if docker storage driver other than devicemapper
        pod_host_ip = self.pod_name[0]["pod_host_ip"]
        cmd = "docker info -f '{{json .Driver}}'"
        device_driver = command.cmd_run(cmd, pod_host_ip)
        if device_driver != '"devicemapper"':
            self.skipTest(
                "Skipping this test case as docker storage driver is not "
                "set to devicemapper")

        # Validate LVM environment is present
        custom = (r'":spec.containers[*].env[?(@.name==\"{}\")]'
                  r'.value"'.format(ENV_NAME))
        env_var_value = openshift_ops.oc_get_custom_resource(
            self.oc_node, "pod", custom, self.h_pod_name)[0]
        err_msg = "Heketi {} environment should has {}".format(
            ENV_NAME, ENV_VALUE)
        self.assertEqual(env_var_value, ENV_VALUE, err_msg)

        # Check docker status is active
        command.cmd_run(DOCKER_SERVICE.format("is-active"), pod_host_ip)

        # Restart the docker service
        self.addCleanup(self._check_docker_status_is_active, pod_host_ip)
        command.cmd_run(DOCKER_SERVICE.format("restart"), pod_host_ip)

        # Wait for docker service to become active
        self._wait_for_docker_service_status(pod_host_ip, "active", "running")

        # Wait for glusterfs pods to be ready
        openshift_ops.wait_for_pods_be_ready(
            self.oc_node, len(self.gluster_servers), "glusterfs=storage-pod")

        # Check the docker pool is available after docker restart
        cmd = "ls -lrt /dev/docker-vg/docker-pool"
        command.cmd_run(cmd, pod_host_ip)

        # Create PVC after docker restart
        self.create_and_wait_for_pvcs()

    def _check_heketi_and_gluster_pod_after_node_reboot(self, node_name):
        # Wait for heketi pod to become ready and running
        heketi_pod = openshift_ops.get_pod_names_from_dc(
            self.oc_node, self.heketi_dc_name)[0]
        openshift_ops.wait_for_pod_be_ready(self.oc_node, heketi_pod)
        heketi_ops.hello_heketi(self.oc_node, self.heketi_server_url)

        # Wait for glusterfs pods to become ready if hosted on same node
        heketi_node_ip = openshift_ops.oc_get_custom_resource(
            self.oc_node, 'pod', '.:status.hostIP', heketi_pod)[0]
        if heketi_node_ip in self.gluster_servers:
            gluster_pod = openshift_ops.get_gluster_pod_name_for_specific_node(
                self.oc_node, node_name)

            # Wait for glusterfs pod to become ready
            openshift_ops.wait_for_pod_be_ready(self.oc_node, gluster_pod)
            services = (
                ("glusterd", "running"), ("gluster-blockd", "running"),
                ("tcmu-runner", "running"), ("gluster-block-target", "exited"))
            for service, state in services:
                openshift_ops.check_service_status_on_pod(
                    self.oc_node, gluster_pod, service, "active", state)

    @pytest.mark.tier2
    def test_udev_usage_in_container(self):
        """Validate LVM inside container does not use udev"""

        # Skip the TC if independent mode deployment
        if not self.is_containerized_gluster():
            self.skipTest(
                "Skipping this test case as it needs to run on "
                "converged mode deployment")

        h_client, h_url = self.heketi_client_node, self.heketi_server_url
        server_info = list(g.config.get('gluster_servers').values())[0]
        server_node = server_info.get('manage')
        additional_device = server_info.get('additional_devices')[0]

        # Get pod name from on host
        for pod_info in self.pod_name:
            if pod_info.get('pod_hostname') == server_node:
                pod_name = pod_info.get('pod_name')
                break

        # Create file volume
        vol_info = heketi_ops.heketi_volume_create(
            h_client, h_url, self.volume_size, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete, h_client, h_url,
            vol_info.get("id"))

        # Create block volume
        block_vol_info = heketi_ops.heketi_blockvolume_create(
            h_client, h_url, self.volume_size, json=True)
        self.addCleanup(
            heketi_ops.heketi_blockvolume_delete, h_client, h_url,
            block_vol_info.get("id"))

        # Check dmeventd service in container
        err_msg = "dmeventd.service is running on setup"
        with self.assertRaises(AssertionError, msg=err_msg):
            openshift_ops.oc_rsh(
                self.oc_node, pod_name, "systemctl is-active dmeventd.service")

        # Service dmeventd should not be running in background
        with self.assertRaises(AssertionError, msg=err_msg):
            openshift_ops.oc_rsh(
                self.oc_node, pod_name, "ps aux | grep dmeventd.service")

        # Perform a pvscan in contaier
        openshift_ops.oc_rsh(
            self.oc_node, pod_name, "pvscan")

        # Get heketi node to add new device
        heketi_node_list = heketi_ops.heketi_node_list(h_client, h_url)
        for h_node_id in heketi_node_list:
            h_node_info = heketi_ops.heketi_node_info(
                h_client, h_url, h_node_id, json=True)
            h_node_host = h_node_info.get('hostnames', {}).get('manage')[0]
            if h_node_host == server_node:
                break

        # Add new device to the node
        heketi_ops.heketi_device_add(
            h_client, h_url, additional_device, h_node_id)
        h_node_info = heketi_ops.heketi_node_info(
            h_client, h_url, h_node_id, json=True)
        h_device_id = [
            device.get('id')
            for device in h_node_info.get('devices')
            if device.get('name') == additional_device]
        self.addCleanup(
            heketi_ops.heketi_device_delete, h_client, h_url, h_device_id[0])
        self.addCleanup(
            heketi_ops.heketi_device_remove, h_client, h_url, h_device_id[0])
        self.addCleanup(
            heketi_ops.heketi_device_disable, h_client, h_url, h_device_id[0])

        # Reboot the node on which device is added
        self.addCleanup(
            self._check_heketi_and_gluster_pod_after_node_reboot, server_node)
        node_ops.node_reboot_by_command(server_node)

        # Wait node to become NotReady
        custom = r'":.status.conditions[?(@.type==\"Ready\")]".status'
        for w in waiter.Waiter(300, 10):
            status = openshift_ops.oc_get_custom_resource(
                self.oc_node, 'node', custom, server_node)
            if status[0] == 'False':
                break
        if w.expired:
            raise exceptions.ExecutionError(
                "Failed to bring node down {}".format(server_node))

        # Wait for node to become ready
        openshift_ops.wait_for_ocp_node_be_ready(self.oc_node, server_node)

        # Wait for heketi and glusterfs pod to become ready
        self._check_heketi_and_gluster_pod_after_node_reboot(server_node)

        # Perform a pvscan in contaier
        openshift_ops.oc_rsh(self.oc_node, pod_name, "pvscan")
