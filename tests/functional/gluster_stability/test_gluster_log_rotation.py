import datetime
import re

import pytest

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import command
from openshiftstoragelibs import heketi_version
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import openshift_ops
from openshiftstoragelibs import waiter

TCMU_CONF = "/etc/tcmu/tcmu.conf"
TCMU_RUNNER_LOG = "/var/log/glusterfs/gluster-block/tcmu-runner.log"
TCMU_ROTATE_LOG = "/etc/logrotate.d/gluster-block"

# RE to check time and date from logs
LOG_REGEX = r"^(\d+-\d+-\d+.*\d+:\d+:\d+)\.\d+.*\[(\S+)\](.*):(\d+):(.*)$"


class TestGlusterBlockLog(baseclass.GlusterBlockBaseClass):
    """Class to validate logs for gluster block"""

    def setUp(self):
        super(TestGlusterBlockLog, self).setUp()
        self.node = self.ocp_master_node[0]
        self.g_node = self.gluster_servers[0]
        self.timeformat = "%Y-%m-%d %H:%M:%S"

    def _set_log_level(self, node, level, msg, exec_time):
        delete_log_level = r'sed -i "/\(^log_level.*=.*[0-9]\)/d" {}'
        set_log_level = r'sed -i "\$alog_level = {}" {}'
        check_log_msg = r'sed -n "/.*\({}\).*/{{p;}}" {} | tail -1'

        # Set log level
        openshift_ops.cmd_run_on_gluster_pod_or_node(
            self.node, set_log_level.format(level, TCMU_CONF),
            gluster_node=node)
        self.addCleanup(
            openshift_ops.cmd_run_on_gluster_pod_or_node,
            self.node, delete_log_level.format(TCMU_CONF), gluster_node=node)

        # Validate log level
        log_msg = "log level now is {}".format(msg)
        for w in waiter.Waiter(120, 3):
            out = openshift_ops.cmd_run_on_gluster_pod_or_node(
                self.node, check_log_msg.format(log_msg, TCMU_RUNNER_LOG),
                gluster_node=node)
            match = re.match(LOG_REGEX, out)
            if (match
                    and exec_time < datetime.datetime.strptime(
                        match.group(1), self.timeformat)):
                break

        if w.expired:
            raise exceptions.ExecutionError(
                "Log level '{}:{}' of tcmu did not get changed on node"
                " {}".format(level, msg, node))

        openshift_ops.cmd_run_on_gluster_pod_or_node(
            self.node, delete_log_level.format(TCMU_CONF), gluster_node=node)

    @pytest.mark.tier3
    def test_gluster_block_log_rotation(self):
        "Verify log-rotation for gluster-block logs on independent mode"

        if self.is_containerized_gluster():
            self.skipTest(
                "Skipping this test case as it is not independent mode setup")

        version = heketi_version.get_heketi_version(self.heketi_client_node)
        if (version < '9.0.0-14'):
            self.skipTest(
                "This test case is not supported for < heketi 9.0.0-14 version"
                " due to bug BZ-1790788")

        # Get system current date and time
        get_system_time = "date '+%F %T'"

        # Get current system date & time
        exec_time = command.cmd_run(get_system_time, hostname=self.g_node)
        exec_time = datetime.datetime.strptime(exec_time, self.timeformat)

        # Set log level to debug or higher level
        self._set_log_level(self.g_node, 5, 'DEBUG SCSI CMD', exec_time)

        # Get initial size on file gluster-block log
        cmd = 'ls -lh {} | cut -d " " -f5'.format(TCMU_RUNNER_LOG)
        initial_log_size = command.cmd_run(cmd, hostname=self.g_node)

        # Create PVCs and pod with I/O
        pvc_names = self.create_and_wait_for_pvcs(pvc_amount=5)
        self.create_dcs_with_pvc(pvc_names)

        # Get log size after PVC create
        after_log_size = command.cmd_run(cmd, hostname=self.g_node)
        self.assertGreaterEqual(
            after_log_size, initial_log_size,
            "gluster-block log size has not changed")

        # Rotate logs manually
        log_rotate_cmd = "logrotate -vf {}".format(TCMU_ROTATE_LOG)
        command.cmd_run(log_rotate_cmd, hostname=self.g_node)

        # Get log size after log rotate
        final_log_size = command.cmd_run(cmd, hostname=self.g_node)
        self.assertLess(
            final_log_size, after_log_size,
            "Failed: log rotation is unsuccessful")
