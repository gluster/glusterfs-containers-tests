from pkg_resources import parse_version

import ddt
from glusto.core import Glusto as g
import pytest

from openshiftstoragelibs.baseclass import GlusterBlockBaseClass
from openshiftstoragelibs import command
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import gluster_ops
from openshiftstoragelibs import openshift_ops
from openshiftstoragelibs import waiter


@ddt.ddt
class TestLoggingAndGlusterRegistryValidation(GlusterBlockBaseClass):

    def setUp(self):
        """Initialize all the variables necessary for test cases."""
        super(TestLoggingAndGlusterRegistryValidation, self).setUp()

        try:
            logging_config = g.config['openshift']['logging']
            self._logging_project_name = logging_config['logging_project_name']
            self._logging_fluentd_ds = logging_config['logging_fluentd_ds']
            self._logging_es_dc = logging_config['logging_es_dc']
            self._logging_kibana_dc = logging_config['logging_kibana_dc']
            self._registry_heketi_server_url = (
                g.config['openshift']['registry_heketi_config'][
                    'heketi_server_url'])
            self._registry_project_name = (
                g.config['openshift']['registry_project_name'])
            self._registry_servers_info = g.config['gluster_registry_servers']
        except KeyError as err:
            msg = "Config file doesn't have key {}".format(err)
            g.log.error(msg)
            self.skipTest(msg)

        # Skip the test if iscsi-initiator-utils version is not the expected
        cmd = ("rpm -q iscsi-initiator-utils "
               "--queryformat '%{version}-%{release}\n'"
               "| cut -d '.' -f 1,2,3,4")
        e_pkg_version = "6.2.0.874-17"
        for g_server in self.gluster_servers:
            out = self.cmd_run(cmd, g_server)
            if parse_version(out) < parse_version(e_pkg_version):
                msg = ("Skip test since isci initiator utils version actual: "
                       "{out} is less than expected: {ver} on node {server},"
                       " for more info refer to BZ-1624670"
                       .format(out=out, ver=e_pkg_version, server=g_server))
                g.log.error(msg)
                self.skipTest(msg)

        self._master = self.ocp_master_node[0]
        cmd = "oc project --short=true"
        current_project = command.cmd_run(cmd, self._master)
        openshift_ops.switch_oc_project(
            self._master, self._logging_project_name)
        self.addCleanup(
            openshift_ops.switch_oc_project, self._master, current_project)

    def _get_es_pod_and_verify_iscsi_sessions(self):
        """Fetch es pod and verify iscsi sessions"""
        pvc_custom = ":.spec.volumes[*].persistentVolumeClaim.claimName"

        # Get the elasticsearch pod name nad PVC name
        es_pod = openshift_ops.get_pod_name_from_dc(
            self._master, self._logging_es_dc)
        pvc_name = openshift_ops.oc_get_custom_resource(
            self._master, "pod", pvc_custom, es_pod)[0]

        # Validate iscsi and multipath
        self.verify_iscsi_sessions_and_multipath(
            pvc_name, self._logging_es_dc,
            heketi_server_url=self._registry_heketi_server_url,
            is_registry_gluster=True)
        return es_pod, pvc_name

    def _get_newly_deployed_gluster_pod(self, g_pod_list_before):
        # Fetch pod after delete
        g_pod_list_after = [
            pod["pod_name"]
            for pod in openshift_ops.get_ocp_gluster_pod_details(self._master)]

        # Fetch the new gluster pod
        g_new_pod = list(set(g_pod_list_after) - set(g_pod_list_before))
        self.assertTrue(g_new_pod, "No new gluster pod deployed after delete")
        return g_new_pod

    def _guster_pod_delete_cleanup(self, g_pod_list_before):
        """Cleanup for deletion of gluster pod using force delete"""
        # Switch to gluster project
        openshift_ops.switch_oc_project(
            self._master, self._registry_project_name)
        try:
            # Fetch gluster pod after delete
            pod_name = self._get_newly_deployed_gluster_pod(g_pod_list_before)

            # Check if pod name is empty i.e no new pod come up so use old pod
            openshift_ops.wait_for_pod_be_ready(
                self._master,
                pod_name[0] if pod_name else g_pod_list_before[0], timeout=1)
        except exceptions.ExecutionError:
            # Force delete and wait for new pod to come up
            openshift_ops.oc_delete(
                self._master, 'pod', g_pod_list_before[0], is_force=True)
            openshift_ops.wait_for_resource_absence(
                self._master, 'pod', g_pod_list_before[0])

            # Fetch gluster pod after force delete
            g_new_pod = self._get_newly_deployed_gluster_pod(g_pod_list_before)
            openshift_ops.wait_for_pod_be_ready(self._master, g_new_pod[0])

    @pytest.mark.tier4a
    def test_validate_logging_pods_and_pvc(self):
        """Validate logging pods and PVC"""

        # Wait for kibana pod to be ready
        kibana_pod = openshift_ops.get_pod_name_from_dc(
            self._master, self._logging_kibana_dc)
        openshift_ops.wait_for_pod_be_ready(self._master, kibana_pod)

        # Wait for fluentd pods to be ready
        fluentd_custom = [":.status.desiredNumberScheduled",
                          ":.spec.template.metadata.labels"]
        count_and_selector = openshift_ops.oc_get_custom_resource(
            self._master, "ds", fluentd_custom, self._logging_fluentd_ds)
        selector = count_and_selector[1][4:].replace(":", "=")
        openshift_ops.wait_for_pods_be_ready(
            self._master, int(count_and_selector[0]), selector)

        # Wait for PVC to be bound and elasticsearch pod to be ready
        es_pod = openshift_ops.get_pod_name_from_dc(
            self._master, self._logging_es_dc)
        pvc_custom = ":.spec.volumes[*].persistentVolumeClaim.claimName"
        pvc_name = openshift_ops.oc_get_custom_resource(
            self._master, "pod", pvc_custom, es_pod)[0]
        openshift_ops.verify_pvc_status_is_bound(self._master, pvc_name)
        openshift_ops.wait_for_pod_be_ready(self._master, es_pod)

        # Validate iscsi and multipath
        self.verify_iscsi_sessions_and_multipath(
            pvc_name, self._logging_es_dc,
            heketi_server_url=self._registry_heketi_server_url,
            is_registry_gluster=True)

    @pytest.mark.tier4a
    def test_logging_es_pod_pvc_all_freespace_utilization(self):
        """Validate logging by utilizing all the free space of block PVC bound
           to elsaticsearch pod"""

        # Fetch pod and validate iscsi and multipath
        es_pod, _ = self._get_es_pod_and_verify_iscsi_sessions()

        # Get the available free space
        mount_point = '/elasticsearch/persistent'
        cmd_free_space = (
            "df -kh {} | awk '{{print $4}}' | tail -1".format(mount_point))
        old_available_space = openshift_ops.oc_rsh(
            self._master, es_pod, cmd_free_space)[1]

        # Fill the all the available space
        file_name = '{}/file'.format(mount_point)
        cmd_fill_space = (
            "fallocate -l {} {}".format(old_available_space, file_name))
        with self.assertRaises(AssertionError):
            openshift_ops.oc_rsh(self._master, es_pod, cmd_fill_space)

            # Cleanup the filled space
            cmd_remove_file = 'rm {}'.format(file_name)
            self.addCleanup(
                openshift_ops.oc_rsh, self._master, es_pod, cmd_remove_file)

    @pytest.mark.tier4a
    def test_resping_gluster_pod(self):
        """Validate gluster pod restart with no disruption to elasticsearch pod
        """
        restart_custom = ":status.containerStatuses[0].restartCount"

        # Fetch pod and validate iscsi and multipath
        es_pod, _ = self._get_es_pod_and_verify_iscsi_sessions()

        # Fetch the restart count for the es pod
        restart_count_before = openshift_ops.oc_get_custom_resource(
            self._master, "pod", restart_custom, es_pod)[0]

        # Switch to gluster project
        openshift_ops.switch_oc_project(
            self._master, self._registry_project_name)

        # Fetch the gluster pod list before
        g_pod_list_before = [
            pod["pod_name"]
            for pod in openshift_ops.get_ocp_gluster_pod_details(self._master)]

        # Respin a gluster pod
        openshift_ops.oc_delete(self._master, "pod", g_pod_list_before[0])
        self.addCleanup(self._guster_pod_delete_cleanup, g_pod_list_before)

        # Wait for pod to get absent
        openshift_ops.wait_for_resource_absence(
            self._master, "pod", g_pod_list_before[0])

        # Fetch gluster pod after delete
        g_new_pod = self._get_newly_deployed_gluster_pod(g_pod_list_before)
        openshift_ops.wait_for_pod_be_ready(self._master, g_new_pod[0])

        # Switch to logging project
        openshift_ops.switch_oc_project(
            self._master, self._logging_project_name)

        # Fetch the restart count for the es pod
        restart_count_after = openshift_ops.oc_get_custom_resource(
            self._master, "pod", restart_custom, es_pod)[0]
        self.assertEqual(
            restart_count_before, restart_count_after,
            "Failed disruption to es pod found expecting restart count before"
            " {} and after {} for es pod to be equal after gluster pod"
            " respin".format(restart_count_before, restart_count_after))

    @pytest.mark.tier4a
    def test_kill_bhv_fsd_while_es_pod_running(self):
        """Validate killing of bhv fsd won't effect es pod io's"""

        # Fetch pod and PVC names and validate iscsi and multipath
        es_pod, pvc_name = self._get_es_pod_and_verify_iscsi_sessions()

        # Get the bhv name
        gluster_node = list(self._registry_servers_info.keys())[0]
        openshift_ops.switch_oc_project(
            self._master, self._registry_project_name)
        bhv_name = self.get_block_hosting_volume_by_pvc_name(
            pvc_name, heketi_server_url=self._registry_heketi_server_url,
            gluster_node=gluster_node)

        # Get one of the bricks pid of the bhv
        gluster_volume_status = gluster_ops.get_gluster_vol_status(bhv_name)
        pid = None
        for g_node, g_node_data in gluster_volume_status.items():
            if g_node != gluster_node:
                continue
            for process_name, process_data in g_node_data.items():
                if not process_name.startswith("/var"):
                    continue
                pid = process_data["pid"]
                # When birck is down, pid of the brick is returned as -1.
                # Which is unexepeted situation. So, add appropriate assertion.
                self.assertNotEqual(
                    pid, "-1", "Got unexpected PID (-1) for '{}' gluster vol "
                    "on '{}' node.".format(bhv_name, gluster_node))
                break
            self.assertTrue(
                pid, "Could not find 'pid' in Gluster vol data for '{}' "
                "Gluster node. Data: {}".format(
                    gluster_node, gluster_volume_status))
            break

        # Kill gluster vol brick process using found pid
        cmd_kill = "kill -9 {}".format(pid)
        cmd_start_vol = "gluster v start {} force".format(bhv_name)
        openshift_ops.cmd_run_on_gluster_pod_or_node(
            self._master, cmd_kill, gluster_node)
        self.addCleanup(openshift_ops.cmd_run_on_gluster_pod_or_node,
                        self._master, cmd_start_vol, gluster_node)
        self.addCleanup(openshift_ops.switch_oc_project,
                        self._master, self._registry_project_name)

        # Run I/O on ES pod
        openshift_ops.switch_oc_project(
            self._master, self._logging_project_name)
        file_name = '/elasticsearch/persistent/file1'
        cmd_run_io = 'dd if=/dev/urandom of={} bs=4k count=10000'.format(
            file_name)
        cmd_remove_file = 'rm {}'.format(file_name)
        openshift_ops.oc_rsh(self._master, es_pod, cmd_run_io)
        self.addCleanup(
            openshift_ops.oc_rsh, self._master, es_pod, cmd_remove_file)

    def _delete_and_wait_for_new_es_pod_to_come_up(self):

        # Force delete and wait for es pod to come up
        openshift_ops.switch_oc_project(
            self._master, self._logging_project_name)
        pod_name = openshift_ops.get_pod_name_from_dc(
            self._master, self._logging_es_dc)
        openshift_ops.oc_delete(self._master, 'pod', pod_name, is_force=True)
        openshift_ops.wait_for_resource_absence(self._master, 'pod', pod_name)
        new_pod_name = openshift_ops.get_pod_name_from_dc(
            self._master, self._logging_es_dc)
        openshift_ops.wait_for_pod_be_ready(
            self._master, new_pod_name, timeout=1800)

    @pytest.mark.tier4a
    @ddt.data('delete', 'drain')
    def test_respin_es_pod(self, motive):
        """Validate respin of elastic search pod"""

        # Get the pod name and PVC name
        es_pod = openshift_ops.get_pod_name_from_dc(
            self._master, self._logging_es_dc)
        pvc_custom = ":.spec.volumes[*].persistentVolumeClaim.claimName"
        pvc_name = openshift_ops.oc_get_custom_resource(
            self._master, "pod", pvc_custom, es_pod)[0]

        # Validate iscsi and multipath
        _, _, node = self.verify_iscsi_sessions_and_multipath(
            pvc_name, self._logging_es_dc,
            heketi_server_url=self._registry_heketi_server_url,
            is_registry_gluster=True)
        if motive == 'delete':

            # Delete the es pod
            self.addCleanup(self._delete_and_wait_for_new_es_pod_to_come_up)
            openshift_ops.oc_delete(self._master, "pod", es_pod)
        elif motive == 'drain':

            # Get the number of infra nodes
            infra_node_count_cmd = (
                'oc get nodes '
                '--no-headers -l node-role.kubernetes.io/infra=true|wc -l')
            infra_node_count = command.cmd_run(
                infra_node_count_cmd, self._master)

            # Skip test case if number infra nodes are less than #2
            if int(infra_node_count) < 2:
                self.skipTest('Available number of infra nodes "{}", it should'
                              ' be more than 1'.format(infra_node_count))

            # Cleanup to make node schedulable
            cmd_schedule = (
                'oc adm manage-node {} --schedulable=true'.format(node))
            self.addCleanup(
                command.cmd_run, cmd_schedule, hostname=self._master)

            # Drain the node
            drain_cmd = ('oc adm drain {} --force=true --ignore-daemonsets '
                         '--delete-local-data'.format(node))
            command.cmd_run(drain_cmd, hostname=self._master)

        # Wait for pod to get absent
        openshift_ops.wait_for_resource_absence(self._master, "pod", es_pod)

        # Wait for new pod to come up
        try:
            pod_name = openshift_ops.get_pod_name_from_dc(
                self._master, self._logging_es_dc)
            openshift_ops.wait_for_pod_be_ready(self._master, pod_name)
        except exceptions.ExecutionError:
            self._delete_and_wait_for_new_es_pod_to_come_up()

        # Validate iscsi and multipath
        self.verify_iscsi_sessions_and_multipath(
            pvc_name, self._logging_es_dc,
            heketi_server_url=self._registry_heketi_server_url,
            is_registry_gluster=True)

    @pytest.mark.tier4a
    def test_run_workload_with_logging(self):
        """Validate logs are being generated aifter running workload"""

        # Get the size of used space of logs
        es_pod = openshift_ops.get_pod_name_from_dc(
            self._master, self._logging_es_dc)
        mount_point = "/elasticsearch/persistent"
        cmd_space_check = ('df -kh --output=used {} | sed "/Used/d" |'
                           'sed "s/G//"'.format(mount_point))
        ret, initial_used_percent, err = openshift_ops.oc_rsh(
            self._master, es_pod, cmd_space_check)
        err_msg = "Failed to fetch the size of used space, error {}"
        self.assertFalse(ret, err_msg.format(err))

        # Create 20 pvcs and app pods with io
        openshift_ops.switch_oc_project(
            self._master, self.storage_project_name)
        pvc_count, batch_count = 5, 4
        for _ in range(batch_count):
            pvcs = self.create_and_wait_for_pvcs(pvc_amount=pvc_count)
            self.create_dcs_with_pvc(pvcs)
        self.addCleanup(
            openshift_ops.switch_oc_project,
            self._master, self.storage_project_name)

        # Get and verify the final used size of used space of logs
        openshift_ops.switch_oc_project(
            self._master, self._logging_project_name)
        for w in waiter.Waiter(600, 30):
            ret, final_used_percent, err = openshift_ops.oc_rsh(
                self._master, es_pod, cmd_space_check)
            self.assertFalse(ret, err_msg.format(err))
            if int(initial_used_percent) < int(final_used_percent):
                break
        if w.expired:
            raise AssertionError(
                "Initial used space {} for logs is not less than final "
                "used space {}".format(
                    initial_used_percent, final_used_percent))
