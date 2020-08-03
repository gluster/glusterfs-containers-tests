from pkg_resources import parse_version

import ddt
from glusto.core import Glusto as g
import pytest

from openshiftstoragelibs.baseclass import GlusterBlockBaseClass
from openshiftstoragelibs import command, openshift_ops


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

    @pytest.mark.tier2
    def test_validate_logging_pods_and_pvc(self):
        """Validate metrics pods and PVC"""

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

    @pytest.mark.tier2
    def test_logging_es_pod_pvc_all_freespace_utilization(self):
        """Validate logging by utilizing all the free space of block PVC bound
           to elsaticsearch pod"""

        # Get the elasticsearch pod name nad PVC name
        es_pod = openshift_ops.get_pod_name_from_dc(
            self._master, self._logging_es_dc)
        pvc_custom = ":.spec.volumes[*].persistentVolumeClaim.claimName"
        pvc_name = openshift_ops.oc_get_custom_resource(
            self._master, "pod", pvc_custom, es_pod)[0]

        # Validate iscsi and multipath
        self.verify_iscsi_sessions_and_multipath(
            pvc_name, self._logging_es_dc,
            heketi_server_url=self._registry_heketi_server_url,
            is_registry_gluster=True)

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
