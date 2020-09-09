try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json
from pkg_resources import parse_version

from glusto.core import Glusto as g
import pytest

from openshiftstoragelibs.baseclass import GlusterBlockBaseClass
from openshiftstoragelibs import command
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import openshift_ops


class TestPrometheusAndGlusterRegistryValidation(GlusterBlockBaseClass):

    def setUp(self):
        """Initialize all the variables which are necessary for test cases"""
        super(TestPrometheusAndGlusterRegistryValidation, self).setUp()

        try:
            prometheus_config = g.config['openshift']['prometheus']
            self._prometheus_project_name = prometheus_config[
                'prometheus_project_name']
            self._prometheus_resources_selector = prometheus_config[
                'prometheus_resources_selector']
            self._alertmanager_resources_selector = prometheus_config[
                'alertmanager_resources_selector']
            self._registry_heketi_server_url = (
                g.config['openshift']['registry_heketi_config'][
                    'heketi_server_url'])
            self._registry_project_name = (
                g.config['openshift']['registry_project_name'])
        except KeyError as err:
            self.skipTest("Config file doesn't have key {}".format(err))

        # Skip the test if iscsi-initiator-utils version is not the expected
        cmd = ("rpm -q iscsi-initiator-utils "
               "--queryformat '%{version}-%{release}\n'"
               "| cut -d '.' -f 1,2,3,4")
        e_pkg_version = "6.2.0.874-17"
        for g_server in self.gluster_servers:
            out = self.cmd_run(cmd, g_server)
            if parse_version(out) < parse_version(e_pkg_version):
                self.skipTest(
                    "Skip the test as iscsi-initiator-utils package version {}"
                    "is less than version {} found on the node {}, for more "
                    "info refer to BZ-1624670".format(
                        out, e_pkg_version, g_server))

        self._master = self.ocp_master_node[0]

        # Switch to namespace conatining prometheus pods
        cmd = "oc project --short=true"
        current_project = command.cmd_run(cmd, self._master)
        openshift_ops.switch_oc_project(
            self._master, self._prometheus_project_name)
        self.addCleanup(
            openshift_ops.switch_oc_project, self._master, current_project)

    def _fetch_metric_from_promtheus_pod(self, metric):
        """Fetch metric from prometheus pod using api call"""
        prometheus_pods = list(openshift_ops.oc_get_pods(
            self._master, selector=self._prometheus_resources_selector).keys())
        fetch_metric_cmd = ("curl 'http://localhost:9090/api/v1/query"
                            "?query={}'".format(metric))
        ret, metric_data, _ = openshift_ops.oc_rsh(
            self._master, prometheus_pods[0], fetch_metric_cmd)
        metric_result = json.loads(metric_data)["data"]["result"]
        if (not metric_result) or ret:
            raise exceptions.ExecutionError(
                "Failed to fecth data for metric {}, output {}".format(
                    metric, metric_result))
        return metric_result

    def _get_pod_names_and_pvc_names(self):
        # Get pod names and PVC names
        pod_custom = ".:metadata.name"
        pvc_custom = ":.spec.volumes[*].persistentVolumeClaim.claimName"
        pvc_names, pod_names = [], []
        for selector in (self._prometheus_resources_selector,
                         self._alertmanager_resources_selector):
            pods = openshift_ops.oc_get_custom_resource(
                self._master, "pod", pod_custom, selector=selector)
            pod_names.extend(pods)
            for pod_name in pods:
                pvc_name = openshift_ops.oc_get_custom_resource(
                    self._master, "pod", pvc_custom, pod_name[0])[0]
                pvc_names.append(pvc_name)

        return pod_names, pvc_names

    @pytest.mark.tier2
    def test_promethoues_pods_and_pvcs(self):
        """Validate prometheus pods and PVC"""
        # Wait for PVCs to be bound
        pod_names, pvc_names = self._get_pod_names_and_pvc_names()
        openshift_ops.wait_for_pvcs_be_bound(self._master, pvc_names)

        # Validate that there should be no or zero pods in non-running state
        field_selector, pod_count = "status.phase!=Running", 0
        openshift_ops.wait_for_pods_be_ready(
            self._master, pod_count, field_selector=field_selector)

        # Validate iscsi and multipath
        for (pvc_name, pod_name) in zip(pvc_names, pod_names):
            self.verify_iscsi_sessions_and_multipath(
                pvc_name, pod_name[0], rtype='pod',
                heketi_server_url=self._registry_heketi_server_url,
                is_registry_gluster=True)

        # Try to fetch metric from prometheus pod
        self._fetch_metric_from_promtheus_pod(metric='kube_node_info')
