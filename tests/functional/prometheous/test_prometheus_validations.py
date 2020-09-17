try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json
from pkg_resources import parse_version

import ddt
from glusto.core import Glusto as g
import pytest

from openshiftstoragelibs.baseclass import GlusterBlockBaseClass
from openshiftstoragelibs import command
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import openshift_ops


@ddt.ddt
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

    @ddt.data('delete', 'drain')
    @pytest.mark.tier2
    def test_respin_prometheus_pod(self, motive="delete"):
        """Validate respin of prometheus pod"""
        if motive == 'drain':

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

        # Get PVC names and pod names
        pod_names, pvc_names = self._get_pod_names_and_pvc_names()

        # Validate iscsi and multipath
        for (pvc_name, pod_name) in zip(pvc_names, pod_names):
            _, _, node = self.verify_iscsi_sessions_and_multipath(
                pvc_name, pod_name[0], rtype='pod',
                heketi_server_url=self._registry_heketi_server_url,
                is_registry_gluster=True)

        # Delete the prometheus pods
        if motive == 'delete':
            for pod_name in pod_names:
                openshift_ops.oc_delete(self._master, 'pod', pod_name[0])

        # Drain the node
        elif motive == 'drain':
            drain_cmd = ('oc adm drain {} --force=true --ignore-daemonsets '
                         '--delete-local-data'.format(node))
            command.cmd_run(drain_cmd, hostname=self._master)

            # Cleanup to make node schedulable
            cmd_schedule = (
                'oc adm manage-node {} --schedulable=true'.format(node))
            self.addCleanup(
                command.cmd_run, cmd_schedule, hostname=self._master)

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

    @pytest.mark.tier2
    def test_heketi_and_prometheus_device_count(self):
        """Check if device count is same in heketi and promtheus"""

        cluster_ids_metrics, cluster_ids_promtheus = [], []
        hostnames_metrics, hostnames_promtheus = [], []
        total_value_metrics, total_value_promtheus = 0, 0

        metrics = heketi_ops.get_heketi_metrics(
            self.heketi_client_node, self.heketi_server_url)
        heketi_device_count_metric = metrics.get('heketi_device_count')
        for result in heketi_device_count_metric:
            cluster_ids_metrics.append(result.get('cluster'))
            hostnames_metrics.append(result.get('hostname'))
            total_value_metrics += int(result.get('value'))

        metric_result = self._fetch_metric_from_promtheus_pod(
            metric='heketi_device_count')
        for result in metric_result:
            total_value_promtheus += int(result.get('value')[1])
            cluster_ids_promtheus.append(result.get('metric')['cluster'])
            hostnames_promtheus.append(result.get('metric')['hostname'])

        self.assertEqual(cluster_ids_metrics, cluster_ids_promtheus,
                         "Cluster ID's are not same")
        self.assertEqual(hostnames_metrics, hostnames_promtheus,
                         "Hostnames are not same")
        self.assertEqual(total_value_metrics, total_value_promtheus,
                         "Total device counts are not same")

    def _get_and_manipulate_metric_data(self, metrics):
        """Create a dict of metric names and total values"""
        metric_data = dict()
        for metric in metrics:
            out = self._fetch_metric_from_promtheus_pod(metric)
            total_value = 0
            for matric_result in out:
                total_value += int(matric_result["value"][1])
            metric_data[out[0]["metric"]["__name__"]] = total_value
        return metric_data

    @pytest.mark.tier2
    @ddt.data('creation', 'expansion')
    def test_promethoues_validation_while_creation_or_expansion(self, motive):
        """Validate mertics data after volume creation or expansion"""

        # Define the variables to perform validations
        metrics = ['heketi_device_size_bytes', 'heketi_device_free_bytes',
                   'heketi_device_used_bytes', 'heketi_device_brick_count']
        h_client, h_server = self.heketi_client_node, self.heketi_server_url
        vol_size = 1

        # Collect the metrics data from prometheus pod
        if motive == 'creation':
            initial_result = self._get_and_manipulate_metric_data(metrics)

        # Create a volume
        volume_id = heketi_ops.heketi_volume_create(
            h_client, h_server, vol_size, json=True)["bricks"][0]["volume"]
        self.addCleanup(
            heketi_ops.heketi_volume_delete, h_client, h_server, volume_id)

        # Expand the volume
        if motive == 'expansion':
            initial_result = self._get_and_manipulate_metric_data(metrics)
            heketi_ops.heketi_volume_expand(
                h_client, h_server, volume_id, vol_size)

        # Fetch the latest metrics data form prometheus pod
        final_result = self._get_and_manipulate_metric_data(metrics)

        # Validate the data variation
        for metric in metrics:
            msg = (
                "intial {} and final value {} of metric '{} should be".format(
                    initial_result[metric], final_result[metric], metric))
            if metric == 'heketi_device_size_bytes':
                self.assertEqual(initial_result[metric], final_result[metric],
                                 msg + " same")
            if metric == 'heketi_device_free_bytes':
                self.assertGreater(initial_result[metric],
                                   final_result[metric], msg + " differnt")
            if metric == ('heketi_device_used_bytes'
                          or 'heketi_device_brick_count'):
                self.assertLess(
                    initial_result[metric], final_result[metric],
                    msg + " differnt")
