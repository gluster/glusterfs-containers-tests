try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json
import time

import ddt
from glusto.core import Glusto as g
import pytest

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import openshift_ops


@ddt.ddt
class TestPrometheusValidationFile(baseclass.BaseClass):
    """Prometheus Validations for file volumes"""

    @classmethod
    def setUpClass(cls):
        super(TestPrometheusValidationFile, cls).setUpClass()

        # Metrics of which the data need to retrieve in this class
        cls.metrics = ('kubelet_volume_stats_inodes_free',
                       'kubelet_volume_stats_inodes',
                       'kubelet_volume_stats_inodes_used',
                       'kubelet_volume_stats_available_bytes',
                       'kubelet_volume_stats_capacity_bytes',
                       'kubelet_volume_stats_used_bytes')

    def setUp(self):
        """Initialize all the variables which are necessary for test cases"""
        super(TestPrometheusValidationFile, self).setUp()

        try:
            prometheus_config = g.config['openshift']['prometheus']
            self._prometheus_project_name = prometheus_config[
                'prometheus_project_name']
            self._prometheus_resources_selector = prometheus_config[
                'prometheus_resources_selector']
            self._alertmanager_resources_selector = prometheus_config[
                'alertmanager_resources_selector']
        except KeyError as err:
            self.skipTest("Config file doesn't have key {}".format(err))

        self._master = self.ocp_master_node[0]

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

    def _get_and_manipulate_metric_data(self, metrics, pvc):
        """Create a dict of metric names and total values"""

        # Switch to namespace containing prometheus pods
        openshift_ops.switch_oc_project(self._master,
                                        self._prometheus_project_name)
        self.addCleanup(openshift_ops.switch_oc_project,
                        self._master, self.storage_project_name)

        metric_data = dict()
        for metric in metrics:
            out = self._fetch_metric_from_promtheus_pod(metric)
            for matric_result in out:
                if matric_result["metric"]["persistentvolumeclaim"] == pvc:
                    metric_data[matric_result["metric"][
                        "__name__"]] = matric_result["value"][1]
        return metric_data

    def _run_io_on_the_pod(self, pod_name, number_of_files):
        for each in range(number_of_files):
            cmd = "touch /mnt/file{}".format(each)
            ret, _, err = openshift_ops.oc_rsh(self._master, pod_name, cmd)
            self.assertFalse(ret, "Failed to run the IO with error msg {}".
                             format(err))

    @pytest.mark.tier2
    def test_prometheus_volume_metrics_on_pod_restart(self):
        """Validate volume metrics using prometheus before and after pod
        restart"""

        # Create PVC and wait for it to be in 'Bound' state
        pvc_name = self.create_and_wait_for_pvc()
        pod_name = openshift_ops.oc_create_tiny_pod_with_volume(
            self._master, pvc_name, "autotest-volume",
            image=self.io_container_image_cirros)
        self.addCleanup(openshift_ops.oc_delete, self._master, 'pod', pod_name,
                        raise_on_absence=False)

        # Wait for POD be up and running
        openshift_ops.wait_for_pod_be_ready(
            self._master, pod_name, timeout=60, wait_step=2)

        # Write data on the volume and wait for 2 mins and sleep is must for
        # prometheus to get the exact values of the metrics
        self._run_io_on_the_pod(pod_name, 30)
        time.sleep(120)

        # Fetching the metrics and storing in initial_metrics as dictionary
        initial_metrics = self._get_and_manipulate_metric_data(
            self.metrics, pvc_name)

        # Mark the current node unschedulable on which app pod is running
        openshift_ops.switch_oc_project(
            self._master, self.storage_project_name)
        pod_info = openshift_ops.oc_get_pods(self._master, name=pod_name)
        openshift_ops.oc_adm_manage_node(
            self._master, '--schedulable=false',
            nodes=[pod_info[pod_name]["node"]])
        self.addCleanup(
            openshift_ops.oc_adm_manage_node, self._master,
            '--schedulable=true', nodes=[pod_info[pod_name]["node"]])

        # Delete the existing pod and create a new pod
        openshift_ops.oc_delete(self._master, 'pod', pod_name)
        pod_name = openshift_ops.oc_create_tiny_pod_with_volume(
            self._master, pvc_name, "autotest-volume")
        self.addCleanup(openshift_ops.oc_delete, self._master, 'pod', pod_name)

        # Wait for POD be up and running and prometheus to refresh the data
        openshift_ops.wait_for_pod_be_ready(
            self._master, pod_name, timeout=60, wait_step=2)
        time.sleep(120)

        # Fetching the metrics and storing in final_metrics as dictionary and
        # validating with initial_metrics
        final_metrics = self._get_and_manipulate_metric_data(
            self.metrics, pvc_name)
        self.assertEqual(dict(initial_metrics), dict(final_metrics),
                         "Metrics are different post pod restart")
