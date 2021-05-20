try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json
import time

import ddt
from glusto.core import Glusto as g
from glustolibs.gluster import rebalance_ops
import pytest

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import openshift_ops
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import waiter


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

    def _fetch_initial_metrics(self, vol_name_prefix=None,
                               volume_expansion=False):

        # Create PVC and wait for it to be in 'Bound' state
        sc_name = self.create_storage_class(
            vol_name_prefix=vol_name_prefix,
            allow_volume_expansion=volume_expansion)

        if vol_name_prefix:
            pvc_name = self.create_and_wait_for_pvc(
                pvc_name_prefix=vol_name_prefix, sc_name=sc_name)
        else:
            pvc_name = self.create_and_wait_for_pvc(sc_name=sc_name)

        # Create DC and attach with pvc
        self.dc_name, pod_name = self.create_dc_with_pvc(pvc_name)
        for w in waiter.Waiter(120, 10):
            initial_metrics = self._get_and_manipulate_metric_data(
                self.metrics, pvc_name)
            if bool(initial_metrics) and len(initial_metrics) == 6:
                break
        if w.expired:
            raise AssertionError("Unable to fetch metrics for the pvc")
        return pvc_name, pod_name, initial_metrics

    def _perform_io_and_fetch_metrics(
            self, pod_name, pvc_name, filename, dirname,
            metric_data, operation):
        """Create 1000 files and dirs and validate with old metrics"""
        openshift_ops.switch_oc_project(
            self._master, self.storage_project_name)
        if operation == "create":
            cmds = ("touch /mnt/{}{{1..1000}}".format(filename),
                    "mkdir /mnt/{}{{1..1000}}".format(dirname))
        else:
            cmds = ("rm -rf /mnt/large_file",
                    "rm -rf /mnt/{}{{1..1000}}".format(filename),
                    "rm -rf /mnt/{}{{1..1000}}".format(dirname))
        for cmd in cmds:
            self.cmd_run("oc rsh {} {}".format(pod_name, cmd))

        # Fetch the new metrics and compare the inodes used and bytes used
        for w in waiter.Waiter(120, 10):
            after_io_metrics = self._get_and_manipulate_metric_data(
                self.metrics, pvc_name)
            if operation == "create":
                if (int(after_io_metrics[
                    'kubelet_volume_stats_inodes_used']) > int(
                    metric_data['kubelet_volume_stats_inodes_used']) and int(
                    after_io_metrics[
                        'kubelet_volume_stats_used_bytes']) > int(
                        metric_data['kubelet_volume_stats_used_bytes'])):
                    break
            else:
                if int(metric_data[
                        'kubelet_volume_stats_used_bytes']) > int(
                        after_io_metrics['kubelet_volume_stats_used_bytes']):
                    break
        if w.expired:
            raise AssertionError(
                "After data is modified metrics like bytes used and inodes "
                "used are not reflected in prometheus")

    def _run_io_on_the_pod(self, pod_name, number_of_files):
        for each in range(number_of_files):
            cmd = "touch /mnt/file{}".format(each)
            ret, _, err = openshift_ops.oc_rsh(self._master, pod_name, cmd)
            self.assertFalse(ret, "Failed to run the IO with error msg {}".
                             format(err))

    @podcmd.GlustoPod()
    def _rebalance_completion(self, volume_name):
        """Rebalance start and completion after expansion."""
        ret, _, err = rebalance_ops.rebalance_start(
            'auto_get_gluster_endpoint', volume_name)
        self.assertFalse(
            ret, "Rebalance for {} volume not started with error {}".format(
                volume_name, err))

        for w in waiter.Waiter(240, 10):
            reb_status = rebalance_ops.get_rebalance_status(
                'auto_get_gluster_endpoint', volume_name)
            if reb_status["aggregate"]["statusStr"] == "completed":
                break
        if w.expired:
            raise AssertionError(
                "Failed to complete the rebalance in 240 seconds")

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

    @pytest.mark.tier2
    def test_prometheus_basic_validation(self):
        """ Validate basic volume metrics using prometheus """

        # Fetch the metrics and storing initial_metrics as dictionary
        pvc_name, pod_name, initial_metrics = self._fetch_initial_metrics(
            volume_expansion=False)

        # Create 1000 files and fetch the metrics that the data is updated
        self._perform_io_and_fetch_metrics(
            pod_name=pod_name, pvc_name=pvc_name,
            filename="filename1", dirname="dirname1",
            metric_data=initial_metrics, operation="create")

        # Write the IO half the size of the volume and validated from
        # prometheus pod that the size change is reflected
        size_to_write = int(initial_metrics[
            'kubelet_volume_stats_capacity_bytes']) // 2
        openshift_ops.switch_oc_project(
            self._master, self.storage_project_name)
        cmd = ("dd if=/dev/urandom of=/mnt/large_file bs={} count=1024".
               format(size_to_write // 1024))
        ret, _, err = openshift_ops.oc_rsh(self._master, pod_name, cmd)
        self.assertFalse(ret, 'Failed to write file due to err {}'.format(err))

        # Fetching the metrics and validating the data change is reflected
        for w in waiter.Waiter(120, 10):
            half_io_metrics = self._get_and_manipulate_metric_data(
                ['kubelet_volume_stats_used_bytes'], pvc_name)
            if bool(half_io_metrics) and (int(
                    half_io_metrics['kubelet_volume_stats_used_bytes'])
                    > size_to_write):
                break
        if w.expired:
            raise AssertionError(
                "After Data is written on the pvc, metrics like inodes used "
                "and bytes used are not reflected in the prometheus")

        # Delete the files from the volume and wait for the
        # updated details reflected in prometheus
        self._perform_io_and_fetch_metrics(
            pod_name=pod_name, pvc_name=pvc_name,
            filename="filename1", dirname="dirname1",
            metric_data=half_io_metrics, operation="delete")

    @pytest.mark.tier2
    def test_prometheus_pv_resize(self):
        """ Validate prometheus metrics with pv resize"""

        # Fetch the metrics and storing initial_metrics as dictionary
        pvc_name, pod_name, initial_metrics = self._fetch_initial_metrics(
            vol_name_prefix="for-pv-resize", volume_expansion=True)

        # Write data on the pvc and confirm it is reflected in the prometheus
        self._perform_io_and_fetch_metrics(
            pod_name=pod_name, pvc_name=pvc_name,
            filename="filename1", dirname="dirname1",
            metric_data=initial_metrics, operation="create")

        # Resize the pvc to 2GiB
        openshift_ops.switch_oc_project(
            self._master, self.storage_project_name)
        pvc_size = 2
        openshift_ops.resize_pvc(self._master, pvc_name, pvc_size)
        openshift_ops.wait_for_events(self._master, obj_name=pvc_name,
                                      event_reason='VolumeResizeSuccessful')
        openshift_ops.verify_pvc_size(self._master, pvc_name, pvc_size)
        pv_name = openshift_ops.get_pv_name_from_pvc(
            self._master, pvc_name)
        openshift_ops.verify_pv_size(self._master, pv_name, pvc_size)

        heketi_volume_name = heketi_ops.heketi_volume_list_by_name_prefix(
            self.heketi_client_node, self.heketi_server_url,
            "for-pv-resize", json=True)[0][2]
        self.assertIsNotNone(
            heketi_volume_name, "Failed to fetch volume with prefix {}".
            format("for-pv-resize"))

        openshift_ops.oc_delete(self._master, 'pod', pod_name)
        openshift_ops.wait_for_resource_absence(self._master, 'pod', pod_name)
        pod_name = openshift_ops.get_pod_name_from_dc(
            self._master, self.dc_name)
        openshift_ops.wait_for_pod_be_ready(self._master, pod_name)

        # Check whether the metrics are updated or not
        for w in waiter.Waiter(120, 10):
            resize_metrics = self._get_and_manipulate_metric_data(
                self.metrics, pvc_name)
            if bool(resize_metrics) and int(resize_metrics[
                'kubelet_volume_stats_capacity_bytes']) > int(
                    initial_metrics['kubelet_volume_stats_capacity_bytes']):
                break
        if w.expired:
            raise AssertionError("Failed to reflect PVC Size after resizing")
        openshift_ops.switch_oc_project(
            self._master, self.storage_project_name)
        time.sleep(240)

        # Lookup and trigger rebalance and wait for the its completion
        for _ in range(100):
            self.cmd_run("oc rsh {} ls /mnt/".format(pod_name))
        self._rebalance_completion(heketi_volume_name)

        # Write data on the resized pvc and compared with the resized_metrics
        self._perform_io_and_fetch_metrics(
            pod_name=pod_name, pvc_name=pvc_name,
            filename="secondfilename", dirname="seconddirname",
            metric_data=resize_metrics, operation="create")
