from glusto.core import Glusto as g

from openshiftstoragelibs.baseclass import GlusterBlockBaseClass
from openshiftstoragelibs import command
from openshiftstoragelibs.openshift_ops import (
    get_pod_name_from_rc,
    oc_get_custom_resource,
    switch_oc_project,
    verify_pvc_status_is_bound,
    wait_for_pod_be_ready,
)


class TestMetricsAndGlusterRegistryValidation(GlusterBlockBaseClass):

    def setUp(self):
        """Initialize all the variables necessary for test cases."""
        super(TestMetricsAndGlusterRegistryValidation, self).setUp()

        try:
            metrics_config = g.config['openshift']['metrics']
            self.metrics_project_name = metrics_config['metrics_project_name']
            self.metrics_rc_hawkular_cassandra = (
                metrics_config['metrics_rc_hawkular_cassandra'])
            self.metrics_rc_hawkular_metrics = (
                metrics_config['metrics_rc_hawkular_metrics'])
            self.metrics_rc_heapster = metrics_config['metrics_rc_heapster']
            self.registry_heketi_server_url = (
                g.config['openshift']['registry_heketi_config'][
                    'heketi_server_url'])
        except KeyError as err:
            msg = ("Config file doesn't have key %s" % err)
            g.log.error(msg)
            self.skipTest(msg)

        self.master = self.ocp_master_node[0]
        cmd = "oc project --short=true"
        current_project = command.cmd_run(cmd, self.master)
        switch_oc_project(self.master, self.metrics_project_name)
        self.addCleanup(switch_oc_project, self.master, current_project)

    def test_validate_metrics_pods_and_pvc(self):
        """Validate metrics pods and PVC"""
        # Get cassandra pod name and PVC name
        hawkular_cassandra = get_pod_name_from_rc(
            self.master, self.metrics_rc_hawkular_cassandra)
        custom = ":.spec.volumes[*].persistentVolumeClaim.claimName"
        pvc_name = oc_get_custom_resource(
            self.master, "pod", custom, hawkular_cassandra)[0]

        # Wait for pods to get ready and PVC to be bound
        verify_pvc_status_is_bound(self.master, pvc_name)
        wait_for_pod_be_ready(self.master, hawkular_cassandra)
        hawkular_metrics = get_pod_name_from_rc(
            self.master, self.metrics_rc_hawkular_metrics)
        wait_for_pod_be_ready(self.master, hawkular_metrics)
        heapster = get_pod_name_from_rc(self.master, self.metrics_rc_heapster)
        wait_for_pod_be_ready(self.master, heapster)

        # Validate iscsi and multipath
        self.verify_iscsi_sessions_and_multipath(
            pvc_name, self.metrics_rc_hawkular_cassandra, rtype='rc',
            heketi_server_url=self.registry_heketi_server_url,
            is_registry_gluster=True)
