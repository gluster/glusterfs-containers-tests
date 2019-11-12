from pkg_resources import parse_version

from glusto.core import Glusto as g

from openshiftstoragelibs.baseclass import GlusterBlockBaseClass
from openshiftstoragelibs import command
from openshiftstoragelibs import exceptions
from openshiftstoragelibs.openshift_ops import (
    get_ocp_gluster_pod_details,
    get_pod_name_from_rc,
    oc_delete,
    oc_get_custom_resource,
    switch_oc_project,
    verify_pvc_status_is_bound,
    wait_for_pod_be_ready,
    wait_for_pods_be_ready,
    wait_for_resources_absence,
)
from openshiftstoragelibs.openshift_storage_libs import (
    get_active_and_enabled_devices_from_mpath,
    get_iscsi_block_devices_by_path,
    get_mpath_name_from_device_name,
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
            self.registry_project_name = (
                g.config['openshift']['registry_project_name'])
            self.registry_servers_info = g.config['gluster_registry_servers']
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

    def verify_cassandra_pod_multipath_and_iscsi(self):
        # Validate iscsi and multipath
        hawkular_cassandra = get_pod_name_from_rc(
            self.master, self.metrics_rc_hawkular_cassandra)
        custom = ":.spec.volumes[*].persistentVolumeClaim.claimName"
        pvc_name = oc_get_custom_resource(
            self.master, "pod", custom, hawkular_cassandra)[0]
        iqn, hacount, node = self.verify_iscsi_sessions_and_multipath(
            pvc_name, self.metrics_rc_hawkular_cassandra, rtype='rc',
            heketi_server_url=self.registry_heketi_server_url,
            is_registry_gluster=True)
        return hawkular_cassandra, pvc_name, iqn, hacount, node

    def test_verify_metrics_data_during_gluster_pod_respin(self):
        # Add check for CRS version
        switch_oc_project(self.master, self.registry_project_name)
        if not self.is_containerized_gluster():
            self.skipTest(
                "Skipping this test case as CRS version check "
                "can not be implemented")

        # Verify multipath and iscsi for cassandra pod
        switch_oc_project(self.master, self.metrics_project_name)
        hawkular_cassandra, pvc_name, iqn, _, node = (
            self.verify_cassandra_pod_multipath_and_iscsi())

        # Get the ip of active path
        device_and_ip = get_iscsi_block_devices_by_path(node, iqn)
        mpath = get_mpath_name_from_device_name(
            node, list(device_and_ip.keys())[0])
        active_passive_dict = get_active_and_enabled_devices_from_mpath(
            node, mpath)
        node_ip = device_and_ip[active_passive_dict['active'][0]]

        # Get the name of gluster pod from the ip
        switch_oc_project(self.master, self.registry_project_name)
        gluster_pods = get_ocp_gluster_pod_details(self.master)
        pod_name = list(
            filter(lambda pod: (pod["pod_host_ip"] == node_ip), gluster_pods)
        )[0]["pod_name"]
        err_msg = "Failed to get the gluster pod name {} with active path"
        self.assertTrue(pod_name, err_msg.format(pod_name))

        # Delete the pod
        oc_delete(self.master, 'pod', pod_name)
        wait_for_resources_absence(self.master, 'pod', pod_name)

        # Wait for new pod to come up
        pod_count = len(self.registry_servers_info.keys())
        selector = "glusterfs-node=pod"
        wait_for_pods_be_ready(self.master, pod_count, selector)

        # Validate cassandra pod state, multipath and issci
        switch_oc_project(self.master, self.metrics_project_name)
        wait_for_pod_be_ready(self.master, hawkular_cassandra, timeout=2)
        self.verify_iscsi_sessions_and_multipath(
            pvc_name, self.metrics_rc_hawkular_cassandra,
            rtype='rc', heketi_server_url=self.registry_heketi_server_url,
            is_registry_gluster=True)

    def cassandra_pod_delete_cleanup(self):
        """Cleanup for deletion of cassandra pod using force delete"""
        try:
            # Check if pod is up or ready
            pod_name = get_pod_name_from_rc(
                self.master, self.metrics_rc_hawkular_cassandra)
            wait_for_pod_be_ready(self.master, pod_name, timeout=1)
        except exceptions.ExecutionError:
            # Force delete and wait for new pod to come up
            oc_delete(self.master, 'pod', pod_name, is_force=True)
            wait_for_resources_absence(self.master, 'pod', pod_name)
            new_pod_name = get_pod_name_from_rc(
                self.master, self.metrics_rc_hawkular_cassandra)
            wait_for_pod_be_ready(self.master, new_pod_name)

    def test_metrics_during_cassandra_pod_respin(self):
        """Validate cassandra pod respin"""
        old_cassandra_pod, pvc_name, _, _, _ = (
            self.verify_cassandra_pod_multipath_and_iscsi())

        # Delete the cassandra pod and wait for new pod to be ready
        oc_delete(self.master, 'pod', old_cassandra_pod)
        self.addCleanup(self.cassandra_pod_delete_cleanup)
        wait_for_resources_absence(self.master, 'pod', old_cassandra_pod)
        new_cassandra_pod = get_pod_name_from_rc(
            self.master, self.metrics_rc_hawkular_cassandra)
        wait_for_pod_be_ready(self.master, new_cassandra_pod)

        # Validate iscsi and multipath
        self.verify_iscsi_sessions_and_multipath(
            pvc_name, self.metrics_rc_hawkular_cassandra,
            rtype='rc', heketi_server_url=self.registry_heketi_server_url,
            is_registry_gluster=True)
