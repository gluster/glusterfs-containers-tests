from openshiftstoragelibs.baseclass import GlusterBlockBaseClass
from openshiftstoragelibs.command import cmd_run
from openshiftstoragelibs.openshift_ops import (
    get_pod_name_from_dc,
    oc_adm_manage_node,
    oc_delete,
    oc_get_schedulable_nodes,
    wait_for_pod_be_ready,
    wait_for_resource_absence,
)
from openshiftstoragelibs.openshift_storage_libs import (
    get_iscsi_session,
)


class TestGlusterBlockStability(GlusterBlockBaseClass):
    '''Class that contain gluster-block stability TC'''

    def setUp(self):
        super(TestGlusterBlockStability, self).setUp()
        self.node = self.ocp_master_node[0]

    def initiator_side_failures(self):
        self.create_storage_class()
        self.create_and_wait_for_pvc()

        # Create app pod
        dc_name, pod_name = self.create_dc_with_pvc(self.pvc_name)

        iqn, _, node = self.verify_iscsi_sessions_and_multipath(
            self.pvc_name, dc_name)

        # Make node unschedulabe where pod is running
        oc_adm_manage_node(
            self.node, '--schedulable=false', nodes=[node])

        # Make node schedulabe where pod is running
        self.addCleanup(
            oc_adm_manage_node, self.node, '--schedulable=true',
            nodes=[node])

        # Delete pod so it get respun on any other node
        oc_delete(self.node, 'pod', pod_name)
        wait_for_resource_absence(self.node, 'pod', pod_name)

        # Wait for pod to come up
        pod_name = get_pod_name_from_dc(self.node, dc_name)
        wait_for_pod_be_ready(self.node, pod_name)

        # Get the iscsi session from the previous node to verify logout
        iscsi = get_iscsi_session(node, iqn, raise_on_error=False)
        self.assertFalse(iscsi)

        self.verify_iscsi_sessions_and_multipath(self.pvc_name, dc_name)

    def test_initiator_side_failures_initiator_and_target_on_different_node(
            self):

        nodes = oc_get_schedulable_nodes(self.node)

        # Get list of all gluster nodes
        cmd = ("oc get pods --no-headers -l glusterfs-node=pod "
               "-o=custom-columns=:.spec.nodeName")
        g_nodes = cmd_run(cmd, self.node)
        g_nodes = g_nodes.split('\n') if g_nodes else g_nodes

        # Skip test case if required schedulable node count not met
        if len(set(nodes) - set(g_nodes)) < 2:
            self.skipTest("skipping test case because it needs at least two"
                          " nodes schedulable")

        # Make containerized Gluster nodes unschedulable
        if g_nodes:
            # Make gluster nodes unschedulable
            oc_adm_manage_node(
                self.node, '--schedulable=false',
                nodes=g_nodes)

            # Make gluster nodes schedulable
            self.addCleanup(
                oc_adm_manage_node, self.node, '--schedulable=true',
                nodes=g_nodes)

        self.initiator_side_failures()

    def test_initiator_side_failures_initiator_and_target_on_same_node(self):
        # Note: This test case is supported for containerized gluster only.

        nodes = oc_get_schedulable_nodes(self.node)

        # Get list of all gluster nodes
        cmd = ("oc get pods --no-headers -l glusterfs-node=pod "
               "-o=custom-columns=:.spec.nodeName")
        g_nodes = cmd_run(cmd, self.node)
        g_nodes = g_nodes.split('\n') if g_nodes else g_nodes

        # Get the list of nodes other than gluster
        o_nodes = list((set(nodes) - set(g_nodes)))

        # Skip the test case if it is crs setup
        if not g_nodes:
            self.skipTest("skipping test case because it is not a "
                          "containerized gluster setup. "
                          "This test case is for containerized gluster only.")

        # Make other nodes unschedulable
        oc_adm_manage_node(
            self.node, '--schedulable=false', nodes=o_nodes)

        # Make other nodes schedulable
        self.addCleanup(
            oc_adm_manage_node, self.node, '--schedulable=true', nodes=o_nodes)

        self.initiator_side_failures()
