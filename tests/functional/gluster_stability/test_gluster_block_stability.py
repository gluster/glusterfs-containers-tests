import six

from openshiftstoragelibs.baseclass import GlusterBlockBaseClass
from openshiftstoragelibs.command import cmd_run
from openshiftstoragelibs.exceptions import ConfigError
from openshiftstoragelibs.heketi_ops import (
    heketi_node_info,
    heketi_node_list,
)
from openshiftstoragelibs.node_ops import (
    find_vm_name_by_ip_or_hostname,
    node_reboot_by_command,
    power_off_vm_by_name,
    power_on_vm_by_name,
)
from openshiftstoragelibs.openshift_ops import (
    cmd_run_on_gluster_pod_or_node,
    get_ocp_gluster_pod_details,
    get_pod_name_from_dc,
    get_pv_name_from_pvc,
    oc_adm_manage_node,
    oc_delete,
    oc_get_custom_resource,
    oc_get_schedulable_nodes,
    oc_rsh,
    scale_dcs_pod_amount_and_wait,
    wait_for_ocp_node_be_ready,
    wait_for_pod_be_ready,
    wait_for_resource_absence,
    wait_for_service_status_on_gluster_pod_or_node,
)
from openshiftstoragelibs.openshift_storage_libs import (
    get_active_and_enabled_devices_from_mpath,
    get_iscsi_block_devices_by_path,
    get_iscsi_session,
    get_mpath_name_from_device_name,
)
from openshiftstoragelibs.openshift_storage_version import (
    get_openshift_storage_version
)
from openshiftstoragelibs.openshift_version import (
    get_openshift_version
)
from openshiftstoragelibs.waiter import Waiter


class TestGlusterBlockStability(GlusterBlockBaseClass):
    '''Class that contain gluster-block stability TC'''

    def setUp(self):
        super(TestGlusterBlockStability, self).setUp()
        self.node = self.ocp_master_node[0]

        # TODO(vamahaja): Add check for CRS version
        if not self.is_containerized_gluster():
            self.skipTest(
                "Skipping this test case as CRS version check "
                "is not implemented")

        if get_openshift_storage_version() <= "3.9":
            self.skipTest(
                "Skipping this test case as multipath validation "
                "is not supported in OCS 3.9")

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

    def test_target_side_failures_gluster_blockd_kill_when_ios_going_on(self):
        """Run I/Os on block volume while gluster-blockd is stoped"""
        self.create_and_wait_for_pvc()

        # Create app pod
        dc_name, pod_name = self.create_dc_with_pvc(self.pvc_name)

        iqn, hacount, node = self.verify_iscsi_sessions_and_multipath(
            self.pvc_name, dc_name)

        cmd_run_io = 'dd if=/dev/urandom of=/mnt/%s bs=4k count=10000'

        # Get the paths
        devices = get_iscsi_block_devices_by_path(node, iqn)
        mpath = get_mpath_name_from_device_name(node, list(devices.keys())[0])
        mpath_dev = get_active_and_enabled_devices_from_mpath(node, mpath)
        paths = mpath_dev['active'] + mpath_dev['enabled']

        for path in paths:
            node_ip = devices[path]

            # Stop gluster-blockd service
            cmd_run_on_gluster_pod_or_node(
                self.node, 'systemctl stop gluster-blockd', node_ip)
            self.addCleanup(
                cmd_run_on_gluster_pod_or_node, self.node,
                'systemctl start gluster-blockd', node_ip)

            wait_for_pod_be_ready(self.node, pod_name, 6, 3)

            # Verify tcmu-runner, gluster-block-target service is running
            wait_for_service_status_on_gluster_pod_or_node(
                self.node, 'tcmu-runner', 'active', 'running', node_ip)
            wait_for_service_status_on_gluster_pod_or_node(
                self.node, 'gluster-block-target', 'active', 'exited', node_ip)

            self.verify_all_paths_are_up_in_multipath(mpath, hacount, node)

            # Run I/O
            oc_rsh(self.node, pod_name, cmd_run_io % 'file1')

            # Start service and verify status
            cmd_run_on_gluster_pod_or_node(
                self.node, 'systemctl start gluster-blockd', node_ip)
            wait_for_service_status_on_gluster_pod_or_node(
                self.node, 'gluster-blockd', 'active', 'running', node_ip)

            # Run I/O
            oc_rsh(self.node, pod_name, cmd_run_io % 'file2')

        # Verify that active path is same as before
        mpath_dev_new = get_active_and_enabled_devices_from_mpath(node, mpath)
        self.assertEqual(mpath_dev['active'][0], mpath_dev_new['active'][0])

    def test_target_side_failures_tcmu_runner_kill_when_ios_going_on(self):
        """Run I/Os on block volume while tcmu-runner is stoped"""
        self.create_and_wait_for_pvc()

        # Create app pod
        dc_name, pod_name = self.create_dc_with_pvc(self.pvc_name)

        iqn, hacount, node = self.verify_iscsi_sessions_and_multipath(
            self.pvc_name, dc_name)

        # Run I/O
        cmd_run_io = 'dd if=/dev/urandom of=/mnt/%s bs=4k count=10000'

        devices = get_iscsi_block_devices_by_path(node, iqn)
        mpath = get_mpath_name_from_device_name(node, list(devices.keys())[0])
        mpath_dev = get_active_and_enabled_devices_from_mpath(node, mpath)

        paths = mpath_dev['active'] + mpath_dev['enabled']

        for path in paths:
            node_ip = devices[path]

            # Stop tcmu-runner service
            cmd_run_on_gluster_pod_or_node(
                self.node, 'systemctl stop tcmu-runner', node_ip)
            start_svc = ('systemctl start gluster-blockd gluster-block-target '
                         'tcmu-runner')
            self.addCleanup(
                cmd_run_on_gluster_pod_or_node, self.node, start_svc, node_ip)

            wait_for_pod_be_ready(self.node, pod_name, 6, 3)

            # Verify gluster-blockd gluster-block-target service is not running
            wait_for_service_status_on_gluster_pod_or_node(
                self.node, 'gluster-blockd', 'inactive', 'dead', node_ip,
                raise_on_error=False)
            wait_for_service_status_on_gluster_pod_or_node(
                self.node, 'gluster-block-target', 'failed',
                'Result: exit-code', node_ip, raise_on_error=False)
            wait_for_service_status_on_gluster_pod_or_node(
                self.node, 'tcmu-runner', 'inactive', 'dead', node_ip,
                raise_on_error=False)

            # Wait for path to be failed
            for w in Waiter(120, 5):
                out = cmd_run('multipath -ll %s | grep %s' % (
                    mpath, path), node)
                if 'failed faulty running' in out:
                    break
            if w.expired:
                self.assertIn(
                    'failed faulty running', out, 'path %s of mpath %s is '
                    'still up and running. It should not be running '
                    'because tcmu-runner is down' % (path, mpath))

            # Run I/O
            wait_for_pod_be_ready(self.node, pod_name, 6, 3)
            oc_rsh(self.node, pod_name, cmd_run_io % 'file2')

            # Start services
            cmd_run_on_gluster_pod_or_node(self.node, start_svc, node_ip)

            # Verify services are running
            wait_for_service_status_on_gluster_pod_or_node(
                self.node, 'tcmu-runner', 'active', 'running', node_ip)
            wait_for_service_status_on_gluster_pod_or_node(
                self.node, 'gluster-block-target', 'active', 'exited', node_ip)
            wait_for_service_status_on_gluster_pod_or_node(
                self.node, 'gluster-blockd', 'active', 'running', node_ip)

            # Wait for path to come up
            self.verify_all_paths_are_up_in_multipath(mpath, hacount, node)

            # Run I/O
            wait_for_pod_be_ready(self.node, pod_name, 6, 3)
            oc_rsh(self.node, pod_name, cmd_run_io % 'file3')

        # Verify it returns to the original active path
        for w in Waiter(120, 5):
            mpath_dev_new = get_active_and_enabled_devices_from_mpath(
                node, mpath)
            if mpath_dev['active'][0] == mpath_dev_new['active'][0]:
                break
        if w.expired:
            self.assertEqual(
                mpath_dev['active'][0], mpath_dev_new['active'][0])

        # Verify that all the paths are up
        self.verify_all_paths_are_up_in_multipath(mpath, hacount, node)

    def test_initiator_side_failure_restart_pod_when_target_node_is_down(self):
        """Restart app pod when one gluster node is down"""
        # Skip test if does not meets requirements
        try:
            vm_name = find_vm_name_by_ip_or_hostname(self.node)
        except (NotImplementedError, ConfigError) as e:
            self.skipTest(e)

        # Get heketi node list
        h_nodes_ids = heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)

        # Get the ips and hostname of gluster nodes from heketi
        h_nodes = {}
        for node in h_nodes_ids:
            info = heketi_node_info(
                self.heketi_client_node, self.heketi_server_url, node,
                json=True)
            h_nodes[info['hostnames']['storage'][0]] = (
                info['hostnames']['manage'][0])

        pvc_name = self.create_and_wait_for_pvc()
        pv_name = get_pv_name_from_pvc(self.node, pvc_name)

        # Create app pod
        dc_name, pod_name = self.create_dc_with_pvc(self.pvc_name)

        iqn, hacount, p_node = self.verify_iscsi_sessions_and_multipath(
            self.pvc_name, dc_name)

        # Get list of containerized gluster nodes
        g_nodes = get_ocp_gluster_pod_details(self.node)

        # Get target portals for the PVC
        targets = oc_get_custom_resource(
            self.node, 'pv', ':.spec.iscsi.portals,:.spec.iscsi.targetPortal',
            name=pv_name)
        targets = [item.strip('[').strip(
            ']') for item in targets if isinstance(item, str)]

        # Select hostname for powering off
        if h_nodes[targets[0]] == p_node:
            vm_hostname = h_nodes[targets[1]]
        else:
            vm_hostname = h_nodes[targets[0]]

        # Find VM Name for powering it off
        vm_name = find_vm_name_by_ip_or_hostname(vm_hostname)

        # Unschedulable Node if containerised glusterfs
        if g_nodes:
            oc_adm_manage_node(self.node, '--schedulable=false', [vm_hostname])
            self.addCleanup(
                oc_adm_manage_node, self.node, '--schedulable', [vm_hostname])

        # Power off gluster node
        power_off_vm_by_name(vm_name)
        self.addCleanup(power_on_vm_by_name, vm_name)

        # Delete pod so it get respun
        oc_delete(self.node, 'pod', pod_name)
        wait_for_resource_absence(self.node, 'pod', pod_name)

        # Wait for pod to come up when 1 target node is down
        pod_name = get_pod_name_from_dc(self.node, dc_name)
        wait_for_pod_be_ready(self.node, pod_name, timeout=120, wait_step=5)

    def test_initiator_and_target_on_same_node_app_pod_deletion(self):
        """Test iscsi login and logout functionality on deletion of an app
        pod when initiator and target are on the same node.
        """
        if not self.is_containerized_gluster():
            self.skipTest("Skipping this TC as it's not supported on non"
                          " containerized glusterfs setup.")

        schedulable_nodes = oc_get_schedulable_nodes(self.node)

        # Get list of all gluster nodes
        g_pods = get_ocp_gluster_pod_details(self.node)
        g_nodes = [pod['pod_hostname'] for pod in g_pods]

        # Get the list of schedulable nodes other than gluster nodes
        o_nodes = list((set(schedulable_nodes) - set(g_nodes)))

        # Make nodes unschedulable other than gluster nodes
        oc_adm_manage_node(
            self.node, '--schedulable=false', nodes=o_nodes)
        self.addCleanup(
            oc_adm_manage_node, self.node, '--schedulable=true', nodes=o_nodes)

        # Create 10 PVC's and 10 DC's
        pvcs = self.create_and_wait_for_pvcs(pvc_amount=10)
        dcs = self.create_dcs_with_pvc(pvcs)

        # Wait for app pods and verify block sessions
        pvc_and_dc = {}
        for pvc in pvcs:
            dc_name, pod_name = dcs[pvc]
            wait_for_pod_be_ready(self.node, pod_name, wait_step=10)

            iqn, _, p_node = self.verify_iscsi_sessions_and_multipath(
                pvc, dc_name)
            pvc_and_dc[pvc] = {
                'dc_name': dc_name,
                'pod_name': pod_name,
                'p_node': p_node,
                'iqn': iqn
            }

        # Delete 5 pods permanently
        scale_dcs_pod_amount_and_wait(
            self.node, [pvc_and_dc[pvc]['dc_name'] for pvc in pvcs[:5]],
            pod_amount=0)

        # Wait for logout, for permanently deleted pods
        temp_pvcs = pvcs[:5]
        for w in Waiter(900, 10):
            if not temp_pvcs:
                break
            for pvc in temp_pvcs:
                # Get the iscsi from the previous node to verify logout
                iscsi = get_iscsi_session(
                    pvc_and_dc[pvc]['p_node'], pvc_and_dc[pvc]['iqn'],
                    raise_on_error=False)
                if not iscsi:
                    temp_pvcs.remove(pvc)
        msg = ("logout of iqn's for PVC's: '%s' did not happen" % temp_pvcs)
        self.assertFalse(temp_pvcs, msg)

        # Start IO and delete remaining pods, so they can re-spin
        _file = '/mnt/file'
        cmd_run_io = 'dd if=/dev/urandom of=%s bs=4k count=1000' % _file
        for pvc in pvcs[5:]:
            oc_rsh(self.node, pvc_and_dc[pvc]['pod_name'], cmd_run_io)
            oc_delete(self.node, 'pod', pvc_and_dc[pvc]['pod_name'])

        # Wait for remaining pods to come up
        for pvc in pvcs[5:]:
            wait_for_resource_absence(
                self.node, 'pod', pvc_and_dc[pvc]['pod_name'])

            # Wait for new pod to come up
            pod_name = get_pod_name_from_dc(
                self.node, pvc_and_dc[pvc]['dc_name'])
            wait_for_pod_be_ready(self.node, pod_name, wait_step=20)

            # Verify file
            _, out, _ = oc_rsh(self.node, pod_name, 'ls -l %s' % _file)
            msg = ('Expected size: 4096000 of file: %s is not present '
                   'in out: %s' % (_file, out))
            self.assertIn('4096000', out, msg)

            # Verify block sessions
            self.verify_iscsi_sessions_and_multipath(
                pvc, pvc_and_dc[pvc]['dc_name'])

    def get_initiator_node_and_mark_other_nodes_unschedulable(self):

        ocp_version = get_openshift_version(self.node)
        if ocp_version < '3.10':
            self.skipTest(
                "Block functionality doesn't work in OCP less than 3.10")

        # Get all nodes
        all_nodes = oc_get_custom_resource(
            self.node, 'no', ':.metadata.name,:.spec.unschedulable')
        all_nodes = {node[0]: node[1] for node in all_nodes}

        # Get master nodes
        master_nodes = oc_get_custom_resource(
            self.node, 'node', ':.metadata.name',
            selector='node-role.kubernetes.io/master=true')
        master_nodes = [node[0] for node in master_nodes]

        # Get list of all gluster nodes
        g_nodes = oc_get_custom_resource(
            self.node, 'pod', ':.spec.nodeName', selector='glusterfs-node=pod')
        g_nodes = [node[0] for node in g_nodes]

        # Get the list of nodes other than gluster and master
        o_nodes = list(
            (set(all_nodes.keys()) - set(g_nodes) - set(master_nodes)))

        # Find a schedulable nodes in other nodes if not skip the test
        initiator_nodes = [
            node for node in o_nodes if all_nodes[node] == '<none>']

        if not initiator_nodes:
            self.skipTest('Sufficient schedulable nodes are not available')

        # Get schedulable node
        schedulable_nodes = oc_get_schedulable_nodes(self.node)

        # Get the list of nodes which needs to be marked as unschedulable
        unschedule_nodes = list(
            set(schedulable_nodes) - set([initiator_nodes[0]]))

        # Mark all schedulable, gluster and master nodes as unschedulable
        # except one node on which app pods wil run
        oc_adm_manage_node(
            self.node, '--schedulable=false', nodes=unschedule_nodes)
        self.addCleanup(
            oc_adm_manage_node, self.node, '--schedulable=true',
            nodes=unschedule_nodes)

        return initiator_nodes[0]

    def test_initiator_and_target_on_diff_node_abrupt_reboot_of_initiator_node(
            self):
        """Abrupt reboot initiator node to make sure paths rediscovery is
        happening.
        """
        ini_node = self.get_initiator_node_and_mark_other_nodes_unschedulable()

        # Create 5 PVC's and 5 DC's
        pvcs = self.create_and_wait_for_pvcs(pvc_amount=5)
        dcs = self.create_dcs_with_pvc(pvcs)

        for pvc, (dc_name, _) in dcs.items():
            self.verify_iscsi_sessions_and_multipath(pvc, dc_name)

        # Run I/O on app pods
        _file, base_size, count = '/mnt/file', 4096, 1000
        file_size = base_size * count
        cmd_run_io = 'dd if=/dev/urandom of=%s bs=%s count=%s' % (
            _file, base_size, count)

        for _, pod_name in dcs.values():
            oc_rsh(self.node, pod_name, cmd_run_io)

        # Reboot initiator node where all the app pods are running
        node_reboot_by_command(ini_node)
        wait_for_ocp_node_be_ready(self.node, ini_node)

        # Wait for pods to be ready after node reboot
        pod_names = scale_dcs_pod_amount_and_wait(
            self.node, [dc[0] for dc in dcs.values()])

        for pvc, (dc_name, _) in dcs.items():
            pod_name = pod_names[dc_name][0]
            # Verify file
            _, out, _ = oc_rsh(self.node, pod_name, 'ls -l %s' % _file)
            msg = ("Expected size %s of file '%s' is not present "
                   "in out '%s'" % (file_size, _file, out))
            self.assertIn(six.text_type(file_size), out, msg)

            self.verify_iscsi_sessions_and_multipath(pvc, dc_name)
