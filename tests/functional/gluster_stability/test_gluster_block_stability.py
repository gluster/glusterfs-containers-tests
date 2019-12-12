import re
from unittest import skip

import ddt
from glusto.core import Glusto as g
from glustolibs.gluster.block_libs import get_block_list
from pkg_resources import parse_version
import six

from openshiftstoragelibs.baseclass import GlusterBlockBaseClass
from openshiftstoragelibs.command import cmd_run
from openshiftstoragelibs.exceptions import (
    ConfigError,
    ExecutionError,
)
from openshiftstoragelibs.gluster_ops import (
    match_heketi_and_gluster_block_volumes_by_prefix
)
from openshiftstoragelibs.heketi_ops import (
    get_block_hosting_volume_list,
    get_total_free_space,
    heketi_blockvolume_create,
    heketi_blockvolume_delete,
    heketi_blockvolume_info,
    heketi_blockvolume_list,
    heketi_blockvolume_list_by_name_prefix,
    heketi_node_info,
    heketi_node_list,
    heketi_volume_delete,
    heketi_volume_info,
)
from openshiftstoragelibs.node_ops import (
    find_vm_name_by_ip_or_hostname,
    node_add_iptables_rules,
    node_delete_iptables_rules,
    node_reboot_by_command,
    power_off_vm_by_name,
    power_on_vm_by_name,
)
from openshiftstoragelibs.openshift_ops import (
    cmd_run_on_gluster_pod_or_node,
    get_ocp_gluster_pod_details,
    get_pod_name_from_dc,
    get_pv_name_from_pvc,
    get_vol_names_from_pv,
    get_pvc_status,
    kill_service_on_gluster_pod_or_node,
    match_pv_and_heketi_block_volumes,
    oc_adm_manage_node,
    oc_create_pvc,
    oc_delete,
    oc_get_custom_resource,
    oc_get_pv,
    oc_get_schedulable_nodes,
    oc_rsh,
    restart_service_on_gluster_pod_or_node,
    scale_dcs_pod_amount_and_wait,
    wait_for_events,
    wait_for_ocp_node_be_ready,
    wait_for_pod_be_ready,
    wait_for_pvcs_be_bound,
    wait_for_resource_absence,
    wait_for_resources_absence,
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
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import utils
from openshiftstoragelibs.waiter import Waiter


@ddt.ddt
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

    def bulk_app_pods_creation_with_block_pv(self, app_pod_count):
        prefix = "autotest-%s" % utils.get_random_str()
        self.create_storage_class(sc_name_prefix=prefix,
                                  create_vol_name_prefix=True, set_hacount=3)
        size_of_pvc = 1

        # Create pvs & dc's
        pvc_names = self.create_and_wait_for_pvcs(
            pvc_size=size_of_pvc, pvc_amount=app_pod_count)
        dcs = self.create_dcs_with_pvc(pvc_names)

        # Validate iscsi sessions & multipath for created pods
        for pvc_name in pvc_names:
            dc_name, pod_name = dcs[pvc_name]
            wait_for_pod_be_ready(self.node, pod_name, wait_step=10)
            iqn, _, ini_node = self.verify_iscsi_sessions_and_multipath(
                pvc_name, dc_name)

        h_blockvol_list = heketi_blockvolume_list_by_name_prefix(
            self.heketi_client_node, self.heketi_server_url, prefix)

        # validate block volumes listed by heketi and pvs
        heketi_blockvolume_ids = sorted([bv[0] for bv in h_blockvol_list])
        match_pv_and_heketi_block_volumes(
            self.node, heketi_blockvolume_ids, pvc_prefix=prefix)

        # validate block volumes listed by heketi and gluster
        heketi_blockvolume_names = sorted([
            bv[1].replace("%s_" % prefix, "") for bv in h_blockvol_list])
        match_heketi_and_gluster_block_volumes_by_prefix(
            heketi_blockvolume_names, "%s_" % prefix)

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

    def test_validate_gluster_ip_utilized_by_blockvolumes(self):
        """ Validate if all gluster nodes IP are
            utilized by blockvolume when using HA=2
        """
        host_ips, block_volumes, gluster_ips_bv = [], [], []
        target_portal_list, pv_info = [], {}
        size_of_pvc, amount_of_pvc, ha_count = 1, 5, 2
        unmatched_gips, unmatched_tpips = [], []

        node_list = heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)
        if len(node_list) < 3:
            self.skipTest("Skip test since number of nodes"
                          "are less than 3.")
        for node_id in node_list:
            node_info = heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            host_ips.append(node_info["hostnames"]["storage"][0])

        # Create new sc with ha count as 2
        sc_name = self.create_storage_class(set_hacount=ha_count)

        # Create pvcs
        pvc_names = self.create_and_wait_for_pvcs(
            pvc_size=size_of_pvc, pvc_amount=amount_of_pvc, sc_name=sc_name)

        # From PVC get pv_name,targetportal_ip & blockvolumes
        # Attach pvc to app pods
        for pvc_name in pvc_names:
            pv_name = get_pv_name_from_pvc(self.node, pvc_name)
            pv_info = oc_get_pv(self.node, pv_name)
            target_portal_list.extend(
                pv_info.get('spec').get('iscsi').get('portals'))
            target_portal_list.append(
                pv_info.get('spec').get('iscsi').get('targetPortal'))
            block_volume = oc_get_custom_resource(
                self.node, 'pv',
                r':.metadata.annotations."gluster\.org\/volume\-id"',
                name=pv_name)[0]
            block_volumes.append(block_volume)
        self.create_dcs_with_pvc(pvc_names)

        # Validate that all gluster ips are utilized by block volume.
        for bv_id in block_volumes:
            block_volume_info = heketi_blockvolume_info(
                self.heketi_client_node, self.heketi_server_url,
                bv_id, json=True)
            gluster_ips_bv.append(block_volume_info["blockvolume"]["hosts"])
        gluster_ips_bv_flattend = [
            ip for list_of_lists in gluster_ips_bv for ip in list_of_lists]
        host_ips = sorted(set(host_ips))
        gluster_ips_bv_flattend = sorted(set(gluster_ips_bv_flattend))
        target_portal_list = sorted(set(target_portal_list))
        unmatched_gips = (set(host_ips) ^ set(gluster_ips_bv_flattend))
        unmatched_tpips = (set(host_ips) ^ set(target_portal_list))
        self.assertFalse(
            unmatched_gips,
            "Could not match glusterips in blockvolumes, difference is %s "
            % unmatched_gips)
        self.assertFalse(
            unmatched_tpips,
            "Could not match glusterips in pv describe, difference is %s "
            % unmatched_tpips)

    @ddt.data('tcmu-runner', 'gluster-blockd')
    def test_volume_create_delete_when_block_services_are_down(self, service):
        """Create and Delete PVC's when block related services gluster-blockd,
        tcmu-runner are down.
        """
        # Get heketi Node count for HA count
        h_nodes_ids = heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)

        # Create SC with HA = heketi node count
        vol_name_prefix = 'vol-%s' % utils.get_random_str(size=5)
        sc_name = self.create_storage_class(
            vol_name_prefix=vol_name_prefix, hacount=len(h_nodes_ids))

        # Get gluster node IP's
        g_nodes = []
        for h_node in h_nodes_ids[:2]:
            g_node = heketi_node_info(
                self.heketi_client_node, self.heketi_server_url, h_node,
                json=True)
            g_nodes.append(g_node['hostnames']['manage'][0])

        stop_svc_cmd = 'systemctl stop %s' % service
        start_svc_cmd = 'systemctl start gluster-blockd'
        if service == 'tcmu-runner':
            start_svc_cmd += ' gluster-block-target tcmu-runner'

        pvc_counts = [5, 10, 15, 20]

        for pvc_count in pvc_counts:
            pvc_names = []
            # Create PVC's
            for i in range(pvc_count):
                pvc_name = oc_create_pvc(self.node, sc_name)
                pvc_names.append(pvc_name)
                self.addCleanup(
                    wait_for_resource_absence, self.node, 'pvc', pvc_name)
                self.addCleanup(
                    oc_delete, self.node, 'pvc', pvc_name,
                    raise_on_absence=False)

            # Stop tcmu-runner/gluster-blockd service
            for g_node in g_nodes:
                cmd_run_on_gluster_pod_or_node(
                    self.node, stop_svc_cmd, g_node)
                self.addCleanup(
                    wait_for_service_status_on_gluster_pod_or_node, self.node,
                    'gluster-blockd', 'active', 'running', g_node,
                    raise_on_error=False)
                self.addCleanup(
                    cmd_run_on_gluster_pod_or_node, self.node, start_svc_cmd,
                    g_node)

            # Get PVC status after stoping tcmu-runner
            statuses = []
            for w in Waiter(10, 5):
                statuses = oc_get_custom_resource(
                    self.node, 'pvc', ':.status.phase', name=pvc_names)
            statuses = statuses[0].split('\n')
            self.assertEqual(len(statuses), len(pvc_names))

            msg = ("All PVC's got Bound before bringing the tcmu-runner "
                   "service down.")
            self.assertNotEqual(statuses.count('Bound'), 20, msg)

            # Continue if all PVC's got bound before bringing down services
            if statuses.count('Bound') == pvc_count:
                # Start services
                for g_node in g_nodes:
                    cmd_run_on_gluster_pod_or_node(
                        self.node, start_svc_cmd, g_node)
                    wait_for_service_status_on_gluster_pod_or_node(
                        self.node, 'gluster-blockd', 'active', 'running',
                        g_node, raise_on_error=False)
                continue
            else:
                break

        err = 'Please check if gluster-block daemon is operational'
        err_msg = 'Did not receive any response from gluster-block daemon'
        # Wait for PVC creation Failure events
        for pvc, status in zip(pvc_names, statuses):
            if status == 'Bound':
                continue
            events = wait_for_events(
                self.node, obj_name=pvc, obj_type='PersistentVolumeClaim',
                event_reason='ProvisioningFailed', event_type='Warning')
            for event in events:
                if err in event['message'] or err_msg in event['message']:
                    break
            msg = "Did not found '%s' or '%s' in events '%s'" % (
                err, err_msg, events)
            self.assertTrue(
                (err in event['message']) or (err_msg in event['message']),
                msg)

        # Verify no PVC's got bound after bringing down block services
        statuses_new = oc_get_custom_resource(
            self.node, 'pvc', ':.status.phase', name=pvc_names)
        statuses_new = statuses_new[0].split('\n')
        self.assertEqual(len(statuses_new), len(pvc_names))
        self.assertEqual(statuses.count('Bound'), statuses_new.count('Bound'))

        # Start services
        for g_node in g_nodes:
            cmd_run_on_gluster_pod_or_node(self.node, start_svc_cmd, g_node)
            wait_for_service_status_on_gluster_pod_or_node(
                self.node, 'gluster-blockd', 'active', 'running', g_node,
                raise_on_error=False)

        # Make sure that PVCs are bound
        wait_for_pvcs_be_bound(self.node, pvc_names, timeout=60)
        try:
            # Stop tcmu-runner/gluster-blockd service
            for g_node in g_nodes:
                cmd_run_on_gluster_pod_or_node(self.node, stop_svc_cmd, g_node)

            # Delete PVC's and check heketi logs for error
            for pvc in pvc_names:
                pv = get_pv_name_from_pvc(self.node, pvc)
                bvol_name = oc_get_custom_resource(
                    self.node, 'pv',
                    ':.metadata.annotations.glusterBlockShare', name=pv)[0]

                regex = (
                    r"(Failed to run command .*gluster-block delete .*\/%s "
                    r"--json.*)" % bvol_name)
                cmd_date = "date -u --rfc-3339=ns | cut -d '+' -f 1"
                date = cmd_run(cmd_date, self.node).split(" ")

                oc_delete(self.node, 'pvc', pvc)

                h_pod_name = get_pod_name_from_dc(
                    self.node, self.heketi_dc_name)
                cmd_logs = "oc logs %s --since-time %sT%sZ" % (
                    h_pod_name, date[0], date[1])

                for w in Waiter(20, 5):
                    logs = cmd_run(cmd_logs, self.node)
                    status_match = re.search(regex, logs)
                    if status_match:
                        break
                msg = "Did not found '%s' in the heketi logs '%s'" % (
                    regex, logs)
                self.assertTrue(status_match, msg)

                with self.assertRaises(ExecutionError):
                    wait_for_resource_absence(self.node, 'pvc', pvc, timeout=5)
        finally:
            # Start services back
            for g_node in g_nodes:
                cmd_run_on_gluster_pod_or_node(
                    self.node, start_svc_cmd, g_node)
                wait_for_service_status_on_gluster_pod_or_node(
                    self.node, 'gluster-blockd', 'active', 'running', g_node,
                    raise_on_error=True)

        # Wait for PVC's deletion after bringing up services
        for pvc in pvc_names:
            wait_for_resource_absence(self.node, 'pvc', pvc, timeout=120)

        # Verify volumes were deleted in heketi as well
        h_vol_list = heketi_blockvolume_list(
            self.heketi_client_node, self.heketi_server_url)
        self.assertNotIn(vol_name_prefix, h_vol_list)

    @skip("Blocked by BZ-1624670")
    def test_path_failures_on_initiator_node_migration_and_pod_restart(self):
        """Verify path failures on initiator node migration
           and app pod restart. Also, make sure that existing
           paths get cleaned up and recreated on new nodes.
        """
        pvc_size, pvc_amount = 1, 20
        free_space, node_count = get_total_free_space(
            self.heketi_client_node, self.heketi_server_url)
        if node_count < 3:
            self.skipTest("Skip test since number of nodes"
                          "online is less than 3.")
        free_space_available = int(free_space / node_count)
        space_required = int(pvc_size * pvc_amount)
        if free_space_available < space_required:
            self.skipTest("Skip test since free_space_available %s"
                          "is less than the space_required %s."
                          % (free_space_available, space_required))

        # Create pvs & dc's
        pvc_names = self.create_and_wait_for_pvcs(
            pvc_size=pvc_size, pvc_amount=pvc_amount,
            timeout=300, wait_step=5)
        dc_and_pod_names = self.create_dcs_with_pvc(
            pvc_names, timeout=600, wait_step=5)

        # Validate iscsi sessions & multipath for created pods
        for pvc, (dc_name, pod_name) in dc_and_pod_names.items():
            iqn, _, ini_node = self.verify_iscsi_sessions_and_multipath(
                pvc, dc_name)
            dc_and_pod_names[pvc] = (dc_name, pod_name, ini_node, iqn)

        # Run IO on app pods
        _file, base_size, count = '/mnt/file', 4096, 1000
        file_size = base_size * count
        cmd_run_io = 'dd if=/dev/urandom of=%s bs=%s count=%s' % (
            _file, base_size, count)
        for _, pod_name, _, _ in dc_and_pod_names.values():
            oc_rsh(self.node, pod_name, cmd_run_io)

        # Start deleting the app pods & wait for new pods to spin up
        dc_names = [dc_name[0] for dc_name in dc_and_pod_names.values()]
        scale_dcs_pod_amount_and_wait(self.node, dc_names, pod_amount=0)
        pod_names = scale_dcs_pod_amount_and_wait(
            self.node, dc_names, pod_amount=1, timeout=1200, wait_step=5)

        temp_pvc_node = {}
        for pvc, (dc_name, _, _, _) in dc_and_pod_names.items():
            _, _, new_node = self.verify_iscsi_sessions_and_multipath(
                pvc, dc_name)
            temp_pvc_node[pvc] = new_node

        # Validate devices are logged out from the old node
        for w in Waiter(900, 10):
            if not temp_pvc_node.items():
                break
            for pvc, new_node in temp_pvc_node.items():
                if new_node != dc_and_pod_names[pvc][2]:
                    iscsi = get_iscsi_session(
                        dc_and_pod_names[pvc][2],
                        dc_and_pod_names[pvc][3],
                        raise_on_error=False)
                    if not iscsi:
                        del temp_pvc_node[pvc]
                else:
                    del temp_pvc_node[pvc]
        msg = ("logout of iqn's for PVC's '%s' did not happen"
               % temp_pvc_node.keys())
        self.assertFalse(temp_pvc_node, msg)

        # Validate data written is present in the respinned app pods
        for pod_name in pod_names.values():
            cmd_run_out = "ls -l %s" % _file
            _, out, _ = oc_rsh(self.node, pod_name[0], cmd_run_out)
            msg = ("Expected size %s of file '%s' is not present "
                   "in out '%s'" % (file_size, _file, out))
            self.assertIn(six.text_type(file_size), out, msg)

    def test_tcmu_runner_failure_while_creating_and_deleting_pvc(self):
        """Kill the tcmu-runner service while creating and deleting PVC's"""

        prefix = "auto-block-test-%s" % utils.get_random_str()

        # Create DC and pod with created PVC.
        sc_name = self.create_storage_class(
            hacount=len(self.gluster_servers), vol_name_prefix=prefix)
        pvc_name = self.create_and_wait_for_pvc(sc_name=sc_name)
        dc_name, pod_name = self.create_dc_with_pvc(pvc_name)

        # Validate iscsi and multipath
        self.verify_iscsi_sessions_and_multipath(pvc_name, dc_name)

        # Create 8 PVC's and add to list
        pvc_names_for_creations, pv_names_for_deletions = [], []
        pvc_names_for_deletions = self.create_and_wait_for_pvcs(
            pvc_amount=8, sc_name=sc_name)

        # Get their pv names and add to list
        for pvc_name in pvc_names_for_deletions:
            pv_names_for_deletions.append(
                get_pv_name_from_pvc(self.node, pvc_name))

        # Delete PVC's without wait which were created earlier
        for delete_pvc_name in pvc_names_for_deletions:
            oc_delete(self.node, 'pvc', delete_pvc_name)

            # Create PVC's without wait at the same time and add to list
            create_pvc_name = oc_create_pvc(self.node, sc_name=sc_name)
            pvc_names_for_creations.append(create_pvc_name)
            self.addCleanup(
                wait_for_resource_absence, self.node, 'pvc', create_pvc_name)
        pvc_names = " ".join(pvc_names_for_creations)
        self.addCleanup(oc_delete, self.node, "pvc", pvc_names)

        # Kill Tcmu-runner service
        services = ("tcmu-runner", "gluster-block-target", "gluster-blockd")
        kill_service_on_gluster_pod_or_node(
            self.node, "tcmu-runner", self.gluster_servers[0])
        self.addCleanup(
            cmd_run_on_gluster_pod_or_node,
            self.node, "systemctl restart  %s" % " ".join(services),
            gluster_node=self.gluster_servers[0])

        # Restart the services
        for service in services:
            state = (
                'exited' if service == 'gluster-block-target' else 'running')
            restart_service_on_gluster_pod_or_node(
                self.node, service, self.gluster_servers[0])
            wait_for_service_status_on_gluster_pod_or_node(
                self.node, service, 'active', state, self.gluster_servers[0])

        # Wait for PV's absence and PVC's getting bound
        wait_for_resources_absence(self.node, 'pv', pv_names_for_deletions)
        wait_for_pvcs_be_bound(self.node, pvc_names_for_creations, timeout=300)

        # Validate volumes in heketi blockvolume list
        heketi_volumes = heketi_blockvolume_list(
            self.heketi_client_node, self.heketi_server_url)
        volume_count = heketi_volumes.count(prefix)
        msg = (
            'Wrong volume count in heketi blockvolume list %s and expected '
            'volume count is 9 ' % volume_count)
        self.assertEqual(9, volume_count, msg)

    def test_initiator_side_failures_create_100_app_pods_with_block_pv(self):

        # Skip test case if OCS version in lower than 3.11.4
        if get_openshift_storage_version() < "3.11.4":
            self.skipTest("Skipping test case due to BZ-1607520, which is"
                          " fixed in OCS 3.11.4")

        # Skip the test if iscsi-initiator-utils version is not the expected
        e_pkg_version = '6.2.0.874-13'
        cmd = ("rpm -q iscsi-initiator-utils"
               " --queryformat '%{version}-%{release}\n'"
               "| cut -d '.' -f 1,2,3,4")
        for g_server in self.gluster_servers:
            out = self.cmd_run(cmd, g_server)
            if parse_version(out) < parse_version(e_pkg_version):
                self.skipTest(
                    "Skip test since isci initiator utils version actual: %s "
                    "is less than expected: %s on node %s, for more info "
                    "refer to BZ-1624670" % (out, e_pkg_version, g_server))

        nodes = oc_get_schedulable_nodes(self.node)

        # Get list of all gluster nodes
        g_pods = get_ocp_gluster_pod_details(self.node)
        g_nodes = [pod['pod_hostname'] for pod in g_pods]

        # Skip test case if required schedulable node count not met
        if len(set(nodes) - set(g_nodes)) < 1:
            self.skipTest("skipping test case because it needs at least one"
                          " node schedulable")

        # Make containerized Gluster nodes unschedulable
        if g_nodes:
            # Make gluster nodes unschedulable
            oc_adm_manage_node(self.node, '--schedulable=false', nodes=g_nodes)

            # Make gluster nodes schedulable
            self.addCleanup(
                oc_adm_manage_node, self.node, '--schedulable=true',
                nodes=g_nodes)

        # Create and validate 100 app pod creations with block PVs attached
        self.bulk_app_pods_creation_with_block_pv(app_pod_count=100)

    def test_delete_block_volume_with_one_node_down(self):
        """Validate deletion of block volume when one node is down"""

        heketi_node_count = len(
            heketi_node_list(
                self.heketi_client_node, self.heketi_server_url))
        if heketi_node_count < 4:
            self.skipTest(
                "At least 4 nodes are required, found %s." % heketi_node_count)

        # Power off one of the node
        try:
            vm_name = find_vm_name_by_ip_or_hostname(self.gluster_servers[3])
        except (NotImplementedError, ConfigError) as e:
            self.skipTest(e)
        power_off_vm_by_name(vm_name)
        self.addCleanup(power_on_vm_by_name, vm_name)

        # Create block volume with hacount 4
        try:
            block_vol_ha4 = heketi_blockvolume_create(
                self.heketi_client_node, self.heketi_server_url, 2,
                ha=heketi_node_count, json=True)
            self.addCleanup(
                heketi_blockvolume_delete, self.heketi_client_node,
                self.heketi_server_url, block_vol_ha4["id"])
        except Exception as e:
            if ("insufficient block hosts online" not in six.text_type(e)):
                raise
            g.log.error("ha=4 Block volume is not created on 3 available "
                        "nodes as expected.")

        # Create block volume with hacount 3
        block_volume = heketi_blockvolume_create(
            self.heketi_client_node, self.heketi_server_url,
            2, ha=3, json=True)
        heketi_blockvolume_delete(
            self.heketi_client_node, self.heketi_server_url,
            block_volume["id"])

    def test_create_block_pvcs_with_network_failure(self):
        """Block port 24010 while creating PVC's, run I/O's and verify
           multipath"""
        chain = 'OS_FIREWALL_ALLOW'
        rules = '-p tcp -m state --state NEW -m tcp --dport 24010 -j ACCEPT'
        sc_name = self.create_storage_class(hacount=len(self.gluster_servers))
        self.create_and_wait_for_pvc(sc_name=sc_name)

        # Create app pod, validate multipath and run I/O
        dc_name, pod_name = self.create_dc_with_pvc(self.pvc_name)
        self.verify_iscsi_sessions_and_multipath(self.pvc_name, dc_name)
        cmd_run_io = 'dd if=/dev/urandom of=/mnt/%s bs=4k count=10000'
        oc_rsh(self.node, pod_name, cmd_run_io % 'file1')

        # Create 5 PVC's, simultaneously close the port and run I/O
        pvc_names_for_creations = self.create_pvcs_not_waiting(
            pvc_amount=5, sc_name=sc_name)
        try:
            node_delete_iptables_rules(self.gluster_servers[0], chain, rules)
            oc_rsh(self.node, pod_name, cmd_run_io % 'file2')
        finally:
            # Open the closed port
            node_add_iptables_rules(self.gluster_servers[0], chain, rules)

        # Wait for PVC's to get bound
        wait_for_pvcs_be_bound(self.node, pvc_names_for_creations)

        # Create app pods and validate multipath
        self.verify_iscsi_sessions_and_multipath(self.pvc_name, dc_name)
        dc_and_pod_names = self.create_dcs_with_pvc(pvc_names_for_creations)
        for pvc_name, dc_with_pod in dc_and_pod_names.items():
            self.verify_iscsi_sessions_and_multipath(pvc_name, dc_with_pod[0])
            oc_rsh(self.node, dc_with_pod[1], cmd_run_io % 'file3')

    @ddt.data('active', 'passive', 'all_passive')
    def test_run_io_and_block_port_on_active_path_network_failure(
            self, path='active'):
        """Run I/O and block port on active or passive path."""
        rules = '-p tcp -m state --state NEW -m tcp --dport %s -j ACCEPT'
        path_nodes, file1, chain = [], 'file1', 'OS_FIREWALL_ALLOW'
        tcmu_port, gluster_blockd_port = 3260, 24010

        # Create storage class and PVC
        self.create_storage_class(hacount=len(self.gluster_servers))
        self.create_and_wait_for_pvc()

        # Create app pod and run I/0
        dc_name, pod_name = self.create_dc_with_pvc(self.pvc_name)
        cmd_run_io = 'dd if=/dev/urandom of=/mnt/%s bs=4k count=10000'
        oc_rsh(self.node, pod_name, cmd_run_io % file1)

        # Verify multipath and iscsi
        iqn, hacount, node = self.verify_iscsi_sessions_and_multipath(
            self.pvc_name, dc_name)

        # Get the active or passive(enabled) node ip
        devices = get_iscsi_block_devices_by_path(node, iqn)
        mpath = get_mpath_name_from_device_name(node, list(devices.keys())[0])
        active_passive_dict = get_active_and_enabled_devices_from_mpath(
            node, mpath)
        if path == 'active':
            path_nodes.append(devices[active_passive_dict['active'][0]])
        elif path == 'passive':
            path_nodes.append(devices[active_passive_dict['enabled'][0]])
        else:
            for passive_device in active_passive_dict['enabled']:
                path_nodes.append(devices[passive_device])

        # Close the port  3260 and 24010 and Run I/O
        for path_node in path_nodes:
            for port in (tcmu_port, gluster_blockd_port):
                node_delete_iptables_rules(path_node, chain, rules % port)
                self.addCleanup(
                    node_add_iptables_rules, path_node, chain, rules % port)
        oc_rsh(self.node, pod_name, cmd_run_io % file1)

        # Open the Ports, Run I/O and verify multipath
        for path_node in path_nodes:
            node_add_iptables_rules(
                path_node, chain, rules % gluster_blockd_port)
            node_add_iptables_rules(path_node, chain, rules % tcmu_port)
        oc_rsh(self.node, pod_name, cmd_run_io % file1)
        self.verify_iscsi_sessions_and_multipath(self.pvc_name, dc_name)

    def test_initiator_failures_reboot_initiator_node_when_target_node_is_down(
            self):
        """Restart initiator node when gluster node is down, to make sure paths
        rediscovery is happening.
        """
        # Skip test if not able to connect to Cloud Provider
        try:
            find_vm_name_by_ip_or_hostname(self.node)
        except (NotImplementedError, ConfigError) as e:
            self.skipTest(e)

        ini_node = self.get_initiator_node_and_mark_other_nodes_unschedulable()

        # Create 5 PVC's and DC's
        pvcs = self.create_and_wait_for_pvcs(pvc_amount=5)
        dcs = self.create_dcs_with_pvc(pvcs)

        # Run I/O on app pods
        _file, base_size, count = '/mnt/file', 4096, 1000
        file_size = base_size * count
        cmd_run_io = 'dd if=/dev/urandom of=%s bs=%s count=%s' % (
            _file, base_size, count)
        for _, pod_name in dcs.values():
            oc_rsh(self.node, pod_name, cmd_run_io)

        vol_info = {}
        for pvc, (dc_name, _) in dcs.items():
            # Get target portals
            pv_name = get_pv_name_from_pvc(self.node, pvc)
            targets = oc_get_custom_resource(
                self.node, 'pv', ':.spec.iscsi.portals,'
                ':.spec.iscsi.targetPortal', name=pv_name)
            targets = [item.strip('[').strip(
                ']') for item in targets if isinstance(item, six.string_types)]

            iqn, hacount, _ = self.verify_iscsi_sessions_and_multipath(
                pvc, dc_name)
            vol_info[pvc] = (iqn, hacount, targets)

        target = targets[0]

        # Get hostname of target node from heketi
        h_node_list = heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)
        for node_id in h_node_list:
            node_info = heketi_node_info(
                self.heketi_client_node, self.heketi_server_url, node_id,
                json=True)
            if node_info['hostnames']['storage'][0] == target:
                target_hostname = node_info['hostnames']['manage'][0]
                break
        self.assertTrue(target_hostname)

        # Find VM Name and power it off
        target_vm_name = find_vm_name_by_ip_or_hostname(target_hostname)
        self.power_off_gluster_node_vm(target_vm_name, target_hostname)

        # Sync I/O
        for _, pod_name in dcs.values():
            oc_rsh(self.node, pod_name, "/bin/sh -c 'cd /mnt && sync'")

        # Reboot initiator node where all the app pods are running
        node_reboot_by_command(ini_node)
        wait_for_ocp_node_be_ready(self.node, ini_node)

        # Wait for pods to be ready after node reboot
        dc_names = [dc[0] for dc in dcs.values()]
        pod_names = scale_dcs_pod_amount_and_wait(self.node, dc_names)

        # Verify one path is down, because one gluster node is down
        for iqn, _, _ in vol_info.values():
            devices = get_iscsi_block_devices_by_path(ini_node, iqn).keys()
            mpath = get_mpath_name_from_device_name(ini_node, list(devices)[0])
            with self.assertRaises(AssertionError):
                self.verify_all_paths_are_up_in_multipath(
                    mpath, hacount, ini_node, timeout=1)

        # Power on gluster node and wait for the services to be up
        self.power_on_gluster_node_vm(target_vm_name, target_hostname)

        # Delete pod so it can rediscover paths while creating new pod
        scale_dcs_pod_amount_and_wait(self.node, dc_names, pod_amount=0)
        pod_names = scale_dcs_pod_amount_and_wait(self.node, dc_names)

        # Verify file
        for pvc, (dc_name, _) in dcs.items():
            pod_name = pod_names[dc_name][0]
            _, out, _ = oc_rsh(self.node, pod_name, 'ls -l %s' % _file)
            msg = ("Expected size %s of file '%s' is not present "
                   "in out '%s'" % (file_size, _file, out))
            self.assertIn(six.text_type(file_size), out, msg)

        # Verify all paths are up and running
        for iqn, hacount, _ in vol_info.values():
            devices = get_iscsi_block_devices_by_path(ini_node, iqn).keys()

            # Get mpath names and verify that only one mpath is there
            mpaths = set()
            for device in devices:
                mpaths.add(get_mpath_name_from_device_name(ini_node, device))
            msg = ("Only one mpath was expected on Node %s, but got %s" % (
                ini_node, mpaths))
            self.assertEqual(1, len(mpaths), msg)

            self.verify_all_paths_are_up_in_multipath(
                list(mpaths)[0], hacount, ini_node, timeout=1)

    def get_vol_id_and_vol_names_from_pvc_names(self, pvc_names):
        vol_details = []
        for pvc_name in pvc_names:
            pv_name = get_pv_name_from_pvc(self.node, pvc_name)
            volume = get_vol_names_from_pv(
                self.node, pv_name, vol_type='block')
            vol_details.append(volume)
        return vol_details

    def check_errors_in_heketi_pod_network_failure_after_deletion(
            self, since_time, vol_names):
        # Get name of heketi pod
        heketi_pod_name = get_pod_name_from_dc(self.node, self.heketi_dc_name)

        # Check for errors in heketi pod logs
        err_cmd = (
            r'oc logs %s --since-time="%s" | grep " Failed to run command '
            r'\[gluster-block delete*"' % (heketi_pod_name, since_time))

        for w in Waiter(30, 5):
            err_out = cmd_run(err_cmd, self.node)
            if any(vol_name in err_out for vol_name in vol_names):
                break
        if w.expired:
            err_msg = ("Expected ERROR for volumes '%s' not generated in "
                       "heketi pod logs" % vol_names)
            raise AssertionError(err_msg)

    def test_delete_block_pvcs_with_network_failure(self):
        """Block port 24010 while deleting PVC's"""
        pvc_amount, pvc_delete_amount = 10, 5
        gluster_node = self.gluster_servers[0]
        chain = 'OS_FIREWALL_ALLOW'
        rules = '-p tcp -m state --state NEW -m tcp --dport 24010 -j ACCEPT'

        sc_name = self.create_storage_class(hacount=len(self.gluster_servers))

        # Get the total free space available
        initial_free_storage = get_total_free_space(
            self.heketi_client_node, self.heketi_server_url)

        # Create  10 PVC's, get their PV names and volume ids
        pvc_names = self.create_and_wait_for_pvcs(
            sc_name=sc_name, pvc_amount=pvc_amount, timeout=240)
        vol_details = self.get_vol_id_and_vol_names_from_pvc_names(pvc_names)
        vol_names = [vol_name['gluster_vol'] for vol_name in vol_details]

        # Delete 5 PVCs not waiting for the results
        oc_delete(self.node, 'pvc', " ".join(pvc_names[:pvc_delete_amount]))

        # Get time to collect logs
        since_time = cmd_run(
            'date -u --rfc-3339=ns| cut -d  "+" -f 1', self.node).replace(
                " ", "T") + "Z"

        try:
            # Close the port #24010 on gluster node and then delete other 5 PVC
            # without wait
            node_delete_iptables_rules(gluster_node, chain, rules)
            oc_delete(
                self.node, 'pvc', ' '.join(pvc_names[pvc_delete_amount:]))
            self.addCleanup(
                wait_for_resources_absence,
                self.node, 'pvc', pvc_names[pvc_delete_amount:])

            # Check  errors in heketi pod logs
            self.check_errors_in_heketi_pod_network_failure_after_deletion(
                since_time, vol_names[pvc_delete_amount:])
        finally:
            # Open port 24010
            node_add_iptables_rules(gluster_node, chain, rules)

        # validate available free space is same
        final_free_storage = get_total_free_space(
            self.heketi_client_node, self.heketi_server_url)
        msg = ("Available free space %s is not same as expected %s"
               % (initial_free_storage, final_free_storage))
        self.assertEqual(initial_free_storage, final_free_storage, msg)

    @podcmd.GlustoPod()
    def test_delete_block_device_pvc_while_io_in_progress(self):
        """Delete block device or pvc while io is in progress"""
        size_of_pvc, amount_of_pvc = 1, 20
        e_pkg_version = '6.2.0.874-13'
        h_node, h_server = self.heketi_client_node, self.heketi_server_url
        free_space, node_count = get_total_free_space(h_node, h_server)

        # Skip the test if node count is less than 3
        if node_count < 3:
            self.skipTest("Skip test since number of nodes"
                          "online is %s which is less than 3." % node_count)

        # Skip the test if iscsi-initiator-utils version is not the expected
        cmd = ("rpm -q iscsi-initiator-utils"
               " --queryformat '%{version}-%{release}\n'"
               "| cut -d '.' -f 1,2,3,4")
        for g_server in self.gluster_servers:
            out = self.cmd_run(cmd, g_server)
            if parse_version(out) < parse_version(e_pkg_version):
                self.skipTest("Skip test since isci initiator utils version "
                              "actual: %s is less than expected: %s "
                              "on node %s, for more info refer to BZ-1624670"
                              % (out, e_pkg_version, g_server))

        # Skip the test if free space is less than required space
        free_space_available = int(free_space / node_count)
        free_space_required = int(size_of_pvc * amount_of_pvc)
        if free_space_available < free_space_required:
            self.skipTest("Skip test since free space available %s"
                          "is less than the required space. %s"
                          % (free_space_available, free_space_required))

        new_bhv_list, bv_list = [], []

        # Get existing list of BHV's
        existing_bhv_list = get_block_hosting_volume_list(h_node, h_server)
        bhv_list = list(existing_bhv_list.keys())

        # Create PVC's & app pods
        pvc_names = self.create_and_wait_for_pvcs(
            pvc_size=size_of_pvc, pvc_amount=amount_of_pvc,
            timeout=600, wait_step=5)
        dc_and_pod_names = self.create_dcs_with_pvc(
            pvc_names, timeout=600, wait_step=5)

        # Get new list of BHV's
        custom = r':.metadata.annotations."gluster\.org\/volume\-id"'
        for pvc_name in pvc_names:
            pv_name = get_pv_name_from_pvc(self.node, pvc_name)
            block_vol_id = oc_get_custom_resource(
                self.node, "pv", custom, name=pv_name)
            bv_list.append(block_vol_id)
            # Get block hosting volume ID
            bhv_id = heketi_blockvolume_info(
                h_node, h_server, block_vol_id[0], json=True
            )["blockhostingvolume"]
            if bhv_id not in bhv_list:
                new_bhv_list.append(bhv_id)
        for bh_id in new_bhv_list:
            self.addCleanup(
                heketi_volume_delete, h_node, h_server,
                bh_id, raise_on_error=False)

        for pvc_name in pvc_names:
            self.addCleanup(
                wait_for_resource_absence, self.node, 'pvc', pvc_name)
            self.addCleanup(
                oc_delete, self.node, 'pvc', pvc_name, raise_on_absence=False)

        # Validate iscsi sessions & multipath for created pods
        for pvc, (dc_name, pod_name) in dc_and_pod_names.items():
            iqn, _, ini_node = self.verify_iscsi_sessions_and_multipath(
                pvc, dc_name)
            dc_and_pod_names[pvc] = (dc_name, pod_name, ini_node, iqn)

        # Run IO on app pods
        _file, base_size, count = '/mnt/file', 4096, 1000
        cmd_run_io = 'dd if=/dev/urandom of=%s bs=%s count=%s' % (
            _file, base_size, count)
        for _, pod_name, _, _ in dc_and_pod_names.values():
            oc_rsh(self.node, pod_name, cmd_run_io)

        # Delete 10 pvcs
        oc_delete(self.node, 'pvc', " ".join(pvc_names[:10]))
        for w in Waiter(10, 1):
            statuses = []
            for pvc_name in pvc_names[:10]:
                status = get_pvc_status(self.node, pvc_name)
                statuses.append(status)
            if statuses.count("Terminating") == 10:
                break
        if w.expired:
            err_msg = "Not all 10 deleted PVCs are in Terminating state."
            raise AssertionError(err_msg)

        # Delete all app pods
        dc_names = [dc[0] for dc in dc_and_pod_names.values()]
        scale_dcs_pod_amount_and_wait(self.node, dc_names, pod_amount=0)

        oc_delete(self.node, 'pvc', " ".join(pvc_names[10:]))

        # Validate no stale mounts or iscsi login exist on initiator side
        temp_pvcs = pvc_names[:]
        for w in Waiter(900, 10):
            if not temp_pvcs:
                break
            for pvc_name in temp_pvcs:
                iscsi = get_iscsi_session(
                    dc_and_pod_names[pvc_name][2],
                    dc_and_pod_names[pvc_name][3],
                    raise_on_error=False)
                if not iscsi:
                    temp_pvcs.remove(pvc_name)
        msg = "logout of iqn's for PVC's '%s' did not happen" % temp_pvcs
        self.assertFalse(temp_pvcs, msg)

        # Validate that Heketi and Gluster do not have block volumes
        for bhv_id in new_bhv_list:
            heketi_vol_info = heketi_volume_info(
                h_node, h_server, bhv_id, json=True)
            self.assertNotIn(
                "blockvolume", heketi_vol_info["blockinfo"].keys())
            gluster_vol_info = get_block_list(
                'auto_get_gluster_endpoint', volname="vol_%s" % bhv_id)
            self.assertIsNotNone(
                gluster_vol_info,
                "Failed to get block list from bhv %s" % bhv_id)
            for blockvol in gluster_vol_info:
                self.assertNotIn("blockvol_", blockvol)

    def test_create_and_delete_block_pvcs_with_network_failure(self):
        """Create and delete volumes after blocking the port 24010 on 51% of
        the nodes"""
        chain, pvc_amount = 'OS_FIREWALL_ALLOW', 5
        rules = '-p tcp -m state --state NEW -m tcp --dport 24010 -j ACCEPT'

        # Create  5 PVC's, get PV names and volume ids
        sc_name = self.create_storage_class(hacount=len(self.gluster_servers))
        pvc_names = self.create_and_wait_for_pvcs(
            sc_name=sc_name, pvc_amount=pvc_amount)
        vol_details = self.get_vol_id_and_vol_names_from_pvc_names(pvc_names)
        vol_names = [vol_name['gluster_vol'] for vol_name in vol_details]
        vol_ids = [vol_id['heketi_vol'] for vol_id in vol_details]

        # Get the time to collect logs
        since_time = cmd_run(
            'date -u --rfc-3339=ns| cut -d  "+" -f 1', self.node).replace(
                " ", "T") + "Z"

        # Close the port 24010 on 51% of the nodes
        for i in range(len(self.gluster_servers) // 2 + 1):
            node_delete_iptables_rules(self.gluster_servers[i], chain, rules)
            self.addCleanup(
                node_add_iptables_rules, self.gluster_servers[i], chain, rules)

        # Create and delete 5 PVC's
        pvc_names_for_creations = self.create_pvcs_not_waiting(
            pvc_amount=pvc_amount, sc_name=sc_name)
        for pvc_name in pvc_names:
            oc_delete(self.node, 'pvc', pvc_name)
            self.addCleanup(
                wait_for_resource_absence, self.node, 'pvc', pvc_name)

        # Check errors in heketi pod logs and get pending creations
        self.check_errors_in_heketi_pod_network_failure_after_deletion(
            since_time, vol_names)

        # Open the port 24010, wait for PVC's to get bound
        for i in range(len(self.gluster_servers) // 2 + 1):
            node_add_iptables_rules(self.gluster_servers[i], chain, rules)
        wait_for_pvcs_be_bound(self.node, pvc_names_for_creations, timeout=300)

        # Verify volume deletion
        blockvolume_list = heketi_blockvolume_list(
            self.heketi_client_node, self.heketi_server_url)
        msg = "Unexpectedly volume '%s' exists in the volume list %s"
        for vol_id in vol_ids:
            self.assertNotIn(
                vol_id, blockvolume_list, msg % (vol_id, blockvolume_list))
