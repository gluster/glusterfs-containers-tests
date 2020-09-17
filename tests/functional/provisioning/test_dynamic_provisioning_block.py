import math
import random
from unittest import skip

from glusto.core import Glusto as g
import pytest

from openshiftstoragelibs.baseclass import GlusterBlockBaseClass
from openshiftstoragelibs.command import cmd_run
from openshiftstoragelibs.exceptions import (
    ConfigError,
    ExecutionError,
)
from openshiftstoragelibs.heketi_ops import (
    get_block_hosting_volume_list,
    get_total_free_space,
    heketi_blockvolume_create,
    heketi_blockvolume_delete,
    heketi_blockvolume_info,
    heketi_blockvolume_list,
    heketi_node_disable,
    heketi_node_enable,
    heketi_node_info,
    heketi_node_list,
    heketi_volume_create,
    heketi_volume_delete,
    heketi_volume_expand,
    heketi_volume_info,
)
from openshiftstoragelibs.node_ops import (
    find_vm_name_by_ip_or_hostname,
    node_reboot_by_command,
)
from openshiftstoragelibs.openshift_ops import (
    cmd_run_on_gluster_pod_or_node,
    get_default_block_hosting_volume_size,
    get_events,
    get_gluster_host_ips_by_pvc_name,
    get_gluster_pod_names_by_pvc_name,
    get_pod_name_from_dc,
    get_pv_name_from_pvc,
    match_pvc_and_pv,
    oc_create_app_dc_with_io,
    oc_create_pvc,
    oc_delete,
    oc_get_custom_resource,
    oc_rsh,
    scale_dc_pod_amount_and_wait,
    verify_pvc_status_is_bound,
    wait_for_events,
    wait_for_pod_be_ready,
    wait_for_pvcs_be_bound,
    wait_for_resource_absence,
    wait_for_resources_absence,
)
from openshiftstoragelibs.openshift_version import get_openshift_version
from openshiftstoragelibs import utils
from openshiftstoragelibs.waiter import Waiter


class TestDynamicProvisioningBlockP0(GlusterBlockBaseClass):
    '''
     Class that contain P0 dynamic provisioning test cases
     for block volume
    '''

    def setUp(self):
        super(TestDynamicProvisioningBlockP0, self).setUp()
        self.node = self.ocp_master_node[0]

    def dynamic_provisioning_glusterblock(
            self, set_hacount, create_vol_name_prefix=False):
        datafile_path = '/mnt/fake_file_for_%s' % self.id()

        # Create DC with attached PVC
        sc_name = self.create_storage_class(
            set_hacount=set_hacount,
            create_vol_name_prefix=create_vol_name_prefix)
        pvc_name = self.create_and_wait_for_pvc(
            pvc_name_prefix='autotest-block', sc_name=sc_name)
        dc_name, pod_name = self.create_dc_with_pvc(pvc_name)

        # Check that we can write data
        for cmd in ("dd if=/dev/urandom of=%s bs=1K count=100",
                    "ls -lrt %s",
                    "rm -rf %s"):
            cmd = cmd % datafile_path
            ret, out, err = oc_rsh(self.node, pod_name, cmd)
            self.assertEqual(
                ret, 0,
                "Failed to execute '%s' command on '%s'." % (cmd, self.node))

    def _dynamic_provisioning_block_with_bhv_cleanup(
            self, sc_name, pvc_size, bhv_list):
        """Dynamic provisioning for glusterblock with BHV Cleanup"""
        h_node, h_url = self.heketi_client_node, self.heketi_server_url
        pvc_name = oc_create_pvc(self.node, sc_name, pvc_size=pvc_size)
        try:
            verify_pvc_status_is_bound(self.node, pvc_name)
            pv_name = get_pv_name_from_pvc(self.node, pvc_name)
            custom = [r':.metadata.annotations."gluster\.org\/volume\-id"']
            bvol_id = oc_get_custom_resource(
                self.node, 'pv', custom, pv_name)[0]
            bhv_id = heketi_blockvolume_info(
                h_node, h_url, bvol_id, json=True)['blockhostingvolume']
            if bhv_id not in bhv_list:
                self.addCleanup(
                    heketi_volume_delete, h_node, h_url, bhv_id)
        finally:
            self.addCleanup(
                wait_for_resource_absence, self.node, 'pvc', pvc_name)
            self.addCleanup(
                oc_delete, self.node, 'pvc', pvc_name, raise_on_absence=True)

    @pytest.mark.tier1
    def test_dynamic_provisioning_glusterblock_hacount_true(self):
        """Validate dynamic provisioning for glusterblock
        """
        self.dynamic_provisioning_glusterblock(set_hacount=True)

    @pytest.mark.tier2
    def test_dynamic_provisioning_glusterblock_hacount_false(self):
        """Validate storage-class mandatory parameters for block
        """
        self.dynamic_provisioning_glusterblock(set_hacount=False)

    @pytest.mark.tier1
    def test_dynamic_provisioning_glusterblock_heketipod_failure(self):
        """Validate PVC with glusterblock creation when heketi pod is down"""
        datafile_path = '/mnt/fake_file_for_%s' % self.id()

        # Create DC with attached PVC
        sc_name = self.create_storage_class()
        app_1_pvc_name = self.create_and_wait_for_pvc(
            pvc_name_prefix='autotest-block', sc_name=sc_name)
        app_1_dc_name, app_1_pod_name = self.create_dc_with_pvc(app_1_pvc_name)

        # Write test data
        write_data_cmd = (
            "dd if=/dev/urandom of=%s bs=1K count=100" % datafile_path)
        ret, out, err = oc_rsh(self.node, app_1_pod_name, write_data_cmd)
        self.assertEqual(
            ret, 0,
            "Failed to execute command %s on %s" % (write_data_cmd, self.node))

        # Remove Heketi pod
        heketi_down_cmd = "oc scale --replicas=0 dc/%s --namespace %s" % (
            self.heketi_dc_name, self.storage_project_name)
        heketi_up_cmd = "oc scale --replicas=1 dc/%s --namespace %s" % (
            self.heketi_dc_name, self.storage_project_name)
        self.addCleanup(self.cmd_run, heketi_up_cmd)
        heketi_pod_name = get_pod_name_from_dc(
            self.node, self.heketi_dc_name, timeout=10, wait_step=3)
        self.cmd_run(heketi_down_cmd)
        wait_for_resource_absence(self.node, 'pod', heketi_pod_name)

        # Create second PVC
        app_2_pvc_name = oc_create_pvc(
            self.node, pvc_name_prefix='autotest-block2', sc_name=sc_name
        )
        self.addCleanup(
            wait_for_resource_absence, self.node, 'pvc', app_2_pvc_name)
        self.addCleanup(
            oc_delete, self.node, 'pvc', app_2_pvc_name
        )

        # Create second app POD
        app_2_dc_name = oc_create_app_dc_with_io(self.node, app_2_pvc_name)
        self.addCleanup(oc_delete, self.node, 'dc', app_2_dc_name)
        self.addCleanup(
            scale_dc_pod_amount_and_wait, self.node, app_2_dc_name, 0)
        app_2_pod_name = get_pod_name_from_dc(self.node, app_2_dc_name)

        # Bring Heketi pod back
        self.cmd_run(heketi_up_cmd)

        # Wait for Heketi POD be up and running
        new_heketi_pod_name = get_pod_name_from_dc(
            self.node, self.heketi_dc_name, timeout=10, wait_step=2)
        wait_for_pod_be_ready(
            self.node, new_heketi_pod_name, wait_step=5, timeout=120)

        # Wait for second PVC and app POD be ready
        verify_pvc_status_is_bound(self.node, app_2_pvc_name)
        wait_for_pod_be_ready(
            self.node, app_2_pod_name, timeout=150, wait_step=3)

        # Verify that we are able to write data
        ret, out, err = oc_rsh(self.node, app_2_pod_name, write_data_cmd)
        self.assertEqual(
            ret, 0,
            "Failed to execute command %s on %s" % (write_data_cmd, self.node))

    @pytest.mark.tier1
    def test_dynamic_provisioning_glusterblock_gluster_pod_or_node_failure(
            self):
        """Create glusterblock PVC when gluster pod or node is down."""
        datafile_path = '/mnt/fake_file_for_%s' % self.id()

        # Create DC with attached PVC
        sc_name = self.create_storage_class()
        pvc_name = self.create_and_wait_for_pvc(
            pvc_name_prefix='autotest-block', sc_name=sc_name)
        dc_name, pod_name = self.create_dc_with_pvc(pvc_name)

        # Run IO in background
        io_cmd = "oc rsh %s dd if=/dev/urandom of=%s bs=1000K count=900" % (
            pod_name, datafile_path)
        async_io = g.run_async(self.node, io_cmd, "root")

        # Check for containerized Gluster
        if self.is_containerized_gluster():
            # Pick up one of the hosts which stores PV brick (4+ nodes case)
            gluster_pod_data = get_gluster_pod_names_by_pvc_name(
                self.node, pvc_name)[0]

            # Delete glusterfs POD from chosen host and wait for
            # spawn of new one
            oc_delete(self.node, 'pod', gluster_pod_data["pod_name"])
            cmd = ("oc get pods -o wide | grep glusterfs | grep %s | "
                   "grep -v Terminating | awk '{print $1}'") % (
                       gluster_pod_data["pod_hostname"])
            for w in Waiter(600, 15):
                new_gluster_pod_name = self.cmd_run(cmd)
                if new_gluster_pod_name:
                    break
            if w.expired:
                error_msg = "exceeded timeout, new gluster pod not created"
                g.log.error(error_msg)
                raise AssertionError(error_msg)
            g.log.info("new gluster pod name is %s" % new_gluster_pod_name)
            wait_for_pod_be_ready(self.node, new_gluster_pod_name)
        else:
            pvc_hosting_node_ip = get_gluster_host_ips_by_pvc_name(
                self.node, pvc_name)[0]
            heketi_nodes = heketi_node_list(
                self.heketi_client_node, self.heketi_server_url)
            node_ip_for_reboot = None
            for heketi_node in heketi_nodes:
                heketi_node_ip = heketi_node_info(
                    self.heketi_client_node, self.heketi_server_url,
                    heketi_node, json=True)["hostnames"]["storage"][0]
                if heketi_node_ip == pvc_hosting_node_ip:
                    node_ip_for_reboot = heketi_node_ip
                    break

            if not node_ip_for_reboot:
                raise AssertionError(
                    "Gluster node IP %s not matched with heketi node %s" % (
                        pvc_hosting_node_ip, heketi_node_ip))

            node_reboot_by_command(node_ip_for_reboot)

        # Check that async IO was not interrupted
        ret, out, err = async_io.async_communicate()
        self.assertEqual(ret, 0, "IO %s failed on %s" % (io_cmd, self.node))

    @pytest.mark.tier2
    def test_glusterblock_logs_presence_verification(self):
        """Validate presence of glusterblock provisioner POD and it's status"""

        # Get glusterblock provisioner dc name
        cmd = ("oc get dc | awk '{ print $1 }' | "
               "grep -e glusterblock -e provisioner")
        dc_name = cmd_run(cmd, self.ocp_master_node[0], True)

        # Get glusterblock provisioner pod name and it's status
        gb_prov_name, gb_prov_status = oc_get_custom_resource(
            self.node, 'pod', custom=':.metadata.name,:.status.phase',
            selector='deploymentconfig=%s' % dc_name)[0]
        self.assertEqual(gb_prov_status, 'Running')

        # Create Secret, SC and PVC
        self.create_storage_class()
        self.create_and_wait_for_pvc()

        # Get list of Gluster nodes
        g_hosts = list(g.config.get("gluster_servers", {}).keys())
        self.assertGreater(
            len(g_hosts), 0,
            "We expect, at least, one Gluster Node/POD:\n %s" % g_hosts)

        # Perform checks on Gluster nodes/PODs
        logs = ("gluster-block-configshell", "gluster-blockd")

        cmd = "tail -n 5 /var/log/glusterfs/gluster-block/%s.log"
        for g_host in g_hosts:
            for log in logs:
                out = cmd_run_on_gluster_pod_or_node(
                    self.ocp_client[0], cmd % log, gluster_node=g_host)
                self.assertTrue(out, "Command '%s' output is empty." % cmd)

    @pytest.mark.tier1
    def test_dynamic_provisioning_glusterblock_heketidown_pvc_delete(self):
        """Validate PVC deletion when heketi is down"""

        # Create Secret, SC and PVCs
        self.create_storage_class()
        self.pvc_name_list = self.create_and_wait_for_pvcs(
            1, 'pvc-heketi-down', 3)

        # remove heketi-pod
        scale_dc_pod_amount_and_wait(self.ocp_client[0],
                                     self.heketi_dc_name,
                                     0,
                                     self.storage_project_name)
        try:
            # delete pvc
            for pvc in self.pvc_name_list:
                oc_delete(self.ocp_client[0], 'pvc', pvc)
            for pvc in self.pvc_name_list:
                with self.assertRaises(ExecutionError):
                    wait_for_resource_absence(
                        self.ocp_client[0], 'pvc', pvc, interval=3, timeout=30)
        finally:
            # bring back heketi-pod
            scale_dc_pod_amount_and_wait(self.ocp_client[0],
                                         self.heketi_dc_name,
                                         1,
                                         self.storage_project_name)

        # verify PVC's are deleted
        for pvc in self.pvc_name_list:
            wait_for_resource_absence(self.ocp_client[0], 'pvc',
                                      pvc,
                                      interval=1, timeout=120)

        # create a new PVC
        self.create_and_wait_for_pvc()

    @pytest.mark.tier1
    def test_recreate_app_pod_with_attached_block_pv(self):
        """Validate app pod attached block device I/O after restart"""
        datafile_path = '/mnt/temporary_test_file'

        # Create DC with POD and attached PVC to it
        sc_name = self.create_storage_class()
        pvc_name = self.create_and_wait_for_pvc(
            pvc_name_prefix='autotest-block', sc_name=sc_name)
        dc_name, pod_name = self.create_dc_with_pvc(pvc_name)

        # Write data
        write_cmd = "oc exec %s -- dd if=/dev/urandom of=%s bs=4k count=10000"
        self.cmd_run(write_cmd % (pod_name, datafile_path))

        # Recreate app POD
        scale_dc_pod_amount_and_wait(self.node, dc_name, 0)
        scale_dc_pod_amount_and_wait(self.node, dc_name, 1)
        new_pod_name = get_pod_name_from_dc(self.node, dc_name)

        # Check presence of already written file
        check_existing_file_cmd = (
            "oc exec %s -- ls %s" % (new_pod_name, datafile_path))
        out = self.cmd_run(check_existing_file_cmd)
        self.assertIn(datafile_path, out)

        # Perform I/O on the new POD
        self.cmd_run(write_cmd % (new_pod_name, datafile_path))

    @pytest.mark.tier1
    def test_volname_prefix_glusterblock(self):
        """Validate custom volname prefix blockvol"""

        self.dynamic_provisioning_glusterblock(
            set_hacount=False, create_vol_name_prefix=True)

        pv_name = get_pv_name_from_pvc(self.node, self.pvc_name)
        vol_name = oc_get_custom_resource(
            self.node, 'pv',
            ':.metadata.annotations.glusterBlockShare', pv_name)[0]

        block_vol_list = heketi_blockvolume_list(
            self.heketi_client_node, self.heketi_server_url)

        self.assertIn(vol_name, block_vol_list)

        self.assertTrue(vol_name.startswith(
            self.sc.get('volumenameprefix', 'autotest')))

    @pytest.mark.tier1
    def test_dynamic_provisioning_glusterblock_reclaim_policy_retain(self):
        """Validate retain policy for gluster-block after PVC deletion"""

        if get_openshift_version() < "3.9":
            self.skipTest(
                "'Reclaim' feature is not supported in OCP older than 3.9")

        self.create_storage_class(reclaim_policy='Retain')
        self.create_and_wait_for_pvc()

        dc_name = oc_create_app_dc_with_io(self.node, self.pvc_name)

        try:
            pod_name = get_pod_name_from_dc(self.node, dc_name)
            wait_for_pod_be_ready(self.node, pod_name)
        finally:
            scale_dc_pod_amount_and_wait(self.node, dc_name, pod_amount=0)
            oc_delete(self.node, 'dc', dc_name)

        # get the name of volume
        pv_name = get_pv_name_from_pvc(self.node, self.pvc_name)

        custom = [r':.metadata.annotations."gluster\.org\/volume\-id"',
                  r':.spec.persistentVolumeReclaimPolicy']
        vol_id, reclaim_policy = oc_get_custom_resource(
            self.node, 'pv', custom, pv_name)

        # checking the retainPolicy of pvc
        self.assertEqual(reclaim_policy, 'Retain')

        # delete the pvc
        oc_delete(self.node, 'pvc', self.pvc_name)

        # check if pv is also deleted or not
        with self.assertRaises(ExecutionError):
            wait_for_resource_absence(
                self.node, 'pvc', self.pvc_name, interval=3, timeout=30)

        # getting the blockvol list
        blocklist = heketi_blockvolume_list(self.heketi_client_node,
                                            self.heketi_server_url)
        self.assertIn(vol_id, blocklist)

        heketi_blockvolume_delete(self.heketi_client_node,
                                  self.heketi_server_url, vol_id)
        blocklist = heketi_blockvolume_list(self.heketi_client_node,
                                            self.heketi_server_url)
        self.assertNotIn(vol_id, blocklist)
        oc_delete(self.node, 'pv', pv_name)
        wait_for_resource_absence(self.node, 'pv', pv_name)

    def verify_free_space(self, free_space):
        # verify free space on nodes otherwise skip test case
        node_list = heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)
        self.assertTrue(node_list)

        free_nodes = 0
        for node in node_list:
            node_info = heketi_node_info(
                self.heketi_client_node, self.heketi_server_url, node,
                json=True)

            if node_info['state'] != 'online':
                continue

            free_size = 0
            self.assertTrue(node_info['devices'])

            for device in node_info['devices']:
                if device['state'] != 'online':
                    continue
                # convert size kb into gb
                device_f_size = device['storage']['free'] / 1048576
                free_size += device_f_size

                if free_size > free_space:
                    free_nodes += 1
                    break

            if free_nodes >= 3:
                break

        if free_nodes < 3:
            self.skipTest("skip test case because required free space is "
                          "not available for creating BHV of size %s /n"
                          "only %s free space is available"
                          % (free_space, free_size))

    @pytest.mark.tier3
    def test_creation_of_block_vol_greater_than_the_default_size_of_BHV_neg(
            self):
        """Verify that block volume creation fails when we create block
        volume of size greater than the default size of BHV.
        Verify that block volume creation succeed when we create BHV
        of size greater than the default size of BHV.
        """

        default_bhv_size = get_default_block_hosting_volume_size(
            self.node, self.heketi_dc_name)
        reserve_size = default_bhv_size * 0.02
        reserve_size = int(math.ceil(reserve_size))

        self.verify_free_space(default_bhv_size + reserve_size + 2)

        with self.assertRaises(AssertionError):
            # create a block vol greater than default BHV size
            bvol_info = heketi_blockvolume_create(
                self.heketi_client_node, self.heketi_server_url,
                (default_bhv_size + 1), json=True)
            self.addCleanup(
                heketi_blockvolume_delete, self.heketi_client_node,
                self.heketi_server_url, bvol_info['id'])

        sc_name = self.create_storage_class()

        # create a block pvc greater than default BHV size
        pvc_name = oc_create_pvc(
            self.node, sc_name, pvc_size=(default_bhv_size + 1))
        self.addCleanup(
            wait_for_resource_absence, self.node, 'pvc', pvc_name)
        self.addCleanup(
            oc_delete, self.node, 'pvc', pvc_name, raise_on_absence=False)

        wait_for_events(
            self.node, pvc_name, obj_type='PersistentVolumeClaim',
            event_type='Warning', event_reason='ProvisioningFailed')

        # create block hosting volume greater than default BHV size
        vol_info = heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url,
            (default_bhv_size + reserve_size + 2), block=True,
            json=True)
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol_info['id'])

        # Cleanup PVC before block hosting volume to avoid failures
        self.addCleanup(
            wait_for_resource_absence, self.node, 'pvc', pvc_name)
        self.addCleanup(
            oc_delete, self.node, 'pvc', pvc_name, raise_on_absence=False)

        verify_pvc_status_is_bound(self.node, pvc_name)

    @pytest.mark.tier3
    def test_creation_of_block_vol_greater_than_the_default_size_of_BHV_pos(
            self):
        """Verify that block volume creation succeed when we create BHV
        of size greater than the default size of BHV.
        """

        default_bhv_size = get_default_block_hosting_volume_size(
            self.node, self.heketi_dc_name)
        reserve_size = default_bhv_size * 0.02
        reserve_size = int(math.ceil(reserve_size))

        self.verify_free_space(default_bhv_size + reserve_size + 2)

        # create block hosting volume greater than default BHV size
        vol_info = heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url,
            (default_bhv_size + reserve_size + 2), block=True,
            json=True)
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol_info['id'])

        # create a block pvc greater than default BHV size
        self.create_and_wait_for_pvc(pvc_size=(default_bhv_size + 1))

    @pytest.mark.tier2
    def test_expansion_of_block_hosting_volume_using_heketi(self):
        """Verify that after expanding block hosting volume we are able to
        consume the expanded space"""

        h_node = self.heketi_client_node
        h_url = self.heketi_server_url
        bvols_in_bhv = set([])
        bvols_pv = set([])

        BHVS = get_block_hosting_volume_list(h_node, h_url)

        free_BHVS_count = 0
        for vol in BHVS.keys():
            info = heketi_volume_info(h_node, h_url, vol, json=True)
            if info['blockinfo']['freesize'] > 0:
                free_BHVS_count += 1
            if free_BHVS_count > 1:
                self.skipTest("Skip test case because there is more than one"
                              " Block Hosting Volume with free space")

        # create block volume of 1gb
        bvol_info = heketi_blockvolume_create(h_node, h_url, 1, json=True)

        expand_size = 20
        try:
            self.verify_free_space(expand_size)
            bhv = bvol_info['blockhostingvolume']
            vol_info = heketi_volume_info(h_node, h_url, bhv, json=True)
            bvols_in_bhv.update(vol_info['blockinfo']['blockvolume'])
        finally:
            # cleanup BHV if there is only one block volume inside it
            if len(bvols_in_bhv) == 1:
                self.addCleanup(
                    heketi_volume_delete, h_node, h_url, bhv, json=True)
            self.addCleanup(
                heketi_blockvolume_delete, h_node, h_url, bvol_info['id'])

        size = vol_info['size']
        free_size = vol_info['blockinfo']['freesize']
        bvol_count = int(free_size / expand_size)
        bricks = vol_info['bricks']

        # create pvs to fill the BHV
        pvcs = self.create_and_wait_for_pvcs(
            pvc_size=(expand_size if bvol_count else free_size),
            pvc_amount=(bvol_count or 1), timeout=300)

        vol_expand = True

        for i in range(2):
            # get the vol ids from pvcs
            for pvc in pvcs:
                pv = get_pv_name_from_pvc(self.node, pvc)
                custom = r':.metadata.annotations."gluster\.org\/volume-id"'
                bvol_id = oc_get_custom_resource(self.node, 'pv', custom, pv)
                bvols_pv.add(bvol_id[0])

            vol_info = heketi_volume_info(h_node, h_url, bhv, json=True)
            bvols = vol_info['blockinfo']['blockvolume']
            bvols_in_bhv.update(bvols)
            self.assertEqual(bvols_pv, (bvols_in_bhv & bvols_pv))

            # Expand BHV and verify bricks and size of BHV
            if vol_expand:
                vol_expand = False
                heketi_volume_expand(
                    h_node, h_url, bhv, expand_size, json=True)
                vol_info = heketi_volume_info(h_node, h_url, bhv, json=True)

                self.assertEqual(size + expand_size, vol_info['size'])
                self.assertFalse(len(vol_info['bricks']) % 3)
                self.assertLess(len(bricks), len(vol_info['bricks']))

                # create more PVCs in expanded BHV
                pvcs = self.create_and_wait_for_pvcs(
                    pvc_size=(expand_size - 1), pvc_amount=1)

    @skip("Blocked by BZ-1769426")
    @pytest.mark.tier1
    def test_targetcli_failure_during_block_pvc_creation(self):
        h_node, h_server = self.heketi_client_node, self.heketi_server_url

        # Disable redundant nodes and leave just 3 nodes online
        h_node_id_list = heketi_node_list(h_node, h_server)
        self.assertGreater(len(h_node_id_list), 2)
        for node_id in h_node_id_list[3:]:
            heketi_node_disable(h_node, h_server, node_id)
            self.addCleanup(heketi_node_enable, h_node, h_server, node_id)

        # Gather info about the Gluster node we are going to use for killing
        # targetcli processes.
        chosen_g_node_id = h_node_id_list[0]
        chosen_g_node_info = heketi_node_info(
            h_node, h_server, chosen_g_node_id, json=True)
        chosen_g_node_ip = chosen_g_node_info['hostnames']['storage'][0]
        chosen_g_node_hostname = chosen_g_node_info['hostnames']['manage'][0]
        chosen_g_node_ip_and_hostname = set((
            chosen_g_node_ip, chosen_g_node_hostname))

        g_pods = oc_get_custom_resource(
            self.node, 'pod', [':.metadata.name', ':.status.hostIP',
                               ':.status.podIP', ':.spec.nodeName'],
            selector='glusterfs-node=pod')
        if g_pods and g_pods[0]:
            for g_pod in g_pods:
                if chosen_g_node_ip_and_hostname.intersection(set(g_pod[1:])):
                    host_to_run_cmds = self.node
                    g_pod_prefix, g_pod = 'oc exec %s -- ' % g_pod[0], g_pod[0]
                    break
            else:
                err_msg = (
                    'Failed to find Gluster pod filtering it by following IPs '
                    'and hostnames: %s\nFound following Gluster pods: %s'
                ) % (chosen_g_node_ip_and_hostname, g_pods)
                g.log.error(err_msg)
                raise AssertionError(err_msg)
        else:
            host_to_run_cmds, g_pod_prefix, g_pod = chosen_g_node_ip, '', ''

        # Schedule deletion of targetcli process
        file_for_bkp, pvc_number = "~/.targetcli/prefs.bin", 10
        self.cmd_run(
            "%scp %s %s_backup" % (g_pod_prefix, file_for_bkp, file_for_bkp),
            hostname=host_to_run_cmds)
        self.addCleanup(
            self.cmd_run,
            "%srm -f %s_backup" % (g_pod_prefix, file_for_bkp),
            hostname=host_to_run_cmds)
        kill_targetcli_services_cmd = (
            "while true; do "
            "  %spkill targetcli || echo 'failed to kill targetcli process'; "
            "done" % g_pod_prefix)
        loop_for_killing_targetcli_process = g.run_async(
            host_to_run_cmds, kill_targetcli_services_cmd, "root")
        try:
            # Create bunch of PVCs
            sc_name, pvc_names = self.create_storage_class(), []
            for i in range(pvc_number):
                pvc_names.append(oc_create_pvc(self.node, sc_name, pvc_size=1))
            self.addCleanup(
                wait_for_resources_absence, self.node, 'pvc', pvc_names)
            self.addCleanup(oc_delete, self.node, 'pvc', ' '.join(pvc_names))

            # Check that we get expected number of provisioning errors
            timeout, wait_step, succeeded_pvcs, failed_pvcs = 120, 1, [], []
            _waiter, err_msg = Waiter(timeout=timeout, interval=wait_step), ""
            for pvc_name in pvc_names:
                _waiter._attempt = 0
                for w in _waiter:
                    events = get_events(
                        self.node, pvc_name, obj_type="PersistentVolumeClaim")
                    for event in events:
                        if event['reason'] == 'ProvisioningSucceeded':
                            succeeded_pvcs.append(pvc_name)
                            break
                        elif event['reason'] == 'ProvisioningFailed':
                            failed_pvcs.append(pvc_name)
                            break
                    else:
                        continue
                    break
                if w.expired:
                    err_msg = (
                        "Failed to get neither 'ProvisioningSucceeded' nor "
                        "'ProvisioningFailed' statuses for all the PVCs in "
                        "time. Timeout was %ss, interval was %ss." % (
                            timeout, wait_step))
                    g.log.error(err_msg)
                    raise AssertionError(err_msg)
            self.assertGreater(len(failed_pvcs), len(succeeded_pvcs))
        finally:
            # Restore targetcli workability
            loop_for_killing_targetcli_process._proc.terminate()

            # Revert breakage back which can be caused by BZ-1769426
            check_bkp_file_size_cmd = (
                "%sls -lah %s | awk '{print $5}'" % (
                    g_pod_prefix, file_for_bkp))
            bkp_file_size = self.cmd_run(
                check_bkp_file_size_cmd, hostname=host_to_run_cmds).strip()
            if bkp_file_size == "0":
                self.cmd_run(
                    "%smv %s_backup %s" % (
                        g_pod_prefix, file_for_bkp, file_for_bkp),
                    hostname=host_to_run_cmds)
                breakage_err_msg = (
                    "File located at '%s' was corrupted (zero size) on the "
                    "%s. Looks like BZ-1769426 took effect. \n"
                    "Don't worry, it has been restored after test failure." % (
                        file_for_bkp,
                        "'%s' Gluster pod" % g_pod if g_pod
                        else "'%s' Gluster node" % chosen_g_node_ip))
                g.log.error(breakage_err_msg)
                if err_msg:
                    breakage_err_msg = "%s\n%s" % (err_msg, breakage_err_msg)
                raise AssertionError(breakage_err_msg)

        # Wait for all the PVCs to be in bound state
        wait_for_pvcs_be_bound(self.node, pvc_names, timeout=300, wait_step=5)

    @pytest.mark.tier3
    def test_creation_of_pvc_when_one_node_is_down(self):
        """Test PVC creation when one node is down than hacount"""
        node_count = len(self.gluster_servers)

        # Check at least 4 nodes available.
        if node_count < 4:
            self.skipTest(
                "At least 4 nodes are required, found %s." % node_count)

        # Skip test if not able to connect to Cloud Provider
        try:
            find_vm_name_by_ip_or_hostname(self.node)
        except (NotImplementedError, ConfigError) as e:
            self.skipTest(e)

        # Get gluster node on which heketi pod is not scheduled
        heketi_node = oc_get_custom_resource(
            self.node, 'pod', custom='.:status.hostIP',
            selector='deploymentconfig=%s' % self.heketi_dc_name)[0]
        gluster_node = random.sample(
            (set(self.gluster_servers) - set(heketi_node)), 1)[0]
        gluster_hostname = self.gluster_servers_info[gluster_node]["manage"]

        # Get VM name by VM hostname
        vm_name = find_vm_name_by_ip_or_hostname(gluster_node)

        # Power off one of the nodes
        self.power_off_gluster_node_vm(vm_name, gluster_hostname)

        # Create a PVC with SC of hacount equal to node count
        sc_name = self.create_storage_class(hacount=node_count)
        pvc_name = oc_create_pvc(self.node, sc_name=sc_name)
        self.addCleanup(wait_for_resource_absence, self.node, 'pvc', pvc_name)
        self.addCleanup(oc_delete, self.node, 'pvc', pvc_name)
        events = wait_for_events(
            self.node, obj_name=pvc_name, obj_type='PersistentVolumeClaim',
            event_type='Warning', event_reason='ProvisioningFailed',
            timeout=180)
        error = 'insufficient block hosts online'
        err_msg = (
            "Haven't found expected error message containing "
            "following string: \n%s\nEvents: %s" % (error, events))
        self.assertTrue(
            list(filter((lambda e: error in e['message']), events)), err_msg)

        # Create a PVC with SC of hacount less than node count
        sc_name = self.create_storage_class(hacount=(node_count - 1))
        self.create_and_wait_for_pvc(sc_name=sc_name)

    @pytest.mark.tier3
    def test_heketi_block_volume_create_with_size_more_than_bhv_free_space(
            self):
        """ Test to create heketi block volume of size greater than
            free space in BHV so that it will create a new BHV.
        """
        h_node, h_url = self.heketi_client_node, self.heketi_server_url

        default_bhv_size = get_default_block_hosting_volume_size(
            self.node, self.heketi_dc_name)
        reserve_size = math.ceil(default_bhv_size * 0.02)
        bhv_list, pvc_size = [], (default_bhv_size - reserve_size)

        # Get existing BHV list
        bhv_list = list(get_block_hosting_volume_list(h_node, h_url).keys())
        for vol in bhv_list:
            info = heketi_volume_info(h_node, h_url, vol, json=True)
            if info['blockinfo']['freesize'] >= pvc_size:
                self.skipTest(
                    "Skip test case since there is atleast one BHV with free"
                    " space {} greater than the default value {}".format(
                        info['blockinfo']['freesize'], pvc_size))

        # To verify if there is enough free space for two BHVs
        self.verify_free_space(2 * (default_bhv_size + 1))

        sc_name = self.create_storage_class()

        self._dynamic_provisioning_block_with_bhv_cleanup(
            sc_name, pvc_size, bhv_list)
        self._dynamic_provisioning_block_with_bhv_cleanup(
            sc_name, pvc_size, bhv_list)
        bhv_post = len(get_block_hosting_volume_list(h_node, h_url))
        err_msg = (
            "New BHVs were not created to satisfy the block PV requests"
            " No. of BHV before the test : {} \n"
            " No. of BHV after the test : {}".format(len(bhv_list), bhv_post))
        self.assertEqual(bhv_post, (len(bhv_list) + 2), err_msg)

    @pytest.mark.tier2
    def test_100gb_block_pvc_create_and_delete_twice(self):
        """Validate creation and deletion of blockvoume of size 100GB"""
        # Define required space, bhv size required for on 100GB block PVC
        size, bhv_size, required_space = 100, 103, 309
        h_node, h_url = self.heketi_client_node, self.heketi_server_url
        prefix = 'autotest-pvc-{}'.format(utils.get_random_str(size=5))

        # Skip test if required free space is not available
        free_space = get_total_free_space(
            self.heketi_client_node, self.heketi_server_url)[0]
        if free_space < required_space:
            self.skipTest("Available free space {} is less than the required "
                          "free space {}".format(free_space, required_space))

        # Create block hosting volume of 103GB required for 100GB block PVC
        bhv = heketi_volume_create(
            h_node, h_url, bhv_size, block=True, json=True)['id']
        self.addCleanup(heketi_volume_delete, h_node, h_url, bhv)

        for _ in range(2):
            # Create PVC of size 100GB
            pvc_name = self.create_and_wait_for_pvc(
                pvc_size=size, pvc_name_prefix=prefix)
            match_pvc_and_pv(self.node, prefix)

            # Delete the PVC
            oc_delete(self.node, 'pvc', pvc_name)
            wait_for_resource_absence(self.node, 'pvc', pvc_name)
