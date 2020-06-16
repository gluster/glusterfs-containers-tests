from glustolibs.gluster.snap_ops import (
    snap_create,
    snap_delete,
    snap_list,
)
import pytest

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.command import cmd_run
from openshiftstoragelibs.heketi_ops import (
    get_total_free_space,
    heketi_node_info,
    heketi_volume_create,
    heketi_volume_delete,
    heketi_volume_expand,
    heketi_volume_info,
)
from openshiftstoragelibs.gluster_ops import get_gluster_vol_status
from openshiftstoragelibs.openshift_ops import cmd_run_on_gluster_pod_or_node
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import utils


class TestHeketiVolumeOperations(BaseClass):
    """
    Class to test heketi volume operations - create, expand
    """

    @classmethod
    def setUpClass(cls):
        super(TestHeketiVolumeOperations, cls).setUpClass()
        cls.volume_size = 1

    @pytest.mark.tier1
    def test_heketi_with_default_options(self):
        """
        Test to create volume with default options.
        """

        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url,
                                        self.volume_size, json=True)
        self.assertTrue(vol_info, ("Failed to create heketi volume of size %s"
                                   % self.volume_size))
        self.addCleanup(
            heketi_volume_delete,
            self.heketi_client_node, self.heketi_server_url, vol_info['id'])

        self.assertEqual(vol_info['size'], self.volume_size,
                         ("Failed to create volume with default options."
                          "Expected Size: %s, Actual Size: %s"
                          % (self.volume_size, vol_info['size'])))

    @pytest.mark.tier1
    def test_heketi_with_expand_volume(self):
        """
        Test volume expand and size if updated correctly in heketi-cli info
        """

        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url,
                                        self.volume_size, json=True)
        self.assertTrue(vol_info, ("Failed to create heketi volume of size %s"
                                   % self.volume_size))
        self.addCleanup(
            heketi_volume_delete,
            self.heketi_client_node, self.heketi_server_url, vol_info['id'])
        self.assertEqual(vol_info['size'], self.volume_size,
                         ("Failed to create volume."
                          "Expected Size: %s, Actual Size: %s"
                          % (self.volume_size, vol_info['size'])))
        volume_id = vol_info["id"]
        expand_size = 2
        ret = heketi_volume_expand(self.heketi_client_node,
                                   self.heketi_server_url, volume_id,
                                   expand_size)
        self.assertTrue(ret, ("Failed to expand heketi volume of id %s"
                              % volume_id))
        volume_info = heketi_volume_info(self.heketi_client_node,
                                         self.heketi_server_url,
                                         volume_id, json=True)
        expected_size = self.volume_size + expand_size
        self.assertEqual(volume_info['size'], expected_size,
                         ("Volume Expansion failed Expected Size: %s, Actual "
                          "Size: %s" % (str(expected_size),
                                        str(volume_info['size']))))

    @pytest.mark.tier1
    def test_heketi_volume_mount(self):
        self.node = self.ocp_master_node[0]
        try:
            cmd_run('rpm -q glusterfs-fuse', self.node)
        except AssertionError:
            self.skipTest(
                "gluster-fuse package is not present on Node %s" % self.node)

        h_node, h_url = self.heketi_client_node, self.heketi_server_url

        # Create volume
        vol_info = heketi_volume_create(h_node, h_url, 2, json=True)
        self.addCleanup(heketi_volume_delete, h_node, h_url, vol_info['id'])

        mount_point = vol_info['mount']['glusterfs']['device']
        mount_dir = '/mnt/dir-%s' % utils.get_random_str()
        mount_cmd = 'mount -t glusterfs %s %s' % (mount_point, mount_dir)

        # Create directory to mount volume
        cmd_run('mkdir %s' % mount_dir, self.node)
        self.addCleanup(cmd_run, 'rm -rf %s' % mount_dir, self.node)

        # Mount volume
        cmd_run(mount_cmd, self.node)
        self.addCleanup(cmd_run, 'umount %s' % mount_dir, self.node)

        # Run I/O to make sure Mount point works
        _file = 'file'
        run_io_cmd = (
            'dd if=/dev/urandom of=%s/%s bs=4k count=1000' % (
                mount_dir, _file))

        # Verify size of volume
        cmd_run(run_io_cmd, self.node)
        size = cmd_run(
            'df -kh --output=size %s | tail -1' % mount_dir, self.node).strip()
        self.assertEqual('2.0G', size)

        # Verify file on gluster vol bricks
        for brick in vol_info['bricks']:
            node_id = brick['node']
            node_info = heketi_node_info(h_node, h_url, node_id, json=True)
            brick_host = node_info['hostnames']['storage'][0]
            cmd_run_on_gluster_pod_or_node(self.node, 'ls %s/%s' % (
                brick['path'], _file), brick_host)

    @pytest.mark.tier1
    @podcmd.GlustoPod()
    def test_heketi_volume_snapshot_create(self):
        """Test heketi volume snapshot create operation"""
        h_volume_size = 1
        snap_name = 'snap_test_heketi_volume_snapshot_create_1'
        h_node, h_url = self.heketi_client_node, self.heketi_server_url

        h_volume_info = heketi_volume_create(
            h_node, h_url, h_volume_size, json=True)
        self.addCleanup(
            heketi_volume_delete, h_node, h_url, h_volume_info["id"])

        h_volume_name = h_volume_info["name"]
        ret, _, _ = snap_create(
            'auto_get_gluster_endpoint',
            h_volume_name, snap_name, timestamp=False)
        self.addCleanup(
            podcmd.GlustoPod()(snap_delete),
            "auto_get_gluster_endpoint", snap_name)
        self.assertEqual(
            ret, 0, "Failed to create snapshot {} for heketi volume {}"
            .format(snap_name, h_volume_name))
        ret, out, _ = snap_list('auto_get_gluster_endpoint')
        self.assertEqual(
            ret, 0, "Failed to list snapshot {} for heketi volume"
            .format(snap_name))
        self.assertIn(
            snap_name, out, "Heketi volume snapshot {} not found in {}"
            .format(snap_name, out))

    def _get_bricks_pids(self, vol_name):
        """Return list having bricks pids with gluster pod ip"""
        pids = []

        g_volume_status = get_gluster_vol_status(vol_name)
        self.assertTrue(
            g_volume_status, "Failed to get the gluster volume status for the "
            "volume {}".format(vol_name))
        for g_node, g_node_data in g_volume_status.items():
            for process_name, process_data in g_node_data.items():
                if process_name.startswith("/var"):
                    pid = process_data["pid"]
                    pids.append([g_node, pid])
        return pids

    @pytest.mark.tier1
    @podcmd.GlustoPod()
    def test_heketi_volume_snapshot_create_with_one_brick_down(self):
        """
        Test heketi volume snapshot create with one brick down
        """
        h_vol_size = 1
        self.node = self.ocp_master_node[0]
        snap_name = 'snap_creation_test_with_one_brick_down'
        h_node, h_url = self.heketi_client_node, self.heketi_server_url

        h_vol_info = heketi_volume_create(h_node, h_url, h_vol_size, json=True)
        self.addCleanup(heketi_volume_delete, h_node, h_url, h_vol_info["id"])
        h_volume_name = h_vol_info["name"]
        pids_before = self._get_bricks_pids(h_volume_name)
        self.assertTrue(
            pids_before,
            "Failed to get the brick process for volume {}".format(
                h_volume_name))

        # kill only one brick process
        cmd = "kill -9 {}".format(pids_before[0][1])
        cmd_run_on_gluster_pod_or_node(self.node, cmd, pids_before[0][0])
        pids_after = self._get_bricks_pids(h_volume_name)
        self.assertTrue(
            pids_after,
            "Failed to get the brick process for volume {}".format(
                h_volume_name))
        self.assertTrue(
            pids_after[0][1],
            "Failed to kill brick process {} on brick {}".format(
                pids_before[0][1], pids_after[0][0]))

        # Get the snapshot list
        ret, out, err = snap_list('auto_get_gluster_endpoint')
        self.assertFalse(
            ret,
            "Failed to list snapshot from gluster side due to error"
            " {}".format(err))
        snap_list_before = out.split("\n")
        ret, out, err = snap_create(
            'auto_get_gluster_endpoint', h_volume_name,
            snap_name, timestamp=False)
        exp_err_msg = "Snapshot command failed\n"
        self.assertTrue(
            ret, "Failed to run snapshot create cmd from gluster side "
            "with error {}".format(err))
        self.assertEqual(
            out, exp_err_msg,
            "Expecting error msg {} and {} to match".format(
                out, exp_err_msg))

        # Check for count after snapshot creation
        ret, out, err = snap_list('auto_get_gluster_endpoint')
        self.assertFalse(
            ret,
            "Failed to list snapshot from gluster with error {}".format(err))
        snap_list_after = out.split("\n")
        self.assertEqual(
            snap_list_before, snap_list_after,
            "Expecting Snapshot count before {} and after creation {} to be "
            "same".format(snap_list_before, snap_list_after))

    @pytest.mark.tier1
    def test_heketi_volume_create_mutiple_sizes(self):
        """Validate creation of heketi volume with differnt sizes"""
        sizes, required_space = [15, 50, 100], 495
        h_node, h_url = self.heketi_client_node, self.heketi_server_url

        # Skip test if space is not available
        available_space = get_total_free_space(h_node, h_url)[0]
        if required_space > available_space:
            self.skipTest("Required space {} greater than the available space "
                          "{}".format(required_space, available_space))

        # Create volume 3 times, each time different size
        for size in sizes:
            vol_id = heketi_volume_create(h_node, h_url, size, json=True)['id']
            self.addCleanup(heketi_volume_delete, h_node, h_url, vol_id)
