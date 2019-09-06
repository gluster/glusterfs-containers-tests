from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.command import cmd_run
from openshiftstoragelibs.heketi_ops import (
    heketi_node_info,
    heketi_volume_create,
    heketi_volume_delete,
    heketi_volume_expand,
    heketi_volume_info,
)
from openshiftstoragelibs.openshift_ops import cmd_run_on_gluster_pod_or_node
from openshiftstoragelibs import utils


class TestHeketiVolumeOperations(BaseClass):
    """
    Class to test heketi volume operations - create, expand
    """

    @classmethod
    def setUpClass(cls):
        super(TestHeketiVolumeOperations, cls).setUpClass()
        cls.volume_size = 1

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
