import pytest

from glustolibs.gluster import volume_ops

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import heketi_version
from openshiftstoragelibs import podcmd


class TestHeketiBrickEvict(BaseClass):
    """Test Heketi brick evict functionality."""

    def setUp(self):
        super(TestHeketiBrickEvict, self).setUp()

        version = heketi_version.get_heketi_version(self.heketi_client_node)
        if version < '9.0.0-14':
            self.skipTest(
                "heketi-client package {} does not support brick evict".format(
                    version.v_str))

        node_list = heketi_ops.heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)

        if len(node_list) > 3:
            return

        for node_id in node_list:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url, node_id,
                json=True)
            if len(node_info["devices"]) < 2:
                self.skipTest("does not have extra device/node to evict brick")

    @podcmd.GlustoPod()
    def _get_gluster_vol_info(self, file_vol):
        """Get Gluster vol info.

        Args:
            ocp_client (str): Node to execute OCP commands.
            file_vol (str): file volume name.

        Returns:
            dict: Info of the given gluster vol.
        """
        g_vol_info = volume_ops.get_volume_info(
            "auto_get_gluster_endpoint", file_vol)

        if not g_vol_info:
            raise AssertionError("Failed to get volume info for gluster "
                                 "volume {}".format(file_vol))
        if file_vol in g_vol_info:
            g_vol_info = g_vol_info.get(file_vol)
        return g_vol_info

    @pytest.mark.tier1
    def test_heketi_brick_evict(self):
        """Test brick evict basic functionality and verify it replace a brick
        properly
        """
        h_node, h_server = self.heketi_client_node, self.heketi_server_url

        size = 1
        vol_info_old = heketi_ops.heketi_volume_create(
            h_node, h_server, size, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete, h_node, h_server,
            vol_info_old['id'])
        heketi_ops.heketi_brick_evict(
            h_node, h_server, vol_info_old["bricks"][0]['id'])

        vol_info_new = heketi_ops.heketi_volume_info(
            h_node, h_server, vol_info_old['id'], json=True)

        bricks_old = set({brick['path'] for brick in vol_info_old["bricks"]})
        bricks_new = set({brick['path'] for brick in vol_info_new["bricks"]})
        self.assertEqual(
            len(bricks_new - bricks_old), 1,
            "Brick was not replaced with brick evict for vol \n {}".format(
                vol_info_new))

        gvol_info = self._get_gluster_vol_info(vol_info_new['name'])
        gbricks = set(
            {brick['name'].split(":")[1]
                for brick in gvol_info["bricks"]["brick"]})
        self.assertEqual(
            bricks_new, gbricks, "gluster vol info and heketi vol info "
            "mismatched after brick evict {} \n {}".format(
                gvol_info, vol_info_new))
