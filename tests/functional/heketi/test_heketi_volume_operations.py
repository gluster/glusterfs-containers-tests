from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.heketi_ops import (
    heketi_volume_create,
    heketi_volume_delete,
    heketi_volume_expand,
    heketi_volume_info,
)


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
