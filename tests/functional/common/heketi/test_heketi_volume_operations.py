#!/usr/bin/python

from glusto.core import Glusto as g
from cnslibs.common.heketi_ops import (heketi_create_topology,
                                       heketi_topology_load,
                                       heketi_volume_delete,
                                       heketi_volume_create,
                                       heketi_volume_expand,
                                       heketi_volume_info)
from cnslibs.common.heketi_libs import HeketiClientSetupBaseClass
from cnslibs.common.exceptions import ExecutionError, ConfigError


class TestHeketiVolumeOperations(HeketiClientSetupBaseClass):
    """
    Class to test heketi volume operations - create, expand
    """

    @classmethod
    def setUpClass(cls):
        super(TestHeketiVolumeOperations, cls).setUpClass()

        if cls.deployment_type == "crs_heketi_outside_openshift":
            ret = heketi_create_topology(cls.heketi_client_node,
                                         cls.topology_info)
            if not ret:
                raise ConfigError("Failed to create heketi topology file on %s"
                                  % cls.heketi_client_node)

            ret = heketi_topology_load(cls.heketi_client_node,
                                       cls.heketi_server_url)
            if not ret:
                raise ConfigError("Failed to load heketi topology on %s"
                                  % cls.heketi_client_node)

        cls.volume_id = None

    def volume_cleanup(self):
        """
        Method to cleanup volume in self.addCleanup()
        """
        if self.volume_id is not None:
            out = heketi_volume_delete(self.heketi_client_node,
                                       self.heketi_server_url, self.volume_id)
            output_str = 'Volume %s deleted' % self.volume_id
            if output_str not in out:
                raise ExecutionError("Failed to delete heketi volume of id %s"
                                     % self.volume_id)

    def test_heketi_with_default_options(self):
        """
        Test to create volume with default options.
        """

        volume_size = self.heketi_volume['size']
        kwargs = {"json": True}
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url, volume_size,
                                        **kwargs)
        self.assertTrue(vol_info, ("Failed to create heketi volume of size %s"
                                   % str(volume_size)))
        self.volume_id = vol_info['id']
        self.addCleanup(self.volume_cleanup)

        self.assertEqual(vol_info['size'], int(volume_size),
                         ("Failed to create volume with default options."
                          "Expected Size: %s, Actual Size: %s"
                          % (str(volume_size), str(vol_info['size']))))

    def test_heketi_with_expand_volume(self):
        """
        Test volume expand and size if updated correctly in heketi-cli info
        """

        kwargs = {"json": True}
        volume_size = self.heketi_volume['size']
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url, volume_size,
                                        **kwargs)
        self.assertTrue(vol_info, ("Failed to create heketi volume of size %s"
                                   % str(volume_size)))
        self.volume_id = vol_info['id']
        self.addCleanup(self.volume_cleanup)
        self.assertEqual(vol_info['size'], int(volume_size),
                         ("Failed to create volume."
                          "Expected Size: %s, Actual Size: %s"
                          % (str(volume_size), str(vol_info['size']))))

        expand_size = self.heketi_volume['expand_size']
        ret = heketi_volume_expand(self.heketi_client_node,
                                   self.heketi_server_url, self.volume_id,
                                   expand_size)
        self.assertTrue(ret, ("Failed to expand heketi volume of id %s"
                              % self.volume_id))
        volume_info = heketi_volume_info(self.heketi_client_node,
                                         self.heketi_server_url,
                                         self.volume_id, **kwargs)
        expected_size = int(volume_size) + int(expand_size)
        self.assertEqual(volume_info['size'], expected_size,
                         ("Volume Expansion failed Expected Size: %s, Actual "
                          "Size: %s" % (str(expected_size),
                                        str(volume_info['size']))))
