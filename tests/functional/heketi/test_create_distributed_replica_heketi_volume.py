from __future__ import division
import math

import ddt
from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import get_volume_list, get_volume_info

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.heketi_ops import (
    heketi_device_disable,
    heketi_device_enable,
    heketi_node_disable,
    heketi_node_enable,
    heketi_node_info,
    heketi_node_list,
    heketi_volume_delete,
    heketi_volume_list,
)
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import utils


@ddt.ddt
class TestHeketiVolume(BaseClass):

    def setUp(self):
        super(TestHeketiVolume, self).setUp()
        self.master_node = list(g.config['ocp_servers']['master'].keys())[0]
        self.gluster_node = list(g.config["gluster_servers"].keys())[0]

    def _get_free_space(self):
        """Get free space in each heketi device"""
        free_spaces = []
        heketi_node_id_list = heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)
        for node_id in heketi_node_id_list:
            node_info_dict = heketi_node_info(self.heketi_client_node,
                                              self.heketi_server_url,
                                              node_id, json=True)
            total_free_space = 0
            for device in node_info_dict["devices"]:
                total_free_space += device["storage"]["free"]
            free_spaces.append(total_free_space)
        total_free_space = int(math.floor(sum(free_spaces) / (1024**2)))
        return total_free_space

    def _get_vol_size(self):
        # Get available free space disabling redundant nodes
        min_free_space_gb = 5
        heketi_url = self.heketi_server_url
        node_ids = heketi_node_list(self.heketi_client_node, heketi_url)
        self.assertTrue(node_ids)
        nodes = {}
        min_free_space = min_free_space_gb * 1024**2
        for node_id in node_ids:
            node_info = heketi_node_info(
                self.heketi_client_node, heketi_url, node_id, json=True)
            if (node_info['state'].lower() != 'online'
                    or not node_info['devices']):
                continue
            if len(nodes) > 2:
                out = heketi_node_disable(
                    self.heketi_client_node, heketi_url, node_id)
                self.assertTrue(out)
                self.addCleanup(
                    heketi_node_enable,
                    self.heketi_client_node, heketi_url, node_id)
            for device in node_info['devices']:
                if device['state'].lower() != 'online':
                    continue
                free_space = device['storage']['free']
                if free_space < min_free_space:
                    out = heketi_device_disable(
                        self.heketi_client_node, heketi_url, device['id'])
                    self.assertTrue(out)
                    self.addCleanup(
                        heketi_device_enable,
                        self.heketi_client_node, heketi_url, device['id'])
                    continue
                if node_id not in nodes:
                    nodes[node_id] = []
                nodes[node_id].append(device['storage']['free'])

        # Skip test if nodes requirements are not met
        if (len(nodes) < 3
                or not all(map((lambda _l: len(_l) > 1), nodes.values()))):
            raise self.skipTest(
                "Could not find 3 online nodes with, "
                "at least, 2 online devices having free space "
                "bigger than %dGb." % min_free_space_gb)

        # Calculate size of a potential distributed vol
        vol_size_gb = int(min(map(max, nodes.values())) / (1024 ** 2)) + 1
        return vol_size_gb

    @podcmd.GlustoPod()
    def _create_distributed_replica_vol(
            self, validate_cleanup, block=False):

        # Create distributed vol
        vol_size_gb = self._get_vol_size()
        heketi_url = self.heketi_server_url
        h_volume_name = "autotests-heketi-volume-%s" % utils.get_random_str()
        try:
            heketi_vol = self.create_heketi_volume_with_name_and_wait(
                h_volume_name, vol_size_gb, json=True)
        except AssertionError as e:
            # NOTE: rare situation when we need to decrease size of a volume.
            #       and we expect this vol to be distributed.
            g.log.info("Failed to create distributed '%s'Gb volume. "
                       "Trying to create another one, smaller for 1Gb.")
            if not ('more required' in str(e)
                    and ('Insufficient suitable allocatable extents for '
                         'logical volume' in str(e))):
                raise

            vol_size_gb -= 1
            heketi_vol = self.create_heketi_volume_with_name_and_wait(
                h_volume_name, vol_size_gb, json=True)
        g.log.info("Successfully created distributed volume.")

        vol_name = heketi_vol['name']
        vol_id = heketi_vol["bricks"][0]["volume"]

        # Get gluster volume info
        g.log.info("Get gluster volume '%s' info" % vol_name)
        gluster_vol = get_volume_info(
            'auto_get_gluster_endpoint', volname=vol_name)
        self.assertTrue(
            gluster_vol, "Failed to get volume '%s' info" % vol_name)
        g.log.info("Successfully got volume '%s' info" % vol_name)
        gluster_vol = gluster_vol[vol_name]
        self.assertEqual(
            gluster_vol["typeStr"], "Distributed-Replicate",
            "'%s' gluster vol isn't a Distributed-Replicate volume" % vol_name)

        # Check amount of bricks
        brick_amount = len(gluster_vol['bricks']['brick'])
        self.assertEqual(brick_amount % 3, 0,
                         "Brick amount is expected to be divisible by 3. "
                         "Actual amount is '%s'" % brick_amount)
        self.assertGreater(brick_amount, 3,
                           "Brick amount is expected to be bigger than 3. "
                           "Actual amount is '%s'." % brick_amount)

        # Run unique actions to Validate whether deleting a dist-rep
        # volume is handled by heketi else return
        if not validate_cleanup:
            return

        # Get the free space after creating heketi volume
        free_space_after_creating_vol = self._get_free_space()

        # Delete heketi volume
        g.log.info("Deleting heketi volume '%s'" % vol_id)
        volume_deleted = heketi_volume_delete(
            self.heketi_client_node, heketi_url, vol_id)
        self.assertTrue(
            volume_deleted, "Failed to delete heketi volume '%s'" % vol_id)
        g.log.info("Heketi volume '%s' has successfully been deleted" % vol_id)

        # Check the heketi volume list
        g.log.info("List heketi volumes")
        heketi_volumes = heketi_volume_list(
            self.heketi_client_node, self.heketi_server_url, json=True)
        self.assertTrue(heketi_volumes, "Failed to list heketi volumes")
        g.log.info("Heketi volumes have successfully been listed")
        heketi_volumes = heketi_volumes.get('volumes', heketi_volumes)
        self.assertNotIn(vol_id, heketi_volumes)
        self.assertNotIn(vol_name, heketi_volumes)

        # Check the gluster volume list
        g.log.info("Get the gluster volume list")
        gluster_volumes = get_volume_list('auto_get_gluster_endpoint')
        self.assertTrue(gluster_volumes, "Unable to get Gluster volume list")

        g.log.info("Successfully got Gluster volume list" % gluster_volumes)
        self.assertNotIn(vol_id, gluster_volumes)
        self.assertNotIn(vol_name, gluster_volumes)

        # Get the used space after deleting heketi volume
        free_space_after_deleting_vol = self._get_free_space()

        # Compare the free space before and after deleting the volume
        g.log.info("Comparing the free space before and after deleting volume")
        self.assertLessEqual(
            free_space_after_creating_vol + (3 * vol_size_gb),
            free_space_after_deleting_vol)
        g.log.info("Volume successfully deleted and space is reallocated. "
                   "Free space after creating volume %s. "
                   "Free space after deleting volume %s." % (
                       free_space_after_creating_vol,
                       free_space_after_deleting_vol))

    def test_to_create_distribute_replicated_vol(self):
        """Validate 2x3 vol type creation when the volume cannot be
           carved out of a single device
        """
        self._create_distributed_replica_vol(validate_cleanup=False)

    def test_to_create_and_delete_dist_rep_vol(self):
        """Validate whether deleting a dist-rep volume is handled by heketi"""
        self._create_distributed_replica_vol(validate_cleanup=True)

    @ddt.data(True, False)
    def test_create_and_delete_dist_replicated_bhv(self, validate_cleanup):
        """Validate distributed replicated bhv using heketi-cli"""
        self._create_distributed_replica_vol(
            validate_cleanup, block=True)
