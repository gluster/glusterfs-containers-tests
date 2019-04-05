try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json

from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import get_volume_list, get_volume_info
import six

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.heketi_ops import (
    heketi_blockvolume_create,
    heketi_blockvolume_delete,
    heketi_cluster_delete,
    heketi_cluster_list,
    heketi_node_delete,
    heketi_node_info,
    heketi_node_list,
    heketi_topology_info,
    heketi_volume_create,
    heketi_volume_delete,
    heketi_volume_expand,
    heketi_volume_info,
    heketi_volume_list,
)
from openshiftstoragelibs import podcmd


class TestHeketiVolume(BaseClass):
    """
    Class to test heketi volume create
    """
    @classmethod
    def setUpClass(cls):
        super(TestHeketiVolume, cls).setUpClass()
        cls.volume_size = 1

    @podcmd.GlustoPod()
    def test_volume_create_and_list_volume(self):
        """Validate heketi and gluster volume list"""
        g.log.info("List gluster volumes before Heketi volume creation")
        existing_g_vol_list = get_volume_list('auto_get_gluster_endpoint')
        self.assertTrue(existing_g_vol_list, ("Unable to get volumes list"))

        g.log.info("List heketi volumes before volume creation")
        existing_h_vol_list = heketi_volume_list(
            self.heketi_client_node, self.heketi_server_url,
            json=True)["volumes"]
        g.log.info("Heketi volumes successfully listed")

        g.log.info("Create a heketi volume")
        out = heketi_volume_create(self.heketi_client_node,
                                   self.heketi_server_url,
                                   self.volume_size, json=True)
        g.log.info("Heketi volume successfully created" % out)
        volume_id = out["bricks"][0]["volume"]
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, volume_id)

        g.log.info("List heketi volumes after volume creation")
        h_vol_list = heketi_volume_list(
            self.heketi_client_node, self.heketi_server_url,
            json=True)["volumes"]
        g.log.info("Heketi volumes successfully listed")

        g.log.info("List gluster volumes after Heketi volume creation")
        g_vol_list = get_volume_list('auto_get_gluster_endpoint')
        self.assertTrue(g_vol_list, ("Unable to get volumes list"))
        g.log.info("Successfully got the volumes list")

        # Perform checks
        self.assertEqual(
            len(existing_g_vol_list) + 1, len(g_vol_list),
            "Expected creation of only one volume in Gluster creating "
            "Heketi volume. Here is lists before and after volume creation: "
            "%s \n%s" % (existing_g_vol_list, g_vol_list))
        self.assertEqual(
            len(existing_h_vol_list) + 1, len(h_vol_list),
            "Expected creation of only one volume in Heketi. Here is lists "
            "of Heketi volumes before and after volume creation: %s\n%s" % (
                existing_h_vol_list, h_vol_list))

    @podcmd.GlustoPod()
    def test_create_vol_and_retrieve_vol_info(self):
        """Validate heketi and gluster volume info"""

        g.log.info("Create a heketi volume")
        out = heketi_volume_create(self.heketi_client_node,
                                   self.heketi_server_url,
                                   self.volume_size, json=True)
        self.assertTrue(out, ("Failed to create heketi "
                        "volume of size %s" % self.volume_size))
        g.log.info("Heketi volume successfully created" % out)
        volume_id = out["bricks"][0]["volume"]
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, volume_id)

        g.log.info("Retrieving heketi volume info")
        out = heketi_volume_info(
            self.heketi_client_node, self.heketi_server_url, volume_id,
            json=True)
        self.assertTrue(out, ("Failed to get heketi volume info"))
        g.log.info("Successfully got the heketi volume info")
        name = out["name"]

        vol_info = get_volume_info('auto_get_gluster_endpoint', volname=name)
        self.assertTrue(vol_info, "Failed to get volume info %s" % name)
        g.log.info("Successfully got the volume info %s" % name)

    def topology_volumes_with_bricks(self):
        g.log.info("Retrieving heketi topology info")
        topology_info_out = heketi_topology_info(self.heketi_client_node,
                                                 self.heketi_server_url,
                                                 json=True)
        indented_topology = json.dumps(topology_info_out, indent=4)
        g.log.info("Successfully got the heketi topology info")
        topology_volumes = dict()
        for c in topology_info_out['clusters']:
            for v in c['volumes']:
                topology_volumes.update({v['name']: len(v['bricks'])})
        return topology_volumes, indented_topology

    def test_create_vol_and_retrieve_topology_info(self):
        volume_names = []
        volume_ids = []

        # Create 3 volumes and make 3rd volume of type distributed replica
        g.log.info("Creating 3 volumes")
        for i in range(3):
            out = heketi_volume_create(self.heketi_client_node,
                                       self.heketi_server_url,
                                       self.volume_size, json=True)
            g.log.info("Heketi volume %s successfully created" % out)
            volume_names.append(out["name"])
            volume_ids.append(out["bricks"][0]["volume"])
            self.addCleanup(heketi_volume_delete, self.heketi_client_node,
                            self.heketi_server_url, volume_ids[i],
                            raise_on_error=(i == 2))
        heketi_volume_expand(self.heketi_client_node,
                             self.heketi_server_url, volume_ids[1], 1)

        # Check if volume is shown in the heketi topology
        topology_volumes, indented_topology = (
            self.topology_volumes_with_bricks())
        for vol_name in volume_names:
            self.assertIn(vol_name, topology_volumes.keys(), (
                "volume %s  not found in the heketi topology\n Topology "
                "info:\n%s" % (vol_name, indented_topology)))
            bricks = 6 if vol_name == volume_names[1] else 3
            self.assertGreaterEqual(
                topology_volumes[vol_name], bricks, 'Bricks of the volume:'
                '%s are %s and it should be more or equal to %s' %
                (vol_name, topology_volumes[vol_name], bricks))

        # Delete first 2 volumes and verify their deletion in the topology
        for vol_id in volume_ids[:2]:
            g.log.info("Deleting volume %s" % vol_id)
            heketi_volume_delete(self.heketi_client_node,
                                 self.heketi_server_url, vol_id)
        topology_volumes.clear()
        topology_volumes, indented_topology = (
            self.topology_volumes_with_bricks())
        for vol_name in volume_names[:2]:
            self.assertNotIn(vol_name, topology_volumes.keys(), (
                                "volume %s shown in the heketi topology after "
                                "deletion\nTopology info:\n%s" % (
                                    vol_name, indented_topology)))

        # Check the existence of third volume
        self.assertIn(volume_names[2], topology_volumes.keys(), ("volume %s "
                      "not shown in the heketi topology\nTopology info\n%s" % (
                          volume_ids[2], indented_topology)))
        g.log.info("Sucessfully verified the topology info")

    def test_to_check_deletion_of_cluster(self):
        """Validate deletion of cluster with volumes"""
        # List heketi volumes
        g.log.info("List heketi volumes")
        volumes = heketi_volume_list(self.heketi_client_node,
                                     self.heketi_server_url,
                                     json=True)
        if (len(volumes["volumes"]) == 0):
            g.log.info("Creating heketi volume")
            out = heketi_volume_create(self.heketi_client_node,
                                       self.heketi_server_url,
                                       self.volume_size, json=True)
            self.assertTrue(out, ("Failed to create heketi "
                            "volume of size %s" % self.volume_size))
            g.log.info("Heketi volume successfully created" % out)
            volume_id = out["bricks"][0]["volume"]
            self.addCleanup(
                heketi_volume_delete, self.heketi_client_node,
                self.heketi_server_url, volume_id)

        # List heketi cluster's
        g.log.info("Listing heketi cluster list")
        out = heketi_cluster_list(self.heketi_client_node,
                                  self.heketi_server_url,
                                  json=True)
        self.assertTrue(out, ("Failed to list heketi cluster"))
        g.log.info("All heketi cluster successfully listed")
        cluster_id = out["clusters"][0]

        # Deleting a heketi cluster
        g.log.info("Trying to delete a heketi cluster"
                   " which contains volumes and/or nodes:"
                   " Expected to fail")
        self.assertRaises(
            AssertionError,
            heketi_cluster_delete,
            self.heketi_client_node, self.heketi_server_url, cluster_id,
        )
        g.log.info("Expected result: Unable to delete cluster %s"
                   " because it contains volumes "
                   " and/or nodes" % cluster_id)

        # To confirm deletion failed, check heketi cluster list
        g.log.info("Listing heketi cluster list")
        out = heketi_cluster_list(self.heketi_client_node,
                                  self.heketi_server_url,
                                  json=True)
        self.assertTrue(out, ("Failed to list heketi cluster"))
        g.log.info("All heketi cluster successfully listed")

    def test_to_check_deletion_of_node(self):
        """Validate deletion of a node which contains devices"""

        # Create Heketi volume to make sure we have devices with usages
        heketi_url = self.heketi_server_url
        vol = heketi_volume_create(
            self.heketi_client_node, heketi_url, 1, json=True)
        self.assertTrue(vol, "Failed to create heketi volume.")
        g.log.info("Heketi volume successfully created")
        volume_id = vol["bricks"][0]["volume"]
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, volume_id)

        # Pick up suitable node
        node_ids = heketi_node_list(self.heketi_client_node, heketi_url)
        self.assertTrue(node_ids)
        for node_id in node_ids:
            node_info = heketi_node_info(
                self.heketi_client_node, heketi_url, node_id, json=True)
            if (node_info['state'].lower() != 'online' or
                    not node_info['devices']):
                continue
            for device in node_info['devices']:
                if device['state'].lower() != 'online':
                    continue
                if device['storage']['used']:
                    node_id = node_info['id']
                    break
        else:
            self.assertTrue(
                node_id,
                "Failed to find online node with online device which "
                "has some usages.")

        # Try to delete the node by its ID
        g.log.info("Trying to delete the node which contains devices in it. "
                   "Expecting failure.")
        self.assertRaises(
            AssertionError,
            heketi_node_delete,
            self.heketi_client_node, heketi_url, node_id)

        # Make sure our node hasn't been deleted
        g.log.info("Listing heketi node list")
        node_list = heketi_node_list(self.heketi_client_node, heketi_url)
        self.assertTrue(node_list, ("Failed to list heketi nodes"))
        self.assertIn(node_id, node_list)
        node_info = heketi_node_info(
            self.heketi_client_node, heketi_url, node_id, json=True)
        self.assertEqual(node_info['state'].lower(), 'online')

    def test_blockvolume_create_no_free_space(self):
        """Validate error is returned when free capacity is exhausted"""

        # Create first small blockvolume
        blockvol1 = heketi_blockvolume_create(
            self.heketi_client_node, self.heketi_server_url, 1, json=True)
        self.assertTrue(blockvol1, "Failed to create block volume.")
        self.addCleanup(
            heketi_blockvolume_delete, self.heketi_client_node,
            self.heketi_server_url, blockvol1['id'])

        # Get info about block hosting volumes
        file_volumes = heketi_volume_list(
            self.heketi_client_node, self.heketi_server_url, json=True)
        self.assertTrue(file_volumes)
        self.assertIn("volumes", file_volumes)
        self.assertTrue(file_volumes["volumes"])
        max_block_hosting_vol_size, file_volumes_debug_info = 0, []
        for vol_id in file_volumes["volumes"]:
            vol = heketi_volume_info(
                self.heketi_client_node, self.heketi_server_url,
                vol_id, json=True)
            current_block_hosting_vol_size = vol.get('size', 0)
            if current_block_hosting_vol_size > max_block_hosting_vol_size:
                max_block_hosting_vol_size = current_block_hosting_vol_size
            if current_block_hosting_vol_size:
                file_volumes_debug_info.append(six.text_type({
                    'id': vol.get('id', '?'),
                    'name': vol.get('name', '?'),
                    'size': current_block_hosting_vol_size,
                    'blockinfo': vol.get('blockinfo', '?'),
                }))
        self.assertGreater(max_block_hosting_vol_size, 0)

        # Try to create blockvolume with size bigger than available
        too_big_vol_size = max_block_hosting_vol_size + 1
        try:
            blockvol2 = heketi_blockvolume_create(
                self.heketi_client_node, self.heketi_server_url,
                too_big_vol_size, json=True)
        except AssertionError:
            return

        if blockvol2 and blockvol2.get('id'):
            self.addCleanup(
                heketi_blockvolume_delete, self.heketi_client_node,
                self.heketi_server_url, blockvol2['id'])
        block_hosting_vol = heketi_volume_info(
            self.heketi_client_node, self.heketi_server_url,
            blockvol2.get('blockhostingvolume'), json=True)
        self.assertGreater(
            block_hosting_vol.get('size', -2), blockvol2.get('size', -1),
            ("Block volume unexpectedly was created. "
             "Calculated 'max free size' is '%s'.\nBlock volume info is: %s \n"
             "File volume info, which hosts block volume: \n%s,"
             "Block hosting volumes which were considered: \n%s" % (
                 max_block_hosting_vol_size, blockvol2, block_hosting_vol,
                 '\n'.join(file_volumes_debug_info))))
