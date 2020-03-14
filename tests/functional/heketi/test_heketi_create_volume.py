try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json

from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import get_volume_list, get_volume_info
import mock
import pytest
import six

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs import command
from openshiftstoragelibs.heketi_ops import (
    get_heketi_volume_and_brick_count_list,
    heketi_blockvolume_create,
    heketi_blockvolume_delete,
    heketi_cluster_delete,
    heketi_cluster_list,
    heketi_db_check,
    heketi_node_delete,
    heketi_node_info,
    heketi_node_list,
    heketi_volume_create,
    heketi_volume_delete,
    heketi_volume_expand,
    heketi_volume_info,
    heketi_volume_list,
)
from openshiftstoragelibs.openshift_ops import cmd_run_on_gluster_pod_or_node
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import waiter


class TestHeketiVolume(BaseClass):
    """
    Class to test heketi volume create
    """
    @classmethod
    def setUpClass(cls):
        super(TestHeketiVolume, cls).setUpClass()
        cls.volume_size = 1

    @pytest.mark.tier0
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

    @pytest.mark.tier1
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

    @pytest.mark.tier0
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
        topology_volumes = get_heketi_volume_and_brick_count_list(
            self.heketi_client_node, self.heketi_server_url)
        existing_volumes = [v for v, _ in topology_volumes]
        for v in volume_names:
            self.assertIn(v, existing_volumes)
        for v, b_count in topology_volumes:
            expected_bricks_count = 6 if v == volume_names[1] else 3
            self.assertGreaterEqual(
                b_count, expected_bricks_count,
                'Bricks number of the %s volume is %s and it is expected '
                'to be greater or equal to %s' % (
                    v, b_count, expected_bricks_count))

        # Delete first 2 volumes and verify their deletion in the topology
        for vol_id in volume_ids[:2]:
            g.log.info("Deleting volume %s" % vol_id)
            heketi_volume_delete(self.heketi_client_node,
                                 self.heketi_server_url, vol_id)
        topology_volumes = get_heketi_volume_and_brick_count_list(
            self.heketi_client_node, self.heketi_server_url)
        existing_volumes = [v for v, _ in topology_volumes]
        for vol_name in volume_names[:2]:
            self.assertNotIn(vol_name, existing_volumes, (
                "volume %s shown in the heketi topology after deletion"
                "\nTopology info:\n%s" % (
                    vol_name, existing_volumes)))

        # Check the existence of third volume
        self.assertIn(
            volume_names[2], existing_volumes, "volume %s not "
            "shown in the heketi topology\nTopology info"
            "\n%s" % (volume_ids[2], existing_volumes))
        g.log.info("Sucessfully verified the topology info")

    @pytest.mark.tier1
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

    @pytest.mark.tier0
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
            if (node_info['state'].lower() != 'online'
                    or not node_info['devices']):
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

    @pytest.mark.tier1
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

    @pytest.mark.tier1
    @podcmd.GlustoPod()
    def test_heketi_volume_create_with_cluster_node_down(self):
        if len(self.gluster_servers) < 5:
            self.skipTest("Nodes in the cluster are %s which is less than 5"
                          % len(self.gluster_servers))

        cmd_glusterd_stop = "systemctl stop glusterd"
        cmd_glusterd_start = "systemctl start glusterd"

        try:
            # Kill glusterd on 2 of the nodes
            for gluster_server in self.gluster_servers[:2]:
                cmd_run_on_gluster_pod_or_node(
                    self.ocp_master_node[0], cmd_glusterd_stop,
                    gluster_node=gluster_server)

            # Create heketi volume, get the volume id and volume name
            volume_info = heketi_volume_create(
                self.heketi_client_node, self.heketi_server_url,
                self.volume_size, json=True)
            volume_id = volume_info["id"]
            volume_name = volume_info['name']
            self.addCleanup(
                heketi_volume_delete, self.heketi_client_node,
                self.heketi_server_url, volume_id)
        finally:
            for gluster_server in self.gluster_servers[:2]:
                self.addCleanup(
                    cmd_run_on_gluster_pod_or_node, self.ocp_master_node[0],
                    cmd_glusterd_start, gluster_node=gluster_server)

        # Verify volume creation at the gluster side
        g_vol_list = get_volume_list('auto_get_gluster_endpoint')
        self.assertTrue(g_vol_list, "Failed to get gluster volume list")

        msg = "volume: %s not found in the volume list: %s" % (
            volume_name, g_vol_list)
        self.assertIn(volume_name, g_vol_list, msg)

    @pytest.mark.tier1
    def test_verify_pending_entries_in_db(self):
        """Verify pending entries of volumes and bricks in db during
        volume creation from heketi side
        """
        h_volume_size = 100
        h_db_chk_bfr_v_creation = heketi_db_check(
            self.heketi_client_node, self.heketi_server_url)

        if (h_db_chk_bfr_v_creation["bricks"]["pending"] != 0
                or h_db_chk_bfr_v_creation["volumes"]["pending"] != 0):
            self.skipTest(
                "Skip TC due to unexpected bricks/volumes pending operations")

        # Verify bricks and volume pending operation before creation
        self.assertEqual(h_db_chk_bfr_v_creation["bricks"]["pending"], 0)
        self.assertEqual(h_db_chk_bfr_v_creation["volumes"]["pending"], 0)

        # Temporary replace g.run with g.async_run in heketi_volume_create func
        # to be able to run it in background.Also, avoid parsing the output as
        # it won't be json at that moment. Parse it after reading the async
        # operation results.

        def run_async(cmd, hostname, raise_on_error=True):
            return g.run_async(host=hostname, command=cmd)

        with mock.patch.object(
                json, 'loads', side_effect=(lambda j: j)):
            with mock.patch.object(command, 'cmd_run', side_effect=run_async):
                h_vol_creation_async_op = heketi_volume_create(
                    self.heketi_client_node,
                    self.heketi_server_url, h_volume_size, json=True)

        for w in waiter.Waiter(timeout=5, interval=1):
            h_db_chk_during_v_creation = heketi_db_check(
                self.heketi_client_node, self.heketi_server_url)
            if h_db_chk_during_v_creation["bricks"]["pending"] != 0:
                break
        if w.expired:
            err_msg = "No pending operation in Heketi db"
            g.log.error(err_msg)
            raise exceptions.ExecutionError(err_msg)

        retcode, stdout, stderr = h_vol_creation_async_op.async_communicate()
        heketi_vol = json.loads(stdout)
        volume_id = heketi_vol["id"]
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, volume_id, raise_on_error=True)

        # Verify volume pending operation during creation
        self.assertFalse(h_db_chk_during_v_creation["bricks"]["pending"] % 3)
        self.assertEqual(
            h_db_chk_bfr_v_creation["volumes"]["pending"] + 1,
            h_db_chk_during_v_creation["volumes"]["pending"])

        h_db_chk_after_v_creation = heketi_db_check(
            self.heketi_client_node, self.heketi_server_url)

        # Verify bricks and volume pending operation after creation
        self.assertEqual(h_db_chk_after_v_creation["bricks"]["pending"], 0)
        self.assertEqual(h_db_chk_after_v_creation["volumes"]["pending"], 0)
