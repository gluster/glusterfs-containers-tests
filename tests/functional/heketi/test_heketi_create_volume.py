try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json

import ddt
from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import (
    get_volume_info,
    get_volume_list,
    volume_start,
    volume_stop,
)
import mock
import pytest
import six

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs import command
from openshiftstoragelibs.heketi_ops import (
    get_heketi_volume_and_brick_count_list,
    get_total_free_space,
    heketi_blockvolume_create,
    heketi_blockvolume_delete,
    heketi_blockvolume_info,
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
    hello_heketi,
)
from openshiftstoragelibs.openshift_ops import (
    cmd_run_on_gluster_pod_or_node,
    get_default_block_hosting_volume_size,
    get_pod_name_from_dc,
    oc_delete,
    wait_for_pod_be_ready,
    wait_for_resource_absence,
    wait_for_service_status_on_gluster_pod_or_node,
)
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import utils
from openshiftstoragelibs import waiter


@ddt.ddt
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

    @pytest.mark.tier2
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
                    wait_for_service_status_on_gluster_pod_or_node,
                    self.ocp_master_node[0], 'glusterd', 'active', 'running',
                    gluster_server)
                self.addCleanup(
                    cmd_run_on_gluster_pod_or_node, self.ocp_master_node[0],
                    cmd_glusterd_start, gluster_node=gluster_server)

        # Verify volume creation at the gluster side
        g_vol_list = get_volume_list(self.gluster_servers[2])
        self.assertTrue(g_vol_list, "Failed to get gluster volume list")

        msg = "volume: %s not found in the volume list: %s" % (
            volume_name, g_vol_list)
        self.assertIn(volume_name, g_vol_list, msg)

    def _respin_heketi_pod(self):
        h_node, h_url = self.heketi_client_node, self.heketi_server_url
        ocp_node = self.ocp_master_node[0]

        # get heketi-pod name
        heketi_pod_name = get_pod_name_from_dc(ocp_node, self.heketi_dc_name)
        # delete heketi-pod (it restarts the pod)
        oc_delete(
            ocp_node, "pod", heketi_pod_name,
            collect_logs=self.heketi_logs_before_delete)
        wait_for_resource_absence(ocp_node, "pod", heketi_pod_name)

        # get new heketi-pod name
        heketi_pod_name = get_pod_name_from_dc(ocp_node, self.heketi_dc_name)
        wait_for_pod_be_ready(ocp_node, heketi_pod_name)

        # check heketi server is running
        err_msg = "Heketi server %s is not alive" % h_url
        self.assertTrue(hello_heketi(h_node, h_url), err_msg)

    def _cleanup_heketi_volumes(self, existing_volumes):
        h_node, h_url = self.heketi_client_node, self.heketi_server_url

        volumes = heketi_volume_list(h_node, h_url, json=True).get("volumes")
        new_volumes = list(set(volumes) - set(existing_volumes))
        for volume in new_volumes:
            h_vol_info = heketi_volume_info(h_node, h_url, volume, json=True)
            if h_vol_info.get("block"):
                for block_vol in (
                        h_vol_info.get("blockinfo").get("blockvolume")):
                    heketi_blockvolume_delete(h_node, h_url, block_vol)
            heketi_volume_delete(h_node, h_url, volume, raise_on_error=False)

    @pytest.mark.tier1
    @ddt.data("", "block")
    def test_verify_delete_heketi_volumes_pending_entries_in_db(
            self, vol_type):
        """Verify pending entries of blockvolumes/volumes and bricks in heketi
           db during blockvolume/volume delete operation.
        """
        # Create a large volumes to observe the pending operation
        vol_count, volume_ids, async_obj = 10, [], []
        h_node, h_url = self.heketi_client_node, self.heketi_server_url

        # Verify file/block volumes pending operation before creation,
        h_db_check_before = heketi_db_check(h_node, h_url)
        h_db_check_bricks_before = h_db_check_before.get("bricks")
        h_db_check_vol_before = (
            h_db_check_before.get("{}volumes".format(vol_type)))

        # Delete heketi pod to clean db operations
        if(h_db_check_bricks_before.get("pending")
                or h_db_check_vol_before.get("pending")):
            self._respin_heketi_pod()

        # Calculate heketi volume size
        free_space, nodenum = get_total_free_space(h_node, h_url)
        free_space_available = int(free_space / nodenum)
        if free_space_available > vol_count:
            h_volume_size = int(free_space_available / vol_count)
            if h_volume_size > 50:
                h_volume_size = 50
        else:
            h_volume_size, vol_count = 1, free_space_available

        # Create BHV in case blockvolume size is greater than default BHV size
        if vol_type:
            default_bhv_size = get_default_block_hosting_volume_size(
                h_node, self.heketi_dc_name)
            if default_bhv_size < h_volume_size:
                h_volume_name = "autotest-{}".format(utils.get_random_str())
                bhv_info = self.create_heketi_volume_with_name_and_wait(
                    h_volume_name, free_space_available,
                    raise_on_cleanup_error=False, block=True, json=True)
                free_space_available -= (
                    int(bhv_info.get("blockinfo").get("reservedsize")) + 1)
                h_volume_size = int(free_space_available / vol_count)

        # Create file/block volumes
        for _ in range(vol_count):
            vol_id = eval("heketi_{}volume_create".format(vol_type))(
                h_node, h_url, h_volume_size, json=True).get("id")
            volume_ids.append(vol_id)
            self.addCleanup(
                eval("heketi_{}volume_delete".format(vol_type)),
                h_node, h_url, vol_id, raise_on_error=False)

        def run_async(cmd, hostname, raise_on_error=True):
            async_op = g.run_async(host=hostname, command=cmd)
            async_obj.append(async_op)
            return async_op

        bhv_list = []
        for vol_id in volume_ids:
            # Get BHV ids to delete in case of block volumes
            if vol_type:
                vol_info = (
                    heketi_blockvolume_info(h_node, h_url, vol_id, json=True))
                if not vol_info.get("blockhostingvolume") in bhv_list:
                    bhv_list.append(vol_info.get("blockhostingvolume"))

            # Temporary replace g.run with g.async_run in heketi_volume_delete
            # and heketi_blockvolume_delete func to be able to run it in
            # background.
            with mock.patch.object(
                    command, 'cmd_run', side_effect=run_async):
                eval("heketi_{}volume_delete".format(vol_type))(
                    h_node, h_url, vol_id)

        # Wait for pending operations to get generate
        for w in waiter.Waiter(timeout=30, interval=3):
            h_db_check = heketi_db_check(h_node, h_url)
            h_db_check_vol = h_db_check.get("{}volumes".format(vol_type))
            if h_db_check_vol.get("pending"):
                h_db_check_bricks = h_db_check.get("bricks")
                break
        if w.expired:
            raise exceptions.ExecutionError(
                "No any pending operations found during {}volumes deletion "
                "{}".format(vol_type, h_db_check_vol.get("pending")))

        # Verify bricks pending operation during creation
        if not vol_type:
            self.assertTrue(
                h_db_check_bricks.get("pending"),
                "Expecting at least one bricks pending count")
            self.assertFalse(
                h_db_check_bricks.get("pending") % 3,
                "Expecting bricks pending count to be multiple of 3 but "
                "found {}".format(h_db_check_bricks.get("pending")))

        # Verify file/block volume pending operation during delete
        for w in waiter.Waiter(timeout=120, interval=10):
            h_db_check = heketi_db_check(h_node, h_url)
            h_db_check_vol = h_db_check.get("{}volumes".format(vol_type))
            h_db_check_bricks = h_db_check.get("bricks")
            if ((not h_db_check_bricks.get("pending"))
                    and (not h_db_check_vol.get("pending"))):
                break
        if w.expired:
            raise exceptions.AssertionError(
                "Failed to delete {}volumes after 120 secs".format(vol_type))

        # Check that all background processes got exited
        for obj in async_obj:
            ret, out, err = obj.async_communicate()
            self.assertFalse(
                ret, "Failed to delete {}volume due to error: {}".format(
                    vol_type, err))

        # Delete BHV created during block volume creation
        if vol_type:
            for bhv_id in bhv_list:
                heketi_volume_delete(h_node, h_url, bhv_id)

        # Verify bricks and volume pending operations
        h_db_check_after = heketi_db_check(h_node, h_url)
        h_db_check_bricks_after = h_db_check_after.get("bricks")
        h_db_check_vol_after = (
            h_db_check_after.get("{}volumes".format(vol_type)))
        act_brick_count = h_db_check_bricks_after.get("pending")
        act_vol_count = h_db_check_vol_after.get("pending")

        # Verify bricks pending operation after delete
        err_msg = "{} operations are pending for {} after {}volume deletion"
        if not vol_type:
            self.assertFalse(
                act_brick_count, err_msg.format(
                    act_brick_count, "brick", vol_type))

        # Verify file/bock volumes pending operation after delete
        self.assertFalse(
            act_vol_count, err_msg.format(act_vol_count, "volume", vol_type))

        act_brick_count = h_db_check_bricks_after.get("total")
        act_vol_count = h_db_check_vol_after.get("total")
        exp_brick_count = h_db_check_bricks_before.get("total")
        exp_vol_count = h_db_check_vol_before.get("total")
        err_msg = "Actual {} and expected {} {} counts are not matched"

        # Verify if initial and final file/block volumes are same
        self.assertEqual(
            act_vol_count, exp_vol_count,
            err_msg.format(act_vol_count, exp_vol_count, "volume"))

        # Verify if initial and final bricks are same
        self.assertEqual(
            act_brick_count, exp_brick_count,
            err_msg.format(act_brick_count, exp_brick_count, "brick"))

    @pytest.mark.tier1
    @ddt.data('', 'block')
    def test_verify_create_heketi_volumes_pending_entries_in_db(
            self, vol_type):
        """Verify pending entries of file/block volumes in db during
           volumes creation from heketi side
        """
        # Create large volumes to observe the pending operations
        vol_count, h_vol_creation_async_op = 3, []
        h_node, h_url = self.heketi_client_node, self.heketi_server_url

        # Verify file/block volumes pending operation before creation,
        h_db_check_before = heketi_db_check(h_node, h_url)
        h_db_check_vol_before = (
            h_db_check_before.get("{}volumes".format(vol_type)))

        # Delete heketi pod to clean db operations
        if(h_db_check_vol_before.get("pending")
                or h_db_check_before.get("bricks").get("pending")):
            self._respin_heketi_pod()

        # Calculate heketi volume size
        free_space, nodenum = get_total_free_space(h_node, h_url)
        free_space_available = int(free_space / nodenum)
        if free_space_available > vol_count:
            h_volume_size = int(free_space_available / vol_count)
            if h_volume_size > 30:
                h_volume_size = 30
        else:
            h_volume_size, vol_count = 1, free_space_available

        # Get existing heketi volume list
        existing_volumes = heketi_volume_list(h_node, h_url, json=True)

        # Add cleanup function to clean stale volumes created during test
        self.addCleanup(
            self._cleanup_heketi_volumes, existing_volumes.get("volumes"))

        # Create BHV in case blockvolume size is greater than default BHV size
        if vol_type:
            default_bhv_size = get_default_block_hosting_volume_size(
                h_node, self.heketi_dc_name)
            if default_bhv_size < h_volume_size:
                h_volume_name = "autotest-{}".format(utils.get_random_str())
                bhv_info = self.create_heketi_volume_with_name_and_wait(
                    h_volume_name, free_space_available,
                    raise_on_cleanup_error=False, block=True, json=True)
                free_space_available -= (
                    int(bhv_info.get("blockinfo").get("reservedsize")) + 1)
                h_volume_size = int(free_space_available / vol_count)

        # Temporary replace g.run with g.async_run in heketi_blockvolume_create
        # func to be able to run it in background.Also, avoid parsing the
        # output as it won't be json at that moment. Parse it after reading
        # the async operation results.
        def run_async(cmd, hostname, raise_on_error=True):
            return g.run_async(host=hostname, command=cmd)

        for count in range(vol_count):
            with mock.patch.object(json, 'loads', side_effect=(lambda j: j)):
                with mock.patch.object(
                        command, 'cmd_run', side_effect=run_async):
                    h_vol_creation_async_op.append(
                        eval("heketi_{}volume_create".format(vol_type))(
                            h_node, h_url, h_volume_size, json=True))

        # Check for pending operations
        for w in waiter.Waiter(timeout=120, interval=10):
            h_db_check = heketi_db_check(h_node, h_url)
            h_db_check_vol = h_db_check.get("{}volumes".format(vol_type))
            if h_db_check_vol.get("pending"):
                h_db_check_bricks = h_db_check.get("bricks")
                break
        if w.expired:
            raise exceptions.ExecutionError(
                "No any pending operations found during {}volumes creation "
                "{}".format(vol_type, h_db_check_vol.get("pending")))

        # Verify bricks pending operation during creation
        if not vol_type:
            self.assertTrue(
                h_db_check_bricks.get("pending"),
                "Expecting at least one bricks pending count")
            self.assertFalse(
                h_db_check_bricks.get("pending") % 3,
                "Expecting bricks pending count to be multiple of 3 but "
                "found {}".format(h_db_check_bricks.get("pending")))

        # Wait for all counts of pending operations to be zero
        for w in waiter.Waiter(timeout=300, interval=10):
            h_db_check = heketi_db_check(h_node, h_url)
            h_db_check_vol = h_db_check.get("{}volumes".format(vol_type))
            if not h_db_check_vol.get("pending"):
                break
        if w.expired:
            raise exceptions.ExecutionError(
                "Expecting no pending operations after 300 sec but "
                "found {} operation".format(h_db_check_vol.get("pending")))

        # Get heketi server DB details
        h_db_check_after = heketi_db_check(h_node, h_url)
        h_db_check_vol_after = (
            h_db_check_after.get("{}volumes".format(vol_type)))
        h_db_check_bricks_after = h_db_check_after.get("bricks")

        # Verify if initial and final file/block volumes are same
        act_vol_count = h_db_check_vol_after.get("total")
        exp_vol_count = h_db_check_vol_before.get("total") + vol_count
        err_msg = (
            "Actual {} and expected {} {}volume counts are not matched".format(
                act_vol_count, exp_vol_count, vol_type))
        self.assertEqual(act_vol_count, exp_vol_count, err_msg)

        # Verify if initial and final bricks are same for file volume
        volumes = heketi_volume_list(h_node, h_url, json=True).get("volumes")
        new_volumes = list(set(volumes) - set(existing_volumes))
        exp_brick_count = 0
        for volume in new_volumes:
            vol_info = heketi_volume_info(h_node, h_url, volume, json=True)
            exp_brick_count += len(vol_info.get("bricks"))

        err_msg = "Actual {} and expected {} bricks counts are not matched"
        act_brick_count = h_db_check_bricks_after.get("total")
        self.assertEqual(
            act_brick_count, exp_brick_count, err_msg.format(
                act_brick_count, exp_brick_count))

    @pytest.mark.tier1
    @podcmd.GlustoPod()
    def test_volume_creation_after_stopping_heketidb_volume(self):
        """Validate volume creation after stopping heketidb volume"""
        # Stop heketidbstorage volume
        ret, _, err = volume_stop(
            "auto_get_gluster_endpoint", "heketidbstorage")
        self.addCleanup(
            podcmd.GlustoPod()(volume_start), "auto_get_gluster_endpoint",
            "heketidbstorage")
        self.assertFalse(
            ret, "Failed to stop gluster volume "
            "heketidbstorage. error: {}".format(err))

        # Try to create heketi volume to make sure it fails
        err_msg = (
            "Unexpectedly: Volume has been created when gluster "
            "volume heketidbstorage is stopped")
        with self.assertRaises(AssertionError, msg=err_msg) as e:
            volume_name = heketi_volume_create(
                self.heketi_client_node, self.heketi_server_url,
                self.volume_size, json=True)
            self.addCleanup(
                heketi_volume_delete, self.heketi_client_node,
                self.heketi_server_url, volume_name["bricks"][0]["volume"])
        self.assertIn(
            "transport endpoint is not connected", six.text_type(e.exception))

    @pytest.mark.tier0
    def test_heketi_volume_create_with_clusterid(self):
        """Validate creation of heketi volume with clusters argument"""
        h_node, h_url = self.heketi_client_node, self.heketi_server_url

        # Get one of the cluster id from heketi cluster list
        creation_cluster_id = heketi_cluster_list(
            h_node, h_url, json=True)['clusters'][0]

        # Create a heketi volume specific to cluster list
        volume_id = heketi_volume_create(
            h_node, h_url, self.volume_size, clusters=creation_cluster_id,
            json=True)["bricks"][0]["volume"]
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, volume_id)

        # Get the cluster id from heketi volume info
        info_cluster_id = heketi_volume_info(
            h_node, h_url, volume_id, json=True)['cluster']

        # Match the creation cluster id  with the info cluster id
        self.assertEqual(
            info_cluster_id, creation_cluster_id,
            "Volume creation cluster id {} not matching the info cluster id "
            "{}".format(creation_cluster_id, info_cluster_id))
