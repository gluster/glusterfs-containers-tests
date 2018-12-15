import time

from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import get_volume_list, get_volume_info
import six

from cnslibs.common.exceptions import ExecutionError
from cnslibs.common.heketi_libs import HeketiBaseClass
from cnslibs.common.heketi_ops import (heketi_volume_create,
                                       heketi_volume_list,
                                       heketi_volume_info,
                                       heketi_blockvolume_create,
                                       heketi_blockvolume_delete,
                                       heketi_cluster_list,
                                       heketi_cluster_delete,
                                       heketi_node_info,
                                       heketi_node_list,
                                       heketi_node_delete)
from cnslibs.common.openshift_ops import get_ocp_gluster_pod_names
from cnslibs.common import podcmd


class TestHeketiVolume(HeketiBaseClass):
    """
    Class to test heketi volume create
    """
    @classmethod
    def setUpClass(cls):
        super(TestHeketiVolume, cls).setUpClass()
        cls.volume_size = cls.heketi_volume['size']

    @podcmd.GlustoPod()
    def test_volume_create_and_list_volume(self):
        """
        Create a heketi volume and list the volume
        compare the volume with gluster volume list
        """
        g.log.info("Create a heketi volume")
        out = heketi_volume_create(self.heketi_client_node,
                                   self.heketi_server_url,
                                   self.volume_size, json=True)
        self.assertTrue(out, ("Failed to create heketi "
                        "volume of size %s" % str(self.volume_size)))
        g.log.info("Heketi volume successfully created" % out)
        volume_id = out["bricks"][0]["volume"]
        self.addCleanup(self.delete_volumes, volume_id)

        g.log.info("List heketi volumes")
        volumes = heketi_volume_list(self.heketi_client_node,
                                     self.heketi_server_url,
                                     json=True)
        self.assertTrue(volumes, ("Failed to list heketi volumes"))
        g.log.info("Heketi volumes successfully listed")

        g.log.info("List gluster volumes")
        if self.deployment_type == "cns":
            gluster_pod = get_ocp_gluster_pod_names(
                self.heketi_client_node)[1]
            p = podcmd.Pod(self.heketi_client_node, gluster_pod)
            out = get_volume_list(p)
        else:
            out = get_volume_list(self.heketi_client_node)
        self.assertTrue(out, ("Unable to get volumes list"))
        g.log.info("Successfully got the volumes list")

        # Check the volume count are equal
        self.assertEqual(
            len(volumes["volumes"]), len(out),
            "Lengths of gluster '%s' and heketi '%s' volume lists are "
            "not equal." % (out, volumes)
        )
        g.log.info("Heketi volumes list %s and"
                   " gluster volumes list %s" % ((volumes), (out)))

    @podcmd.GlustoPod()
    def test_create_vol_and_retrieve_vol_info(self):
        """
        Create a heketi volume and retrieve the volume info
        and get gluster volume info
        """

        g.log.info("Create a heketi volume")
        out = heketi_volume_create(self.heketi_client_node,
                                   self.heketi_server_url,
                                   self.volume_size, json=True)
        self.assertTrue(out, ("Failed to create heketi "
                        "volume of size %s" % str(self.volume_size)))
        g.log.info("Heketi volume successfully created" % out)
        volume_id = out["bricks"][0]["volume"]
        self.addCleanup(self.delete_volumes, volume_id)

        g.log.info("Retrieving heketi volume info")
        out = heketi_volume_info(
            self.heketi_client_node, self.heketi_server_url, volume_id,
            json=True)
        self.assertTrue(out, ("Failed to get heketi volume info"))
        g.log.info("Successfully got the heketi volume info")
        name = out["name"]

        if self.deployment_type == "cns":
            gluster_pod = get_ocp_gluster_pod_names(
                self.heketi_client_node)[1]
            p = podcmd.Pod(self.heketi_client_node, gluster_pod)
            out = get_volume_info(p, volname=name)
        else:
            out = get_volume_info(self.heketi_client_node,
                                  volname=name)
        self.assertTrue(out, ("Failed to get volume info %s" % name))
        g.log.info("Successfully got the volume info %s" % name)

    def test_to_check_deletion_of_cluster(self):
        """
        Deletion of a cluster with volumes
        and/ or nodes should fail
        """
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
                            "volume of size %s" % str(self.volume_size)))
            g.log.info("Heketi volume successfully created" % out)
            volume_id = out["bricks"][0]["volume"]
            self.addCleanup(self.delete_volumes, volume_id)

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
            ExecutionError,
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
        """Deletion of a node which contains devices"""

        # Create Heketi volume to make sure we have devices with usages
        heketi_url = self.heketi_server_url
        vol = heketi_volume_create(
            self.heketi_client_node, heketi_url, 1, json=True)
        self.assertTrue(vol, "Failed to create heketi volume.")
        g.log.info("Heketi volume successfully created")
        volume_id = vol["bricks"][0]["volume"]
        self.addCleanup(self.delete_volumes, volume_id)

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
            ExecutionError,
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
        """Test case CNS-550"""

        # Create first small blockvolume
        blockvol1 = heketi_blockvolume_create(
            self.heketi_client_node, self.heketi_server_url, 1, json=True)
        self.assertTrue(blockvol1, "Failed to create block volume.")
        self.addCleanup(
            heketi_blockvolume_delete, self.heketi_client_node,
            self.heketi_server_url, blockvol1['id'])

        # Sleep for couple of seconds to avoid races
        time.sleep(2)

        # Get info about block hosting volume available space
        file_volumes = heketi_volume_list(
            self.heketi_client_node, self.heketi_server_url, json=True)
        self.assertTrue(file_volumes)
        max_freesize = 0
        file_volumes_debug_info = []
        for vol_id in file_volumes["volumes"]:
            vol = heketi_volume_info(
                self.heketi_client_node, self.heketi_server_url,
                vol_id, json=True)
            current_freesize = vol.get("blockinfo", {}).get("freesize", 0)
            if current_freesize > max_freesize:
                max_freesize = current_freesize
            if current_freesize:
                file_volumes_debug_info.append(six.text_type({
                    'id': vol.get('id', '?'),
                    'name': vol.get('name', '?'),
                    'size': vol.get('size', '?'),
                    'blockinfo': vol.get('blockinfo', '?'),
                }))
        self.assertGreater(max_freesize, 0)

        # Try to create blockvolume with size bigger than available
        too_big_vol_size = max_freesize + 1
        try:
            blockvol2 = heketi_blockvolume_create(
                self.heketi_client_node, self.heketi_server_url,
                too_big_vol_size, json=True)
        except ExecutionError:
            return

        if blockvol2 and blockvol2.get('id'):
            self.addCleanup(
                heketi_blockvolume_delete, self.heketi_client_node,
                self.heketi_server_url, blockvol2['id'])
        self.assertFalse(
            blockvol2,
            "Volume unexpectedly was created. Calculated 'max free size' is "
            "'%s'.\nBlock volume info is: %s \n"
            "Block hosting volumes which were considered: \n%s" % (
                max_freesize, blockvol2, '\n'.join(file_volumes_debug_info)))
