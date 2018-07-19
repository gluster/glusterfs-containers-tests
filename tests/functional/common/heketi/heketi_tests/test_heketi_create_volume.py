#!/usr/bin/python

from glustolibs.gluster.exceptions import ExecutionError, ConfigError
from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import get_volume_list, get_volume_info
from cnslibs.common.heketi_libs import HeketiClientSetupBaseClass
from cnslibs.common.heketi_ops import (heketi_volume_create,
                                       heketi_volume_list,
                                       heketi_volume_info,
                                       heketi_volume_delete,
                                       heketi_cluster_list,
                                       heketi_cluster_delete,
                                       heketi_node_list,
                                       heketi_node_delete)
from cnslibs.common import heketi_ops, podcmd
from cnslibs.common.openshift_ops import oc_rsh, get_ocp_gluster_pod_names

class TestHeketiVolume(HeketiClientSetupBaseClass):
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
        name = out["name"]

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
        if (len(volumes["volumes"]) != len(out)):
            raise ExecutionError("Heketi volume list %s is"
                                 " not equal to gluster"
                                 " volume list %s" % ((volumes), (out)))
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
        out = heketi_ops.heketi_volume_info(self.heketi_client_node,
                                            self.heketi_server_url,
                                            volume_id, json=True)
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
        if (len(volumes["volumes"])== 0):
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
        out = heketi_cluster_delete(self.heketi_client_node,
                                    self.heketi_server_url,
                                    cluster_id)
        self.assertFalse(out, ("Successfully deleted a "
                         "cluster %s" % cluster_id))
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
        """
        Deletion of a node which contains devices
        """

        # List of heketi node
        g.log.info("List heketi nodes")
        heketi_node_id_list = heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)
        self.assertTrue(heketi_node_id_list, ("List of node IDs is empty."))

        g.log.info("Successfully got the list of nodes")
        for node_id in heketi_node_id_list:
            g.log.info("Retrieve the node info")
            node_info_dict = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            if not(node_info_dict["devices"][1]["storage"]["used"]):
                raise ConfigError("No device in node %s" % node_id)
            g.log.info("Used space in device %s" % node_info_dict[
                       "devices"][1]["storage"]["used"])
        node_id = heketi_node_id_list[0]

        # Deleting a node
        g.log.info("Trying to delete a node which"
                   " contains devices in it:"
                   " Expected to fail")
        out = heketi_node_delete(self.heketi_client_node,
                                 self.heketi_server_url,
                                 node_id)
        self.assertFalse(out, ("Successfully deletes a "
                         "node %s" % str(node_id)))
        g.log.info("Expected result: Unable to delete "
                   "node %s because it contains devices")

        # To confrim deletion failed, check node list
        # TODO: fix following, it doesn't verify absence of the deleted nodes
        g.log.info("Listing heketi node list")
        node_list = heketi_node_list(self.heketi_client_node,
                                     self.heketi_server_url)
        self.assertTrue(node_list, ("Failed to list heketi nodes"))
        g.log.info("Successfully got the list of nodes")
