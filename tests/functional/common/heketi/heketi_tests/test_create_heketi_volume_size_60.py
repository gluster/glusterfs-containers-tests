#!/usr/bin/python

from __future__ import division
import math

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ConfigError
from glustolibs.gluster.volume_ops import get_volume_list, get_volume_info
from cnslibs.common.heketi_libs import HeketiClientSetupBaseClass
from cnslibs.common.heketi_ops import (heketi_node_list,
                                       heketi_node_info,
                                       heketi_volume_create,
                                       heketi_volume_list,
                                       heketi_volume_info,
                                       heketi_volume_delete,
                                       heketi_topology_info)
from cnslibs.common import heketi_ops, podcmd
from cnslibs.common.openshift_ops import oc_rsh, get_ocp_gluster_pod_names


class TestHeketiVolume(HeketiClientSetupBaseClass):

    def get_free_space(self):
        """
        Get free space in each devices
        """
        free_spaces = []
        heketi_node_id_list = []
        device_list = []
        heketi_node_list_string = heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)
        for line in heketi_node_list_string.strip().split("\n"):
            heketi_node_id_list.append(line.strip().split(
                "Cluster")[0].strip().split(":")[1])
        for node_id in heketi_node_id_list:
            node_info_dict = heketi_node_info(self.heketi_client_node,
                                              self.heketi_server_url,
                                              node_id, json=True)
            total_free_space = 0
            for device in node_info_dict["devices"]:
                total_free_space += device["storage"]["free"]
            free_spaces.append(total_free_space)
        min_free_space = min(free_spaces)
        total_free_space = sum(free_spaces)/(1024**2)
        optimum_space = min_free_space / (1024 * 1024 * 10)
        free_space = int(math.floor(optimum_space))
        total_free_space = int(math.floor(total_free_space))

        return total_free_space, free_spaces

    @podcmd.GlustoPod()
    def test_to_create_distribute_replicated_vol(self):
        """
        Create distribute replicate heketi
        volume and run heketi-cli volume info
        """

        hosts = []
        size = 610
        g.log.info("Creating a heketi volume")
        out = heketi_volume_create(self.heketi_client_node,
                                   self.heketi_server_url,
                                   size, json=True)
        self.assertTrue(out, ("Failed to create "
                        "heketi volume of size %s" % str(size)))
        g.log.info("Successfully created heketi volume"
                   " of size %s" % str(size))
        volume_id = out["bricks"][0]["volume"]
        self.addCleanup(self.delete_volumes, volume_id)

        # Correct the backupvol file servers are updated
        gluster_servers = []
        g.log.info("Checking backupvol file servers are updated")
        mount_node = (out["mount"]["glusterfs"]
                      ["device"].strip().split(":")[0])
        hosts.append(mount_node)
        backup_volfile_server_list = (out["mount"]["glusterfs"]["options"]                                      ["backup-volfile-servers"].strip().split(","))
        for backup_volfile_server in backup_volfile_server_list:
            hosts.append(backup_volfile_server)
        for gluster_server in g.config["gluster_servers"].keys():
            gluster_servers.append(g.config["gluster_servers"]
                                   [gluster_server]["storage"])
        self.assertEqual(set(hosts), set(gluster_servers))
        g.log.info("Correctly updated backupvol file servers")

        # Retrieve heketi volume info
        g.log.info("Retrieving heketi volume info")
        out = heketi_ops.heketi_volume_info(self.heketi_client_node,
                                            self.heketi_server_url,
                                            volume_id, json=True)
        self.assertTrue(out, ("Failed to get heketi volume info"))
        g.log.info("Successfully got the heketi volume info")
        name = out["name"]

        # Get gluster volume info
        g.log.info("Get gluster volume info")
        if self.deployment_type == "cns":
            gluster_pod = get_ocp_gluster_pod_names(
                self.heketi_client_node)[1]
            p = podcmd.Pod(self.heketi_client_node, gluster_pod)
            out = get_volume_info(p, volname=name)
        else:
            out = get_volume_info(self.heketi_client_node,
                                  volname=name)
        self.assertTrue(out, ("Failed to get volume info"))
        g.log.info("Successfully got the volume info")
        self.assertEqual(out[name]["typeStr"], "Distributed-Replicate",
                         "Not a Distributed-Replicate volume")

    @podcmd.GlustoPod()
    def test_to_create_and_delete_dist_rep_vol(self):
        """
        Create distribute replicate heketi
        volume and delete it and check the available
        space
        """

        size = 610
        g.log.info("Creating a heketi volume")
        out = heketi_volume_create(self.heketi_client_node,
                                   self.heketi_server_url,
                                   size, json=True)
        self.assertTrue(out, ("Failed to create "
                        "heketi volume of size %s" % str(size)))
        g.log.info("Successfully created heketi volume"
                   " of size %s" % str(size))
        volume_id = out["bricks"][0]["volume"]
        name = out["name"]

        # Get gluster volume info
        g.log.info("Get gluster volume info")
        if self.deployment_type == "cns":
            gluster_pod = get_ocp_gluster_pod_names(
                self.heketi_client_node)[1]
            p = podcmd.Pod(self.heketi_client_node, gluster_pod)
            out = get_volume_info(p, volname=name)
        else:
            out = get_volume_info(self.heketi_client_node,
                                  volname=name)
        self.assertTrue(out, ("Failed to get volume info"))
        g.log.info("Successfully got the volume info")
        self.assertEqual(out[name]["typeStr"], "Distributed-Replicate",
                         "Not a Distributed-Replicate volume")

        # Get the free space
        # After creating heketi volume
        free_space_after_creating_vol, _ = self.get_free_space()

        # Delete heketi volumes of size 60gb which was created
        g.log.info("Deleting heketi volumes")
        out = heketi_volume_delete(self.heketi_client_node,
                                   self.heketi_server_url,
                                   volume_id)
        if not out:
            raise ExecutionError("Failed to delete "
                                 "heketi volume %s" % volume_id)
        g.log.info("Heketi volume successfully deleted %s" % out)

        # Check the heketi volume list
        g.log.info("List heketi volumes")
        volumes = heketi_volume_list(self.heketi_client_node,
                                     self.heketi_server_url,
                                     json=True)
        self.assertTrue(volumes, ("Failed to list heketi volumes"))
        g.log.info("Heketi volumes successfully listed")

        # Check the gluster volume list
        g.log.info("Get the gluster volume list")
        if self.deployment_type == "cns":
            gluster_pod = get_ocp_gluster_pod_names(
                self.heketi_client_node)[1]
            p = podcmd.Pod(self.heketi_client_node, gluster_pod)
            out = get_volume_list(p)
        else:
            out = get_volume_list(self.heketi_client_node)
        self.assertTrue(out, ("Unable to get volume list"))
        g.log.info("Successfully got the volume list" % out)

        # Check the volume count are equal
        if (len(volumes["volumes"]) != len(out)):
            raise ExecutionError("Heketi volume list %s is"
                                 " not equal to gluster"
                                 " volume list %s" % ((volumes), (out)))
        g.log.info("Heketi volumes list %s and"
                   " gluster volumes list %s" % ((volumes), (out)))

        # Get the used space
        # After deleting heketi volume
        free_space_after_deleting_vol, _ = self.get_free_space()

        # Compare the free size before and after deleting volume
        g.log.info("Comparing the free space before and after"
                   " deleting volume")
        self.assertTrue(free_space_after_creating_vol < free_space_after_deleting_vol)
        g.log.info("Volume successfully deleted and space is"
                   " reallocated. Free space after creating"
                   " volume %s, Free space after deleting"
                   " volume %s" % ((free_space_after_creating_vol),
                    (free_space_after_deleting_vol)))
