#!/usr/bin/python

from __future__ import division
import math

from glusto.core import Glusto as g
from glustolibs.gluster.exceptions import ExecutionError
from glustolibs.gluster.volume_ops import get_volume_info
from cnslibs.common.heketi_libs import HeketiClientSetupBaseClass
from cnslibs.common.heketi_ops import (heketi_volume_create,
                                       heketi_device_disable,
                                       heketi_device_enable,
                                       heketi_device_info,
                                       heketi_volume_delete)
from cnslibs.common import podcmd
from cnslibs.common.openshift_ops import oc_rsh, get_ocp_gluster_pod_names


class TestHeketiVolume(HeketiClientSetupBaseClass):

    @classmethod
    def setUpClass(cls):
        super(TestHeketiVolume, cls).setUpClass()
        cls.volume_size = cls.heketi_volume['size']

    @podcmd.GlustoPod()
    def test_to_disable_device_and_create_vol(self):
        """
        Disabling a device
        """
        size = 610
        volume_id_list = []
        # Create heketi volume
        g.log.info("Creating a heketi volume")
        out = heketi_volume_create(self.heketi_client_node,
                                   self.heketi_server_url,
                                   self.volume_size, json=True)
        self.assertTrue(out, ("Failed to create "
                        "heketi volume of size %s" % str(self.volume_size)))
        g.log.info("Successfully created heketi volume of size %s" % str(self.volume_size))
        volume_id = out["bricks"][0]["volume"]
        device_id = out["bricks"][0]["device"]

        # Disable device
        g.log.info("Disabling a device")
        out = heketi_device_disable(self.heketi_client_node,
                                    self.heketi_server_url,
                                    device_id)
        self.assertTrue(out, ("Failed to disable "
                        "the device %s" % device_id))
        g.log.info("Successfully disabled device %s" % device_id)

        # Get device info
        g.log.info("Retrieving device info")
        out = heketi_device_info(self.heketi_client_node,
                                 self.heketi_server_url,
                                 device_id, json=True)
        self.assertTrue(out, ("Failed to get device info %s" % device_id))
        g.log.info("Successfully retrieved device info %s" % device_id)
        name = out["name"]
        if out["state"] != "offline":
            raise ExecutionError("Device %s is now online" % name)
        g.log.info("Device %s is now offine" % name)

        # Try to create heketi volume
        g.log.info("Creating heketi volume:"
                   " Expected to fail")
        out = heketi_volume_create(self.heketi_client_node,
                                   self.heketi_server_url,
                                   size, json=True)
        self.assertFalse(out, ("Successfully created "
                         "volume of size %s, Node"
                         " has more than one device" % str(size)))
        g.log.info("Expected output: Failed to create a "
                   "volume of size %s since device "
                   "is disabled %s" % (str(size), device_id))

        # Enable the device
        g.log.info("Enable the device")
        out = heketi_device_enable(self.heketi_client_node,
                                   self.heketi_server_url,
                                   device_id)
        self.assertTrue(out, ("Failed to enable the "
                        "device %s" % device_id))
        g.log.info("Successfully enabled device %s" % device_id)

        # Get device info
        g.log.info("Retrieving device info")
        out = heketi_device_info(self.heketi_client_node,
                                 self.heketi_server_url,
                                 device_id,
                                 json=True)
        self.assertTrue(out, ("Failed to get device info %s" % device_id))
        g.log.info("Successfully retrieved device info %s" % device_id)
        name = out["name"]
        if out["state"] != "online":
            raise ExecutionError("Device %s is now offline" % name)
        g.log.info("Device %s is now online" % name)

        # Create heketi volume of size
        g.log.info("Creating heketi volume")
        out = heketi_volume_create(self.heketi_client_node,
                                   self.heketi_server_url,
                                   size, json=True)
        self.assertTrue(out, ("Failed to create volume "
                        "of size %s" % str(size)))
        g.log.info("Successfully created volume of size %s" % str(size))
        name = out["name"]
        volume_id = out["bricks"][0]["volume"]
        volume_id_list.append(volume_id)
        self.addCleanup(self.delete_volumes, volume_id_list)

        # Get gluster volume info
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
