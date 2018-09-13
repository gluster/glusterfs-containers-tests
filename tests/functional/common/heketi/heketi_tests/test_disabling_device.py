from glusto.core import Glusto as g
from glustolibs.gluster.volume_ops import get_volume_info

from cnslibs.common import exceptions
from cnslibs.common import heketi_libs
from cnslibs.common import heketi_ops
from cnslibs.common import openshift_ops
from cnslibs.common import podcmd


class TestDisableHeketiDevice(heketi_libs.HeketiClientSetupBaseClass):
    @podcmd.GlustoPod()
    def test_create_volumes_enabling_and_disabling_heketi_devices(self):
        """Test case CNS-763"""

        # Get nodes info
        node_id_list = heketi_ops.heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)
        node_info_list = []
        for node_id in node_id_list[0:3]:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            node_info_list.append(node_info)

        # Disable 4th and other nodes
        if len(node_id_list) > 3:
            for node in node_id_list[3:]:
                heketi_ops.heketi_node_disable(
                    self.heketi_client_node, self.heketi_server_url, node_id)
                self.addCleanup(
                    heketi_ops.heketi_node_enable, self.heketi_client_node,
                    self.heketi_server_url, node_id)

        # Disable second and other devices on the first 3 nodes
        for node_info in node_info_list[0:3]:
            devices = node_info["devices"]
            self.assertTrue(
                devices, "Node '%s' does not have devices." % node_info["id"])
            if devices[0]["state"].strip().lower() != "online":
                self.skipTest("Test expects first device to be enabled.")
            if len(devices) < 2:
                continue
            for device in node_info["devices"][1:]:
                out = heketi_ops.heketi_device_disable(
                    self.heketi_client_node, self.heketi_server_url,
                    device["id"])
                self.assertTrue(
                    out, "Failed to disable the device %s" % device["id"])
                self.addCleanup(
                    heketi_ops.heketi_device_enable,
                    self.heketi_client_node, self.heketi_server_url,
                    device["id"])

        # Create heketi volume
        out = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 1, json=True)
        self.assertTrue(out, "Failed to create heketi volume of size 1")
        g.log.info("Successfully created heketi volume of size 1")
        device_id = out["bricks"][0]["device"]
        self.addCleanup(self.delete_volumes, [out["bricks"][0]["volume"]])

        # Disable device
        g.log.info("Disabling '%s' device" % device_id)
        out = heketi_ops.heketi_device_disable(
            self.heketi_client_node, self.heketi_server_url, device_id)
        self.assertTrue(out, "Failed to disable the device %s" % device_id)
        g.log.info("Successfully disabled device %s" % device_id)

        try:
            # Get device info
            g.log.info("Retrieving '%s' device info" % device_id)
            out = heketi_ops.heketi_device_info(
                self.heketi_client_node, self.heketi_server_url,
                device_id, json=True)
            self.assertTrue(out, "Failed to get device info %s" % device_id)
            g.log.info("Successfully retrieved device info %s" % device_id)
            name = out["name"]
            if out["state"].lower().strip() != "offline":
                raise exceptions.ExecutionError(
                    "Device %s is not in offline state." % name)
            g.log.info("Device %s is now offine" % name)

            # Try to create heketi volume
            g.log.info("Creating heketi volume: Expected to fail.")
            try:
                out = heketi_ops.heketi_volume_create(
                    self.heketi_client_node, self.heketi_server_url, 1,
                    json=True)
            except exceptions.ExecutionError:
                g.log.info("Volume was not created as expected.")
            else:
                self.addCleanup(
                    self.delete_volumes, [out["bricks"][0]["volume"]])
                msg = "Volume unexpectedly created. Out: %s" % out
                assert False, msg
        finally:
            # Enable the device back
            g.log.info("Enable '%s' device back." % device_id)
            out = heketi_ops.heketi_device_enable(
                self.heketi_client_node, self.heketi_server_url, device_id)
            self.assertTrue(out, "Failed to enable the device %s" % device_id)
            g.log.info("Successfully enabled device %s" % device_id)

        # Get device info
        out = heketi_ops.heketi_device_info(
            self.heketi_client_node, self.heketi_server_url, device_id,
            json=True)
        self.assertTrue(out, ("Failed to get device info %s" % device_id))
        g.log.info("Successfully retrieved device info %s" % device_id)
        name = out["name"]
        if out["state"] != "online":
            raise exceptions.ExecutionError(
                "Device %s is not in online state." % name)

        # Create heketi volume of size
        out = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 1, json=True)
        self.assertTrue(out, "Failed to create volume of size 1")
        self.addCleanup(self.delete_volumes, [out["bricks"][0]["volume"]])
        g.log.info("Successfully created volume of size 1")
        name = out["name"]

        # Get gluster volume info
        if self.deployment_type == "cns":
            gluster_pod = openshift_ops.get_ocp_gluster_pod_names(
                self.heketi_client_node)[1]
            p = podcmd.Pod(self.heketi_client_node, gluster_pod)
            out = get_volume_info(p, volname=name)
        else:
            out = get_volume_info(self.heketi_client_node, volname=name)
        self.assertTrue(out, "Failed to get '%s' volume info." % name)
        g.log.info("Successfully got the '%s' volume info." % name)
