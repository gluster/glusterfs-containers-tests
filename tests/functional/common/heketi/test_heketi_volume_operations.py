from unittest import skip

from glusto.core import Glusto as g
from cnslibs.common.heketi_ops import (heketi_create_topology,
                                       heketi_topology_load,
                                       heketi_volume_delete,
                                       heketi_volume_create,
                                       heketi_volume_expand,
                                       heketi_volume_info,
                                       heketi_topology_info,
                                       heketi_device_add,
                                       heketi_device_enable,
                                       heketi_device_disable,
                                       heketi_device_remove,
                                       heketi_device_delete,
                                       heketi_node_info,
                                       heketi_node_list)
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

    def volume_cleanup(self, volume_id):
        """
        Method to cleanup volume in self.addCleanup()
        """
        if volume_id is not None:
            out = heketi_volume_delete(self.heketi_client_node,
                                       self.heketi_server_url,
                                       volume_id)
            output_str = 'Volume %s deleted' % volume_id
            if output_str not in out:
                raise ExecutionError("Failed to delete heketi volume of"
                                     "id %s" % volume_id)

    def add_device(self, device_name, node_id):
        """
        Adds a device through heketi-cli
        """
        ret = heketi_device_add(self.heketi_client_node,
                                self.heketi_server_url,
                                device_name,
                                node_id)

        self.assertTrue(ret, ("Failed to add a device %s" % device_name))

    def detach_devices_attached(self, device_id_list):
        """
        All the devices attached are gracefully
        detached in this function
        """
        if not isinstance(device_id_list, (list, set, tuple)):
            device_id_list = [device_id_list]

        for device_id in device_id_list:
            device_disable = heketi_device_disable(
                self.heketi_client_node, self.heketi_server_url, device_id)
            self.assertNotEqual(
                device_disable, False,
                "Device %s could not be disabled" % device_id)
            device_remove = heketi_device_remove(
                self.heketi_client_node, self.heketi_server_url, device_id)
            self.assertNotEqual(
                device_remove, False,
                "Device %s could not be removed" % device_id)
            device_delete = heketi_device_delete(
                self.heketi_client_node, self.heketi_server_url, device_id)
            self.assertNotEqual(
                device_delete, False,
                "Device %s could not be deleted" % device_id)

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
        self.addCleanup(self.volume_cleanup, vol_info['id'])

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
        self.addCleanup(self.volume_cleanup, vol_info['id'])
        self.assertEqual(vol_info['size'], int(volume_size),
                         ("Failed to create volume."
                          "Expected Size: %s, Actual Size: %s"
                          % (str(volume_size), str(vol_info['size']))))
        volume_id = vol_info["id"]
        expand_size = self.heketi_volume['expand_size']
        ret = heketi_volume_expand(self.heketi_client_node,
                                   self.heketi_server_url, volume_id,
                                   expand_size)
        self.assertTrue(ret, ("Failed to expand heketi volume of id %s"
                              % volume_id))
        volume_info = heketi_volume_info(self.heketi_client_node,
                                         self.heketi_server_url,
                                         volume_id, **kwargs)
        expected_size = int(volume_size) + int(expand_size)
        self.assertEqual(volume_info['size'], expected_size,
                         ("Volume Expansion failed Expected Size: %s, Actual "
                          "Size: %s" % (str(expected_size),
                                        str(volume_info['size']))))

    @skip("Blocked by BZ-1629889")
    def test_heketi_with_device_removal_insuff_space(self):
        """
        Test to create volume consuming all space and then adding new device
        and then trying to remove an existing device. We should get an error
        saying insufficient space when removing device.
        """
        device_id_list = []

        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url,
                                        650,
                                        json=True)

        self.assertNotEqual(vol_info, False, "Failed to create heketi volume")
        self.addCleanup(self.volume_cleanup, vol_info["id"])

        node_id_list = heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)

        for node_id in node_id_list[:2]:
            device_present = False
            node_info = heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)

            self.assertNotEqual(
                node_info, False,
                "Heketi node info on node %s failed" % node_id)

            node_ip = node_info["hostnames"]["storage"][0]

            for gluster_server in g.config["gluster_servers"].keys():
                gluster_server_ip = (g.config["gluster_servers"]
                                     [gluster_server]["storage"])
                if gluster_server_ip == node_ip:
                    device_name = (g.config["gluster_servers"][gluster_server]
                                   ["additional_devices"][0])
                    break
            device_addition_info = heketi_device_add(
                self.heketi_client_node, self.heketi_server_url,
                device_name, node_id, json=True)

            self.assertNotEqual(device_addition_info, False,
                                "Device %s addition failed" % device_name)

            node_info_after_addition = heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)

            self.assertNotEqual(node_info_after_addition, False,
                                "Node info failed for node %s" % node_id)

            self.assertNotEqual(
                node_info_after_addition["devices"], [],
                "No devices in node %s" % node_id)

            for device in node_info_after_addition["devices"]:
                if device["name"] == device_name:
                    device_present = True
                    device_id_list.append(device["id"])
                    break

            self.assertEqual(device_present, True,
                             "device %s not present" % device["id"])

        self.addCleanup(self.detach_devices_attached, device_id_list)

        node_1_id = node_id_list[0]

        node_1_info = heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_1_id, json=True)

        self.assertNotEqual(node_1_info, False,
                            "Node info failed for node %s" % node_1_id)
        self.assertNotEqual(
            node_1_info["devices"], [],
            "No devices in node %s" % node_1_id)
        device = any([d for d in node_1_info["devices"]
                      if device["id"] != device_id_list[0]])
        device_disable = heketi_device_disable(
            self.heketi_client_node, self.heketi_server_url,
            device["id"])
        self.assertNotEqual(
            device_disable, False,
            "Device %s could not be disabled" % device["id"])
        ret, out, err = heketi_device_remove(
            self.heketi_client_node, self.heketi_server_url,
            device["id"],
            raw_cli_output=True)
        self.assertNotEqual(ret, 0, "Device %s removal successfull")
        msg = "Error: Failed to remove device, error: No " +\
              "Replacement was found for resource requested to be " +\
              "removed"
        self.assertEqual(
            msg, err.strip(),
            "Device %s removal failed due to invalid reason")
        device_enable = heketi_device_enable(
            self.heketi_client_node, self.heketi_server_url,
            device["id"])
        self.assertNotEqual(
            device_enable, False,
            "Device %s could not be enabled" % device["id"])

    @skip("Blocked by BZ-1629889")
    def test_device_remove_basic_validation(self):
        """
        Test to create volume after a device removal and with new device added.
        """

        volume_size = self.heketi_volume['size']

        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url, volume_size,
                                        json=True)
        self.assertTrue(vol_info, ("Failed to create heketi volume of size %s"
                                   % str(volume_size)))
        self.addCleanup(self.volume_cleanup, vol_info['id'])

        self.assertEqual(vol_info['size'], int(volume_size),
                         ("Failed to create volume with default options."
                          "Expected Size: %s, Actual Size: %s"
                          % (str(volume_size), str(vol_info['size']))))

        # 1. Device addition
        gluster_srvrs = self.gluster_servers

        device_name = (g.config["gluster_servers"][gluster_srvrs[0]]
                       ["additional_devices"][0])
        manage_hostname = (g.config["gluster_servers"]
                           [gluster_srvrs[0]]["manage"])
        # Now, get node id of corresponding hostname
        topo_info = heketi_topology_info(self.heketi_client_node,
                                         self.heketi_server_url,
                                         json=True)

        self.assertNotEqual(
            topo_info["clusters"][0]["nodes"], [],
            "Nodes don't exist, empty cluster")

        node_id = None
        for node in topo_info["clusters"][0]["nodes"]:
            if manage_hostname == node['hostnames']["manage"][0]:
                node_id = node["id"]
                break
        self.assertNotEqual(
            node_id, None,
            "No information about node_id for %s" % manage_hostname)
        self.add_device(device_name, node_id)

        # get other device id for deletion
        topo_info = heketi_topology_info(self.heketi_client_node,
                                         self.heketi_server_url,
                                         json=True)

        self.assertNotEqual(
            topo_info["clusters"][0]["nodes"], [],
            "No information about nodes")

        device_id_flag1 = False
        device_id_flag2 = False

        for node in topo_info["clusters"][0]["nodes"]:
            if node["id"] != node_id:
                continue
            self.assertNotEqual(
                node["devices"], [],
                "Device list empty for node %s" % node_id)
            for device in node["devices"]:
                device_id = device["id"]
                if device_name != device["name"]:
                    device_id_flag1 = True
                else:
                    self.addCleanup(self.detach_devices_attached, device_id)
                    device_id_flag2 = True
                if device_id_flag1 and device_id_flag2:
                    break
            break

        self.detach_devices_attached(device_id)

        # Check whether deleted device removed from topology info.
        topo_info = heketi_topology_info(self.heketi_client_node,
                                         self.heketi_server_url,
                                         json=True)
        match = False
        for device in topo_info["clusters"][0]["nodes"][0]["devices"]:
            if device_id == device["id"]:
                match = True
                break
        self.assertFalse(match, "Device:%s still present in topology"
                         % device_id)

        # Volume creation after device update
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url, volume_size,
                                        json=True)
        self.assertTrue(vol_info, ("Failed to create heketi volume of size %s"
                                   % str(volume_size)))
        self.addCleanup(self.volume_cleanup, vol_info['id'])
