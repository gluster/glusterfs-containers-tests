from __future__ import division

from cnslibs.common.exceptions import ExecutionError
from cnslibs.common.heketi_libs import HeketiClientSetupBaseClass
from cnslibs.common import heketi_ops


class TestVolumeDeleteTestCases(HeketiClientSetupBaseClass):
    """
    Class for volume deletion related test cases

    """

    def get_free_space_summary_devices(self):
        """
        Calculates free space across all devices
        """

        heketi_node_id_list = heketi_ops.heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)

        total_free_space = 0
        for node_id in heketi_node_id_list:
            node_info_dict = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            for device in node_info_dict["devices"]:
                total_free_space += (device["storage"]
                                     ["free"] / (1024 ** 2))

        return total_free_space

    def test_delete_heketi_volume(self):
        """
        Method to test heketi volume deletion and whether it
        frees up used space after deletion
        """

        creation_output_dict = heketi_ops.heketi_volume_create(
            self.heketi_client_node,
            self.heketi_server_url, 10, json=True)

        self.assertNotEqual(creation_output_dict, False,
                            "Volume creation failed")

        volume_id = creation_output_dict["name"].strip().split("_")[1]
        free_space_after_creation = self.get_free_space_summary_devices()

        deletion_output = heketi_ops.heketi_volume_delete(
            self.heketi_client_node, self.heketi_server_url, volume_id)

        self.assertNotEqual(deletion_output, False,
                            "Deletion of volume failed, id: %s" % volume_id)

        free_space_after_deletion = self.get_free_space_summary_devices()

        self.assertTrue(
            free_space_after_deletion > free_space_after_creation,
            "Free space is not reclaimed after deletion of %s" % volume_id)

    def test_delete_heketidb_volume(self):
        """
        Method to test heketidb volume deletion via heketi-cli
        """
        volume_id_list = []
        heketidbexists = False
        msg = "Error: Cannot delete volume containing the Heketi database"

        for i in range(0, 2):
            volume_info = heketi_ops.heketi_volume_create(
                self.heketi_client_node, self.heketi_server_url,
                10, json=True)
            self.assertNotEqual(volume_info, False, "Volume creation failed")
            volume_id_list.append(volume_info["id"])

        self.addCleanup(self.delete_volumes, volume_id_list)

        volume_list_info = heketi_ops.heketi_volume_list(
            self.heketi_client_node,
            self.heketi_server_url, json=True)

        self.assertNotEqual(volume_list_info, False,
                            "Heketi volume list command failed")

        if volume_list_info["volumes"] == []:
            raise ExecutionError("Heketi volume list empty")

        for volume_id in volume_list_info["volumes"]:
            volume_info = heketi_ops.heketi_volume_info(
                self.heketi_client_node, self.heketi_server_url,
                volume_id, json=True)

            if volume_info["name"] == "heketidbstorage":
                heketidbexists = True
                delete_ret, delete_output, delete_error = (
                    heketi_ops.heketi_volume_delete(
                        self.heketi_client_node,
                        self.heketi_server_url, volume_id,
                        raw_cli_output=True))

                self.assertNotEqual(delete_ret, 0, "Return code not 0")
                self.assertEqual(
                    delete_error.strip(), msg,
                    "Invalid reason for heketidb deletion failure")

        if not heketidbexists:
            raise ExecutionError(
                "Warning: heketidbstorage doesn't exist in list of volumes")
