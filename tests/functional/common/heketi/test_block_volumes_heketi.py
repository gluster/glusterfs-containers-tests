
from cnslibs.common.heketi_ops import (heketi_blockvolume_create,
                                       heketi_blockvolume_delete,
                                       heketi_blockvolume_list,
                                       heketi_volume_create,
                                       heketi_volume_delete
                                       )
from cnslibs.common.heketi_libs import HeketiBaseClass


class TestBlockVolumeOps(HeketiBaseClass):
    """
        Class to test heketi block volume deletion with and without block
        volumes existing, heketi block volume list, heketi block volume info
        and heketi block volume creation with name and block volumes creation
        after manually creating a Block Hosting volume.
        Test cases : CNS-[530,535,532,807]

    """

    def test_create_block_vol_after_host_vol_creation(self):
        """Test Case CNS-530 """
        block_host_create_info = heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 5,
            json=True, block=True)
        self.assertNotEqual(block_host_create_info, False,
                            "Block host volume creation failed")
        block_hosting_vol_id = block_host_create_info["id"]
        self.addCleanup(self.delete_volumes, block_hosting_vol_id)
        block_vol = heketi_blockvolume_create(
            self.heketi_client_node, self.heketi_server_url, 1, json=True)
        self.assertNotEqual(block_vol, False, "Block volume creation failed")
        self.addCleanup(self.delete_block_volumes, block_vol["id"])

    def test_block_host_volume_delete_without_block_volumes(self):
        """Test Case CNS-535 """
        block_host_create_info = heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 1, json=True,
            block=True)
        self.assertNotEqual(block_host_create_info, False,
                            "Block host volume creation failed")
        block_hosting_vol_id = block_host_create_info["id"]
        self.addCleanup(heketi_volume_delete, self.heketi_client_node,
                        self.heketi_server_url, block_hosting_vol_id,
                        raise_on_error=False)
        block_host_delete_output = heketi_volume_delete(
            self.heketi_client_node, self.heketi_server_url,
            block_hosting_vol_id, json=True)
        self.assertNotEqual(
            block_host_delete_output, False,
            "Block host volume delete failed, ID: %s" % block_hosting_vol_id)

    def test_block_volume_delete(self):
        """Test Case CNS-532 """
        block_vol = heketi_blockvolume_create(
            self.heketi_client_node, self.heketi_server_url, 1, json=True)
        self.assertNotEqual(block_vol, False,
                            "Block volume creation has failed")
        self.addCleanup(heketi_blockvolume_delete, self.heketi_client_node,
                        self.heketi_server_url, block_vol["id"],
                        raise_on_error=False)
        block_delete_output = heketi_blockvolume_delete(
            self.heketi_client_node, self.heketi_server_url,
            block_vol["id"], json=True)
        self.assertNotEqual(block_delete_output, False,
                            "deletion of block volume has failed, ID: %s"
                            % block_vol["id"])
        volume_list = heketi_blockvolume_list(
            self.heketi_client_node, self.heketi_server_url, json=True)
        self.assertNotIn(block_vol["id"], volume_list["blockvolumes"],
                         "The block volume has not been successfully deleted,"
                         " ID is %s" % block_vol["id"])

    def test_block_volume_list(self):
        """Test Case CNS-807 """
        created_vol_ids = []
        for count in range(3):
            block_vol = heketi_blockvolume_create(
                self.heketi_client_node, self.heketi_server_url,
                1, json=True)
            self.assertNotEqual(block_vol, False,
                                "Block volume creation has failed")
            self.addCleanup(self.delete_block_volumes,  block_vol["id"])
            created_vol_ids.append(block_vol["id"])
        volumes = heketi_blockvolume_list(
            self.heketi_client_node, self.heketi_server_url, json=True)
        existing_vol_ids = volumes.values()[0]
        for vol_id in created_vol_ids:
            self.assertIn(vol_id, existing_vol_ids,
                          "Block vol with '%s' ID is absent in the "
                          "list of block volumes." % vol_id)
