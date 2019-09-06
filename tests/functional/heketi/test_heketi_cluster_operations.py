import ddt

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import heketi_ops


@ddt.ddt
class TestClusterOperationsTestCases(baseclass.BaseClass):
    """Class for heketi cluster creation related test cases"""

    @ddt.data("", "block", "file")
    def test_heketi_cluster_create(self, disable_volume_type):
        """Test heketi cluster creation"""
        kwargs = {"json": True}
        if disable_volume_type:
            kwargs.update({disable_volume_type: False})

        # Create heketi cluster
        cluster_info = heketi_ops.heketi_cluster_create(
            self.heketi_client_node, self.heketi_server_url, **kwargs)
        self.addCleanup(
            heketi_ops.heketi_cluster_delete, self.heketi_client_node,
            self.heketi_server_url, cluster_info["id"])

        # Validate block and file options
        err_msg = "Cluster with %s option, unexpectedly set to %s"
        if disable_volume_type:
            self.assertFalse(
                cluster_info[disable_volume_type],
                err_msg % (disable_volume_type, "True"))
        else:
            self.assertTrue(
                cluster_info["block"], err_msg % ("block", "False"))
            self.assertTrue(cluster_info["file"], err_msg % ("file", "False"))

    def test_heketi_cluster_list(self):
        """Test and validateheketi cluster list operation"""
        # Create heketi cluster
        cluster_info = heketi_ops.heketi_cluster_create(
            self.heketi_client_node, self.heketi_server_url, json=True)
        self.addCleanup(
            heketi_ops.heketi_cluster_delete, self.heketi_client_node,
            self.heketi_server_url, cluster_info["id"])

        # Get heketi cluster list and validate presence of newly
        # created cluster
        cluster_list = heketi_ops.heketi_cluster_list(
            self.heketi_client_node, self.heketi_server_url, json=True)
        err_msg = (
            "Cluster id %s not found in cluster list %s"
            % (cluster_info["id"], cluster_list["clusters"]))
        self.assertIn(cluster_info["id"], cluster_list["clusters"], err_msg)

    def test_heketi_cluster_info(self):
        """Test and validateheketi cluster info operation"""
        # Create heketi cluster
        cluster_info = heketi_ops.heketi_cluster_create(
            self.heketi_client_node, self.heketi_server_url, json=True)
        self.addCleanup(
            heketi_ops.heketi_cluster_delete, self.heketi_client_node,
            self.heketi_server_url, cluster_info["id"])

        # Get newly created heketi cluster info
        get_cluster_info = heketi_ops.heketi_cluster_info(
            self.heketi_client_node, self.heketi_server_url,
            cluster_info["id"], json=True)

        # Validate newly created heketi cluster info
        params = (
            ("id", cluster_info["id"]),
            ("block", True),
            ("file", True),
            ("blockvolumes", []),
            ("volumes", []),
            ("nodes", []))
        for param, value in params:
            self.assertEqual(get_cluster_info[param], value)

    def test_heketi_cluster_delete(self):
        """Test and validateheketi cluster delete operation"""
        # Create heketi cluster
        cluster_info = heketi_ops.heketi_cluster_create(
            self.heketi_client_node, self.heketi_server_url, json=True)

        # Delete newly created cluster
        heketi_ops.heketi_cluster_delete(
            self.heketi_client_node, self.heketi_server_url,
            cluster_info["id"])

        # Get heketi cluster list and check for absence of deleted cluster
        cluster_list = heketi_ops.heketi_cluster_list(
            self.heketi_client_node, self.heketi_server_url, json=True)
        err_msg = (
            "Cluster id %s was not expected in cluster list %s"
            % (cluster_info["id"], cluster_list["clusters"]))
        self.assertNotIn(cluster_info["id"], cluster_list["clusters"], err_msg)
