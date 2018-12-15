from glusto.core import Glusto as g

from cnslibs.common.heketi_libs import HeketiBaseClass
from cnslibs.common.heketi_ops import (heketi_volume_create,
                                       heketi_volume_delete)
from cnslibs.common.openshift_ops import get_ocp_gluster_pod_names


class TestHeketiVolume(HeketiBaseClass):
    """Check volume bricks presence in fstab files on Gluster PODs."""

    def _find_bricks_in_fstab_files(self, brick_paths, present):
        """Make sure that vol brick paths either exist or not in fstab file."""
        oc_node = self.ocp_master_nodes[0]
        gluster_pods = get_ocp_gluster_pod_names(oc_node)
        get_fstab_entries_cmd = "oc exec %s -- cat /var/lib/heketi/fstab"
        fstab_files_data = ''
        for gluster_pod in gluster_pods:
            ret, out, err = g.run(oc_node, get_fstab_entries_cmd % gluster_pod)
            self.assertEqual(
                ret, 0,
                "Failed to read fstab file on '%s' gluster POD. "
                "\nOut: %s \nError: %s" % (gluster_pod, out, err))
            fstab_files_data += '%s\n' % out
        assertion_method = self.assertIn if present else self.assertNotIn
        for brick_path in brick_paths:
            assertion_method(brick_path, fstab_files_data)

    def test_to_check_entry_in_fstab_file(self):
        """Test case CNS-778"""

        # Create heketi volume
        vol = heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, size=1, json=True)
        self.assertTrue(vol, "Failed to create 1Gb heketi volume")
        vol_id = vol["bricks"][0]["volume"]
        self.addCleanup(
            heketi_volume_delete,
            self.heketi_client_node, self.heketi_server_url, vol_id,
            raise_on_error=False)

        # Gather brick paths
        brick_paths = [p['path'].rstrip("/brick") for p in vol["bricks"]]

        # Make sure that volume's brick paths exist in the fstab files
        self._find_bricks_in_fstab_files(brick_paths, present=True)

        # Delete heketi volume
        out = heketi_volume_delete(
            self.heketi_client_node, self.heketi_server_url, vol_id)
        self.assertTrue(out, "Failed to delete heketi volume %s" % vol_id)

        # Make sure that volume's brick paths are absent in the fstab file
        self._find_bricks_in_fstab_files(brick_paths, present=False)
