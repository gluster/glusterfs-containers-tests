from glusto.core import Glusto as g
import pytest

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.heketi_ops import (
    heketi_volume_create,
    heketi_volume_delete,
)
from openshiftstoragelibs import openshift_ops


class TestHeketiVolume(BaseClass):
    """Check volume bricks presence in fstab files on Gluster PODs."""

    def _find_bricks(self, brick_paths, present):
        """Make sure that vol brick paths either exist or not in fstab file."""
        oc_node = self.ocp_master_node[0]
        cmd = (
            'bash -c "'
            'if [ -d "%s" ]; then echo present; else echo absent; fi"')
        g_hosts = list(g.config.get("gluster_servers", {}).keys())
        results = []
        assertion_method = self.assertIn if present else self.assertNotIn
        for brick_path in brick_paths:
            for g_host in g_hosts:
                out = openshift_ops.cmd_run_on_gluster_pod_or_node(
                    oc_node, cmd % brick_path, gluster_node=g_host)
                results.append(out)
            assertion_method('present', results)

    @pytest.mark.tier1
    def test_validate_brick_paths_on_gluster_pods_or_nodes(self):
        """Validate brick paths after creation and deletion of a volume."""

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
        brick_paths = [p['path'] for p in vol["bricks"]]

        # Make sure that volume's brick paths exist in the fstab files
        self._find_bricks(brick_paths, present=True)

        # Delete heketi volume
        out = heketi_volume_delete(
            self.heketi_client_node, self.heketi_server_url, vol_id)
        self.assertTrue(out, "Failed to delete heketi volume %s" % vol_id)

        # Make sure that volume's brick paths are absent in the fstab file
        self._find_bricks(brick_paths, present=False)
