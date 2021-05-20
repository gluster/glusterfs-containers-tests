from __future__ import division

from glusto.core import Glusto as g
import mock
import pytest

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import command
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import openshift_ops
from openshiftstoragelibs import waiter


HEKETI_COMMAND_TIMEOUT = g.config.get("common", {}).get(
    "heketi_command_timeout", 120)
TIMEOUT_PREFIX = "timeout %s " % HEKETI_COMMAND_TIMEOUT


class TestVolumeDeleteTestCases(baseclass.BaseClass):
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

    def _heketi_pod_delete_cleanup(self, ocp_node):
        """Cleanup for deletion of heketi pod using force delete"""
        try:
            pod_name = openshift_ops.get_pod_name_from_dc(
                ocp_node, self.heketi_dc_name)

            # Check if heketi pod name is ready state
            openshift_ops.wait_for_pod_be_ready(ocp_node, pod_name, timeout=1)
        except exceptions.ExecutionError:
            # Force delete and wait for new pod to come up
            openshift_ops.oc_delete(ocp_node, 'pod', pod_name, is_force=True)
            openshift_ops.wait_for_resource_absence(
                self.ocp_master_node[0], 'pod', pod_name)

            # Fetch heketi pod after force delete
            pod_name = openshift_ops.get_pod_name_from_dc(
                ocp_node, self.heketi_dc_name)
            openshift_ops.wait_for_pod_be_ready(ocp_node, pod_name)

    @pytest.mark.tier1
    def test_delete_heketi_volume(self):
        """
        Method to test heketi volume deletion and whether it
        frees up used space after deletion
        """

        volume_info = heketi_ops.heketi_volume_create(
            self.heketi_client_node,
            self.heketi_server_url, 10, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete,
            self.heketi_client_node, self.heketi_server_url,
            volume_info["id"], raise_on_error=False)

        free_space_after_creation = self.get_free_space_summary_devices()

        heketi_ops.heketi_volume_delete(
            self.heketi_client_node, self.heketi_server_url,
            volume_info["id"])

        free_space_after_deletion = self.get_free_space_summary_devices()

        self.assertTrue(
            free_space_after_deletion > free_space_after_creation,
            "Free space is not reclaimed after deletion "
            "of %s" % volume_info["id"])

    @pytest.mark.tier1
    def test_delete_heketidb_volume(self):
        """Method to test heketidb volume deletion via heketi-cli."""
        for i in range(0, 2):
            volume_info = heketi_ops.heketi_volume_create(
                self.heketi_client_node, self.heketi_server_url,
                10, json=True)
            self.addCleanup(
                heketi_ops.heketi_volume_delete, self.heketi_client_node,
                self.heketi_server_url, volume_info["id"])

        volume_list_info = heketi_ops.heketi_volume_list(
            self.heketi_client_node,
            self.heketi_server_url, json=True)

        self.assertTrue(
            volume_list_info["volumes"], "Heketi volume list empty.")

        for volume_id in volume_list_info["volumes"]:
            volume_info = heketi_ops.heketi_volume_info(
                self.heketi_client_node, self.heketi_server_url,
                volume_id, json=True)

            if volume_info["name"] == "heketidbstorage":
                self.assertRaises(
                    AssertionError,
                    heketi_ops.heketi_volume_delete,
                    self.heketi_client_node, self.heketi_server_url, volume_id)
                return
        raise exceptions.ExecutionError(
            "Warning: heketidbstorage doesn't exist in list of volumes")

    @pytest.mark.tier2
    def test_heketi_server_stale_operations_during_heketi_pod_reboot(self):
        """
        Validate failed/stale entries in db and performs a cleanup
        of those entries
        """
        volume_id_list, async_obj, ocp_node = [], [], self.ocp_master_node[0]
        h_node, h_server = self.heketi_client_node, self.heketi_server_url
        for i in range(0, 8):
            volume_info = heketi_ops.heketi_volume_create(
                h_node, h_server, 1, json=True)
            volume_id_list.append(volume_info["id"])
            self.addCleanup(
                heketi_ops.heketi_volume_delete, h_node, h_server,
                volume_info["id"], raise_on_error=False)

        def run_async(cmd, hostname, raise_on_error=True):
            async_op = g.run_async(host=hostname, command=cmd)
            async_obj.append(async_op)
            return async_op

        # Temporary replace g.run with g.async_run in heketi_volume_delete
        # to be able to run it in background.
        for vol_id in volume_id_list:
            with mock.patch.object(command, 'cmd_run', side_effect=run_async):
                heketi_ops.heketi_volume_delete(h_node, h_server, vol_id)

        # Restart heketi pod and check pod is running
        heketi_pod_name = openshift_ops.get_pod_name_from_dc(
            ocp_node, self.heketi_dc_name)
        openshift_ops.oc_delete(
            ocp_node, 'pod', heketi_pod_name,
            collect_logs=self.heketi_logs_before_delete)
        self.addCleanup(self._heketi_pod_delete_cleanup, ocp_node)
        openshift_ops.wait_for_resource_absence(
            ocp_node, 'pod', heketi_pod_name)
        heketi_pod_name = openshift_ops.get_pod_name_from_dc(
            ocp_node, self.heketi_dc_name)
        openshift_ops.wait_for_pod_be_ready(ocp_node, heketi_pod_name)
        self.assertTrue(
            heketi_ops.hello_heketi(h_node, h_server),
            "Heketi server {} is not alive".format(h_server))

        # Wait for pending operations to get generate
        for w in waiter.Waiter(timeout=30, interval=3):
            h_db_check = heketi_ops.heketi_db_check(h_node, h_server)
            h_db_check_vol = h_db_check.get("volumes")
            h_db_check_bricks = h_db_check.get("bricks")
            if ((h_db_check_vol.get("pending"))
                    and (h_db_check_bricks.get("pending"))):
                break
        if w.expired:
            raise exceptions.ExecutionError(
                "No any pending operations found during volumes deletion "
                "volumes:{}, Bricks:{} ".format(
                    h_db_check_vol.get("pending"),
                    h_db_check_bricks.get("pending")))

        # Verify pending bricks are multiples of 3
        self.assertFalse(
            h_db_check_bricks.get("pending") % 3,
            "Expecting bricks pending count to be multiple of 3 but "
            "found {}".format(h_db_check_bricks.get("pending")))

        # Verify and Wait for pending operations to complete
        for w in waiter.Waiter(timeout=120, interval=10):
            h_db_check = heketi_ops.heketi_db_check(h_node, h_server)
            h_db_check_vol = h_db_check.get("volumes")
            h_db_check_bricks = h_db_check.get("bricks")
            if ((not h_db_check_bricks.get("pending"))
                    and (not h_db_check_vol.get("pending"))):
                break
        if w.expired:
            raise AssertionError(
                "Failed to delete volumes after 120 secs")
