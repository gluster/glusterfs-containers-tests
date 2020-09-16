from jsondiff import diff
try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json

import pytest
import re

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.command import cmd_run
from openshiftstoragelibs.exceptions import ExecutionError
from openshiftstoragelibs.heketi_ops import (
    heketi_topology_info,
    heketi_volume_create,
    heketi_volume_delete,
    heketi_volume_expand,
    hello_heketi,
)
from openshiftstoragelibs.openshift_ops import (
    oc_get_custom_resource,
    get_pod_name_from_dc,
    oc_delete,
    scale_dc_pod_amount_and_wait,
    wait_for_pod_be_ready,
    wait_for_resource_absence,
)


class TestRestartHeketi(BaseClass):

    def _heketi_pod_delete_cleanup(self):
        """Cleanup for deletion of heketi pod using force delete"""
        try:
            pod_name = get_pod_name_from_dc(
                self.ocp_master_node[0], self.heketi_dc_name)

            # Check if heketi pod name is ready state
            wait_for_pod_be_ready(self.ocp_master_node[0], pod_name, timeout=1)
        except ExecutionError:
            # Force delete and wait for new pod to come up
            oc_delete(self.ocp_master_node[0], 'pod', pod_name, is_force=True)
            wait_for_resource_absence(self.ocp_master_node[0], 'pod', pod_name)

            # Fetch heketi pod after force delete
            pod_name = get_pod_name_from_dc(
                self.ocp_master_node[0], self.heketi_dc_name)
            wait_for_pod_be_ready(self.ocp_master_node[0], pod_name)

    @pytest.mark.tier1
    def test_restart_heketi_pod(self):
        """Validate restarting heketi pod"""

        # create heketi volume
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url,
                                        size=1, json=True)
        self.assertTrue(vol_info, "Failed to create heketi volume of size 1")
        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol_info['id'], raise_on_error=False)
        topo_info = heketi_topology_info(self.heketi_client_node,
                                         self.heketi_server_url,
                                         json=True)

        # get heketi-pod name
        heketi_pod_name = get_pod_name_from_dc(self.ocp_master_node[0],
                                               self.heketi_dc_name)

        # delete heketi-pod (it restarts the pod)
        oc_delete(self.ocp_master_node[0], 'pod',
                  heketi_pod_name, collect_logs=self.heketi_logs_before_delete)
        wait_for_resource_absence(self.ocp_master_node[0],
                                  'pod', heketi_pod_name)

        # get new heketi-pod name
        heketi_pod_name = get_pod_name_from_dc(self.ocp_master_node[0],
                                               self.heketi_dc_name)
        wait_for_pod_be_ready(self.ocp_master_node[0],
                              heketi_pod_name)

        # check heketi server is running
        self.assertTrue(
            hello_heketi(self.heketi_client_node, self.heketi_server_url),
            "Heketi server %s is not alive" % self.heketi_server_url
        )

        # compare the topology
        new_topo_info = heketi_topology_info(self.heketi_client_node,
                                             self.heketi_server_url,
                                             json=True)
        self.assertEqual(new_topo_info, topo_info, "topology info is not same,"
                         " difference - %s" % diff(topo_info, new_topo_info))

        # create new volume
        vol_info = heketi_volume_create(self.heketi_client_node,
                                        self.heketi_server_url,
                                        size=2, json=True)
        self.assertTrue(vol_info, "Failed to create heketi volume of size 20")
        heketi_volume_delete(
            self.heketi_client_node, self.heketi_server_url, vol_info['id'])

    @pytest.mark.tier1
    def test_set_heketi_vol_size_and_brick_amount_limits(self):
        # Get Heketi secret name
        cmd_get_heketi_secret_name = (
            "oc get dc -n %s %s -o jsonpath='{.spec.template.spec.volumes"
            "[?(@.name==\"config\")].secret.secretName}'" % (
                self.storage_project_name, self.heketi_dc_name))
        heketi_secret_name = self.cmd_run(cmd_get_heketi_secret_name)

        # Read Heketi secret data
        self.node = self.ocp_master_node[0]
        heketi_secret_data_str_base64 = oc_get_custom_resource(
            self.node, "secret", r":.data.'heketi\.json'",  # noqa
            name=heketi_secret_name)[0]
        heketi_secret_data_str = self.cmd_run(
            "echo %s | base64 -d" % heketi_secret_data_str_base64)
        heketi_secret_data = json.loads(heketi_secret_data_str)

        # Update Heketi secret data
        brick_min_size_gb, brick_max_size_gb = 2, 4
        heketi_secret_data["glusterfs"].update({
            "brick_min_size_gb": brick_min_size_gb,
            "brick_max_size_gb": brick_max_size_gb,
            "max_bricks_per_volume": 3,
        })
        heketi_secret_data_patched = json.dumps(heketi_secret_data)
        heketi_secret_data_str_encoded = self.cmd_run(
            "echo '%s' |base64" % heketi_secret_data_patched).replace('\n', '')
        h_client, h_server = self.heketi_client_node, self.heketi_server_url
        try:
            # Patch Heketi secret
            cmd_patch_heketi_secret = (
                'oc patch secret -n %s %s -p '
                '"{\\"data\\": {\\"heketi.json\\": \\"%s\\"}}"'
            ) % (self.storage_project_name, heketi_secret_name, "%s")
            self.cmd_run(
                cmd_patch_heketi_secret % heketi_secret_data_str_encoded)

            # Recreate the Heketi pod to make it reuse updated configuration
            scale_dc_pod_amount_and_wait(self.node, self.heketi_dc_name, 0)
            scale_dc_pod_amount_and_wait(self.node, self.heketi_dc_name, 1)

            # Try to create too small and too big volumes
            # It must fail because allowed range is not satisfied
            for gb in (brick_min_size_gb - 1, brick_max_size_gb + 1):
                try:
                    vol_1 = heketi_volume_create(
                        h_client, h_server, size=gb, json=True)
                except AssertionError:
                    pass
                else:
                    self.addCleanup(
                        heketi_volume_delete, h_client, h_server, vol_1['id'])
                    self.assertFalse(
                        vol_1,
                        "Volume '%s' got unexpectedly created. Heketi server "
                        "configuration haven't made required effect." % (
                            vol_1.get('id', 'failed_to_get_heketi_vol_id')))

            # Create the smallest allowed volume
            vol_2 = heketi_volume_create(
                h_client, h_server, size=brick_min_size_gb, json=True)
            self.addCleanup(
                heketi_volume_delete, h_client, h_server, vol_2['id'])

            # Try to expand volume, it must fail due to the brick amount limit
            self.assertRaises(
                AssertionError, heketi_volume_expand, h_client,
                h_server, vol_2['id'], 2)

            # Create the largest allowed volume
            vol_3 = heketi_volume_create(
                h_client, h_server, size=brick_max_size_gb, json=True)
            heketi_volume_delete(h_client, h_server, vol_3['id'])
        finally:
            # Revert the Heketi configuration back
            self.cmd_run(
                cmd_patch_heketi_secret % heketi_secret_data_str_base64)
            scale_dc_pod_amount_and_wait(self.node, self.heketi_dc_name, 0)
            scale_dc_pod_amount_and_wait(self.node, self.heketi_dc_name, 1)

        # Create volume less than the old minimum limit
        vol_4 = heketi_volume_create(
            h_client, h_server, size=(brick_min_size_gb - 1), json=True)
        self.addCleanup(heketi_volume_delete, h_client, h_server, vol_4['id'])

        # Create volume bigger than the old maximum limit and expand it
        vol_5 = heketi_volume_create(
            h_client, h_server, size=(brick_max_size_gb + 1), json=True)
        self.addCleanup(heketi_volume_delete, h_client, h_server, vol_5['id'])
        heketi_volume_expand(h_client, h_server, vol_5['id'], 2)

    @pytest.mark.tier1
    def test_heketi_logs_after_heketi_pod_restart(self):

        h_node, h_server = self.heketi_client_node, self.heketi_server_url
        find_string_in_log = r"Started background pending operations cleaner"
        ocp_node = self.ocp_master_node[0]

        # Restart heketi pod
        heketi_pod_name = get_pod_name_from_dc(ocp_node, self.heketi_dc_name)
        oc_delete(
            ocp_node, 'pod', heketi_pod_name,
            collect_logs=self.heketi_logs_before_delete)
        self.addCleanup(self._heketi_pod_delete_cleanup)
        wait_for_resource_absence(ocp_node, 'pod', heketi_pod_name)
        heketi_pod_name = get_pod_name_from_dc(ocp_node, self.heketi_dc_name)
        wait_for_pod_be_ready(ocp_node, heketi_pod_name)
        self.assertTrue(
            hello_heketi(h_node, h_server),
            "Heketi server {} is not alive".format(h_server))

        # Collect logs after heketi pod restart
        cmd = "oc logs {}".format(heketi_pod_name)
        out = cmd_run(cmd, hostname=ocp_node)

        # Validate string is present in heketi logs
        pending_check = re.compile(find_string_in_log)
        entry_list = pending_check.findall(out)
        self.assertIsNotNone(
            entry_list, "Failed to find entries in heketi logs")

        for entry in entry_list:
            self.assertEqual(
                entry, find_string_in_log,
                "Failed to validate, Expected {}; Actual {}". format(
                    find_string_in_log, entry))
