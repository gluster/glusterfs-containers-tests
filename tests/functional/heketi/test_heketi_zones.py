import itertools
try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json

import ddt
from glusto.core import Glusto as g
import pytest

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import command
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import openshift_ops
from openshiftstoragelibs import openshift_storage_libs
from openshiftstoragelibs import utils


@ddt.ddt
class TestHeketiZones(baseclass.BaseClass):

    @classmethod
    def setUpClass(cls):
        super(TestHeketiZones, cls).setUpClass()
        clusters = heketi_ops.heketi_cluster_list(
            cls.heketi_client_node, cls.heketi_server_url, json=True)
        cls.cluster_id = clusters['clusters'][0]
        cls.allow_heketi_zones_update = g.config.get("common", {}).get(
            "allow_heketi_zones_update", False)

    def setUp(self):
        super(TestHeketiZones, self).setUp()
        self.node = self.ocp_master_node[0]
        self.h_client = self.heketi_client_node
        self.h_server = self.heketi_server_url

    def _set_heketi_zones(self, unique_zones_amount=1):
        h = heketi_ops.cmd_run_on_heketi_pod
        heketi_db_dir = h("mount | grep heketidbstorage | awk '{print $3}'")

        # Copy the Heketi DB file to a backup file to be able to export it
        h("cp %s/heketi.db heketi_export.db" % heketi_db_dir)

        # Export the Heketi DB to a json file
        h("rm -f heketi_db_export.json")
        h("heketi db export --dbfile=heketi_export.db "
          "--jsonfile=heketi_db_export.json")
        h("rm heketi_export.db")

        # Read json file info
        heketi_db_original_str = h("cat heketi_db_export.json")
        heketi_db_data = json.loads(heketi_db_original_str)

        # Process the Heketi DB data
        node_count = len([
            n for n in heketi_db_data['nodeentries'].values()
            if (n['State'] == 'online'
                and n['Info']['cluster'] == self.cluster_id)
        ])
        if node_count < unique_zones_amount:
            self.skipTest(
                "Not enough amount of online nodes (%s) to set '%s' unique "
                "zones." % (node_count, unique_zones_amount))
        zones = itertools.cycle(range(unique_zones_amount))
        for n in heketi_db_data['nodeentries'].values():
            if (n['State'] == 'online'
                    and n['Info']['cluster'] == self.cluster_id):
                n['Info']['zone'] = next(zones) + 1

        # Schedule revert-back of the Heketi DB
        cmd_put_data_to_a_json_file = (
            "bash -ec \"\"\"echo '%s' > heketi_db_import.json\"\"\"")
        cmd_remove_heketi_import_db_file = "rm -f heketi_import.db"
        cmd_import_json_file = (
            "heketi db import --jsonfile=heketi_db_import.json "
            "--dbfile=heketi_import.db")
        cmd_apply_imported_data = (
            "mv -f heketi_import.db %s/heketi.db" % heketi_db_dir)
        cmd_remove_heketi_import_json_files = (
            "rm -f heketi_db_export.json heketi_db_import.json")

        for i in (1, 0):
            self.addCleanup(
                openshift_ops.scale_dc_pod_amount_and_wait,
                self.node, self.heketi_dc_name, i, wait_step=3)
        for cmd in (cmd_remove_heketi_import_json_files,
                    cmd_apply_imported_data,
                    cmd_import_json_file,
                    cmd_remove_heketi_import_db_file,
                    cmd_put_data_to_a_json_file % (
                        heketi_db_original_str.replace('"', '\\"'))):
            self.addCleanup(h, cmd)

        # Import the Heketi DB data
        heketi_db_updated_str = json.dumps(heketi_db_data).replace('"', '\\"')
        for cmd in (cmd_put_data_to_a_json_file % heketi_db_updated_str,
                    cmd_remove_heketi_import_db_file,
                    cmd_import_json_file,
                    cmd_apply_imported_data,
                    cmd_remove_heketi_import_json_files):
            h(cmd)

        # Apply changes by recreating the Heketi POD
        openshift_ops.scale_dc_pod_amount_and_wait(
            self.node, self.heketi_dc_name, 0, wait_step=3)
        openshift_ops.scale_dc_pod_amount_and_wait(
            self.node, self.heketi_dc_name, 1, wait_step=3)

        return heketi_db_data

    def _get_online_nodes(self):
        node_ids = heketi_ops.heketi_node_list(self.h_client, self.h_server)
        online_nodes = []
        for node_id in node_ids:
            node_info = heketi_ops.heketi_node_info(
                self.h_client, self.h_server, node_id, json=True)
            if (node_info["state"] == "online"
                    and node_info['cluster'] == self.cluster_id):
                online_nodes.append(
                    (node_info["zone"], node_info['hostnames']['storage']))
        return online_nodes

    def _check_for_available_zones(self, zone_count):
        # Check amount of available online heketi nodes
        online_nodes = self._get_online_nodes()
        node_count = len(online_nodes)

        # Check current amount of the Heketi zones
        actual_heketi_zones_amount = len(set([n[0] for n in online_nodes]))
        if zone_count != actual_heketi_zones_amount:
            if self.allow_heketi_zones_update:
                if zone_count > node_count:
                    self.skipTest(
                        "Not enough online nodes '%s' to test '%s' "
                        "unique Heketi zones." % (node_count, zone_count))
                heketi_db_data = self._set_heketi_zones(zone_count)
                online_nodes = [
                    (n['Info']['zone'], n['Info']['hostnames']['storage'])
                    for n in heketi_db_data['nodeentries'].values()
                ]
            else:
                self.skipTest(
                    "Required amount of the Heketi zones (%s < %s) is not "
                    "satisfied and 'common.allow_heketi_zones_update' config "
                    "option is set to 'False'." % (
                        zone_count, actual_heketi_zones_amount))

    def _validate_brick_placement_in_correct_zone_or_with_expand_pvc(
            self, heketi_zone_checking, pvc_name, zone_count, expand=False):
        online_nodes = self._get_online_nodes()

        for i in range(2):
            # Validate brick placement if heketi zone checking is 'strict'
            if heketi_zone_checking == 'strict':
                brick_hosts_ips = (
                    openshift_ops.get_gluster_host_ips_by_pvc_name(
                        self.node, pvc_name))
                placement_zones = {}
                for brick_host_ip in brick_hosts_ips:
                    for node_zone, node_ips in online_nodes:
                        if brick_host_ip not in node_ips:
                            continue
                        placement_zones[node_zone] = placement_zones.get(
                            node_zone, 0) + 1
                        break
                actual_zone_count = len(placement_zones)
                # NOTE(vponomar): '3' is default amount of volume replicas.
                # And it is just impossible to find more actual zones than
                # amount of replicas/bricks.
                brick_number = len(brick_hosts_ips)
                expected_zone_count = (
                    brick_number if brick_number < zone_count else zone_count)
                self.assertEqual(
                    expected_zone_count, actual_zone_count,
                    "PVC '%s' is incorrectly placed on the Heketi nodes "
                    "according to their zones. Expected '%s' unique zones, "
                    "got '%s'." % (pvc_name, zone_count, actual_zone_count))

            # Expand PVC if needed
            if expand:
                expand_size, expand = 2, False
                openshift_storage_libs.enable_pvc_resize(self.node)
                openshift_ops.resize_pvc(self.node, pvc_name, expand_size)
                openshift_ops.verify_pvc_size(self.node, pvc_name, expand_size)
            else:
                break

    def _check_heketi_pod_to_come_up_after_changing_env(self):
        # Wait for heketi pod get to restart
        heketi_pod = openshift_ops.get_pod_names_from_dc(
            self.node, self.heketi_dc_name)[0]
        openshift_ops.wait_for_resource_absence(self.node, "pod", heketi_pod)
        new_heketi_pod = openshift_ops.get_pod_names_from_dc(
            self.node, self.heketi_dc_name)[0]
        openshift_ops.wait_for_pod_be_ready(
            self.node, new_heketi_pod, wait_step=20)

    def _set_zone_check_env_in_heketi_dc(self, heketi_zone_checking):
        # Set env option zone checking in heketi dc
        set_env = (
            'HEKETI_POST_REQUEST_VOLUME_OPTIONS="user.heketi.zone-checking'
            ' {}"').format(heketi_zone_checking)
        unset_env, e_list = "HEKETI_POST_REQUEST_VOLUME_OPTIONS-", "--list"
        cmd_set_env = (
            "oc set env dc/{} {}".format(self.heketi_dc_name, set_env))
        cmd_unset_env = (
            "oc set env dc/{} {}".format(self.heketi_dc_name, unset_env))
        command.cmd_run(cmd_set_env, hostname=self.node)
        self._check_heketi_pod_to_come_up_after_changing_env()
        self.addCleanup(self._check_heketi_pod_to_come_up_after_changing_env)
        self.addCleanup(command.cmd_run, cmd_unset_env, hostname=self.node)

        # List all envs and validate if env is set successfully
        env = set_env.replace('"', '')
        cmd_list_env = (
            "oc set env dc/{} {}".format(self.heketi_dc_name, e_list))
        env_list = command.cmd_run(cmd_list_env, hostname=self.node)
        self.assertIn(env, env_list, "Failed to set env {}".format(env))

    def _create_sc_for_zone_check_tc(self, prefix, heketi_zone_checking,
                                     expand=False, is_arbiter_vol=False):
        # Create storage class setting "user.heketi.zone-checking" up
        sc_name = self.create_storage_class(
            sc_name_prefix=prefix, vol_name_prefix=prefix,
            allow_volume_expansion=expand, is_arbiter_vol=is_arbiter_vol,
            heketi_zone_checking=heketi_zone_checking)

        return sc_name

    @pytest.mark.tier1
    @ddt.data(
        (1, "strict", False),
        (1, "strict", True),
        (2, "strict", False),
        (2, "strict", True),
        (3, "strict", False),
        (3, "strict", True),
        (4, "strict", False),
        (4, "strict", True),
        (1, "none", False),
        (1, "none", True),
        (2, "none", False),
        (2, "none", True),
        (3, "none", False),
        (3, "none", True),
        # PVC expansion cases:
        (3, "strict", False, True),
        (3, "strict", True, True),
        (1, "none", False, True),
        (1, "none", True, True),
        (2, "none", False, True),
        (2, "none", True, True),
        (3, "none", False, True),
        (3, "none", True, True),
        # Cases with minimum 4 nodes
        (3, "strict", False, False, 4),
        (3, "strict", True, False, 4),
    )
    @ddt.unpack
    def test_check_pvc_placement_based_on_the_heketi_zones(
            self, zone_count, heketi_zone_checking, is_arbiter_vol,
            expand=False, node_count=None, is_create_sc=True,
            is_set_env=False):

        # Check amount of available online nodes
        if node_count:
            online_node_count = len(self._get_online_nodes())
            if online_node_count < node_count:
                self.skipTest(
                    'Available node count {} is less than expected node '
                    'count {}'.format(online_node_count, node_count))

        # Check amount of available online heketi zones
        self._check_for_available_zones(zone_count)

        # Create storage class if test case  requiures creation of sc
        prefix, sc_name = "autotests-heketi-zones", None
        if is_create_sc:
            sc_name = self._create_sc_for_zone_check_tc(
                prefix, heketi_zone_checking,
                expand=expand, is_arbiter_vol=is_arbiter_vol)

        # Set zone check env in heketi dc if test case requires that
        if is_set_env:
            self._set_zone_check_env_in_heketi_dc(heketi_zone_checking)

        # PVC creation should fail when zones are below 3 and check is strict
        if heketi_zone_checking == "strict" and zone_count < 3:
            self.assertRaises(
                exceptions.ExecutionError, self.create_and_wait_for_pvc,
                pvc_name_prefix=prefix, sc_name=sc_name, timeout=30)

        else:
            # Create PVC using above storage class
            pvc_name = self.create_and_wait_for_pvc(
                pvc_name_prefix=prefix, sc_name=sc_name)

            # Validate brick placement and expand if needed
            self._validate_brick_placement_in_correct_zone_or_with_expand_pvc(
                heketi_zone_checking, pvc_name, zone_count, expand=expand)

            # Make sure that gluster vol has appropriate option set
            vol_info = openshift_ops.get_gluster_vol_info_by_pvc_name(
                self.node, pvc_name)
            self.assertIn('user.heketi.zone-checking', vol_info['options'])
            self.assertEqual(
                vol_info['options']['user.heketi.zone-checking'],
                heketi_zone_checking)
            if is_arbiter_vol:
                self.assertIn('user.heketi.arbiter', vol_info['options'])
                self.assertEqual(
                    vol_info['options']['user.heketi.arbiter'], 'true')

            # Create app DC with the above PVC
            self.create_dc_with_pvc(pvc_name, timeout=120, wait_step=3)

    def _get_online_devices_and_nodes_with_zone(self):
        """
        This function returns the list of nodes and devices associated to zone

        Returns:
             dict: dict with zone, devices and nodes values
                   e.g.,
                    {    zone_1: {
                            "nodes": [
                                node1, node2, node3],
                            "devices": [
                                device1, device2]
                        },
                        zone_2: {
                            "nodes": [
                                node1, node2, node3],
                            "devices": [
                                device1, device2]
                        }
                    }
        """
        zone_devices_nodes = dict()
        topology_info = heketi_ops.heketi_topology_info(
            self.h_client, self.h_server, json=True)
        for cluster in topology_info['clusters']:
            for node in cluster['nodes']:
                if node['state'] == 'online':
                    if node['zone'] not in zone_devices_nodes:
                        zone_devices_nodes[node['zone']] = dict()

                    if 'nodes' not in zone_devices_nodes[node['zone']]:
                        zone_devices_nodes[node['zone']]['nodes'] = []

                    (zone_devices_nodes[node['zone']][
                        'nodes'].append(node['id']))

                for device in node['devices']:
                    if device['state'] == 'online':
                        if node['zone'] not in zone_devices_nodes:
                            zone_devices_nodes[node['zone']] = dict()

                        if 'devices' not in zone_devices_nodes[node['zone']]:
                            zone_devices_nodes[node['zone']]['devices'] = []

                        (zone_devices_nodes[
                            node['zone']]['devices'].append(device['id']))

        return zone_devices_nodes

    def _create_dcs_and_check_brick_placement(
            self, prefix, sc_name, heketi_zone_checking, zone_count):
        app_pods = []

        # Create multiple PVCs using storage class
        pvc_names = self.create_and_wait_for_pvcs(
            pvc_name_prefix=prefix, pvc_amount=5, sc_name=sc_name)

        # Create app dcs with I/O
        for pvc_name in pvc_names:
            app_dc = openshift_ops.oc_create_app_dc_with_io(
                self.node, pvc_name=pvc_name, dc_name_prefix=prefix)
            self.addCleanup(openshift_ops.oc_delete, self.node, 'dc', app_dc)

            # Get pod names
            pod_name = openshift_ops.get_pod_name_from_dc(self.node, app_dc)
            app_pods.append(pod_name)

        # Validate brick placement in heketi zones
        self._validate_brick_placement_in_correct_zone_or_with_expand_pvc(
            heketi_zone_checking, pvc_name, zone_count)

        return app_pods

    @pytest.mark.tier1
    @ddt.data(
        (3, False),
        (3, True),
        (4, True),
        (3, False, True),
        (3, True, True),
        (4, True, True),
    )
    @ddt.unpack
    def test_check_node_disable_based_on_heketi_zone(
            self, zone_count, is_disable_on_different_zone, is_set_env=False):
        """Validate node disable in different heketi zones"""
        expected_node_count, heketi_zone_checking, sc_name = 4, "strict", None
        prefix = "hzone-{}".format(utils.get_random_str())

        # Check amount of available online nodes
        online_node_count = len(self._get_online_nodes())
        if online_node_count < expected_node_count:
            self.skipTest(
                'Available node count {} is less than expected node '
                'count {}'.format(online_node_count, expected_node_count))

        # Check amount of available online heketi zones
        self._check_for_available_zones(zone_count)

        # Get the online devices and nodes w.r.t. to zone
        zone_devices_nodes = self._get_online_devices_and_nodes_with_zone()

        # Create sc or else directly set env to "strict" inside dc
        is_create_sc = not is_set_env
        if is_create_sc:
            self._create_sc_for_zone_check_tc(
                prefix, heketi_zone_checking)
        if is_set_env:
            self._set_zone_check_env_in_heketi_dc(heketi_zone_checking)

        # Choose a zone and node_id to disable the device
        for zone, nodes_and_devices in zone_devices_nodes.items():
            if zone_count == 3:
                # Select a node with a zone having multiple nodes in same
                # zone to cover the test cases disable node in same zone
                if len(nodes_and_devices['nodes']) > 1:
                    zone_with_disabled_node = zone
                    disabled_node = nodes_and_devices['nodes'][0]
                    break

            else:
                # Select node from any of the zones
                zone_with_disabled_node = zone
                disabled_node = nodes_and_devices['nodes'][0]
                break

        # Disable the selected node
        heketi_ops.heketi_node_disable(
            self.h_client, self.h_server, disabled_node)
        self.addCleanup(heketi_ops.heketi_node_enable, self.h_client,
                        self.h_server, disabled_node)

        # Create some DCs with PVCs and check brick placement in heketi zones
        pod_names = self._create_dcs_and_check_brick_placement(
            prefix, sc_name, heketi_zone_checking, zone_count)

        # Enable disabled node
        heketi_ops.heketi_node_enable(
            self.h_client, self.h_server, disabled_node)

        if is_disable_on_different_zone:
            # Select the new  node in a different zone
            for zone, nodes_and_devices in zone_devices_nodes.items():
                if zone != zone_with_disabled_node:
                    new_node_to_disable = nodes_and_devices['nodes'][0]
                    break

        else:
            # Select the new node in the same zone
            new_node_to_disable = zone_devices_nodes[
                zone_with_disabled_node]['nodes'][1]

        # Disable the newly selected node
        heketi_ops.heketi_node_disable(
            self.h_client, self.h_server, new_node_to_disable)
        self.addCleanup(heketi_ops.heketi_node_enable, self.h_client,
                        self.h_server, new_node_to_disable)

        # Verify if pods are in ready state
        for pod_name in pod_names:
            openshift_ops.wait_for_pod_be_ready(
                self.node, pod_name, timeout=5, wait_step=2)

    @pytest.mark.tier1
    @ddt.data(
        (3, False),
        (3, True),
        (4, True),
        (3, False, True),
        (3, True, True),
        (4, True, True),
    )
    @ddt.unpack
    def test_check_device_disable_based_on_heketi_zone(
            self, zone_count, is_disable_on_different_zone, is_set_env=False):
        """Validate device disable in different heketi zones"""
        online_device_count, expected_device_count = 0, 4
        expected_node_count, heketi_zone_checking, sc_name = 4, "strict", None
        prefix = "hzone-{}".format(utils.get_random_str())

        # Check amount of available online nodes
        online_node_count = len(self._get_online_nodes())
        if online_node_count < expected_node_count:
            self.skipTest(
                'Available node count {} is less than expected node '
                'count {}'.format(online_node_count, expected_node_count))

        # Check amount of available online heketi zones
        self._check_for_available_zones(zone_count)

        # Get the online devices and nodes w.r.t. to zone
        zone_devices_nodes = self._get_online_devices_and_nodes_with_zone()

        # Check amount of available online heketi devices
        for zone in zone_devices_nodes:
            online_device_count += len(
                zone_devices_nodes[zone]['devices'])
        if online_device_count < expected_device_count:
            self.skipTest(
                "Expected the heketi device count {} is greater than the "
                "available device count {}".format(
                    expected_device_count, online_device_count))

        # Create sc or else directly set env to "strict" inside dc
        is_create_sc = not is_set_env
        if is_create_sc:
            sc_name = self._create_sc_for_zone_check_tc(
                prefix, heketi_zone_checking)
        if is_set_env:
            self._set_zone_check_env_in_heketi_dc(heketi_zone_checking)

        # Choose a zone and device_id to disable the device
        for zone, nodes_and_devices in zone_devices_nodes.items():
            if zone_count == 3:
                # Select a device with a zone having multiple nodes in
                # same zone to cover the test cases "disable in same zone"
                if len(nodes_and_devices['devices']) > 1:
                    zone_with_disabled_device = zone
                    disabled_device = nodes_and_devices['devices'][0]
                    break

            else:
                # Select device from any of the zones
                zone_with_disabled_device = zone
                disabled_device = nodes_and_devices['devices'][0]
                break

        # Disable the selected device
        heketi_ops.heketi_device_disable(
            self.h_client, self.h_server, disabled_device)
        self.addCleanup(heketi_ops.heketi_device_enable, self.h_client,
                        self.h_server, disabled_device)

        # Create some DCs with PVCs and check brick placement in heketi zones
        pod_names = self._create_dcs_and_check_brick_placement(
            prefix, sc_name, heketi_zone_checking, zone_count)

        # Enable disabled device
        heketi_ops.heketi_device_enable(
            self.h_client, self.h_server, disabled_device)

        if is_disable_on_different_zone:
            # Select the new device in a different zone
            for zone, nodes_and_devices in zone_devices_nodes.items():
                if zone != zone_with_disabled_device:
                    new_device_to_disable = nodes_and_devices['devices'][0]
                    break

        else:
            # Select the new device in the same zone
            new_device_to_disable = zone_devices_nodes[
                zone_with_disabled_device]['devices'][1]

        # Disable the newly selected device
        heketi_ops.heketi_device_disable(
            self.h_client, self.h_server, new_device_to_disable)
        self.addCleanup(heketi_ops.heketi_device_enable, self.h_client,
                        self.h_server, new_device_to_disable)

        # Verify if pods are in ready state
        for pod_name in pod_names:
            openshift_ops.wait_for_pod_be_ready(
                self.node, pod_name, timeout=5, wait_step=2)
