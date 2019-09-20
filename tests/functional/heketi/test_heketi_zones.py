import itertools
try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json

import ddt
from glusto.core import Glusto as g

from openshiftstoragelibs import baseclass
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import openshift_ops


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
        node_ids = heketi_ops.heketi_node_list(
            self.heketi_client_node, self.heketi_server_url)
        online_nodes = []
        for node_id in node_ids:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, self.heketi_server_url,
                node_id, json=True)
            if (node_info["state"] == "online"
                    and node_info['cluster'] == self.cluster_id):
                online_nodes.append(
                    (node_info["zone"], node_info['hostnames']['storage']))
        return online_nodes

    @ddt.data(
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
    )
    @ddt.unpack
    def test_check_pvc_placement_based_on_the_heketi_zones(
            self, zone_count, heketi_zone_checking, is_arbiter_vol):
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

        # Create storage class setting "user.heketi.zone-checking" option up
        prefix = "autotests-heketi-zones"
        sc_name = self.create_storage_class(
            sc_name_prefix=prefix, vol_name_prefix=prefix,
            is_arbiter_vol=is_arbiter_vol,
            heketi_zone_checking=heketi_zone_checking)

        # Create PVC using above storage class
        pvc_name = self.create_and_wait_for_pvc(
            pvc_name_prefix=prefix, sc_name=sc_name)

        # Validate brick placement if heketi zone checking is set to 'strict'
        if heketi_zone_checking == 'strict':
            brick_hosts_ips = openshift_ops.get_gluster_host_ips_by_pvc_name(
                self.node, pvc_name)
            placement_zones = set()
            for brick_host_ip in brick_hosts_ips:
                for node_zone, node_ips in online_nodes:
                    if brick_host_ip not in node_ips:
                        continue
                    placement_zones.add(node_zone)
                    break
            actual_zone_count = len(placement_zones)
            # NOTE(vponomar): '3' is default amount of volume replicas.
            # And it is just impossible to find more actual zones than amount
            # of replicas/bricks.
            expected_zone_count = 3 if zone_count > 3 else zone_count
            self.assertEqual(
                expected_zone_count, actual_zone_count,
                "PVC '%s' is incorrectly placed on the Heketi nodes "
                "according to their zones. Expected '%s' unique zones, got "
                "'%s'." % (pvc_name, zone_count, actual_zone_count))

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
