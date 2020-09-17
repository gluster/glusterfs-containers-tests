import re

import ddt
from glustolibs.gluster import volume_ops
import pytest

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs import heketi_version
from openshiftstoragelibs import node_ops
from openshiftstoragelibs import openshift_ops
from openshiftstoragelibs import podcmd

G_BRICK_REGEX = r'^.*:.*\/(brick_.*)\/.*$'
H_BRICK_REGEX = r'^.*\/(brick_.*)$'


@ddt.ddt
class TestHeketiServerStateExamineGluster(BaseClass):

    def setUp(self):
        super(TestHeketiServerStateExamineGluster, self).setUp()
        self.node = self.ocp_master_node[0]
        version = heketi_version.get_heketi_version(self.heketi_client_node)
        if version < '8.0.0-7':
            self.skipTest("heketi-client package %s does not support server "
                          "state examine gluster" % version.v_str)

    @pytest.mark.tier2
    def test_volume_inconsistencies(self):
        # Examine Gluster cluster and Heketi that there is no inconsistencies
        out = heketi_ops.heketi_examine_gluster(
            self.heketi_client_node, self.heketi_server_url)
        if ("heketi volume list matches with volume list of all nodes"
                not in out['report']):
            self.skipTest(
                "heketi and Gluster are inconsistent to each other")

        # create volume
        vol = heketi_ops.heketi_volume_create(
            self.heketi_client_node, self.heketi_server_url, 1, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, vol['id'])

        # delete volume from gluster cluster directly
        openshift_ops.cmd_run_on_gluster_pod_or_node(
            self.node,
            "gluster vol stop %s force --mode=script" % vol['name'])
        openshift_ops.cmd_run_on_gluster_pod_or_node(
            self.node,
            "gluster vol delete %s --mode=script" % vol['name'])

        # verify that heketi is reporting inconsistencies
        out = heketi_ops.heketi_examine_gluster(
            self.heketi_client_node, self.heketi_server_url)
        self.assertNotIn(
            "heketi volume list matches with volume list of all nodes",
            out['report'])

    @pytest.mark.tier1
    @ddt.data('', 'block')
    def test_compare_real_vol_count_with_db_check_info(self, vol_type):
        """Validate file/block volumes using heketi db check."""

        # Create File/Block volume
        block_vol = getattr(heketi_ops, 'heketi_%svolume_create' % vol_type)(
            self.heketi_client_node, self.heketi_server_url, 1, json=True)
        self.addCleanup(
            getattr(heketi_ops, 'heketi_%svolume_delete' % vol_type),
            self.heketi_client_node, self.heketi_server_url, block_vol["id"])

        # Check Heketi DB using Heketi CLI
        db_result = heketi_ops.heketi_db_check(
            self.heketi_client_node, self.heketi_server_url)
        vol_count = db_result["%svolumes" % vol_type]["total"]
        vol_list = getattr(heketi_ops, 'heketi_%svolume_list' % vol_type)(
            self.heketi_client_node, self.heketi_server_url, json=True)
        count = len(vol_list["%svolumes" % vol_type])
        self.assertEqual(
            count, vol_count,
            "%svolume count doesn't match expected "
            "result %s, actual result is %s" % (vol_type, count, vol_count))

    @pytest.mark.tier1
    @ddt.data('device_count', 'node_count', 'bricks_count')
    def test_verify_db_check(self, count_type):
        """Validate the nodes, devices and bricks count in heketi db"""
        # Get the total number of  nodes, devices and bricks from db check
        db_info = heketi_ops.heketi_db_check(
            self.heketi_client_node, self.heketi_server_url)
        db_devices_count = db_info["devices"]["total"]
        db_nodes_count = db_info["nodes"]["total"]
        db_bricks_count = db_info["bricks"]["total"]

        # Get the total number of nodes, devices and bricks from topology info
        topology_info = heketi_ops.heketi_topology_info(
            self.heketi_client_node, self.heketi_server_url, json=True)
        topology_devices_count, topology_nodes_count = 0, 0
        topology_bricks_count = 0
        for cluster in topology_info['clusters']:
            topology_nodes_count += len(cluster['nodes'])

            if count_type == 'bricks_count' or 'device_count':
                for node in cluster['nodes']:
                    topology_devices_count += len(node['devices'])

                    if count_type == 'bricks_count':
                        for device in node['devices']:
                            topology_bricks_count += len(device['bricks'])

        # Compare the device count
        if count_type == 'device_count':
            msg = ("Devices count in db check {} and in topology info {} is "
                   "not same".format(db_devices_count, topology_devices_count))
            self.assertEqual(topology_devices_count, db_devices_count, msg)

        # Compare the node count
        elif count_type == 'node_count':
            msg = (
                "Nodes count in db check {} and nodes count in topology info "
                "{} is not same".format(db_nodes_count, topology_nodes_count))
            self.assertEqual(topology_nodes_count, db_nodes_count, msg)

        # Compare the bricks count
        elif count_type == 'bricks_count':
            msg = ("Bricks count in db check {} and bricks count in topology "
                   "info {} is not same".format(
                       db_bricks_count, topology_bricks_count))
            self.assertEqual(topology_bricks_count, db_bricks_count, msg)

    @pytest.mark.tier2
    @ddt.data('', 'block')
    def test_compare_heketi_volumes(self, vol_type):
        """Validate file/block volume count using heketi gluster examine"""
        # Create some file/block volumes
        vol_size = 1
        h_node, h_url = self.heketi_client_node, self.heketi_server_url

        for i in range(5):
            volume = eval(
                "heketi_ops.heketi_{}volume_create".format(vol_type))(
                h_node, h_url, vol_size, json=True)['id']
            self.addCleanup(
                eval("heketi_ops.heketi_{}volume_delete".format(vol_type)),
                h_node, h_url, volume)

        # Get the list of file/block volumes from heketi gluster examine
        out = heketi_ops.heketi_examine_gluster(
            self.heketi_client_node, self.heketi_server_url)
        examine_volumes, clusters = [], out['heketidb']['clusterentries']
        for cluster in clusters.values():
            examine_volumes += cluster['Info']['{}volumes'.format(vol_type)]

        # Get list of file/block volume from heketi blockvolume list
        heketi_volumes = eval(
            "heketi_ops.heketi_{}volume_list".format(vol_type))(
            h_node, h_url, json=True)['{}volumes'.format(vol_type)]

        # Compare file/block volume list
        self.assertEqual(
            heketi_volumes,
            examine_volumes,
            "Heketi {}volume list {} and list of blockvolumes in heketi "
            "gluster examine {} are not same".format(
                vol_type, heketi_volumes, examine_volumes))

    @pytest.mark.tier2
    def test_validate_report_after_node_poweroff(self):
        """Validate node report in heketi gluster examine after poweroff"""
        # Skip test if not able to connect to Cloud Provider
        try:
            node_ops.find_vm_name_by_ip_or_hostname(self.node)
        except (NotImplementedError, exceptions.ConfigError) as err:
            self.skipTest(err)

        # Power off one of the gluster node
        g_node = list(self.gluster_servers_info.values())[0]['manage']
        vm_name = node_ops.find_vm_name_by_ip_or_hostname(g_node)
        self.power_off_gluster_node_vm(vm_name, g_node)

        # Check the information of offline node in gluster examine output
        msg = "could not fetch data from node {}".format(g_node)
        examine_msg = heketi_ops.heketi_examine_gluster(
            self.heketi_client_node, self.heketi_server_url)['report'][1]
        self.assertEqual(
            examine_msg, msg, "Failed to generate error report for node {} in"
            " gluster examine output".format(g_node))

    @pytest.mark.tier1
    @podcmd.GlustoPod()
    def test_compare_brick_mount_status(self):
        """Compare the brick mount status from all nodes"""
        h_node_ip_list, h_mount_point, dev_paths = [], [], []
        g_nodes, brick_list = [], []
        cmd = "df -h {} | awk '{{print $6}}' | tail -1"
        h_node, h_url = self.heketi_client_node, self.heketi_server_url

        # Create a volume and fetch  the gluster volume info
        vol = heketi_ops.heketi_volume_create(h_node, h_url, 1, json=True)
        self.addCleanup(
            heketi_ops.heketi_volume_delete, h_node, h_url, vol['id'])
        vol_name = vol['name']
        g_vol_info = volume_ops.get_volume_info(
            'auto_get_gluster_endpoint', vol_name)
        self.assertTrue(
            g_vol_info, "Failed to get the volume info of {}".format(vol_name))

        # Fetch bricks details from gluster vol info
        for brick_detail in g_vol_info[vol_name]['bricks']['brick']:
            brick = re.findall(G_BRICK_REGEX, brick_detail['name'])
            self.assertTrue(
                brick, "Failed to get brick for volume {}".format(vol_name))
            brick_list.append(brick[0])

        # Extract node data from examine glusterfs
        h_examine_gluster = heketi_ops.heketi_examine_gluster(h_node, h_url)
        h_node_details = h_examine_gluster.get("clusters")[0].get('NodesData')
        self.assertTrue(
            h_node_details,
            "Failed to get the node details {}".format(h_node_details))
        h_brick_details = (h_node_details[0]['VolumeInfo']['Volumes']
                           ['VolumeList'][0]['Bricks']['BrickList'])

        # Fetch brick ip from examine glusterfs
        for i in range(len(h_brick_details)):
            node_bricks = h_brick_details[i]['Name']
            self.assertTrue(
                node_bricks,
                "Failed to get the node bricks data {}".format(node_bricks))
            h_node_ip_list.append(node_bricks.split(":")[0])

        # Extract mount point and mount status
        for h_node_detail in h_node_details:
            for node_detail in h_node_detail['BricksMountStatus']['Statuses']:
                # Fetch brick from heketi examine
                brick = re.findall(H_BRICK_REGEX, node_detail['MountPoint'])
                self.assertTrue(
                    brick,
                    "Failed to get the brick details from "
                    "{}".format(node_detail))

                # Check if the mount point is of new volume
                if brick[0] in brick_list:
                    dev_paths.append(node_detail['Device'])
                    h_mount_point.append(node_detail['MountPoint'])
                    h_mount_status = node_detail['Mounted']

                    # verify if the Mount status is True
                    self.assertTrue(
                        h_mount_status,
                        "Expecting mount status to be true but found"
                        " {}".format(h_mount_status))

        h_nodes_ids = heketi_ops.heketi_node_list(h_node, h_url)
        for node in h_nodes_ids:
            g_node = heketi_ops.heketi_node_info(
                h_node, h_url, node, json=True)
            g_nodes.append(g_node['hostnames']['manage'][0])

        # Validate mount point from heketi and gluster side
        for dev_path in dev_paths:
            # Fetch the mount path with respect to dev path
            for g_node in g_nodes:
                g_mount_point = openshift_ops.cmd_run_on_gluster_pod_or_node(
                    self.node, cmd.format(dev_path, g_node))
                if g_mount_point:
                    self.assertIn(
                        g_mount_point, h_mount_point,
                        "Failed to match mount point {} from gluster side and"
                        " {}".format(g_mount_point, h_mount_point))
