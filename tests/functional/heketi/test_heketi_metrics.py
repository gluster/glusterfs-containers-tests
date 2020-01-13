import pytest

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.heketi_ops import (
    get_heketi_metrics,
    heketi_cluster_info,
    heketi_cluster_list,
    heketi_topology_info,
    heketi_volume_create,
    heketi_volume_delete,
    heketi_volume_list,
)
from openshiftstoragelibs import heketi_version
from openshiftstoragelibs.openshift_ops import (
    get_pod_name_from_dc,
    scale_dc_pod_amount_and_wait,
    wait_for_pod_be_ready,
)


class TestHeketiMetrics(BaseClass):

    def setUp(self):
        super(TestHeketiMetrics, self).setUp()
        self.node = self.ocp_master_node[0]
        version = heketi_version.get_heketi_version(self.heketi_client_node)
        if version < '6.0.0-14':
            self.skipTest("heketi-client package %s does not support heketi "
                          "metrics functionality" % version.v_str)

    def verify_heketi_metrics_with_topology_info(self):
        topology = heketi_topology_info(
            self.heketi_client_node, self.heketi_server_url, json=True)

        metrics = get_heketi_metrics(
            self.heketi_client_node, self.heketi_server_url)

        self.assertTrue(topology)
        self.assertIn('clusters', list(topology.keys()))
        self.assertGreater(len(topology['clusters']), 0)

        self.assertTrue(metrics)
        self.assertGreater(len(metrics.keys()), 0)

        self.assertEqual(
            len(topology['clusters']), metrics['heketi_cluster_count'])

        for cluster in topology['clusters']:
            self.assertIn('nodes', list(cluster.keys()))
            self.assertGreater(len(cluster['nodes']), 0)

            cluster_id = cluster['id']

            cluster_ids = ([obj['cluster']
                           for obj in metrics['heketi_nodes_count']])
            self.assertIn(cluster_id, cluster_ids)
            for node_count in metrics['heketi_nodes_count']:
                if node_count['cluster'] == cluster_id:
                    self.assertEqual(
                        len(cluster['nodes']), node_count['value'])

            cluster_ids = ([obj['cluster']
                           for obj in metrics['heketi_volumes_count']])
            self.assertIn(cluster_id, cluster_ids)
            for vol_count in metrics['heketi_volumes_count']:
                if vol_count['cluster'] == cluster_id:
                    self.assertEqual(
                        len(cluster['volumes']), vol_count['value'])

            for node in cluster['nodes']:
                self.assertIn('devices', list(node.keys()))
                self.assertGreater(len(node['devices']), 0)

                hostname = node['hostnames']['manage'][0]

                cluster_ids = ([obj['cluster']
                               for obj in metrics['heketi_device_count']])
                self.assertIn(cluster_id, cluster_ids)
                hostnames = ([obj['hostname']
                             for obj in metrics['heketi_device_count']])
                self.assertIn(hostname, hostnames)
                for device_count in metrics['heketi_device_count']:
                    if (device_count['cluster'] == cluster_id
                            and device_count['hostname'] == hostname):
                        self.assertEqual(
                            len(node['devices']), device_count['value'])

                for device in node['devices']:
                    device_name = device['name']
                    device_size_t = device['storage']['total']
                    device_free_t = device['storage']['free']
                    device_used_t = device['storage']['used']

                    cluster_ids = ([obj['cluster']
                                   for obj in
                                   metrics['heketi_device_brick_count']])
                    self.assertIn(cluster_id, cluster_ids)
                    hostnames = ([obj['hostname']
                                 for obj in
                                 metrics['heketi_device_brick_count']])
                    self.assertIn(hostname, hostnames)
                    devices = ([obj['device']
                               for obj in
                               metrics['heketi_device_brick_count']])
                    self.assertIn(device_name, devices)
                    for brick_count in metrics['heketi_device_brick_count']:
                        if (brick_count['cluster'] == cluster_id
                                and brick_count['hostname'] == hostname
                                and brick_count['device'] == device_name):
                            self.assertEqual(
                                len(device['bricks']), brick_count['value'])

                    cluster_ids = ([obj['cluster']
                                   for obj in metrics['heketi_device_size']])
                    self.assertIn(cluster_id, cluster_ids)
                    hostnames = ([obj['hostname']
                                 for obj in metrics['heketi_device_size']])
                    self.assertIn(hostname, hostnames)
                    devices = ([obj['device']
                               for obj in metrics['heketi_device_size']])
                    self.assertIn(device_name, devices)
                    for device_size in metrics['heketi_device_size']:
                        if (device_size['cluster'] == cluster_id
                                and device_size['hostname'] == hostname
                                and device_size['device'] == device_name):
                            self.assertEqual(
                                device_size_t, device_size['value'])

                    cluster_ids = ([obj['cluster']
                                   for obj in metrics['heketi_device_free']])
                    self.assertIn(cluster_id, cluster_ids)
                    hostnames = ([obj['hostname']
                                 for obj in metrics['heketi_device_free']])
                    self.assertIn(hostname, hostnames)
                    devices = ([obj['device']
                               for obj in metrics['heketi_device_free']])
                    self.assertIn(device_name, devices)
                    for device_free in metrics['heketi_device_free']:
                        if (device_free['cluster'] == cluster_id
                                and device_free['hostname'] == hostname
                                and device_free['device'] == device_name):
                            self.assertEqual(
                                device_free_t, device_free['value'])

                    cluster_ids = ([obj['cluster']
                                   for obj in metrics['heketi_device_used']])
                    self.assertIn(cluster_id, cluster_ids)
                    hostnames = ([obj['hostname']
                                 for obj in metrics['heketi_device_used']])
                    self.assertIn(hostname, hostnames)
                    devices = ([obj['device']
                               for obj in metrics['heketi_device_used']])
                    self.assertIn(device_name, devices)
                    for device_used in metrics['heketi_device_used']:
                        if (device_used['cluster'] == cluster_id
                                and device_used['hostname'] == hostname
                                and device_used['device'] == device_name):
                            self.assertEqual(
                                device_used_t, device_used['value'])

    def verify_volume_count(self):
        metrics = get_heketi_metrics(
            self.heketi_client_node,
            self.heketi_server_url)
        self.assertTrue(metrics['heketi_volumes_count'])

        for vol_count in metrics['heketi_volumes_count']:
            self.assertTrue(vol_count['cluster'])
            cluster_info = heketi_cluster_info(
                self.heketi_client_node,
                self.heketi_server_url,
                vol_count['cluster'], json=True)
            self.assertEqual(vol_count['value'], len(cluster_info['volumes']))

    @pytest.mark.tier0
    def test_heketi_metrics_with_topology_info(self):
        """Validate heketi metrics generation"""
        self.verify_heketi_metrics_with_topology_info()

    @pytest.mark.tier0
    def test_heketi_metrics_heketipod_failure(self):
        """Validate heketi metrics after heketi pod failure"""
        scale_dc_pod_amount_and_wait(
            self.ocp_master_node[0], self.heketi_dc_name, pod_amount=0)
        self.addCleanup(
            scale_dc_pod_amount_and_wait, self.ocp_master_node[0],
            self.heketi_dc_name, pod_amount=1)

        # verify that metrics is not accessable when heketi pod is down
        with self.assertRaises(AssertionError):
            get_heketi_metrics(
                self.heketi_client_node,
                self.heketi_server_url,
                prometheus_format=True)

        scale_dc_pod_amount_and_wait(
            self.ocp_master_node[0], self.heketi_dc_name, pod_amount=1)

        pod_name = get_pod_name_from_dc(
            self.ocp_master_node[0], self.heketi_dc_name, self.heketi_dc_name)
        wait_for_pod_be_ready(self.ocp_master_node[0], pod_name, wait_step=5)

        for i in range(3):
            vol = heketi_volume_create(
                self.heketi_client_node,
                self.heketi_server_url, 1, json=True)

            self.assertTrue(vol)

            self.addCleanup(
                heketi_volume_delete,
                self.heketi_client_node,
                self.heketi_server_url,
                vol['id'],
                raise_on_error=False)

            vol_list = heketi_volume_list(
                self.heketi_client_node,
                self.heketi_server_url)

            self.assertIn(vol['id'], vol_list)

        self.verify_heketi_metrics_with_topology_info()

    @pytest.mark.tier0
    def test_heketi_metrics_validating_vol_count_on_vol_creation(self):
        """Validate heketi metrics VolumeCount after volume creation"""

        for i in range(3):
            # Create volume
            vol = heketi_volume_create(
                self.heketi_client_node,
                self.heketi_server_url, 1, json=True)
            self.assertTrue(vol)
            self.addCleanup(
                heketi_volume_delete,
                self.heketi_client_node,
                self.heketi_server_url,
                vol['id'],
                raise_on_error=False)

            vol_list = heketi_volume_list(
                self.heketi_client_node,
                self.heketi_server_url)

            self.assertIn(vol['id'], vol_list)

        self.verify_volume_count()

    @pytest.mark.tier0
    def test_heketi_metrics_validating_vol_count_on_vol_deletion(self):
        """Validate heketi metrics VolumeCount after volume deletion"""

        vol_list = []

        for i in range(3):
            # Create volume
            vol = heketi_volume_create(
                self.heketi_client_node,
                self.heketi_server_url, 1, json=True)

            self.assertTrue(vol)

            self.addCleanup(
                heketi_volume_delete,
                self.heketi_client_node,
                self.heketi_server_url,
                vol['id'],
                raise_on_error=False)

            volume_list = heketi_volume_list(
                self.heketi_client_node,
                self.heketi_server_url)

            self.assertIn(vol['id'], volume_list)
            vol_list.append(vol)

        for vol in vol_list:
            # delete volume
            heketi_volume_delete(
                self.heketi_client_node,
                self.heketi_server_url,
                vol['id'])
            volume_list = heketi_volume_list(
                self.heketi_client_node,
                self.heketi_server_url)
            self.assertNotIn(vol['id'], volume_list)
            self.verify_volume_count()

    @pytest.mark.tier0
    def test_heketi_metrics_validating_cluster_count(self):
        """Validate 'cluster count' in heketi metrics"""
        cluster_list = heketi_cluster_list(
            self.heketi_client_node, self.heketi_server_url, json=True)

        self.assertTrue(cluster_list)
        self.assertTrue(cluster_list.get('clusters'))

        metrics = get_heketi_metrics(
            self.heketi_client_node, self.heketi_server_url)

        self.assertTrue(metrics)
        self.assertTrue(metrics.get('heketi_cluster_count'))

        self.assertEqual(
            len(cluster_list['clusters']), metrics['heketi_cluster_count'])

    @pytest.mark.tier0
    def test_heketi_metrics_validating_existing_node_count(self):
        """Validate existing 'node count' in heketi metrics"""
        metrics = get_heketi_metrics(
            self.heketi_client_node, self.heketi_server_url)

        self.assertTrue(metrics)
        self.assertTrue(metrics.get('heketi_nodes_count'))

        for cluster in metrics['heketi_nodes_count']:
            cluster_info = heketi_cluster_info(
                self.heketi_client_node, self.heketi_server_url,
                cluster['cluster'], json=True)

            self.assertTrue(cluster_info)
            self.assertTrue(cluster_info.get('nodes'))

            self.assertEqual(len(cluster_info['nodes']), cluster['value'])
