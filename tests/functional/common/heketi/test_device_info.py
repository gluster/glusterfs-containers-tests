from cnslibs.common import heketi_libs
from cnslibs.common import heketi_ops


class TestHeketiDeviceInfo(heketi_libs.HeketiBaseClass):

    def test_heketi_devices_info_verification(self):
        """Validate whether device related information is displayed"""

        # Get devices from topology info
        devices_from_topology = {}
        topology_info = heketi_ops.heketi_topology_info(
            self.heketi_client_node, self.heketi_server_url, json=True)
        self.assertTrue(topology_info)
        self.assertIn('clusters', list(topology_info.keys()))
        self.assertGreater(len(topology_info['clusters']), 0)
        for cluster in topology_info['clusters']:
            self.assertIn('nodes', list(cluster.keys()))
            self.assertGreater(len(cluster['nodes']), 0)
            for node in cluster['nodes']:
                self.assertIn('devices', list(node.keys()))
                self.assertGreater(len(node['devices']), 0)
                for device in node['devices']:
                    # Expected keys are state, storage, id, name and bricks.
                    self.assertIn('id', list(device.keys()))
                    devices_from_topology[device['id']] = device

        # Get devices info and make sure data are consistent and complete
        for device_id, device_from_t_info in devices_from_topology.items():
            device_info = heketi_ops.heketi_device_info(
                self.heketi_client_node, self.heketi_server_url,
                device_id, json=True)
            self.assertTrue(device_info)

            # Verify 'id', 'name', 'state' and 'storage' data
            for key in ('id', 'name', 'state', 'storage', 'bricks'):
                self.assertIn(key, list(device_from_t_info.keys()))
                self.assertIn(key, list(device_info.keys()))
            self.assertEqual(device_info['id'], device_from_t_info['id'])
            self.assertEqual(device_info['name'], device_from_t_info['name'])
            self.assertEqual(device_info['state'], device_from_t_info['state'])
            device_info_storage = device_info['storage']
            device_from_t_info_storage = device_from_t_info['storage']
            device_info_storage_keys = list(device_info_storage.keys())
            device_from_t_info_storage_keys = list(
                device_from_t_info_storage.keys())
            for key in ('total', 'used', 'free'):
                self.assertIn(key, device_info_storage_keys)
                self.assertIn(key, device_from_t_info_storage_keys)
                self.assertEqual(
                    device_info_storage[key], device_from_t_info_storage[key])
                self.assertIsInstance(device_info_storage[key], int)
                self.assertGreater(device_info_storage[key], -1)

            # Verify 'bricks' data
            self.assertEqual(
                len(device_info['bricks']), len(device_from_t_info['bricks']))
            brick_match_count = 0
            for brick in device_info['bricks']:
                for brick_from_t in device_from_t_info['bricks']:
                    if brick_from_t['id'] != brick['id']:
                        continue
                    brick_match_count += 1
                    brick_from_t_keys = list(brick_from_t.keys())
                    brick_keys = list(brick.keys())
                    for key in ('device', 'volume', 'size', 'path', 'id',
                                'node'):
                        self.assertIn(key, brick_from_t_keys)
                        self.assertIn(key, brick_keys)
                        self.assertEqual(brick[key], brick_from_t[key])
            self.assertEqual(brick_match_count, len(device_info['bricks']))
