from collections import OrderedDict
import datetime
import unittest

from glusto.core import Glusto as g

from cnslibs.common.exceptions import ExecutionError, ConfigError
from cnslibs.common.heketi_ops import (hello_heketi,
                                       heketi_volume_delete,
                                       heketi_blockvolume_delete)
from cnslibs.common import openshift_ops


class HeketiBaseClass(unittest.TestCase):
    """
    This class initializes heketi config variables, constructs topology info
    dictionary and check if heketi server is alive.
    """

    @classmethod
    def setUpClass(cls):
        """
        setUpClass of HeketiBaseClass
        """

        super(HeketiBaseClass, cls).setUpClass()

        # Initializes heketi config variables
        cls.cns_username = g.config['cns']['setup']['cns_username']
        cls.cns_password = g.config['cns']['setup']['cns_password']
        cls.cns_project_name = g.config['cns']['setup']['cns_project_name']
        cls.ocp_master_nodes = g.config['ocp_servers']['master'].keys()
        cls.ocp_master_node = cls.ocp_master_nodes[0]

        heketi_config = g.config['cns']['heketi_config']
        cls.heketi_dc_name = heketi_config['heketi_dc_name']
        cls.heketi_service_name = heketi_config['heketi_service_name']
        cls.heketi_client_node = heketi_config['heketi_client_node']
        cls.heketi_server_url = heketi_config['heketi_server_url']
        cls.heketi_cli_user = heketi_config['heketi_cli_user']
        cls.heketi_cli_key = heketi_config['heketi_cli_key']
        cls.gluster_servers = g.config['gluster_servers'].keys()
        cls.gluster_servers_info = g.config['gluster_servers']
        cls.topo_info = g.config['cns']['trusted_storage_pool_list']

        # Constructs topology info dictionary
        cls.topology_info = OrderedDict()
        for i in range(len(cls.topo_info)):
            cluster = 'cluster' + str(i + 1)
            cls.topology_info[cluster] = OrderedDict()
            for index, node in enumerate(cls.topo_info[i]):
                node_name = 'gluster_node' + str(index + 1)
                cls.topology_info[cluster][node_name] = {
                    'manage': cls.gluster_servers_info[node]['manage'],
                    'storage': cls.gluster_servers_info[node]['storage'],
                    'zone': cls.gluster_servers_info[node]['zone'],
                    'devices': cls.gluster_servers_info[node]['devices'],
                }

        # Checks if heketi server is alive
        if not hello_heketi(cls.heketi_client_node, cls.heketi_server_url):
            raise ConfigError("Heketi server %s is not alive"
                              % cls.heketi_server_url)

        # Switch to the storage project
        if not openshift_ops.oc_login(
                cls.ocp_master_node, cls.cns_username, cls.cns_password):
            raise ExecutionError("Failed to do oc login on node %s"
                                 % cls.ocp_master_node)
        if not openshift_ops.switch_oc_project(
                cls.ocp_master_node, cls.cns_project_name):
            raise ExecutionError("Failed to switch oc project on node %s"
                                 % cls.ocp_master_node)

        # Have a unique string to recognize the test run for logging
        if 'glustotest_run_id' not in g.config:
            g.config['glustotest_run_id'] = (
                datetime.datetime.now().strftime('%H_%M_%d_%m_%Y'))
        cls.glustotest_run_id = g.config['glustotest_run_id']
        msg = "Setupclass: %s : %s" % (cls.__name__, cls.glustotest_run_id)
        g.log.info(msg)

    def setUp(self):
        super(HeketiBaseClass, self).setUp()
        msg = "Starting Test : %s : %s" % (self.id(), self.glustotest_run_id)
        g.log.info(msg)

    def delete_volumes(self, volume_ids):
        """
        Delete volumes by their IDs and raise error with list of failures
        Input: (volume_ids) It can be a single volume ID
        or a list of volume IDs
        """
        errored_ids = []

        if not isinstance(volume_ids, (list, set, tuple)):
            volume_ids = [volume_ids]

        for volume_id in volume_ids:
            out = heketi_volume_delete(
                self.heketi_client_node, self.heketi_server_url, volume_id)
            output_str = 'Volume %s deleted' % volume_id
            if output_str not in out:
                errored_ids.append(volume_id)

        if errored_ids:
            raise ExecutionError(
                "Failed to delete following heketi volumes: "
                "%s" % ',\n'.join(errored_ids))

    def delete_block_volumes(self, volume_ids):
        """
        Delete block volumes by their volume IDs and raise an error on failures
        Args:
            volume_ids (str) : Volume ID of the block volume
        """
        if not isinstance(volume_ids, (list, set, tuple)):
            volume_ids = [volume_ids]

        fail = False
        for volume_id in volume_ids:
            block_out = heketi_blockvolume_delete(
                self.heketi_client_node, self.heketi_server_url, volume_id)
            if block_out is False:
                g.log.error("Block volume delete failed %s " % volume_id)
                fail = True
        self.assertFalse(fail, "Failed to delete blockvolumes")

    def tearDown(self):
        super(HeketiBaseClass, self).tearDown()
        msg = "Ending Test: %s : %s" % (self.id(), self.glustotest_run_id)
        g.log.info(msg)

    @classmethod
    def tearDownClass(cls):
        super(HeketiBaseClass, cls).tearDownClass()
        msg = "Teardownclass: %s : %s" % (cls.__name__, cls.glustotest_run_id)
        g.log.info(msg)
