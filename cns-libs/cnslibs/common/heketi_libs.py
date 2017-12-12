#!/usr/bin/python

"""
    Description: Library for heketi BaseClass.
"""

from glusto.core import Glusto as g
import unittest
import datetime
from collections import OrderedDict
from cnslibs.common.exceptions import ExecutionError, ConfigError
from cnslibs.common.heketi_ops import (setup_heketi_ssh_key,
                                       modify_heketi_executor,
                                       export_heketi_cli_server, hello_heketi)
from cnslibs.common.openshift_ops import (oc_login, switch_oc_project,
                                          get_ocp_gluster_pod_names)


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

        cls.deployment_type = g.config['cns']['deployment_type']
        cls.executor = g.config['cns']['executor']
        cls.executor_user = g.config['cns']['executor_user']
        cls.executor_port = g.config['cns']['executor_port']
        cls.heketi_client_node = (g.config['cns']['heketi_config']
                                  ['heketi_client_node'])
        cls.heketi_server_url = (g.config['cns']['heketi_config']
                                 ['heketi_server_url'])
        cls.gluster_servers = g.config['gluster_servers'].keys()
        cls.gluster_servers_info = g.config['gluster_servers']
        cls.topo_info = g.config['cns']['trusted_storage_pool_list']
        cls.heketi_ssh_key = g.config['cns']['heketi_config']['heketi_ssh_key']
        cls.heketi_config_file = (g.config['cns']['heketi_config']
                                  ['heketi_config_file'])
        cls.heketi_volume = {
            'size': g.config['cns']['heketi_volume']['size'],
            'name': g.config['cns']['heketi_volume']['name'],
            'expand_size': g.config['cns']['heketi_volume']['expand_size']}

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

        if cls.deployment_type == "cns":
            if not oc_login(cls.ocp_master_node, cls.cns_username,
                            cls.cns_password):
                raise ExecutionError("Failed to do oc login on node %s"
                                     % cls.ocp_master_node)

            if not switch_oc_project(cls.ocp_master_node,
                                     cls.cns_project_name):
                raise ExecutionError("Failed to switch oc project on node %s"
                                     % cls.ocp_master_node)

            cls.gluster_pods = get_ocp_gluster_pod_names(cls.ocp_master_node)
            g.pod_name = cls.gluster_pods[0]

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

    def tearDown(self):
        super(HeketiBaseClass, self).tearDown()
        msg = "Ending Test: %s : %s" % (self.id(), self.glustotest_run_id)
        g.log.info(msg)

    @classmethod
    def tearDownClass(cls):
        super(HeketiBaseClass, cls).tearDownClass()
        msg = "Teardownclass: %s : %s" % (cls.__name__, cls.glustotest_run_id)
        g.log.info(msg)


class HeketiClientSetupBaseClass(HeketiBaseClass):
    """
    Class to setup heketi ssh keys, modify heketi executor and to export
    heketi cli server.
    """

    @classmethod
    def setUpClass(cls):
        """
        setUpClass of HeketiClientSetupBaseClass
        """

        super(HeketiClientSetupBaseClass, cls).setUpClass()

        if cls.deployment_type == "crs_heketi_outside_openshift":
            # setup heketi ssh keys, if setup is not done already
            conn = g.rpyc_get_connection(cls.heketi_client_node, user="root")
            if conn is None:
                raise ConfigError("Failed to get rpyc connection of heketi "
                                  "node %s" % cls.heket_client_node)

            if conn.modules.os.path.exists(cls.heketi_ssh_key):
                g.log.info("Heketi ssh key is already generated")
            else:
                if not setup_heketi_ssh_key(cls.heketi_client_node,
                                            cls.gluster_servers,
                                            heketi_ssh_key=cls.heketi_ssh_key):
                    raise ConfigError("heketi ssh key setup failed on %s"
                                      % cls.heketi_client_node)

            g.rpyc_close_connection(cls.heketi_client_node, user="root")

            # Modifies heketi executor
            if not modify_heketi_executor(cls.heketi_client_node, cls.executor,
                                          cls.heketi_ssh_key,
                                          cls.executor_user,
                                          int(cls.executor_port)):
                raise ExecutionError("Failed to modify heketi executor on %s"
                                     % cls.heketi_client_node)

            # Exports heketi cli server
            heketi_url = cls.heketi_server_url
            if not export_heketi_cli_server(cls.heketi_client_node,
                                            heketi_cli_server=heketi_url):
                raise ExecutionError("Failed to export heketi cli server on %s"
                                     % cls.heketi_client_node)
