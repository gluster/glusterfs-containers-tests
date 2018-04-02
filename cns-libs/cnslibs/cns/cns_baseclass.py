from collections import OrderedDict
from cnslibs.common import podcmd
from cnslibs.common.exceptions import (
    ConfigError,
    ExecutionError)
from cnslibs.common.heketi_ops import (
    heketi_create_topology,
    hello_heketi)
from cnslibs.common.cns_libs import (
    edit_iptables_cns,
    enable_kernel_module,
    edit_master_config_file,
    edit_multipath_conf_file,
    setup_router,
    start_rpcbind_service,
    start_gluster_blockd_service,
    update_nameserver_resolv_conf,
    update_router_ip_dnsmasq_conf)
from cnslibs.common.docker_libs import (
    docker_add_registry,
    docker_insecure_registry)
from cnslibs.common.openshift_ops import (
    create_namespace,
    get_ocp_gluster_pod_names,
    oc_rsh)
import datetime
from glusto.core import Glusto as g
import unittest


class CnsBaseClass(unittest.TestCase):
    '''
     This class reads the config for variable values that will be used in
     CNS tests.
    '''
    @classmethod
    def setUpClass(cls):
        '''
         Initialize all the variables necessary for testing CNS
        '''
        super(CnsBaseClass, cls).setUpClass()
        g.log.info("cnsbaseclass")
        # Initializes OCP config variables
        cls.ocp_servers_info = g.config['ocp_servers']
        cls.ocp_master_node = g.config['ocp_servers']['master'].keys()
        cls.ocp_master_node_info = g.config['ocp_servers']['master']
        cls.ocp_client = g.config['ocp_servers']['client'].keys()
        cls.ocp_client_info = g.config['ocp_servers']['client']
        cls.ocp_nodes = g.config['ocp_servers']['nodes'].keys()
        cls.ocp_nodes_info = g.config['ocp_servers']['nodes']
        cls.ocp_all_nodes = cls.ocp_nodes + cls.ocp_master_node

        # Initializes CNS config variables
        cls.cns_username = g.config['cns']['setup']['cns_username']
        cls.cns_password = g.config['cns']['setup']['cns_password']
        cls.cns_project_name = g.config['cns']['setup']['cns_project_name']
        cls.add_registry = g.config['cns']['setup']['add_registry']
        cls.insecure_registry = g.config['cns']['setup']['insecure_registry']
        cls.routingconfig_subdomain = (g.config['cns']['setup']
                                       ['routing_config'])
        cls.deployment_type = g.config['cns']['deployment_type']
        cls.executor = g.config['cns']['executor']
        cls.executor_user = g.config['cns']['executor_user']
        cls.executor_port = g.config['cns']['executor_port']

        # Initializes heketi config variables
        heketi_config = g.config['cns']['heketi_config']
        cls.heketi_service_name = heketi_config['heketi_service_name']
        cls.heketi_client_node = heketi_config['heketi_client_node']
        cls.heketi_server_url = heketi_config['heketi_server_url']
        cls.heketi_cli_user = heketi_config['heketi_cli_user']
        cls.heketi_cli_key = heketi_config['heketi_cli_key']
        cls.heketi_ssh_key = heketi_config['heketi_ssh_key']
        cls.heketi_config_file = heketi_config['heketi_config_file']
        cls.heketi_volume = {}
        cls.heketi_volume['size'] = g.config['cns']['heketi_volume']['size']
        cls.heketi_volume['name'] = g.config['cns']['heketi_volume']['name']
        cls.heketi_volume['expand_size'] = (
            g.config['cns']['heketi_volume']['expand_size'])

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

        cls.cns_storage_class = (g.config['cns']['dynamic_provisioning']
                                 ['storage_classes'])
        cls.cns_secret = g.config['cns']['dynamic_provisioning']['secrets']
        cls.cns_pvc_size_number_dict = (g.config['cns']
                                        ['dynamic_provisioning']
                                        ['pvc_size_number'])
        cls.start_count_for_pvc = (g.config['cns']['dynamic_provisioning']
                                   ['start_count_for_pvc'])
        cls.app_pvc_count_dict = (g.config['cns']['dynamic_provisioning']
                                  ['app_pvc_count_dict'])
        cmd = "echo -n %s | base64" % cls.heketi_cli_key
        ret, out, err = g.run(cls.ocp_master_node[0], cmd, "root")
        if ret != 0:
            raise ExecutionError("failed to execute cmd %s on %s out: %s "
                                 "err: %s" % (
                                     cmd, cls.ocp_master_node[0], out, err))
        cls.secret_data_key = out.strip()

        cmd = 'oc project %s' % cls.cns_project_name
        ret, out, err = g.run(cls.ocp_client[0], cmd, "root")
        if ret != 0:
            raise ExecutionError("failed to execute cmd %s on %s out: "
                                 "%s err: %s" % (
                                     cmd, cls.ocp_client[0], out, err))

        if 'glustotest_run_id' not in g.config:
            g.config['glustotest_run_id'] = (
                datetime.datetime.now().strftime('%H_%M_%d_%m_%Y'))
        cls.glustotest_run_id = g.config['glustotest_run_id']
        msg = "Setupclass: %s : %s" % (cls.__name__, cls.glustotest_run_id)
        g.log.info(msg)

    def setUp(self):
        super(CnsBaseClass, self).setUp()
        msg = "Starting Test : %s : %s" % (self.id(), self.glustotest_run_id)
        g.log.info(msg)

    def tearDown(self):
        super(CnsBaseClass, self).tearDown()
        msg = "Ending Test: %s : %s" % (self.id(), self.glustotest_run_id)
        g.log.info(msg)

    @classmethod
    def tearDownClass(cls):
        super(CnsBaseClass, cls).tearDownClass()
        msg = "Teardownclass: %s : %s" % (cls.__name__, cls.glustotest_run_id)
        g.log.info(msg)


class CnsSetupBaseClass(CnsBaseClass):
    '''
     This class does the basic CNS setup
    '''
    @classmethod
    def setUpClass(cls):
        '''
         CNS setup
        '''
        super(CnsSetupBaseClass, cls).setUpClass()
        for node in cls.ocp_all_nodes:
            for mod_name in ('dm_thin_pool', 'dm_multipath',
                    'target_core_user'):
                if not enable_kernel_module(node, mod_name):
                    raise ExecutionError(
                        "failed to enable kernel module %s" % mod_name)
            if not start_rpcbind_service(node):
                raise ExecutionError("failed to start rpcbind service")
            if not edit_iptables_cns(node):
                raise ExecutionError("failed to edit iptables")
        cmd = "systemctl reload iptables"
        cmd_results = g.run_parallel(cls.ocp_all_nodes, cmd, "root")
        for node, ret_values in cmd_results.iteritems():
            ret, out, err = ret_values
            if ret != 0:
                raise ExecutionError("failed to execute cmd %s on %s out: "
                                     "%s err: %s" % (
                                         cmd, node, out, err))
        if not edit_master_config_file(cls.ocp_master_node[0],
                                       cls.routingconfig_subdomain):
            raise ExecutionError("failed to edit master.conf file")
        cmd = "systemctl restart atomic-openshift-node.service"
        cmd_results = g.run_parallel(cls.ocp_nodes, cmd, "root")
        for node, ret_values in cmd_results.iteritems():
            ret, out, err = ret_values
            if ret != 0:
                raise ExecutionError("failed to execute cmd %s on %s out: "
                                     "%s err: %s" % (
                                         cmd, node, out, err))
        cmd = ("systemctl restart  atomic-openshift-master-api "
               "atomic-openshift-master-controllers")
        ret, out, err = g.run(cls.ocp_master_node[0], cmd, "root")
        if ret != 0:
            raise ExecutionError("failed to execute cmd %s on %s out: %s "
                                 "err: %s" % (
                                     cmd, cls.ocp_master_node[0], out, err))
        cmd = ("oc login -u system:admin && oadm policy "
               "add-cluster-role-to-user cluster-admin %s") % cls.cns_username
        ret, out, err = g.run(cls.ocp_master_node[0], cmd, "root")
        if ret != 0:
            raise ExecutionError("failed to execute cmd %s on %s out: %s "
                                 "err: %s" % (
                                     cmd, cls.ocp_master_node[0], out, err))
        for node in cls.ocp_all_nodes:
            ret = docker_add_registry(node, cls.add_registry)
            if not ret:
                raise ExecutionError("failed to edit add_registry in docker "
                                     "file on %s" % node)
            ret = docker_insecure_registry(node, cls.insecure_registry)
            if not ret:
                raise ExecutionError("failed to edit insecure_registry in "
                                     "docker file on %s" % node)
        cmd = "systemctl restart docker"
        cmd_results = g.run_parallel(cls.ocp_all_nodes, cmd, "root")
        for node, ret_values in cmd_results.iteritems():
            ret, out, err = ret_values
            if ret != 0:
                raise ExecutionError("failed to execute cmd %s on %s out: "
                                     "%s err: %s" % (
                                         cmd, node, out, err))
        cmd = ("oc login %s:8443 -u %s -p %s --insecure-skip-tls-verify="
               "true" % (
                  cls.ocp_master_node[0], cls.cns_username, cls.cns_password))
        ret, out, err = g.run(cls.ocp_client[0], cmd, "root")
        if ret != 0:
            raise ExecutionError("failed to execute cmd %s on %s out: "
                                 "%s err: %s" % (
                                     cmd, cls.ocp_client[0], out, err))
        cmd = 'oadm policy add-scc-to-user privileged -z default'
        ret, out, err = g.run(cls.ocp_client[0], cmd, "root")
        if ret != 0:
            raise ExecutionError("failed to execute cmd %s on %s out: "
                                 "%s err: %s" % (
                                     cmd, cls.ocp_client[0], out, err))
        ret = create_namespace(cls.ocp_client[0], cls.cns_project_name)
        if not ret:
            raise ExecutionError("failed to create namespace")
        cmd = 'oc project %s' % cls.cns_project_name
        ret, out, err = g.run(cls.ocp_client[0], cmd, "root")
        if ret != 0:
            raise ExecutionError("failed to execute cmd %s on %s out: "
                                 "%s err: %s" % (
                                     cmd, cls.ocp_client[0], out, err))
        cls.router_name = "%s-router" % cls.cns_project_name
        if not setup_router(cls.ocp_client[0], cls.router_name):
            raise ExecutionError("failed to setup router")
        if not update_router_ip_dnsmasq_conf(cls.ocp_client[0],
                                             cls.router_name,
                                             cls.routingconfig_subdomain):
            raise ExecutionError("failed to update router ip in dnsmasq.conf")
        cmd = "systemctl restart dnsmasq.service"
        ret, out, err = g.run(cls.ocp_client[0], cmd, "root")
        if ret != 0:
            raise ExecutionError("failed to execute cmd %s on %s out: "
                                 "%s err: %s" % (
                                     cmd, cls.ocp_client[0], out, err))
        cmd = 'oc project %s' % cls.cns_project_name
        ret, out, err = g.run(cls.ocp_master_node[0], cmd, "root")
        if ret != 0:
            raise ExecutionError("failed to execute cmd %s on %s out: "
                                 "%s err: %s" % (
                                     cmd, cls.ocp_master_node[0], out, err))
        if not update_router_ip_dnsmasq_conf(cls.ocp_master_node[0],
                                             cls.router_name,
                                             cls.routingconfig_subdomain):
            raise ExecutionError("failed to update router ip in dnsmasq.conf")
        cmd = "systemctl restart dnsmasq.service"
        ret, out, err = g.run(cls.ocp_master_node[0], cmd, "root")
        if ret != 0:
            raise ExecutionError("failed to execute cmd %s on %s out: "
                                 "%s err: %s" % (
                                     cmd, cls.ocp_master_node[0], out, err))
        if not update_nameserver_resolv_conf(cls.ocp_master_node[0], "EOF"):
            raise ExecutionError("failed to update namserver in resolv.conf")
        if cls.ocp_master_node[0] != cls.ocp_client[0]:
            if not update_nameserver_resolv_conf(cls.ocp_client[0]):
                raise ExecutionError("failed to update namserver in resolv.conf")

    @classmethod
    def cns_deploy(cls):
        '''
         This function runs the cns-deploy
        '''
        ret = heketi_create_topology(cls.heketi_client_node,
                                     cls.topology_info)
        if not ret:
            raise ConfigError("Failed to create heketi topology file on %s"
                              % cls.heketi_client_node)
        # temporary workaround till we get fix for bug -
        # https://bugzilla.redhat.com/show_bug.cgi?id=1505948
        cmd = "sed -i s/'exec -it'/'exec -i'/g /usr/bin/cns-deploy"
        ret, out, err = g.run(cls.ocp_client[0], cmd, "root")
        if ret != 0:
            raise ExecutionError("failed to execute cmd %s on %s out: "
                                 "%s err: %s" % (
                                     cmd, cls.ocp_client[0], out, err))
        cmd = ("cns-deploy -n %s -g -c oc -t /usr/share/heketi/templates -l "
               "cns_deploy.log -v -w 600 -y /usr/share/heketi/topology.json" % (
                   cls.cns_project_name))
        ret, out, err = g.run(cls.ocp_client[0], cmd, "root")
        if ret != 0:
            raise ExecutionError("failed to execute cmd %s on %s out: "
                                 "%s err: %s" % (
                                     cmd, cls.ocp_client[0], out, err))
        # Checks if heketi server is alive
        if not hello_heketi(cls.heketi_client_node, cls.heketi_server_url):
            raise ConfigError("Heketi server %s is not alive"
                              % cls.heketi_server_url)


class CnsGlusterBlockBaseClass(CnsBaseClass):
    '''
     This class is for setting up glusterblock on CNS
    '''
    @classmethod
    def setUpClass(cls):
        '''
         Glusterblock setup on CNS
        '''
        super(CnsGlusterBlockBaseClass, cls).setUpClass()
        for node in cls.ocp_all_nodes:
            if not edit_iptables_cns(node):
                raise ExecutionError("failed to edit iptables")
        cmd = "systemctl reload iptables"
        cmd_results = g.run_parallel(cls.ocp_all_nodes, cmd, "root")
        gluster_pod_list = get_ocp_gluster_pod_names(cls.ocp_master_node[0])
        g.log.info("gluster_pod_list - %s" % gluster_pod_list)
        for pod in gluster_pod_list:
            cmd = "systemctl start gluster-blockd"
            ret, out, err = oc_rsh(cls.ocp_master_node[0], pod, cmd)
            if ret != 0:
                raise ExecutionError("failed to execute cmd %s on %s out: "
                                     "%s err: %s" % (
                                         cmd, cls.ocp_master_node[0],
                                         out, err))
        cmd = "mpathconf --enable"
        cmd_results = g.run_parallel(cls.ocp_nodes, cmd, "root")
        for node, ret_values in cmd_results.iteritems():
            ret, out, err = ret_values
            if ret != 0:
                raise ExecutionError("failed to execute cmd %s on %s out: "
                                     "%s err: %s" % (cmd, node, out, err))
        for node in cls.ocp_nodes:
            ret = edit_multipath_conf_file(node)
            if not ret:
                raise ExecutionError("failed to edit multipath.conf file")
        cmd = "systemctl restart multipathd"
        cmd_results = g.run_parallel(cls.ocp_nodes, cmd, "root")
        for node, ret_values in cmd_results.iteritems():
            ret, out, err = ret_values
            if ret != 0:
                raise ExecutionError("failed to execute cmd %s on %s out: "
                                     "%s err: %s" % (cmd, node, out, err))
