from collections import OrderedDict
from cnslibs.common import command
from cnslibs.common.exceptions import (
    ConfigError,
    ExecutionError)
from cnslibs.common.heketi_ops import (
    heketi_blockvolume_delete,
    heketi_create_topology,
    heketi_volume_delete,
    hello_heketi)
from cnslibs.common.cns_libs import (
    edit_iptables_cns,
    enable_kernel_module,
    edit_master_config_file,
    edit_multipath_conf_file,
    setup_router,
    start_rpcbind_service,
    update_nameserver_resolv_conf,
    update_router_ip_dnsmasq_conf)
from cnslibs.common.docker_libs import (
    docker_add_registry,
    docker_insecure_registry)
from cnslibs.common.openshift_ops import (
    create_namespace,
    get_ocp_gluster_pod_names,
    get_pod_name_from_dc,
    get_pv_name_from_pvc,
    oc_create_app_dc_with_io,
    oc_create_pvc,
    oc_create_sc,
    oc_create_secret,
    oc_delete,
    oc_get_custom_resource,
    oc_rsh,
    scale_dc_pod_amount_and_wait,
    verify_pvc_status_is_bound,
    wait_for_pod_be_ready,
    wait_for_resource_absence,
)
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
        cls.heketi_dc_name = heketi_config['heketi_dc_name']
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
        cls.sc = cls.cns_storage_class.get(
            'storage_class1', cls.cns_storage_class.get('file_storage_class'))
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

    def cmd_run(self, cmd, hostname=None, raise_on_error=True):
        if not hostname:
            hostname = self.ocp_master_node[0]
        return command.cmd_run(
            cmd=cmd, hostname=hostname, raise_on_error=raise_on_error)

    def create_secret(self, secret_name_prefix="autotests-secret-"):
        secret_name = oc_create_secret(
            self.ocp_client[0],
            secret_name_prefix=secret_name_prefix,
            namespace=(self.sc.get(
                'secretnamespace',
                self.sc.get('restsecretnamespace', 'default'))),
            data_key=self.heketi_cli_key,
            secret_type=self.sc.get('provisioner', 'kubernetes.io/glusterfs'))
        self.addCleanup(
            oc_delete, self.ocp_client[0], 'secret', secret_name)
        return secret_name

    def create_storage_class(self, secret_name=None,
                             sc_name_prefix="autotests-sc",
                             create_vol_name_prefix=False,
                             allow_volume_expansion=False,
                             reclaim_policy="Delete",
                             set_hacount=None,
                             is_arbiter_vol=False, arbiter_avg_file_size=None):

        # Create secret if one is not specified
        if not secret_name:
            secret_name = self.create_secret()

        # Create storage class
        secret_name_option = "secretname"
        secret_namespace_option = "secretnamespace"
        provisioner = self.sc.get("provisioner", "kubernetes.io/glusterfs")
        if provisioner != "kubernetes.io/glusterfs":
            secret_name_option = "rest%s" % secret_name_option
            secret_namespace_option = "rest%s" % secret_namespace_option
        parameters = {
            "resturl": self.sc["resturl"],
            "restuser": self.sc["restuser"],
            secret_name_option: secret_name,
            secret_namespace_option: self.sc.get(
                "secretnamespace", self.sc.get("restsecretnamespace")),
        }
        if set_hacount:
            parameters["hacount"] = self.sc.get("hacount", "3")
        if is_arbiter_vol:
            parameters["volumeoptions"] = "user.heketi.arbiter true"
            if arbiter_avg_file_size:
                parameters["volumeoptions"] += (
                    ",user.heketi.average-file-size %s" % (
                        arbiter_avg_file_size))
        if create_vol_name_prefix:
            parameters["volumenameprefix"] = self.sc.get(
                "volumenameprefix", "autotest")
        self.sc_name = oc_create_sc(
            self.ocp_client[0],
            sc_name_prefix=sc_name_prefix,
            provisioner=provisioner,
            allow_volume_expansion=allow_volume_expansion,
            reclaim_policy=reclaim_policy,
            **parameters)
        self.addCleanup(oc_delete, self.ocp_client[0], "sc", self.sc_name)
        return self.sc_name

    def create_and_wait_for_pvcs(self, pvc_size=1,
                                 pvc_name_prefix="autotests-pvc",
                                 pvc_amount=1, sc_name=None):
        node = self.ocp_client[0]

        # Create storage class if not specified
        if not sc_name:
            if getattr(self, "sc_name", ""):
                sc_name = self.sc_name
            else:
                sc_name = self.create_storage_class()

        # Create PVCs
        pvc_names = []
        for i in range(pvc_amount):
            pvc_name = oc_create_pvc(
                node, sc_name, pvc_name_prefix=pvc_name_prefix,
                pvc_size=pvc_size)
            pvc_names.append(pvc_name)
            self.addCleanup(
                wait_for_resource_absence, node, 'pvc', pvc_name)

        # Wait for PVCs to be in bound state
        try:
            for pvc_name in pvc_names:
                verify_pvc_status_is_bound(node, pvc_name)
        finally:
            reclaim_policy = oc_get_custom_resource(
                node, 'sc', ':.reclaimPolicy', sc_name)[0]

            for pvc_name in pvc_names:
                if reclaim_policy == 'Retain':
                    pv_name = get_pv_name_from_pvc(node, pvc_name)
                    self.addCleanup(oc_delete, node, 'pv', pv_name,
                                    raise_on_absence=False)
                    custom = (r':.metadata.annotations."gluster\.kubernetes'
                              r'\.io\/heketi\-volume\-id"')
                    vol_id = oc_get_custom_resource(
                        node, 'pv', custom, pv_name)[0]
                    if self.sc.get('provisioner') == "kubernetes.io/glusterfs":
                        self.addCleanup(heketi_volume_delete,
                                        self.heketi_client_node,
                                        self.heketi_server_url, vol_id,
                                        raise_on_error=False)
                    else:
                        self.addCleanup(heketi_blockvolume_delete,
                                        self.heketi_client_node,
                                        self.heketi_server_url, vol_id)
                self.addCleanup(oc_delete, node, 'pvc', pvc_name,
                                raise_on_absence=False)

        return pvc_names

    def create_and_wait_for_pvc(self, pvc_size=1,
                                pvc_name_prefix='autotests-pvc', sc_name=None):
        self.pvc_name = self.create_and_wait_for_pvcs(
            pvc_size=pvc_size, pvc_name_prefix=pvc_name_prefix, sc_name=sc_name
        )[0]
        return self.pvc_name

    def create_dc_with_pvc(self, pvc_name, timeout=300, wait_step=10):
        dc_name = oc_create_app_dc_with_io(self.ocp_client[0], pvc_name)
        self.addCleanup(oc_delete, self.ocp_client[0], 'dc', dc_name)
        self.addCleanup(
            scale_dc_pod_amount_and_wait, self.ocp_client[0], dc_name, 0)
        pod_name = get_pod_name_from_dc(self.ocp_client[0], dc_name)
        wait_for_pod_be_ready(self.ocp_client[0], pod_name,
                              timeout=timeout, wait_step=wait_step)
        return dc_name, pod_name


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
        mod_names = ('dm_thin_pool', 'dm_multipath', 'target_core_user')
        for node in cls.ocp_all_nodes:
            for mod_name in mod_names:
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
                raise ExecutionError(
                    "failed to update namserver in resolv.conf")

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
        cmd = (
            "cns-deploy -n %s -g -c oc -t /usr/share/heketi/templates -l "
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
        cls.sc = cls.cns_storage_class.get(
            'storage_class2',
            cls.cns_storage_class.get('block_storage_class'))
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


class PodScalabilityBaseClass(CnsBaseClass):
    """
    This class is for setting parameters for scalling pods
    """
    @classmethod
    def setUpClass(cls):
        """
        Initialize all the variables necessary for scalling setup
        """

        super(PodScalabilityBaseClass, cls).setUpClass()

        cls.scale_info = g.config['scale']
