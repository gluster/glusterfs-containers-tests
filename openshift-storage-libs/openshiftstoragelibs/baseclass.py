from collections import defaultdict
import datetime
import re
import unittest

from glusto.core import Glusto as g
import six

from openshiftstoragelibs import command
from openshiftstoragelibs.exceptions import (
    CloudProviderError,
    ConfigError,
    ExecutionError,
)
from openshiftstoragelibs.gluster_ops import (
    get_block_hosting_volume_name,
    get_gluster_vol_status,
)
from openshiftstoragelibs.heketi_ops import (
    hello_heketi,
    heketi_blockvolume_delete,
    heketi_blockvolume_info,
    heketi_db_check,
    heketi_volume_create,
    heketi_volume_delete,
    heketi_volume_info,
    heketi_volume_list,
)
from openshiftstoragelibs.node_ops import (
    node_add_iptables_rules,
    node_delete_iptables_rules,
    power_off_vm_by_name,
    power_on_vm_by_name,
)
from openshiftstoragelibs.openshift_ops import (
    get_block_provisioner,
    get_pod_name_from_dc,
    get_pod_name_from_rc,
    get_pv_name_from_pvc,
    oc_create_app_dc_with_io,
    oc_create_busybox_app_dc_with_io,
    oc_create_pvc,
    oc_create_sc,
    oc_create_secret,
    oc_delete,
    oc_get_custom_resource,
    oc_get_pods,
    oc_label,
    scale_dcs_pod_amount_and_wait,
    switch_oc_project,
    wait_for_gluster_pod_be_ready_on_specific_node,
    wait_for_ocp_node_be_ready,
    wait_for_pvcs_be_bound,
    wait_for_pods_be_ready,
    wait_for_resources_absence,
    wait_for_service_status_on_gluster_pod_or_node,
)
from openshiftstoragelibs.process_ops import (
    get_process_info_on_gluster_pod_or_node,
)
from openshiftstoragelibs.openshift_storage_libs import (
    get_iscsi_block_devices_by_path,
    get_iscsi_session,
    get_mpath_name_from_device_name,
    validate_multipath_pod,
)
from openshiftstoragelibs.openshift_version import get_openshift_version
from openshiftstoragelibs.waiter import Waiter

HEKETI_VOLUME_REGEX = "Id:(.*).Cluster:(.*).Name:%s"


class BaseClass(unittest.TestCase):
    """Base class for test classes."""
    ERROR_OR_FAILURE_EXISTS = False
    STOP_ON_FIRST_FAILURE = bool(g.config.get("common", {}).get(
        "stop_on_first_failure", False))
    CHECK_HEKETI_DB_INCONSISTENCIES = (
        g.config.get("common", {}).get("check_heketi_db_inconsistencies", True)
        in (True, 'TRUE', 'True', 'true', 'yes', 'Yes', 'YES'))

    @classmethod
    def setUpClass(cls):
        """Initialize all the variables necessary for test cases."""
        super(BaseClass, cls).setUpClass()

        # Initializes OCP config variables
        cls.ocp_servers_info = g.config['ocp_servers']
        cls.ocp_master_node = list(g.config['ocp_servers']['master'].keys())
        cls.ocp_master_node_info = g.config['ocp_servers']['master']
        cls.ocp_client = list(g.config['ocp_servers']['client'].keys())
        cls.ocp_client_info = g.config['ocp_servers']['client']
        cls.ocp_nodes = list(g.config['ocp_servers']['nodes'].keys())
        cls.ocp_nodes_info = g.config['ocp_servers']['nodes']

        # Initializes storage project config variables
        cls.openshift_config = g.config.get("cns", g.config.get("openshift"))
        cls.storage_project_name = cls.openshift_config.get(
            'storage_project_name',
            cls.openshift_config.get('setup', {}).get('cns_project_name'))

        # Initializes heketi config variables
        heketi_config = cls.openshift_config['heketi_config']
        cls.heketi_dc_name = heketi_config['heketi_dc_name']
        cls.heketi_service_name = heketi_config['heketi_service_name']
        cls.heketi_client_node = heketi_config['heketi_client_node']
        cls.heketi_server_url = heketi_config['heketi_server_url']
        cls.heketi_cli_user = heketi_config['heketi_cli_user']
        cls.heketi_cli_key = heketi_config['heketi_cli_key']

        cls.gluster_servers = list(g.config['gluster_servers'].keys())
        cls.gluster_servers_info = g.config['gluster_servers']

        cls.storage_classes = cls.openshift_config['dynamic_provisioning'][
            'storage_classes']
        cls.sc = cls.storage_classes.get(
            'storage_class1', cls.storage_classes.get('file_storage_class'))
        cls.secret_type = "kubernetes.io/glusterfs"

        cls.heketi_logs_before_delete = bool(
            g.config.get("common", {}).get("heketi_logs_before_delete", False))

        cmd = "echo -n %s | base64" % cls.heketi_cli_key
        ret, out, err = g.run(cls.ocp_master_node[0], cmd, "root")
        if ret != 0:
            raise ExecutionError("failed to execute cmd %s on %s out: %s "
                                 "err: %s" % (
                                     cmd, cls.ocp_master_node[0], out, err))
        cls.secret_data_key = out.strip()

        # Checks if heketi server is alive
        if not hello_heketi(cls.heketi_client_node, cls.heketi_server_url):
            raise ConfigError("Heketi server %s is not alive"
                              % cls.heketi_server_url)

        # Switch to the storage project
        if not switch_oc_project(
                cls.ocp_master_node[0], cls.storage_project_name):
            raise ExecutionError("Failed to switch oc project on node %s"
                                 % cls.ocp_master_node[0])

        if 'glustotest_run_id' not in g.config:
            g.config['glustotest_run_id'] = (
                datetime.datetime.now().strftime('%H_%M_%d_%m_%Y'))
        cls.glustotest_run_id = g.config['glustotest_run_id']
        msg = "Setupclass: %s : %s" % (cls.__name__, cls.glustotest_run_id)
        g.log.info(msg)

    def setUp(self):
        if (BaseClass.STOP_ON_FIRST_FAILURE
                and BaseClass.ERROR_OR_FAILURE_EXISTS):
            self.skipTest("Test is skipped, because of the restriction "
                          "to one test case failure.")

        super(BaseClass, self).setUp()
        if self.CHECK_HEKETI_DB_INCONSISTENCIES:
            try:
                self.heketi_db_inconsistencies = heketi_db_check(
                    self.heketi_client_node, self.heketi_server_url)
            except NotImplementedError as e:
                g.log.info("Can not check Heketi DB inconsistencies due to "
                           "the following error: %s" % e)
            else:
                self.addCleanup(
                    self.check_heketi_db_inconsistencies,
                    self.heketi_db_inconsistencies["totalinconsistencies"])

        msg = "Starting Test : %s : %s" % (self.id(), self.glustotest_run_id)
        g.log.info(msg)

    def tearDown(self):
        super(BaseClass, self).tearDown()
        msg = "Ending Test: %s : %s" % (self.id(), self.glustotest_run_id)
        g.log.info(msg)

    @classmethod
    def tearDownClass(cls):
        super(BaseClass, cls).tearDownClass()
        msg = "Teardownclass: %s : %s" % (cls.__name__, cls.glustotest_run_id)
        g.log.info(msg)

    def cmd_run(self, cmd, hostname=None, raise_on_error=True):
        if not hostname:
            hostname = self.ocp_master_node[0]
        return command.cmd_run(
            cmd=cmd, hostname=hostname, raise_on_error=raise_on_error)

    def check_heketi_db_inconsistencies(
            self, number_of_allowed_heketi_db_inconsistencies):
        current_heketi_db_inconsistencies = heketi_db_check(
            self.heketi_client_node, self.heketi_server_url)
        current_number_of_heketi_db_inconsistencies = (
            current_heketi_db_inconsistencies["totalinconsistencies"])
        error_msg = (
            "Before the test case we had %s inconsistencies, but after "
            "the test case we have %s inconsistencies in the Heketi DB.\n"
            "'heketi-cli db check' command output is following:\n%s" % (
                number_of_allowed_heketi_db_inconsistencies,
                current_number_of_heketi_db_inconsistencies,
                current_heketi_db_inconsistencies))
        self.assertEqual(
            number_of_allowed_heketi_db_inconsistencies,
            current_number_of_heketi_db_inconsistencies,
            error_msg)

    def create_secret(self, secret_name_prefix="autotests-secret",
                      secret_type=None, skip_cleanup=False):
        secret_name = oc_create_secret(
            self.ocp_client[0],
            secret_name_prefix=secret_name_prefix,
            namespace=(self.sc.get(
                'secretnamespace',
                self.sc.get('restsecretnamespace', 'default'))),
            data_key=self.heketi_cli_key,
            secret_type=secret_type or self.secret_type)
        if not skip_cleanup:
            self.addCleanup(
                oc_delete, self.ocp_client[0], 'secret', secret_name)
        return secret_name

    def _create_storage_class(self, provisioner, secret_name=None,
                              sc_name_prefix="autotests-sc",
                              sc_name=None,
                              create_vol_name_prefix=False,
                              vol_name_prefix=None,
                              allow_volume_expansion=False,
                              reclaim_policy="Delete",
                              set_hacount=None,
                              clusterid=None,
                              hacount=None,
                              is_arbiter_vol=False, arbiter_avg_file_size=None,
                              heketi_zone_checking=None, volumeoptions=None,
                              skip_cleanup=False):

        # Create secret if one is not specified
        if not secret_name:
            secret_name = self.create_secret(skip_cleanup=skip_cleanup)

        # Create storage class
        secret_name_option = "secretname"
        secret_namespace_option = "secretnamespace"
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
        if clusterid:
            parameters["clusterid"] = clusterid
        if hacount:
            parameters["hacount"] = six.text_type(hacount)
        elif set_hacount:
            parameters["hacount"] = self.sc.get("hacount", "3")

        if is_arbiter_vol:
            parameters["volumeoptions"] = "user.heketi.arbiter true"
            if arbiter_avg_file_size:
                parameters["volumeoptions"] += (
                    ",user.heketi.average-file-size %s" % (
                        arbiter_avg_file_size))

        if volumeoptions and "volumeoptions" in parameters.keys():
            parameters["volumeoptions"] += ',' + volumeoptions
        elif volumeoptions:
            parameters["volumeoptions"] = volumeoptions

        if heketi_zone_checking:
            if parameters.get("volumeoptions"):
                parameters["volumeoptions"] += (
                    ",user.heketi.zone-checking %s" % heketi_zone_checking)
            else:
                parameters["volumeoptions"] = (
                    "user.heketi.zone-checking %s" % heketi_zone_checking)
        if vol_name_prefix:
            parameters["volumenameprefix"] = vol_name_prefix
        elif create_vol_name_prefix:
            parameters["volumenameprefix"] = self.sc.get(
                "volumenameprefix", "autotest")
        self.sc_name = oc_create_sc(
            self.ocp_client[0],
            sc_name_prefix=sc_name_prefix,
            sc_name=sc_name,
            provisioner=provisioner,
            allow_volume_expansion=allow_volume_expansion,
            reclaim_policy=reclaim_policy,
            **parameters)

        if not skip_cleanup:
            self.addCleanup(oc_delete, self.ocp_client[0], "sc", self.sc_name)
        return self.sc_name

    def create_storage_class(self, secret_name=None,
                             sc_name_prefix="autotests-sc",
                             sc_name=None,
                             create_vol_name_prefix=False,
                             vol_name_prefix=None,
                             allow_volume_expansion=False,
                             reclaim_policy="Delete",
                             set_hacount=None,
                             clusterid=None,
                             hacount=None,
                             is_arbiter_vol=False, arbiter_avg_file_size=None,
                             heketi_zone_checking=None, volumeoptions=None,
                             skip_cleanup=False):

        provisioner = self.get_provisioner_for_sc()

        return self._create_storage_class(
            provisioner=provisioner, secret_name=secret_name,
            sc_name_prefix=sc_name_prefix, sc_name=sc_name,
            create_vol_name_prefix=create_vol_name_prefix,
            vol_name_prefix=vol_name_prefix,
            allow_volume_expansion=allow_volume_expansion,
            reclaim_policy=reclaim_policy, set_hacount=set_hacount,
            clusterid=clusterid, hacount=hacount,
            is_arbiter_vol=is_arbiter_vol,
            arbiter_avg_file_size=arbiter_avg_file_size,
            heketi_zone_checking=heketi_zone_checking,
            volumeoptions=volumeoptions, skip_cleanup=skip_cleanup)

    def get_provisioner_for_sc(self):
        return "kubernetes.io/glusterfs"

    def get_block_provisioner_for_sc(self):
        return get_block_provisioner(self.ocp_client[0])

    def create_and_wait_for_pvcs(
            self, pvc_size=1, pvc_name_prefix="autotests-pvc", pvc_amount=1,
            sc_name=None, timeout=600, wait_step=10, skip_waiting=False,
            skip_cleanup=False):
        """Create multiple PVC's not waiting for it

        Args:
            pvc_size (int): size of PVC, default value is 1
            pvc_name_prefix (str): volume prefix for each PVC, default value is
                                   'autotests-pvc'
            pvc_amount (int): number of PVC's, default value is 1
            sc_name (str): storage class to create PVC, default value is None,
                           which will cause automatic creation of sc.
            timeout (int): timeout time for waiting for PVC's to get bound
            wait_step (int): waiting time between each try of PVC status check
            skip_waiting (bool): boolean value which defines whether
                                 we need to wait for PVC creation or not.
        Returns:
            List: list of PVC names
        """
        node = self.ocp_client[0]

        # Create storage class if not specified
        if not sc_name:
            if getattr(self, "sc_name", ""):
                sc_name = self.sc_name
            else:
                sc_name = self.create_storage_class(skip_cleanup=skip_cleanup)

        # Create PVCs
        pvc_names = []
        for i in range(pvc_amount):
            pvc_name = oc_create_pvc(
                node, sc_name, pvc_name_prefix=pvc_name_prefix,
                pvc_size=pvc_size)
            pvc_names.append(pvc_name)
        if not skip_cleanup:
            self.addCleanup(
                wait_for_resources_absence, node, 'pvc', pvc_names)

        # Wait for PVCs to be in bound state
        try:
            if not skip_waiting:
                wait_for_pvcs_be_bound(node, pvc_names, timeout, wait_step)
        finally:
            if skip_cleanup:
                return pvc_names

            if get_openshift_version() < "3.9":
                reclaim_policy = "Delete"
            else:
                reclaim_policy = oc_get_custom_resource(
                    node, 'sc', ':.reclaimPolicy', sc_name)[0]

            for pvc_name in pvc_names:
                if reclaim_policy == 'Retain':
                    pv_name = get_pv_name_from_pvc(node, pvc_name)
                    if not pv_name and skip_waiting:
                        continue
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
                                        self.heketi_server_url, vol_id,
                                        raise_on_error=False)
                self.addCleanup(oc_delete, node, 'pvc', pvc_name,
                                raise_on_absence=False)
        return pvc_names

    def create_and_wait_for_pvc(
            self, pvc_size=1, pvc_name_prefix='autotests-pvc', sc_name=None,
            timeout=300, wait_step=10, skip_cleanup=False):
        self.pvc_name = self.create_and_wait_for_pvcs(
            pvc_size=pvc_size, pvc_name_prefix=pvc_name_prefix,
            sc_name=sc_name, timeout=timeout, wait_step=wait_step,
            skip_cleanup=skip_cleanup)[0]
        return self.pvc_name

    def create_pvcs_not_waiting(
            self, pvc_size=1, pvc_name_prefix="autotests-pvc",
            pvc_amount=1, sc_name=None, skip_cleanup=False):
        return self.create_and_wait_for_pvcs(
            pvc_size=pvc_size, pvc_name_prefix=pvc_name_prefix,
            pvc_amount=pvc_amount, sc_name=sc_name, skip_waiting=True,
            skip_cleanup=skip_cleanup)

    def create_dcs_with_pvc(
            self, pvc_names, timeout=600, wait_step=5,
            dc_name_prefix='autotests-dc', label=None,
            skip_cleanup=False, is_busybox=False):
        """Create bunch of DCs with app PODs which use unique PVCs.

        Args:
            pvc_names (str/set/list/tuple): List/set/tuple of PVC names
                to attach to app PODs as part of DCs.
            timeout (int): timeout value, default value is 600 seconds.
            wait_step( int): wait step, default value is 5 seconds.
            dc_name_prefix(str): name prefix for deployement config.
            label (dict): keys and value for adding label into DC.
            is_busybox (bool): True for busybox app pod else default is False
        Returns: dictionary with following structure:
            {
                "pvc_name_1": ("dc_name_1", "pod_name_1"),
                "pvc_name_2": ("dc_name_2", "pod_name_2"),
                ...
                "pvc_name_n": ("dc_name_n", "pod_name_n"),
            }
        """
        pvc_names = (
            pvc_names
            if isinstance(pvc_names, (list, set, tuple)) else [pvc_names])
        dc_and_pod_names, dc_names = {}, {}
        function = (oc_create_busybox_app_dc_with_io if is_busybox else
                    oc_create_app_dc_with_io)
        for pvc_name in pvc_names:
            dc_name = function(self.ocp_client[0], pvc_name,
                               dc_name_prefix=dc_name_prefix, label=label)
            dc_names[pvc_name] = dc_name
            if not skip_cleanup:
                self.addCleanup(oc_delete, self.ocp_client[0], 'dc', dc_name)
        if not skip_cleanup:
            self.addCleanup(
                scale_dcs_pod_amount_and_wait, self.ocp_client[0],
                dc_names.values(), 0, timeout=timeout, wait_step=wait_step)

        for pvc_name, dc_name in dc_names.items():
            pod_name = get_pod_name_from_dc(self.ocp_client[0], dc_name)
            dc_and_pod_names[pvc_name] = (dc_name, pod_name)
        scale_dcs_pod_amount_and_wait(
            self.ocp_client[0], dc_names.values(), 1,
            timeout=timeout, wait_step=wait_step)

        return dc_and_pod_names

    def create_dc_with_pvc(
            self, pvc_name, timeout=300, wait_step=10,
            dc_name_prefix='autotests-dc', label=None,
            skip_cleanup=False, is_busybox=False):
        return self.create_dcs_with_pvc(
            pvc_name, timeout, wait_step,
            dc_name_prefix=dc_name_prefix, label=label,
            skip_cleanup=skip_cleanup, is_busybox=is_busybox)[pvc_name]

    def create_heketi_volume_with_name_and_wait(
            self, name, size, raise_on_cleanup_error=True,
            timeout=600, wait_step=10, **kwargs):
        json = kwargs.get("json", False)

        try:
            h_volume_info = heketi_volume_create(
                self.heketi_client_node, self.heketi_server_url,
                size, name=name, **kwargs)
        except Exception as e:
            if ('more required' in six.text_type(e)
                    or ('Failed to allocate new volume' in six.text_type(e))):
                raise

            for w in Waiter(timeout, wait_step):
                h_volumes = heketi_volume_list(
                    self.heketi_client_node, self.heketi_server_url)
                h_volume_match = re.search(
                    HEKETI_VOLUME_REGEX % name, h_volumes)
                if h_volume_match:
                    h_volume_info = heketi_volume_info(
                        self.heketi_client_node, self.heketi_server_url,
                        h_volume_match.group(1), json=json)
                    break

            if w.expired:
                g.log.info(
                    "Heketi volume with name %s not created in 600 sec" % name)
                raise

        self.addCleanup(
            heketi_volume_delete, self.heketi_client_node,
            self.heketi_server_url, h_volume_info["id"],
            raise_on_error=raise_on_cleanup_error)

        return h_volume_info

    def configure_node_to_run_gluster_node(self, storage_hostname):
        glusterd_status_cmd = "systemctl is-active glusterd"
        command.cmd_run(glusterd_status_cmd, storage_hostname)

        ports = ("24010", "3260", "111", "22", "24007", "24008", "49152-49664")
        add_port = " ".join(["--add-port=%s/tcp" % port for port in ports])
        add_firewall_rule_cmd = "firewall-cmd --zone=public %s" % add_port
        command.cmd_run(add_firewall_rule_cmd, storage_hostname)

    def configure_node_to_run_gluster_pod(self, storage_hostname):
        ports = (
            "24010", "3260", "111", "2222", "24007", "24008", "49152:49664")
        iptables_rule_pattern = (
            "-p tcp -m state --state NEW -m %s --%s %s -j ACCEPT")
        iptables_rule_chain = "OS_FIREWALL_ALLOW"
        iptables_rules = []
        for port in ports:
            if ":" in port:
                iptables_rules.append(
                    iptables_rule_pattern % ("multiport", "dports", port))
            else:
                iptables_rules.append(
                    iptables_rule_pattern % ("tcp", "dport", port))
        node_add_iptables_rules(
            storage_hostname, iptables_rule_chain, iptables_rules)
        self.addCleanup(
            node_delete_iptables_rules,
            storage_hostname, iptables_rule_chain, iptables_rules)

        gluster_host_label = "glusterfs=storage-host"
        gluster_pod_label = "glusterfs=storage-pod"
        oc_label(
            self.ocp_client[0], "node", storage_hostname, gluster_host_label,
            overwrite=True)
        self.addCleanup(
            wait_for_pods_be_ready,
            self.ocp_client[0], len(self.gluster_servers),
            selector=gluster_pod_label)
        self.addCleanup(
            oc_label,
            self.ocp_client[0], "node", storage_hostname, "glusterfs-")

        wait_for_pods_be_ready(
            self.ocp_client[0], len(self.gluster_servers) + 1,
            selector=gluster_pod_label)

    def is_containerized_gluster(self):
        cmd = ("oc get pods --no-headers -l glusterfs-node=pod "
               "-o=custom-columns=:.spec.nodeName")
        g_nodes = command.cmd_run(cmd, self.ocp_client[0])
        g_nodes = g_nodes.split('\n') if g_nodes else g_nodes
        return not not g_nodes

    def configure_node_to_run_gluster(self, storage_host_manage):
        if self.is_containerized_gluster():
            self.configure_node_to_run_gluster_pod(storage_host_manage)
        else:
            self.configure_node_to_run_gluster_node(storage_host_manage)

    def _is_error_or_failure_exists(self):
        if hasattr(self, '_outcome'):
            # Python 3.4+
            result = self.defaultTestResult()
            self._feedErrorsToResult(result, self._outcome.errors)
        else:
            # Python 2.7-3.3
            result = getattr(
                self, '_outcomeForDoCleanups', self._resultForDoCleanups)
        ok_result = True
        for attr in ('errors', 'failures'):
            if not hasattr(result, attr):
                continue
            exc_list = getattr(result, attr)
            if exc_list and exc_list[-1][0] is self:
                ok_result = ok_result and not exc_list[-1][1]
        if hasattr(result, '_excinfo'):
            ok_result = ok_result and not result._excinfo
        if ok_result:
            return False
        self.ERROR_OR_FAILURE_EXISTS = True
        BaseClass.ERROR_OR_FAILURE_EXISTS = True
        return True

    def doCleanups(self):
        if (BaseClass.STOP_ON_FIRST_FAILURE
                and (self.ERROR_OR_FAILURE_EXISTS
                     or self._is_error_or_failure_exists())):
            while self._cleanups:
                (func, args, kwargs) = self._cleanups.pop()
                msg = ("Found test case failure. Avoiding run of scheduled "
                       "following cleanup:\nfunc = %s\nargs = %s\n"
                       "kwargs = %s" % (func, args, kwargs))
                g.log.warn(msg)
        return super(BaseClass, self).doCleanups()

    @classmethod
    def doClassCleanups(cls):
        if (BaseClass.STOP_ON_FIRST_FAILURE
                and BaseClass.ERROR_OR_FAILURE_EXISTS):
            while cls._class_cleanups:
                (func, args, kwargs) = cls._class_cleanups.pop()
                msg = ("Found test case failure. Avoiding run of scheduled "
                       "following cleanup:\nfunc = %s\nargs = %s\n"
                       "kwargs = %s" % (func, args, kwargs))
                g.log.warn(msg)
        return super(BaseClass, cls).doClassCleanups()

    def power_on_vm(self, vm_name):
        try:
            power_on_vm_by_name(vm_name)
        except CloudProviderError as e:
            # Try to power on VM, if it raises already powered On error
            # then don't raise exception.
            if 'VM %s is already powered On' % vm_name not in six.text_type(e):
                raise

    def power_off_vm(self, vm_name):
        self.addCleanup(self.power_on_vm, vm_name)
        power_off_vm_by_name(vm_name)

    def power_on_gluster_node_vm(
            self, vm_name, gluster_hostname, timeout=300, wait_step=3):
        # NOTE(Nitin Goyal): Same timeout is used for all functions.

        # Bring up the target node
        power_on_vm_by_name(vm_name)

        # Wait for gluster node and pod to be ready
        if self.is_containerized_gluster():
            wait_for_ocp_node_be_ready(
                self.node, gluster_hostname,
                timeout=timeout, wait_step=wait_step)
            wait_for_gluster_pod_be_ready_on_specific_node(
                self.node, gluster_hostname,
                timeout=timeout, wait_step=wait_step)

        # Wait for gluster services to be up
        for service in ('glusterd', 'gluster-blockd'):
            wait_for_service_status_on_gluster_pod_or_node(
                self.node, service, 'active', 'running', gluster_hostname,
                raise_on_error=False, timeout=timeout, wait_step=wait_step)

    def power_off_gluster_node_vm(
            self, vm_name, gluster_hostname, timeout=300, wait_step=3):
        # NOTE(Nitin Goyal): Same timeout is used for all functions.

        # Wait for gluster services to be up in cleanup
        for service in ('gluster-blockd', 'glusterd'):
            self.addCleanup(
                wait_for_service_status_on_gluster_pod_or_node,
                self.node, service, 'active', 'running', gluster_hostname,
                raise_on_error=False, timeout=timeout, wait_step=wait_step)

        # Wait for gluster pod to be up and node to be ready in cleanup
        if self.is_containerized_gluster():
            self.addCleanup(
                wait_for_gluster_pod_be_ready_on_specific_node, self.node,
                gluster_hostname, timeout=timeout, wait_step=wait_step)
            self.addCleanup(
                wait_for_ocp_node_be_ready, self.node, gluster_hostname,
                timeout=timeout, wait_step=wait_step)

        # Power off vm
        self.addCleanup(self.power_on_vm, vm_name)
        self.power_off_vm(vm_name)


class GlusterBlockBaseClass(BaseClass):
    """Base class for gluster-block test cases."""

    @classmethod
    def setUpClass(cls):
        """Initialize all the variables necessary for test cases."""
        super(GlusterBlockBaseClass, cls).setUpClass()
        cls.sc = cls.storage_classes.get(
            'storage_class2', cls.storage_classes.get('block_storage_class'))
        cls.secret_type = "gluster.org/glusterblock"

    def get_provisioner_for_sc(self):
        return self.get_block_provisioner_for_sc()

    def verify_iscsi_sessions_and_multipath(
            self, pvc_name, rname, rtype='dc', heketi_server_url=None,
            is_registry_gluster=False):
        if not heketi_server_url:
            heketi_server_url = self.heketi_server_url

        # Get storage ips of glusterfs pods
        keys = (list(g.config['gluster_registry_servers'].keys()) if
                is_registry_gluster else self.gluster_servers)
        servers_info = (g.config['gluster_registry_servers'] if
                        is_registry_gluster else self.gluster_servers_info)
        gluster_ips = []
        for key in keys:
            gluster_ips.append(servers_info[key]['storage'])
        gluster_ips.sort()

        # Find iqn and hacount from volume info
        pv_name = get_pv_name_from_pvc(self.ocp_client[0], pvc_name)
        custom = [r':.metadata.annotations."gluster\.org\/volume\-id"']
        vol_id = oc_get_custom_resource(
            self.ocp_client[0], 'pv', custom, pv_name)[0]
        vol_info = heketi_blockvolume_info(
            self.heketi_client_node, heketi_server_url, vol_id, json=True)
        iqn = vol_info['blockvolume']['iqn']
        hacount = int(vol_info['hacount'])

        # Find node on which pod is running
        if rtype == 'dc':
            pod_name = get_pod_name_from_dc(self.ocp_client[0], rname)
            pod_info = oc_get_pods(
                self.ocp_client[0], selector='deploymentconfig=%s' % rname)
        elif rtype == 'pod':
            pod_info = oc_get_pods(self.ocp_client[0], name=rname)
            pod_name = rname
        elif rtype == 'rc':
            pod_name = get_pod_name_from_rc(self.ocp_client[0], rname)
            pod_info = oc_get_pods(
                self.ocp_client[0], selector='name=%s' % rname)
        else:
            raise NameError("Value of rtype should be either 'dc' or 'pod'")

        node = pod_info[pod_name]['node']

        # Get the iscsi sessions info from the node
        iscsi = get_iscsi_session(node, iqn)
        msg = ('Only %s iscsi sessions are present on node %s, expected %s.'
               % (iscsi, node, hacount))
        self.assertEqual(hacount, len(iscsi), msg)
        iscsi.sort()
        msg = ("Only gluster Nodes %s were expected in iscsi sessions, "
               "but got other Nodes %s on Node %s" % (
                   gluster_ips, iscsi, node))
        self.assertEqual(set(iscsi), (set(gluster_ips) & set(iscsi)), msg)

        # Get the paths info from the node
        devices = get_iscsi_block_devices_by_path(node, iqn)
        msg = ("Only %s devices are present on Node %s, expected %s" % (
            devices, node, hacount,))
        self.assertEqual(hacount, len(devices), msg)

        # Get mpath names and verify that only one mpath is there
        mpaths = set()
        for device in devices.keys():
            mpaths.add(get_mpath_name_from_device_name(node, device))
        msg = ("Only one mpath was expected on Node %s, but got %s" % (
            node, mpaths))
        self.assertEqual(1, len(mpaths), msg)

        validate_multipath_pod(
            self.ocp_client[0], pod_name, hacount, mpath=list(mpaths)[0])

        return iqn, hacount, node

    def verify_all_paths_are_up_in_multipath(
            self, mpath_name, hacount, node, timeout=30, interval=5):
        for w in Waiter(timeout, interval):
            out = command.cmd_run('multipath -ll %s' % mpath_name, node)
            count = 0
            for line in out.split('\n'):
                if 'active ready running' in line:
                    count += 1
            if hacount == count:
                break
        msg = "Paths are not up equal to hacount %s in mpath %s on Node %s" % (
            hacount, out, node)
        self.assertEqual(hacount, count, msg)
        for state in ['failed', 'faulty', 'undef']:
            msg = "All paths are not up in mpath %s on Node %s" % (out, node)
            self.assertNotIn(state, out, msg)

    def get_block_hosting_volume_by_pvc_name(
            self, pvc_name, heketi_server_url=None, gluster_node=None,
            ocp_client_node=None):
        """Get block hosting volume of pvc name given

        Args:
            pvc_name (str): pvc name for which the BHV name needs
                            to be returned
        Kwargs:
            heketi_server_url (str): heketi server url to run heketi commands
            gluster_node (str): gluster node where to run gluster commands
            ocp_client_node (str): ocp cleint node where to run oc commands
        """
        if not heketi_server_url:
            heketi_server_url = self.heketi_server_url
        pv_name = get_pv_name_from_pvc(self.ocp_client[0], pvc_name)
        block_volume = oc_get_custom_resource(
            self.ocp_client[0], 'pv',
            r':.metadata.annotations."gluster\.org\/volume\-id"',
            name=pv_name
        )[0]

        # get block hosting volume from block volume
        block_hosting_vol = get_block_hosting_volume_name(
            self.heketi_client_node, heketi_server_url, block_volume,
            gluster_node=gluster_node, ocp_client_node=ocp_client_node)

        return block_hosting_vol


class ScaleUpBaseClass(GlusterBlockBaseClass):
    """Base class for ScaleUp test cases."""

    @classmethod
    def setUpClass(cls):
        """Initialize all the variables necessary for test cases."""
        super(GlusterBlockBaseClass, cls).setUpClass()

        cls.file_sc = cls.storage_classes.get(
            'storage_class1', cls.storage_classes.get('file_storage_class'))
        cls.file_secret_type = "kubernetes.io/glusterfs"

        cls.block_sc = cls.storage_classes.get(
            'storage_class2', cls.storage_classes.get('block_storage_class'))
        cls.block_secret_type = "gluster.org/glusterblock"

    def create_storage_class(self, sc_type, secret_name=None,
                             sc_name_prefix="autotests-sc",
                             sc_name=None,
                             create_vol_name_prefix=False,
                             vol_name_prefix=None,
                             allow_volume_expansion=False,
                             reclaim_policy="Delete",
                             set_hacount=None,
                             clusterid=None,
                             hacount=None,
                             is_arbiter_vol=False, arbiter_avg_file_size=None,
                             heketi_zone_checking=None, volumeoptions=None,
                             skip_cleanup=False):

        if sc_type == "file":
            self.sc = self.file_sc
            self.secret_type = self.file_secret_type
            provisioner = "kubernetes.io/glusterfs"
        elif sc_type == "block":
            self.sc = self.block_sc
            self.secret_type = self.block_secret_type
            provisioner = self.get_block_provisioner_for_sc()
        else:
            msg = (
                "Only file or block sc_type is supported. But got {}.".format(
                    sc_type))
            raise AssertionError(msg)

        return self._create_storage_class(
            provisioner=provisioner, secret_name=secret_name,
            sc_name_prefix=sc_name_prefix, sc_name=sc_name,
            create_vol_name_prefix=create_vol_name_prefix,
            vol_name_prefix=vol_name_prefix,
            allow_volume_expansion=allow_volume_expansion,
            reclaim_policy=reclaim_policy, set_hacount=set_hacount,
            clusterid=clusterid, hacount=hacount,
            is_arbiter_vol=is_arbiter_vol,
            arbiter_avg_file_size=arbiter_avg_file_size,
            heketi_zone_checking=heketi_zone_checking,
            volumeoptions=volumeoptions, skip_cleanup=skip_cleanup)

    def create_pvcs_in_batch(
            self, sc_name, pvc_count, batch_amount=8,
            pvc_name_prefix='auto-scale-pvc', timeout=300, wait_step=5,
            skip_cleanup=False):
        """Create PVC's in batches.

        Args:
            sc_name(str): Name of the storage class.
            pvc_count(int): Count of PVC's to be create.
            batch_amount(int): Amount of PVC's to be create in one batch.
            pvc_name_prefix(str): Name prefix for PVC's.
            timeout (int): timeout for one batch
            wait_step ( int): wait step
            skip_cleanup (bool): skip cleanup or not.

        Returns:
            list: list of PVC's
        """
        pvcs = []

        while pvc_count > 0:
            # Change batch_amount if pvc_count < batch_amount
            if pvc_count < batch_amount:
                batch_amount = pvc_count
            # Create PVC's
            pvcs += self.create_and_wait_for_pvcs(
                sc_name=sc_name, pvc_amount=batch_amount,
                pvc_name_prefix=pvc_name_prefix, timeout=timeout,
                wait_step=wait_step, skip_cleanup=skip_cleanup)
            pvc_count -= batch_amount

        return pvcs

    def create_app_pods_in_batch(
            self, pvcs, pod_count, batch_amount=5,
            dc_name_prefix='auto-scale-dc', label={'scale': 'scale'},
            timeout=300, wait_step=5, skip_cleanup=False):
        """Create App pods in batches.

        Args:
            pvcs (list): List of PVC's for which app pods needs to be created.
            pod_count (int): Count of app pods needs to be created.
            batch_amount (int): Amount of POD's to be create in one batch.
            dc_name_prefix (str): Name prefix for deployement config.
            lable (dict): keys and value for adding label into DC.
            timeout (int): timeout for one batch
            wait_step ( int): wait step
            skip_cleanup (bool): skip cleanup or not.

        Returns:
            dict: dict of DC's as key.
        """
        index, dcs = 0, {}

        if pod_count > len(pvcs):
            raise AssertionError(
                "Pod count {} should be less or equal to PVC's len {}.".format(
                    pod_count, len(pvcs)))

        while pod_count > 0:
            # Change batch_amount if pod_count < 5
            if pod_count < batch_amount:
                batch_amount = pod_count
            # Create DC's
            dcs.update(
                self.create_dcs_with_pvc(
                    pvc_names=pvcs[index:index + batch_amount],
                    dc_name_prefix=dc_name_prefix, label=label,
                    timeout=timeout, wait_step=wait_step,
                    skip_cleanup=self.skip_cleanup))
            index += batch_amount
            pod_count -= batch_amount

        return dcs

    def check_glusterfsd_memory(self):
        faulty_processes = defaultdict(lambda: {})

        for g_node in self.gluster_servers:
            ps_info = get_process_info_on_gluster_pod_or_node(
                self.ocp_master_node[0], g_node, 'glusterfsd', ['pid', 'rss'])

            for ps in ps_info:
                pid, rss = ps['pid'], ps['rss']

                # If process memory is more than 3 gb
                if int(rss) > 3145728:
                    faulty_processes[g_node][pid] = rss

        msg = (
            "Some of gluster pods or nodes are using more than 3gb for "
            "glusterfsd process. {}".format(faulty_processes))
        self.assertFalse(faulty_processes, msg)

    def check_vol_status(self):
        # Check status of all vols
        status = get_gluster_vol_status('all')

        pids = defaultdict(int)
        down_bricks = 0
        for vol in status.keys():
            for host in status[vol].keys():
                for brick_or_shd in status[vol][host].keys():
                    if status[vol][host][brick_or_shd]['status'] != "1":
                        down_bricks += 1
                    pid = status[vol][host][brick_or_shd]['pid']
                    pids[pid] += 1

        # Get Pids which are running more than 250 bricks and raise exception
        exhausted_pids = [pd for pd in pids.keys() if pids[pd] > 250]

        self.assertFalse(
            (exhausted_pids and down_bricks),
            'Pids {} have more than 250 bricks attached to it. {} bricks or '
            'shd are down.'.format(exhausted_pids, down_bricks))
        self.assertFalse(
            exhausted_pids, 'Pids {} have more than 250 bricks attached to'
            ' it.'.format(exhausted_pids))
        self.assertFalse(
            down_bricks, '{} bricks or shd are down.'.format(down_bricks))

    def verify_pods_are_running(self):
        pods = oc_get_pods(self.ocp_master_node[0], selector='scale=scale')

        faulty_pods = {}
        for pod in pods.keys():
            if not (pods[pod]['ready'] == '1/1'
                    and pods[pod]['status'] == 'Running'):
                faulty_pods[pod] = pods[pod]

        msg = "Out of {} pods {} pods are not running. Pods are {}".format(
            len(pods), len(faulty_pods), faulty_pods)
        self.assertFalse(faulty_pods, msg)

    def verify_if_more_than_n_percentage_pod_restarted(
            self, pods_old, selector='scale=scale', percentage=33):
        # Make sure pods did not got restarted
        pods_new = oc_get_pods(self.ocp_master_node[0], selector=selector)
        pods_restart = []
        for pod in pods_new.keys():
            if pods_new[pod]['restarts'] != pods_old[pod]['restarts']:
                pods_restart += [pod]

        if len(pods_restart) > int(len(pods_new.keys()) / (100 / percentage)):
            msg = "Out of {} pods {} pods restarted {}".format(
                len(pods_new), len(pods_restart), pods_restart)
            raise AssertionError(msg)
