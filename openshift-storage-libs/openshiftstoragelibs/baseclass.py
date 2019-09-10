import datetime
import unittest

from glusto.core import Glusto as g

from openshiftstoragelibs import command
from openshiftstoragelibs.exceptions import (
    ConfigError,
    ExecutionError,
)
from openshiftstoragelibs.gluster_ops import (
    get_block_hosting_volume_name,
)
from openshiftstoragelibs.heketi_ops import (
    hello_heketi,
    heketi_blockvolume_delete,
    heketi_blockvolume_info,
    heketi_volume_delete,
)
from openshiftstoragelibs.openshift_ops import (
    get_pod_name_from_dc,
    get_pv_name_from_pvc,
    oc_create_app_dc_with_io,
    oc_create_pvc,
    oc_create_sc,
    oc_create_secret,
    oc_delete,
    oc_get_custom_resource,
    oc_get_pods,
    scale_dcs_pod_amount_and_wait,
    switch_oc_project,
    verify_pvc_status_is_bound,
    wait_for_resource_absence,
)
from openshiftstoragelibs.openshift_storage_libs import (
    get_iscsi_block_devices_by_path,
    get_iscsi_session,
    get_mpath_name_from_device_name,
    validate_multipath_pod,
)
from openshiftstoragelibs.openshift_version import get_openshift_version
from openshiftstoragelibs.waiter import Waiter


class BaseClass(unittest.TestCase):
    """Base class for test classes."""
    ERROR_OR_FAILURE_EXISTS = False
    STOP_ON_FIRST_FAILURE = bool(g.config.get("common", {}).get(
        "stop_on_first_failure", False))

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
        openshift_config = g.config.get("cns", g.config.get("openshift"))
        cls.storage_project_name = openshift_config.get(
            'storage_project_name',
            openshift_config.get('setup', {}).get('cns_project_name'))

        # Initializes heketi config variables
        heketi_config = openshift_config['heketi_config']
        cls.heketi_dc_name = heketi_config['heketi_dc_name']
        cls.heketi_service_name = heketi_config['heketi_service_name']
        cls.heketi_client_node = heketi_config['heketi_client_node']
        cls.heketi_server_url = heketi_config['heketi_server_url']
        cls.heketi_cli_user = heketi_config['heketi_cli_user']
        cls.heketi_cli_key = heketi_config['heketi_cli_key']

        cls.gluster_servers = list(g.config['gluster_servers'].keys())
        cls.gluster_servers_info = g.config['gluster_servers']

        cls.storage_classes = openshift_config['dynamic_provisioning'][
            'storage_classes']
        cls.sc = cls.storage_classes.get(
            'storage_class1', cls.storage_classes.get('file_storage_class'))
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

    def create_secret(self, secret_name_prefix="autotests-secret"):
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
                             sc_name=None,
                             create_vol_name_prefix=False,
                             vol_name_prefix=None,
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
        self.addCleanup(oc_delete, self.ocp_client[0], "sc", self.sc_name)
        return self.sc_name

    def create_and_wait_for_pvcs(self, pvc_size=1,
                                 pvc_name_prefix="autotests-pvc",
                                 pvc_amount=1, sc_name=None,
                                 timeout=120, wait_step=3):
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
                verify_pvc_status_is_bound(node, pvc_name, timeout, wait_step)
        finally:
            if get_openshift_version() < "3.9":
                reclaim_policy = "Delete"
            else:
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
                                        self.heketi_server_url, vol_id,
                                        raise_on_error=False)
                self.addCleanup(oc_delete, node, 'pvc', pvc_name,
                                raise_on_absence=False)

        return pvc_names

    def create_and_wait_for_pvc(self, pvc_size=1,
                                pvc_name_prefix='autotests-pvc', sc_name=None):
        self.pvc_name = self.create_and_wait_for_pvcs(
            pvc_size=pvc_size, pvc_name_prefix=pvc_name_prefix, sc_name=sc_name
        )[0]
        return self.pvc_name

    def create_dcs_with_pvc(self, pvc_names, timeout=600, wait_step=5):
        """Create bunch of DCs with app PODs which use unique PVCs.

        Args:
            pvc_names (str/set/list/tuple): List/set/tuple of PVC names
                to attach to app PODs as part of DCs.
            timeout (int): timeout value, default value is 600 seconds.
            wait_step( int): wait step, default value is 5 seconds.
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
        for pvc_name in pvc_names:
            dc_name = oc_create_app_dc_with_io(self.ocp_client[0], pvc_name)
            dc_names[pvc_name] = dc_name
            self.addCleanup(oc_delete, self.ocp_client[0], 'dc', dc_name)
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

    def create_dc_with_pvc(self, pvc_name, timeout=300, wait_step=10):
        return self.create_dcs_with_pvc(pvc_name, timeout, wait_step)[pvc_name]

    def is_containerized_gluster(self):
        cmd = ("oc get pods --no-headers -l glusterfs-node=pod "
               "-o=custom-columns=:.spec.nodeName")
        g_nodes = command.cmd_run(cmd, self.ocp_client[0])
        g_nodes = g_nodes.split('\n') if g_nodes else g_nodes
        return not not g_nodes

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


class GlusterBlockBaseClass(BaseClass):
    """Base class for gluster-block test cases."""

    @classmethod
    def setUpClass(cls):
        """Initialize all the variables necessary for test cases."""
        super(GlusterBlockBaseClass, cls).setUpClass()
        cls.sc = cls.storage_classes.get(
            'storage_class2', cls.storage_classes.get('block_storage_class'))

    def verify_iscsi_sessions_and_multipath(self, pvc_name, dc_name):
        # Get storage ips of glusterfs pods
        keys = self.gluster_servers
        gluster_ips = []
        for key in keys:
            gluster_ips.append(self.gluster_servers_info[key]['storage'])
        gluster_ips.sort()

        # Find iqn and hacount from volume info
        pv_name = get_pv_name_from_pvc(self.node, pvc_name)
        custom = [r':.metadata.annotations."gluster\.org\/volume\-id"']
        vol_id = oc_get_custom_resource(self.node, 'pv', custom, pv_name)[0]
        vol_info = heketi_blockvolume_info(
            self.heketi_client_node, self.heketi_server_url, vol_id, json=True)
        iqn = vol_info['blockvolume']['iqn']
        hacount = int(vol_info['hacount'])

        # Find node on which pod is running
        pod_name = get_pod_name_from_dc(self.node, dc_name)
        pod_info = oc_get_pods(
            self.node, selector='deploymentconfig=%s' % dc_name)
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
            self.node, pod_name, hacount, mpath=list(mpaths)[0])

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

    def get_block_hosting_volume_by_pvc_name(self, pvc_name):
        """Get block hosting volume of pvc name given

        Args:
            pvc_name (str): pvc name for which the BHV name needs
                            to be returned
        """
        pv_name = get_pv_name_from_pvc(self.node, pvc_name)
        block_volume = oc_get_custom_resource(
            self.node, 'pv',
            r':.metadata.annotations."gluster\.org\/volume\-id"',
            name=pv_name
        )[0]

        # get block hosting volume from block volume
        block_hosting_vol = get_block_hosting_volume_name(
            self.heketi_client_node, self.heketi_server_url, block_volume)

        return block_hosting_vol
