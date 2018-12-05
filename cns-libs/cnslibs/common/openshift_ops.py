"""Library for openshift operations.

Various utility functions for interacting with OCP/OpenShift.
"""

import base64
import json
import re
import types

from glusto.core import Glusto as g
from glustolibs.gluster import volume_ops
from glustolibs.gluster.brick_libs import (
    are_bricks_online,
    get_all_bricks,
    get_online_bricks_list)
import mock
import yaml

from cnslibs.common import command
from cnslibs.common import exceptions
from cnslibs.common import podcmd
from cnslibs.common import utils
from cnslibs.common import waiter
from cnslibs.common.heketi_ops import (
    heketi_blockvolume_info,
    heketi_volume_info,
)

PODS_WIDE_RE = re.compile(
    r'(\S+)\s+(\S+)\s+(\w+)\s+(\d+)\s+(\S+)\s+(\S+)\s+(\S+).*\n')
SERVICE_STATUS = "systemctl status %s"
SERVICE_RESTART = "systemctl restart %s"
SERVICE_STATUS_REGEX = r"Active: active \((.*)\) since .*;.*"
OC_VERSION = None


def oc_get_pods(ocp_node):
    """Gets the pods info with 'wide' option in the current project.

    Args:
        ocp_node (str): Node in which ocp command will be executed.

    Returns:
        dict : dict of pods info in the current project.
    """

    cmd = "oc get -o wide --no-headers=true pods"
    ret, out, err = g.run(ocp_node, cmd)
    if ret != 0:
        g.log.error("Failed to get ocp pods on node %s" % ocp_node)
        raise AssertionError('failed to get pods: %r' % (err,))
    return _parse_wide_pods_output(out)


def _parse_wide_pods_output(output):
    """Parse the output of `oc get -o wide pods`.
    """
    # Interestingly, the output of get pods is "cooked" in such a way that
    # the values in the ready, status, & restart fields are not accessible
    # from YAML/JSON/templating forcing us to scrape the output for
    # these values
    # (at the time of this writing the logic is in
    #  printPodBase in kubernetes/pkg/printers/internalversion/printers.go )
    # Possibly obvious, but if you don't need those values you can
    # use the YAML output directly.
    #
    # TODO: Add unit tests for this parser
    pods_info = {}
    for each_pod_info in PODS_WIDE_RE.findall(output):
        pods_info[each_pod_info[0]] = {
            'ready': each_pod_info[1],
            'status': each_pod_info[2],
            'restarts': each_pod_info[3],
            'age': each_pod_info[4],
            'ip': each_pod_info[5],
            'node': each_pod_info[6],
        }
    return pods_info


def oc_get_pods_full(ocp_node):
    """Gets all the pod info via YAML in the current project.

    Args:
        ocp_node (str): Node in which ocp command will be executed.

    Returns:
        dict: The YAML output converted to python objects
            (a top-level dict)
    """

    cmd = "oc get -o yaml pods"
    ret, out, err = g.run(ocp_node, cmd)
    if ret != 0:
        g.log.error("Failed to get ocp pods on node %s" % ocp_node)
        raise AssertionError('failed to get pods: %r' % (err,))
    return yaml.load(out)


def get_ocp_gluster_pod_names(ocp_node):
    """Gets the gluster pod names in the current project.

    Args:
        ocp_node (str): Node in which ocp command will be executed.

    Returns:
        list : list of gluster pod names in the current project.
            Empty list, if there are no gluster pods.

    Example:
        get_ocp_gluster_pod_names(ocp_node)
    """

    pod_names = oc_get_pods(ocp_node).keys()
    return [pod for pod in pod_names if pod.startswith('glusterfs-')]


def oc_login(ocp_node, username, password):
    """Login to ocp master node.

    Args:
        ocp_node (str): Node in which ocp command will be executed.
        username (str): username of ocp master node to login.
        password (str): password of ocp master node to login.

    Returns:
        bool : True on successful login to ocp master node.
            False otherwise

    Example:
        oc_login(ocp_node, "test","test")
    """

    cmd = "oc login --username=%s --password=%s" % (username, password)
    ret, _, _ = g.run(ocp_node, cmd)
    if ret != 0:
        g.log.error("Failed to login to ocp master node %s" % ocp_node)
        return False
    return True


def switch_oc_project(ocp_node, project_name):
    """Switch to the given project.

    Args:
        ocp_node (str): Node in which ocp command will be executed.
        project_name (str): Project name.
    Returns:
        bool : True on switching to given project.
            False otherwise

    Example:
        switch_oc_project(ocp_node, "storage-project")
    """

    cmd = "oc project %s" % project_name
    ret, _, _ = g.run(ocp_node, cmd)
    if ret != 0:
        g.log.error("Failed to switch to project %s" % project_name)
        return False
    return True


def oc_rsync(ocp_node, pod_name, src_dir_path, dest_dir_path):
    """Sync file from 'src_dir_path' path on ocp_node to
       'dest_dir_path' path on 'pod_name' using 'oc rsync' command.

    Args:
        ocp_node (str): Node on which oc rsync command will be executed
        pod_name (str): Name of the pod on which source directory to be
                        mounted
        src_dir_path (path): Source path from which directory to be mounted
        dest_dir_path (path): destination path to which directory to be
                              mounted
    """
    ret, out, err = g.run(ocp_node, ['oc',
                                     'rsync',
                                     src_dir_path,
                                     '%s:%s' % (pod_name, dest_dir_path)])
    if ret != 0:
        error_msg = 'failed to sync directory in pod: %r; %r' % (out, err)
        g.log.error(error_msg)
        raise AssertionError(error_msg)


def oc_rsh(ocp_node, pod_name, command, log_level=None):
    """Run a command in the ocp pod using `oc rsh`.

    Args:
        ocp_node (str): Node on which oc rsh command will be executed.
        pod_name (str): Name of the pod on which the command will
            be executed.
        command (str|list): command to run.
        log_level (str|None): log level to be passed to glusto's run
            method.

    Returns:
        A tuple consisting of the command return code, stdout, and stderr.
    """
    prefix = ['oc', 'rsh', pod_name]
    if isinstance(command, types.StringTypes):
        cmd = ' '.join(prefix + [command])
    else:
        cmd = prefix + command

    # unpack the tuple to make sure our return value exactly matches
    # our docstring
    ret, stdout, stderr = g.run(ocp_node, cmd, log_level=log_level)
    return (ret, stdout, stderr)


def oc_create(ocp_node, value, value_type='file'):
    """Create a resource based on the contents of the given file name.

    Args:
        ocp_node (str): Node on which the ocp command will run
        value (str): Filename (on remote) or file data
            to be passed to oc create command.
        value_type (str): either 'file' or 'stdin'.
    Raises:
        AssertionError: Raised when resource fails to create.
    """
    if value_type == 'file':
        cmd = ['oc', 'create', '-f', value]
    else:
        cmd = ['echo', '\'%s\'' % value, '|', 'oc', 'create', '-f', '-']
    ret, out, err = g.run(ocp_node, cmd)
    if ret != 0:
        msg = 'Failed to create resource: %r; %r' % (out, err)
        g.log.error(msg)
        raise AssertionError(msg)
    g.log.info('Created resource from %s.' % value_type)


def oc_process(ocp_node, params, filename):
    """Create a resource template based on the contents of the
       given filename and params provided.
    Args:
        ocp_node (str): Node on which the ocp command will run
        filename (str): Filename (on remote) to be passed to
                        oc process command.
    Returns: template generated through process command
    Raises:
        AssertionError: Raised when resource fails to create.
    """

    ret, out, err = g.run(ocp_node, ['oc', 'process', '-f', filename, params])
    if ret != 0:
        error_msg = 'failed to create process: %r; %r' % (out, err)
        g.log.error(error_msg)
        raise AssertionError(error_msg)
    g.log.info('Created resource from file (%s)', filename)

    return out


def oc_create_secret(hostname, secret_name_prefix="autotests-secret-",
                     namespace="default",
                     data_key="password",
                     secret_type="kubernetes.io/glusterfs"):
    """Create secret using data provided as stdin input.

    Args:
        hostname (str): Node on which 'oc create' command will be executed.
        secret_name_prefix (str): secret name will consist of this prefix and
                                  random str.
        namespace (str): name of a namespace to create a secret in
        data_key (str): plain text value for secret which will be transformed
                        into base64 string automatically.
        secret_type (str): type of the secret, which will be created.
    Returns: name of a secret
    """
    secret_name = "%s-%s" % (secret_name_prefix, utils.get_random_str())
    secret_data = json.dumps({
        "apiVersion": "v1",
        "data": {"key": base64.b64encode(data_key)},
        "kind": "Secret",
        "metadata": {
            "name": secret_name,
            "namespace": namespace,
        },
        "type": secret_type,
    })
    oc_create(hostname, secret_data, 'stdin')
    return secret_name


def oc_create_sc(hostname, sc_name_prefix="autotests-sc",
                 provisioner="kubernetes.io/glusterfs",
                 allow_volume_expansion=False,
                 reclaim_policy="Delete", **parameters):
    """Create storage class using data provided as stdin input.

    Args:
        hostname (str): Node on which 'oc create' command will be executed.
        sc_name_prefix (str): sc name will consist of this prefix and
                              random str.
        provisioner (str): name of the provisioner
        allow_volume_expansion (bool): Set it to True if need to allow
                                       volume expansion.
    Kvargs:
        All the keyword arguments are expected to be key and values of
        'parameters' section for storage class.
    """
    allowed_parameters = (
        'resturl', 'secretnamespace', 'restuser', 'secretname',
        'restauthenabled', 'restsecretnamespace', 'restsecretname',
        'hacount', 'clusterids', 'chapauthenabled', 'volumenameprefix',
        'volumeoptions', 'volumetype'
    )
    for parameter in parameters.keys():
        if parameter.lower() not in allowed_parameters:
            parameters.pop(parameter)
    sc_name = "%s-%s" % (sc_name_prefix, utils.get_random_str())
    sc_data = json.dumps({
        "kind": "StorageClass",
        "apiVersion": "storage.k8s.io/v1",
        "metadata": {"name": sc_name},
        "provisioner": provisioner,
        "reclaimPolicy": reclaim_policy,
        "parameters": parameters,
        "allowVolumeExpansion": allow_volume_expansion,
    })
    oc_create(hostname, sc_data, 'stdin')
    return sc_name


def oc_create_pvc(hostname, sc_name=None, pvc_name_prefix="autotests-pvc",
                  pvc_size=1):
    """Create PVC using data provided as stdin input.

    Args:
        hostname (str): Node on which 'oc create' command will be executed.
        sc_name (str): name of a storage class to create PVC in.
        pvc_name_prefix (str): PVC name will consist of this prefix and
                               random str.
        pvc_size (int/str): size of PVC in Gb
    """
    pvc_name = "%s-%s" % (pvc_name_prefix, utils.get_random_str())
    metadata = {"name": pvc_name}
    if sc_name:
        metadata["annotations"] = {
            "volume.kubernetes.io/storage-class": sc_name,
            "volume.beta.kubernetes.io/storage-class": sc_name,
        }
    pvc_data = json.dumps({
        "kind": "PersistentVolumeClaim",
        "apiVersion": "v1",
        "metadata": metadata,
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "%sGi" % pvc_size}}
        },
    })
    oc_create(hostname, pvc_data, 'stdin')
    return pvc_name


def oc_create_app_dc_with_io(
        hostname, pvc_name, dc_name_prefix="autotests-dc-with-app-io",
        replicas=1, space_to_use=1048576):
    """Create DC with app PODs and attached PVC, constantly running I/O.

    Args:
        hostname (str): Node on which 'oc create' command will be executed.
        pvc_name (str): name of the Persistent Volume Claim to attach to
                        the application PODs where constant I/O will run.
        dc_name_prefix (str): DC name will consist of this prefix and
                              random str.
        replicas (int): amount of application POD replicas.
        space_to_use (int): value in bytes which will be used for I/O.
    """
    dc_name = "%s-%s" % (dc_name_prefix, utils.get_random_str())
    container_data = {
        "name": dc_name,
        "image": "cirros",
        "volumeMounts": [{"mountPath": "/mnt", "name": dc_name}],
        "command": ["sh"],
        "args": [
            "-ec",
            "trap \"rm -f /mnt/random-data-$HOSTNAME.log ; exit 0\" SIGTERM; "
            "while true; do "
            " (mount | grep '/mnt') && "
            "  (head -c %s < /dev/urandom > /mnt/random-data-$HOSTNAME.log) ||"
            "   exit 1; "
            " sleep 1 ; "
            "done" % space_to_use,
        ],
        "livenessProbe": {
            "initialDelaySeconds": 3,
            "periodSeconds": 3,
            "exec": {"command": [
                "sh", "-ec",
                "mount | grep '/mnt' && "
                "  head -c 1 < /dev/urandom >> /mnt/random-data-$HOSTNAME.log"
            ]},
        },
    }
    dc_data = json.dumps({
        "kind": "DeploymentConfig",
        "apiVersion": "v1",
        "metadata": {"name": dc_name},
        "spec": {
            "replicas": replicas,
            "triggers": [{"type": "ConfigChange"}],
            "paused": False,
            "revisionHistoryLimit": 2,
            "template": {
                "metadata": {"labels": {"name": dc_name}},
                "spec": {
                    "restartPolicy": "Always",
                    "volumes": [{
                        "name": dc_name,
                        "persistentVolumeClaim": {"claimName": pvc_name},
                    }],
                    "containers": [container_data],
                    "terminationGracePeriodSeconds": 20,
                }
            }
        }
    })
    oc_create(hostname, dc_data, 'stdin')
    return dc_name


def oc_create_tiny_pod_with_volume(hostname, pvc_name, pod_name_prefix='',
                                   mount_path='/mnt'):
    """Create tiny POD from image in 10Mb with attached volume at /mnt"""
    pod_name = "%s-%s" % (pod_name_prefix, utils.get_random_str())
    pod_data = json.dumps({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
        },
        "spec": {
            "terminationGracePeriodSeconds": 20,
            "containers": [{
                "name": pod_name,
                "image": "cirros",  # noqa: 10 Mb! linux image
                "volumeMounts": [{"mountPath": mount_path, "name": "vol"}],
                "command": [
                    "/bin/sh", "-ec",
                    "trap 'exit 0' SIGTERM ; "
                    "while :; do echo '.'; sleep 5 ; done",
                ]
            }],
            "volumes": [{
                "name": "vol",
                "persistentVolumeClaim": {"claimName": pvc_name},
            }],
            "restartPolicy": "Never",
        }
    })
    oc_create(hostname, pod_data, 'stdin')
    return pod_name


def oc_delete(ocp_node, rtype, name, raise_on_absence=True):
    """Delete an OCP resource by name.

    Args:
        ocp_node (str): Node on which the ocp command will run.
        rtype (str): Name of the resource type (pod, storageClass, etc).
        name (str): Name of the resource to delete.
        raise_on_absence (bool): if resource absent raise
                                 exception if value is true,
                                 else return
                                 default value: True
    """
    if not oc_get_yaml(ocp_node, rtype, name,
                       raise_on_error=raise_on_absence):
        return
    cmd = ['oc', 'delete', rtype, name]

    global OC_VERSION
    if not OC_VERSION:
        OC_VERSION = oc_version(ocp_node)

    versions = ['v3.6', 'v3.7', 'v3.9', 'v3.10']
    if not OC_VERSION.rsplit('.', 1)[0] in versions:
        cmd.append('--wait=false')

    ret, out, err = g.run(ocp_node, cmd)
    if ret != 0:
        g.log.error('Failed to delete resource: %s, %s: %r; %r',
                    rtype, name, out, err)
        raise AssertionError('failed to delete resource: %r; %r' % (out, err))
    g.log.info('Deleted resource: %r %r', rtype, name)


def oc_get_custom_resource(ocp_node, rtype, custom, name=None, selector=None,
                           raise_on_error=True):
    """Get an OCP resource by custom column names.

    Args:
        ocp_node (str): Node on which the ocp command will run.
        rtype (str): Name of the resource type (pod, storageClass, etc).
        custom (str): Name of the custom columm to fetch.
        name (str|None): Name of the resource to fetch.
        selector (str|list|None): Column Name or list of column
                                  names select to.
        raise_on_error (bool): If set to true a failure to fetch
            resource inforation will raise an error, otherwise
            an empty dict will be returned.
    Returns:
        list: List containting data about the resource custom column
    Raises:
        AssertionError: Raised when unable to get resource and
            `raise_on_error` is true.
    Example:
        Get all "pvc" with "metadata.name" parameter values:
            pvc_details = oc_get_custom_resource(
                ocp_node, "pvc", ":.metadata.name"
            )
    """
    cmd = ['oc', 'get', rtype, '--no-headers']

    cmd.append('-o=custom-columns=%s' % (
        ','.join(custom) if isinstance(custom, list) else custom))

    if selector:
        cmd.append('--selector %s' % (
            ','.join(selector) if isinstance(selector, list) else selector))

    if name:
        cmd.append(name)

    ret, out, err = g.run(ocp_node, cmd)
    if ret != 0:
        g.log.error('Failed to get %s: %s: %r', rtype, name, err)
        if raise_on_error:
            raise AssertionError('failed to get %s: %s: %r'
                                 % (rtype, name, err))
        return []

    if name:
        return filter(None, map(str.strip, (out.strip()).split(' ')))
    else:
        out_list = []
        for line in (out.strip()).split('\n'):
            out_list.append(filter(None, map(str.strip, line.split(' '))))
        return out_list


def oc_get_yaml(ocp_node, rtype, name=None, raise_on_error=True):
    """Get an OCP resource by name.

    Args:
        ocp_node (str): Node on which the ocp command will run.
        rtype (str): Name of the resource type (pod, storageClass, etc).
        name (str|None): Name of the resource to fetch.
        raise_on_error (bool): If set to true a failure to fetch
            resource inforation will raise an error, otherwise
            an empty dict will be returned.
    Returns:
        dict: Dictionary containting data about the resource
    Raises:
        AssertionError: Raised when unable to get resource and
            `raise_on_error` is true.
    """
    cmd = ['oc', 'get', '-oyaml', rtype]
    if name is not None:
        cmd.append(name)
    ret, out, err = g.run(ocp_node, cmd)
    if ret != 0:
        g.log.error('Failed to get %s: %s: %r', rtype, name, err)
        if raise_on_error:
            raise AssertionError('failed to get %s: %s: %r'
                                 % (rtype, name, err))
        return {}
    return yaml.load(out)


def oc_get_pvc(ocp_node, name):
    """Get information on a persistant volume claim.

    Args:
        ocp_node (str): Node on which the ocp command will run.
        name (str): Name of the PVC.
    Returns:
        dict: Dictionary containting data about the PVC.
    """
    return oc_get_yaml(ocp_node, 'pvc', name)


def oc_get_pv(ocp_node, name):
    """Get information on a persistant volume.

    Args:
        ocp_node (str): Node on which the ocp command will run.
        name (str): Name of the PV.
    Returns:
        dict: Dictionary containting data about the PV.
    """
    return oc_get_yaml(ocp_node, 'pv', name)


def oc_get_all_pvs(ocp_node):
    """Get information on all persistent volumes.

    Args:
        ocp_node (str): Node on which the ocp command will run.
    Returns:
        dict: Dictionary containting data about the PV.
    """
    return oc_get_yaml(ocp_node, 'pv', None)


def create_namespace(hostname, namespace):
    '''
     This function creates namespace
     Args:
         hostname (str): hostname on which we need to
                         create namespace
         namespace (str): project name
     Returns:
         bool: True if successful and if already exists,
               otherwise False
    '''
    cmd = "oc new-project %s" % namespace
    ret, out, err = g.run(hostname, cmd, "root")
    if ret == 0:
        g.log.info("new namespace %s successfully created" % namespace)
        return True
    output = out.strip().split("\n")[0]
    if "already exists" in output:
        g.log.info("namespace %s already exists" % namespace)
        return True
    g.log.error("failed to create namespace %s" % namespace)
    return False


def wait_for_resource_absence(ocp_node, rtype, name,
                              interval=5, timeout=300):
    _waiter = waiter.Waiter(timeout=timeout, interval=interval)
    resource, pv_name = None, None
    for w in _waiter:
        try:
            resource = oc_get_yaml(ocp_node, rtype, name, raise_on_error=True)
        except AssertionError:
            break
    if rtype == 'pvc':
        cmd = "oc get pv -o=custom-columns=:.spec.claimRef.name | grep %s" % (
            name)
        for w in _waiter:
            ret, out, err = g.run(ocp_node, cmd, "root")
            _pv_name = out.strip()
            if _pv_name and not pv_name:
                pv_name = _pv_name
            if ret != 0:
                break
    if w.expired:
        # Gather more info for ease of debugging
        try:
            r_events = get_events(ocp_node, obj_name=name)
        except Exception:
            r_events = '?'
        error_msg = (
            "%s '%s' still exists after waiting for it %d seconds.\n"
            "Resource info: %s\n"
            "Resource related events: %s" % (
                rtype, name, timeout, resource, r_events))
        if rtype == 'pvc' and pv_name:
            try:
                pv_events = get_events(ocp_node, obj_name=pv_name)
            except Exception:
                pv_events = '?'
            error_msg += "\nPV events: %s" % pv_events

        g.log.error(error_msg)
        raise exceptions.ExecutionError(error_msg)


def scale_dc_pod_amount_and_wait(hostname, dc_name,
                                 pod_amount=1, namespace=None):
    """Scale amount of PODs for a DC.

    If pod_amount is 0, then wait for it's absence.
    If pod_amount => 1, then wait for all of a DC PODs to be ready.

    Args:
        hostname (str): Node on which the ocp command will run
        dc_name (str): Name of heketi dc
        pod_amount (int): Number of PODs to scale. Default is 1.
        namespace (str): Namespace of a DC.
    """
    namespace_arg = "--namespace=%s" % namespace if namespace else ""
    scale_cmd = "oc scale --replicas=%d dc/%s %s" % (
        pod_amount, dc_name, namespace_arg)
    command.cmd_run(scale_cmd, hostname=hostname)

    pod_names = get_pod_names_from_dc(hostname, dc_name)
    for pod_name in pod_names:
        if pod_amount == 0:
            wait_for_resource_absence(hostname, 'pod', pod_name)
        else:
            wait_for_pod_be_ready(hostname, pod_name)
    return pod_names


def get_gluster_pod_names_by_pvc_name(ocp_node, pvc_name):
    """Get Gluster POD names, whose nodes store bricks for specified PVC.

    Args:
        ocp_node (str): Node to execute OCP commands on.
        pvc_name (str): Name of a PVC to get related Gluster PODs.
    Returns:
        list: List of dicts, which consist of following 3 key-value pairs:
            pod_name=<pod_name_value>,
            host_name=<host_name_value>,
            host_ip=<host_ip_value>
    """
    # Check storage provisioner
    sp_cmd = (
        r'oc get pvc %s --no-headers -o=custom-columns='
        r':.metadata.annotations."volume\.beta\.kubernetes\.io\/'
        r'storage\-provisioner"' % pvc_name)
    sp_raw = command.cmd_run(sp_cmd, hostname=ocp_node)
    sp = sp_raw.strip()

    # Get node IPs
    if sp == "kubernetes.io/glusterfs":
        pv_info = get_gluster_vol_info_by_pvc_name(ocp_node, pvc_name)
        gluster_pod_nodes_ips = [
            brick["name"].split(":")[0]
            for brick in pv_info["bricks"]["brick"]
        ]
    elif sp == "gluster.org/glusterblock":
        get_gluster_pod_node_ip_cmd = (
            r"""oc get pv --template '{{range .items}}""" +
            r"""{{if eq .spec.claimRef.name "%s"}}""" +
            r"""{{.spec.iscsi.targetPortal}}{{" "}}""" +
            r"""{{.spec.iscsi.portals}}{{end}}{{end}}'""") % (
                pvc_name)
        node_ips_raw = command.cmd_run(
            get_gluster_pod_node_ip_cmd, hostname=ocp_node)
        node_ips_raw = node_ips_raw.replace(
            "[", " ").replace("]", " ").replace(",", " ")
        gluster_pod_nodes_ips = [
            s.strip() for s in node_ips_raw.split(" ") if s.strip()
        ]
    else:
        assert False, "Unexpected storage provisioner: %s" % sp

    # Get node names
    get_node_names_cmd = (
        "oc get node -o wide | grep -e '%s ' | awk '{print $1}'" % (
            " ' -e '".join(gluster_pod_nodes_ips)))
    gluster_pod_node_names = command.cmd_run(
        get_node_names_cmd, hostname=ocp_node)
    gluster_pod_node_names = [
        node_name.strip()
        for node_name in gluster_pod_node_names.split("\n")
        if node_name.strip()
    ]
    node_count = len(gluster_pod_node_names)
    err_msg = "Expected more than one node hosting Gluster PODs. Got '%s'." % (
        node_count)
    assert (node_count > 1), err_msg

    # Get Gluster POD names which are located on the filtered nodes
    get_pod_name_cmd = (
        "oc get pods --all-namespaces "
        "-o=custom-columns=:.metadata.name,:.spec.nodeName,:.status.hostIP | "
        "grep 'glusterfs-' | grep -e '%s '" % "' -e '".join(
            gluster_pod_node_names)
    )
    out = command.cmd_run(
        get_pod_name_cmd, hostname=ocp_node)
    data = []
    for line in out.split("\n"):
        pod_name, host_name, host_ip = [
            el.strip() for el in line.split(" ") if el.strip()]
        data.append({
            "pod_name": pod_name,
            "host_name": host_name,
            "host_ip": host_ip,
        })
    pod_count = len(data)
    err_msg = "Expected 3 or more Gluster PODs to be found. Actual is '%s'" % (
        pod_count)
    assert (pod_count > 2), err_msg
    return data


def cmd_run_on_gluster_pod_or_node(ocp_client_node, cmd):
    """Run shell command on either Gluster POD or Gluster node.

    Args:
        ocp_client_node (str): Node to execute OCP commands on.
        cmd (str): shell command to run.
    Returns:
        Output of a shell command as string object.
    """
    # Containerized Glusterfs
    gluster_pods = get_ocp_gluster_pod_names(ocp_client_node)
    if gluster_pods:
        pod_cmd = "oc exec %s -- %s" % (gluster_pods[0], cmd)
        return command.cmd_run(pod_cmd, hostname=ocp_client_node)

    # Standalone Glusterfs
    for g_host in g.config.get("gluster_servers", {}).keys():
        try:
            return command.cmd_run(cmd, hostname=g_host)
        except Exception as e:
            g.log.error(
                "Failed to run '%s' command on '%s' Gluster node. "
                "Error: %s" % (cmd, g_host,  e))

    raise exceptions.ConfigError(
        "Haven't found neither Gluster PODs nor Gluster nodes.")


def get_gluster_vol_info_by_pvc_name(ocp_node, pvc_name):
    """Get Gluster volume info based on the PVC name.

    Args:
        ocp_node (str): Node to execute OCP commands on.
        pvc_name (str): Name of a PVC to get bound Gluster volume info.
    Returns:
        dict: Dictionary containting data about a Gluster volume.
    """

    # Get PV ID from PVC
    get_pvc_cmd = "oc get pvc %s -o=custom-columns=:.spec.volumeName" % (
        pvc_name)
    pv_name = command.cmd_run(get_pvc_cmd, hostname=ocp_node)
    assert pv_name, "PV name should not be empty: '%s'" % pv_name

    # Get volume ID from PV
    get_pv_cmd = "oc get pv %s -o=custom-columns=:.spec.glusterfs.path" % (
        pv_name)
    vol_id = command.cmd_run(get_pv_cmd, hostname=ocp_node)
    assert vol_id, "Gluster volume ID should not be empty: '%s'" % vol_id

    vol_info_cmd = "gluster v info %s --xml" % vol_id
    vol_info = cmd_run_on_gluster_pod_or_node(ocp_node, vol_info_cmd)

    # Parse XML output to python dict
    with mock.patch('glusto.core.Glusto.run', return_value=(0, vol_info, '')):
        vol_info = volume_ops.get_volume_info(vol_id)
        vol_info = vol_info[list(vol_info.keys())[0]]
        vol_info["gluster_vol_id"] = vol_id
        return vol_info


def get_gluster_blockvol_info_by_pvc_name(ocp_node, heketi_server_url,
                                          pvc_name):
    """Get Gluster block volume info based on the PVC name.

    Args:
        ocp_node (str): Node to execute OCP commands on.
        heketi_server_url (str): Heketi server url
        pvc_name (str): Name of a PVC to get bound Gluster block volume info.
    Returns:
        dict: Dictionary containting data about a Gluster block volume.
    """

    # Get block volume Name and ID from PV which is bound to our PVC
    get_block_vol_data_cmd = (
        r'oc get pv --no-headers -o custom-columns='
        r':.metadata.annotations.glusterBlockShare,'
        r':.metadata.annotations."gluster\.org\/volume\-id",'
        r':.spec.claimRef.name | grep "%s"' % pvc_name)
    out = command.cmd_run(get_block_vol_data_cmd, hostname=ocp_node)
    parsed_out = filter(None, map(str.strip, out.split(" ")))
    assert len(parsed_out) == 3, "Expected 3 fields in following: %s" % out
    block_vol_name, block_vol_id = parsed_out[:2]

    # Get block hosting volume ID
    block_hosting_vol_id = heketi_blockvolume_info(
        ocp_node, heketi_server_url, block_vol_id, json=True
    )["blockhostingvolume"]

    # Get block hosting volume name by it's ID
    block_hosting_vol_name = heketi_volume_info(
        ocp_node, heketi_server_url, block_hosting_vol_id, json=True)['name']

    # Get Gluster block volume info
    vol_info_cmd = "oc exec %s -- gluster-block info %s/%s --json" % (
        get_ocp_gluster_pod_names(ocp_node)[0],
        block_hosting_vol_name, block_vol_name)

    return json.loads(command.cmd_run(vol_info_cmd, hostname=ocp_node))


def wait_for_pod_be_ready(hostname, pod_name,
                          timeout=1200, wait_step=60):
    '''
     This funciton waits for pod to be in ready state
     Args:
         hostname (str): hostname on which we want to check the pod status
         pod_name (str): pod_name for which we need the status
         timeout (int): timeout value,,
                        default value is 1200 sec
         wait_step( int): wait step,
                          default value is 60 sec
     Returns:
         bool: True if pod status is Running and ready state,
               otherwise Raise Exception
    '''
    for w in waiter.Waiter(timeout, wait_step):
        # command to find pod status and its phase
        cmd = ("oc get pods %s -o=custom-columns="
               ":.status.containerStatuses[0].ready,"
               ":.status.phase") % pod_name
        ret, out, err = g.run(hostname, cmd, "root")
        if ret != 0:
            msg = ("failed to execute cmd %s" % cmd)
            g.log.error(msg)
            raise exceptions.ExecutionError(msg)
        output = out.strip().split()

        # command to find if pod is ready
        if output[0] == "true" and output[1] == "Running":
            g.log.info("pod %s is in ready state and is "
                       "Running" % pod_name)
            return True
        elif output[1] == "Error":
            msg = ("pod %s status error" % pod_name)
            g.log.error(msg)
            raise exceptions.ExecutionError(msg)
        else:
            g.log.info("pod %s ready state is %s,"
                       " phase is %s,"
                       " sleeping for %s sec" % (
                           pod_name, output[0],
                           output[1], wait_step))
            continue
    if w.expired:
        err_msg = ("exceeded timeout %s for waiting for pod %s "
                   "to be in ready state" % (timeout, pod_name))
        g.log.error(err_msg)
        raise exceptions.ExecutionError(err_msg)


def get_pod_names_from_dc(hostname, dc_name, timeout=180, wait_step=3):
    """Return list of POD names by their DC.

    Args:
        hostname (str): hostname on which 'oc' commands will be executed.
        dc_name (str): deployment_confidg name
        timeout (int): timeout value. Default value is 180 sec.
        wait_step( int): Wait step, default value is 3 sec.
    Returns:
         list: list of strings which are POD names
    Raises: exceptions.ExecutionError
    """
    get_replicas_amount_cmd = (
        "oc get dc --no-headers --all-namespaces "
        "-o=custom-columns=:.spec.replicas,:.metadata.name "
        "| grep '%s' | awk '{print $1}'" % dc_name)
    replicas = int(command.cmd_run(
        get_replicas_amount_cmd, hostname=hostname))

    get_pod_names_cmd = (
        "oc get pods --all-namespaces -o=custom-columns=:.metadata.name "
        "--no-headers=true --selector deploymentconfig=%s" % dc_name)
    for w in waiter.Waiter(timeout, wait_step):
        out = command.cmd_run(get_pod_names_cmd, hostname=hostname)
        pod_names = [o.strip() for o in out.split('\n') if o.strip()]
        if len(pod_names) != replicas:
            continue
        g.log.info(
            "POD names for '%s' DC are '%s'. "
            "Expected amount of PODs is '%s'.", dc_name, out, replicas)
        return pod_names
    if w.expired:
        err_msg = ("Exceeded %s sec timeout waiting for PODs to appear "
                   "in amount of %s." % (timeout, replicas))
        g.log.error(err_msg)
        raise exceptions.ExecutionError(err_msg)


def get_pod_name_from_dc(hostname, dc_name, timeout=180, wait_step=3):
    return get_pod_names_from_dc(
        hostname, dc_name, timeout=timeout, wait_step=wait_step)[0]


def get_pvc_status(hostname, pvc_name):
    '''
     This function verifies the if pod is running
     Args:
         hostname (str): hostname on which we want
                         to check the pvc status
         pvc_name (str): pod_name for which we
                         need the status
     Returns:
         bool, status (str): True, status of pvc
               otherwise False, error message.
    '''
    cmd = "oc get pvc | grep %s | awk '{print $2}'" % pvc_name
    ret, out, err = g.run(hostname, cmd, "root")
    if ret != 0:
        g.log.error("failed to execute cmd %s" % cmd)
        return False, err
    output = out.strip().split("\n")[0].strip()
    return True, output


def verify_pvc_status_is_bound(hostname, pvc_name, timeout=120, wait_step=3):
    """Verify that PVC gets 'Bound' status in required time.

    Args:
        hostname (str): hostname on which we will execute oc commands
        pvc_name (str): name of PVC to check status of
        timeout (int): total time in seconds we are ok to wait
                       for 'Bound' status of a PVC
        wait_step (int): time in seconds we will sleep before checking a PVC
                         status again.
    Returns: None
    Raises: exceptions.ExecutionError in case of errors.
    """
    pvc_not_found_counter = 0
    for w in waiter.Waiter(timeout, wait_step):
        ret, output = get_pvc_status(hostname, pvc_name)
        if ret is not True:
            msg = ("Failed to execute 'get' command for '%s' PVC. "
                   "Got following responce: %s" % (pvc_name, output))
            g.log.error(msg)
            raise exceptions.ExecutionError(msg)
        if output == "":
            g.log.info("PVC '%s' not found, sleeping for %s "
                       "sec." % (pvc_name, wait_step))
            if pvc_not_found_counter > 0:
                msg = ("PVC '%s' has not been found 2 times already. "
                       "Make sure you provided correct PVC name." % pvc_name)
            else:
                pvc_not_found_counter += 1
                continue
        elif output == "Pending":
            g.log.info("PVC '%s' is in Pending state, sleeping for %s "
                       "sec" % (pvc_name, wait_step))
            continue
        elif output == "Bound":
            g.log.info("PVC '%s' is in Bound state." % pvc_name)
            return pvc_name
        elif output == "Error":
            msg = "PVC '%s' is in 'Error' state." % pvc_name
            g.log.error(msg)
        else:
            msg = "PVC %s has different status - %s" % (pvc_name, output)
            g.log.error(msg)
        if msg:
            raise AssertionError(msg)
    if w.expired:
        # for debug purpose to see why provisioning failed
        cmd = "oc describe pvc %s | grep ProvisioningFailed" % pvc_name
        ret, out, err = g.run(hostname, cmd, "root")
        g.log.info("cmd %s - out- %s err- %s" % (cmd, out, err))
        msg = ("Exceeded timeout of '%s' seconds for verifying PVC '%s' "
               "to reach the 'Bound' status." % (timeout, pvc_name))
        g.log.error(msg)
        raise AssertionError(msg)


def oc_version(hostname):
    '''
     Get Openshift version from oc version command
     Args:
        hostname (str): Node on which the ocp command will run.
     Returns:
        str : oc version if successful,
              otherwise raise Exception
    '''
    cmd = "oc version | grep openshift | cut -d ' ' -f 2"
    ret, out, err = g.run(hostname, cmd, "root")
    if ret != 0:
        msg = ("failed to get oc version err %s; out %s" % (err, out))
        g.log.error(msg)
        raise AssertionError(msg)
    if not out:
        error_msg = "Empty string found for oc version"
        g.log.error(error_msg)
        raise exceptions.ExecutionError(error_msg)

    return out.strip()


def resize_pvc(hostname, pvc_name, size):
    '''
     Resize PVC
     Args:
         hostname (str): hostname on which we want
                         to edit the pvc status
         pvc_name (str): pod_name for which we
                         edit the storage capacity
         size (int): size of pvc to change
     Returns:
         bool: True, if successful
               otherwise raise Exception
    '''
    cmd = ("oc patch  pvc %s "
           "-p='{\"spec\": {\"resources\": {\"requests\": "
           "{\"storage\": \"%dGi\"}}}}'" % (pvc_name, size))
    ret, out, err = g.run(hostname, cmd, "root")
    if ret != 0:
        error_msg = ("failed to execute cmd %s "
                     "out- %s err %s" % (cmd, out, err))
        g.log.error(error_msg)
        raise exceptions.ExecutionError(error_msg)

    g.log.info("successfully edited storage capacity"
               "of pvc %s . out- %s" % (pvc_name, out))
    return True


def verify_pvc_size(hostname, pvc_name, size,
                    timeout=120, wait_step=5):
    '''
     Verify size of PVC
     Args:
         hostname (str): hostname on which we want
                         to verify the size of pvc
         pvc_name (str): pvc_name for which we
                         verify its size
         size (int): size of pvc
         timeout (int): timeout value,
                        verifies the size after wait_step
                        value till timeout
                        default value is 120 sec
         wait_step( int): wait step,
                          default value is 5 sec
     Returns:
         bool: True, if successful
               otherwise raise Exception
    '''
    cmd = ("oc get pvc %s -o=custom-columns="
           ":.spec.resources.requests.storage,"
           ":.status.capacity.storage" % pvc_name)
    for w in waiter.Waiter(timeout, wait_step):
        sizes = command.cmd_run(cmd, hostname=hostname).split()
        spec_size = int(sizes[0].replace("Gi", ""))
        actual_size = int(sizes[1].replace("Gi", ""))
        if spec_size == actual_size == size:
            g.log.info("verification of pvc %s of size %d "
                       "successful" % (pvc_name, size))
            return True
        else:
            g.log.info("sleeping for %s sec" % wait_step)
            continue

    err_msg = ("verification of pvc %s size of %d failed -"
               "spec_size- %d actual_size %d" % (
                   pvc_name, size, spec_size, actual_size))
    g.log.error(err_msg)
    raise AssertionError(err_msg)


def verify_pv_size(hostname, pv_name, size,
                   timeout=120, wait_step=5):
    '''
     Verify size of PV
     Args:
         hostname (str): hostname on which we want
                         to verify the size of pv
         pv_name (str): pv_name for which we
                         verify its size
         size (int): size of pv
         timeout (int): timeout value,
                        verifies the size after wait_step
                        value till timeout
                        default value is 120 sec
         wait_step( int): wait step,
                          default value is 5 sec
     Returns:
         bool: True, if successful
               otherwise raise Exception
    '''
    cmd = ("oc get pv %s -o=custom-columns=:."
           "spec.capacity.storage" % pv_name)
    for w in waiter.Waiter(timeout, wait_step):
        pv_size = command.cmd_run(cmd, hostname=hostname).split()[0]
        pv_size = int(pv_size.replace("Gi", ""))
        if pv_size == size:
            g.log.info("verification of pv %s of size %d "
                       "successful" % (pv_name, size))
            return True
        else:
            g.log.info("sleeping for %s sec" % wait_step)
            continue

    err_msg = ("verification of pv %s size of %d failed -"
               "pv_size- %d" % (pv_name, size, pv_size))
    g.log.error(err_msg)
    raise AssertionError(err_msg)


def get_pv_name_from_pvc(hostname, pvc_name):
    '''
     Returns PV name of the corresponding PVC name
     Args:
         hostname (str): hostname on which we want
                         to find pv name
         pvc_name (str): pvc_name for which we
                         want to find corresponding
                         pv name
     Returns:
         pv_name (str): pv name if successful,
                        otherwise raise Exception
    '''
    cmd = ("oc get pvc %s -o=custom-columns=:."
           "spec.volumeName" % pvc_name)
    pv_name = command.cmd_run(cmd, hostname=hostname)
    g.log.info("pv name is %s for pvc %s" % (
                   pv_name, pvc_name))

    return pv_name


def get_vol_names_from_pv(hostname, pv_name):
    '''
     Returns the heketi and gluster
     vol names of the corresponding PV
     Args:
         hostname (str): hostname on which we want
                         to find vol names
         pv_name (str): pv_name for which we
                        want to find corresponding
                        vol names
     Returns:
         volname (dict): dict if successful
                      {"heketi_vol": heketi_vol_name,
                       "gluster_vol": gluster_vol_name
                    ex: {"heketi_vol": " xxxx",
                         "gluster_vol": "vol_xxxx"]
                    otherwise raise Exception
    '''
    vol_dict = {}
    cmd = (r"oc get pv %s -o=custom-columns="
           r":.metadata.annotations."
           r"'gluster\.kubernetes\.io\/heketi\-volume\-id',"
           r":.spec.glusterfs.path" % pv_name)
    vol_list = command.cmd_run(cmd, hostname=hostname).split()
    vol_dict = {"heketi_vol": vol_list[0],
                "gluster_vol": vol_list[1]}
    g.log.info("gluster vol name is %s and heketi vol name"
               " is %s for pv %s"
               % (vol_list[1], vol_list[0], pv_name))
    return vol_dict


@podcmd.GlustoPod()
def verify_brick_count_gluster_vol(hostname, brick_count,
                                   gluster_vol):
    '''
     Verify brick count for gluster volume
     Args:
         hostname (str): hostname on which we want
                         to check brick count
         brick_count (int): integer value to verify
         gluster_vol (str): gluster vol name
     Returns:
         bool: True, if successful
               otherwise raise Exception
    '''
    gluster_pod = get_ocp_gluster_pod_names(hostname)[1]
    p = podcmd.Pod(hostname, gluster_pod)
    out = get_online_bricks_list(p, gluster_vol)
    if brick_count == len(out):
        g.log.info("successfully verified brick count %s "
                   "for vol %s" % (brick_count, gluster_vol))
        return True
    err_msg = ("verification of brick count %s for vol %s"
               "failed, count found %s" % (
                   brick_count, gluster_vol, len(out)))
    raise AssertionError(err_msg)


@podcmd.GlustoPod()
def verify_brick_status_online_gluster_vol(hostname,
                                           gluster_vol):
    '''
     Verify if all the bricks are online for the
     gluster volume
     Args:
         hostname (str): hostname on which we want
                         to check brick status
         gluster_vol (str): gluster vol name
     Returns:
         bool: True, if successful
               otherwise raise Exception
    '''
    gluster_pod = get_ocp_gluster_pod_names(hostname)[1]
    p = podcmd.Pod(hostname, gluster_pod)
    brick_list = get_all_bricks(p, gluster_vol)
    if brick_list is None:
        error_msg = ("failed to get brick list for vol"
                     " %s" % gluster_vol)
        g.log.error(error_msg)
        raise exceptions.ExecutionError(error_msg)
    out = are_bricks_online(p, gluster_vol, brick_list)
    if out:
        g.log.info("verification of brick status as online"
                   " for gluster vol %s successful"
                   % gluster_vol)
        return True
    error_msg = ("verification of brick status as online"
                 " for gluster vol %s failed" % gluster_vol)

    g.log.error(error_msg)
    raise exceptions.ExecutionError(error_msg)


def verify_gluster_vol_for_pvc(hostname, pvc_name):
    '''
     Verify gluster volume has been created for
     the corresponding PVC
     Also checks if all the bricks of that gluster
     volume are online
     Args:
         hostname (str): hostname on which we want
                         to find gluster vol
         pvc_name (str): pvc_name for which we
                         want to find corresponding
                         gluster vol
     Returns:
         bool: True if successful
               otherwise raise Exception
    '''
    verify_pvc_status_is_bound(hostname, pvc_name)
    pv_name = get_pv_name_from_pvc(hostname, pvc_name)
    vol_dict = get_vol_names_from_pv(hostname, pv_name)
    gluster_vol = vol_dict["gluster_vol"]
    verify_brick_status_online_gluster_vol(hostname,
                                           gluster_vol)

    g.log.info("verification of gluster vol %s for pvc %s is"
               "successful" % (gluster_vol, pvc_name))
    return True


def get_events(hostname,
               obj_name=None, obj_namespace=None, obj_type=None,
               event_reason=None, event_type=None):
    """Return filtered list of events.

    Args:
        hostname (str): hostname of oc client
        obj_name (str): name of an object
        obj_namespace (str): namespace where object is located
        obj_type (str): type of an object, i.e. PersistentVolumeClaim or Pod
        event_reason (str): reason why event was created,
            i.e. Created, Started, Unhealthy, SuccessfulCreate, Scheduled ...
        event_type (str): type of an event, i.e. Normal or Warning
    Returns:
        List of dictionaries, where the latter are of following structure:
        {
            "involvedObject": {
                "kind": "ReplicationController",
                "name": "foo-object-name",
                "namespace": "foo-object-namespace",
            },
            "message": "Created pod: foo-object-name",
            "metadata": {
                "creationTimestamp": "2018-10-19T18:27:09Z",
                "name": "foo-object-name.155f15db4e72cc2e",
                "namespace": "foo-object-namespace",
            },
            "reason": "SuccessfulCreate",
            "reportingComponent": "",
            "reportingInstance": "",
            "source": {"component": "replication-controller"},
            "type": "Normal"
        }
    """
    field_selector = []
    if obj_name:
        field_selector.append('involvedObject.name=%s' % obj_name)
    if obj_namespace:
        field_selector.append('involvedObject.namespace=%s' % obj_namespace)
    if obj_type:
        field_selector.append('involvedObject.kind=%s' % obj_type)
    if event_reason:
        field_selector.append('reason=%s' % event_reason)
    if event_type:
        field_selector.append('type=%s' % event_type)
    cmd = "oc get events -o yaml --field-selector %s" % ",".join(
        field_selector or "''")
    return yaml.load(command.cmd_run(cmd, hostname=hostname))['items']


def wait_for_events(hostname,
                    obj_name=None, obj_namespace=None, obj_type=None,
                    event_reason=None, event_type=None,
                    timeout=120, wait_step=3):
    """Wait for appearence of specific set of events."""
    for w in waiter.Waiter(timeout, wait_step):
        events = get_events(
            hostname=hostname, obj_name=obj_name, obj_namespace=obj_namespace,
            obj_type=obj_type, event_reason=event_reason,
            event_type=event_type)
        if events:
            return events
    if w.expired:
        err_msg = ("Exceeded %ssec timeout waiting for events." % timeout)
        g.log.error(err_msg)
        raise exceptions.ExecutionError(err_msg)


def match_pvc_and_pv(hostname, prefix):
    """Match OCP PVCs and PVs generated

    Args:
        hostname (str): hostname of oc client
        prefix (str): pv prefix used by user at time
                      of pvc creation
    """
    pvc_list = sorted([
        pvc[0]
        for pvc in oc_get_custom_resource(hostname, "pvc", ":.metadata.name")
        if pvc[0].startswith(prefix)
    ])

    pv_list = sorted([
        pv[0]
        for pv in oc_get_custom_resource(
            hostname, "pv", ":.spec.claimRef.name"
        )
        if pv[0].startswith(prefix)
    ])

    assert pvc_list == pv_list, "PVC and PV list match failed"


def match_pv_and_heketi_block_volumes(
        hostname, heketi_block_volumes, pvc_prefix):
    """Match heketi block volumes and OC PVCs

    Args:
        hostname (str): hostname on which we want to check heketi
                        block volumes and OCP PVCs
        heketi_block_volumes (list): list of heketi block volume names
        pvc_prefix (str): pv prefix given by user at the time of pvc creation
    """
    custom_columns = [
        r':.spec.claimRef.name',
        r':.metadata.annotations."pv\.kubernetes\.io\/provisioned\-by"',
        r':.metadata.annotations."gluster\.org\/volume\-id"'
    ]
    pv_block_volumes = sorted([
        pv[2]
        for pv in oc_get_custom_resource(hostname, "pv", custom_columns)
        if pv[0].startswith(pvc_prefix) and pv[1] == "gluster.org/glusterblock"
    ])

    assert pv_block_volumes == heketi_block_volumes, (
        "PV and Heketi Block list match failed")


def check_service_status(
        hostname, podname, service, status, timeout=180, wait_step=3):
    """Checks provided service to be in "Running" status for given
       timeout on given podname

    Args:
        hostname (str): hostname on which we want to check service
        podname (str): pod name on which service needs to be restarted
        service (str): service which needs to be restarted
        status (str): status to be checked
        timeout (int): seconds to wait before service starts having
                       specified 'status'
        wait_step (int): interval in seconds to wait before checking
                         service again.
    """
    err_msg = ("Exceeded timeout of %s sec for verifying %s service to start "
               "having '%s' status" % (timeout, service, status))

    for w in waiter.Waiter(timeout, wait_step):
        ret, out, err = oc_rsh(hostname, podname, SERVICE_STATUS % service)
        if ret != 0:
            err_msg = ("failed to get service %s's status on pod %s" %
                       (service, podname))
            g.log.error(err_msg)
            raise AssertionError(err_msg)

        for line in out.splitlines():
            status_match = re.search(SERVICE_STATUS_REGEX, line)
            if status_match and status_match.group(1) == status:
                return True

    if w.expired:
        g.log.error(err_msg)
        raise exceptions.ExecutionError(err_msg)


def restart_service_on_pod(hostname, podname, service):
    """Restarts service on podname given

    Args:
        hostname (str): hostname on which we want to restart service
        podname (str): pod name on which service needs to be restarted
        service (str): service which needs to be restarted
    Raises:
        AssertionError in case failed to restarts service
    """
    ret, out, err = oc_rsh(hostname, podname, SERVICE_RESTART % service)
    if ret != 0:
        err_msg = ("failed to restart service %s on pod %s" %
                   (service, podname))
        g.log.error(err_msg)
        raise AssertionError(err_msg)
