"""Library for openshift operations.

Various utility functions for interacting with OCP/OpenShift.
"""

import base64
try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json
import re

from glusto.core import Glusto as g
from glustolibs.gluster import volume_ops
import mock
import six
import time
import yaml

from openshiftstoragelibs import command
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import openshift_version
from openshiftstoragelibs import utils
from openshiftstoragelibs import waiter
from openshiftstoragelibs.heketi_ops import (
    heketi_blockvolume_info,
    heketi_volume_info,
)

PODS_WIDE_RE = re.compile(
    r'(\S+)\s+(\S+)\s+(\w+)\s+(\d+)\s+(\S+)\s+(\S+)\s+(\S+).*\n')
SERVICE_STATUS = "systemctl status %s"
SERVICE_RESTART = "systemctl restart %s"
SERVICE_STATUS_REGEX = r"Active: (.*) \((.*)\) since .*;.*"
PGREP_SERVICE = "pgrep %s"
KILL_SERVICE = "kill -9 %s"
IS_ACTIVE_SERVICE = "systemctl is-active %s"


def oc_get_pods(ocp_node, selector=None, name=None):
    """Gets the pods info with 'wide' option in the current project.

    Args:
        ocp_node (str): Node in which ocp command will be executed.
        selector (str): optional option. Selector for OCP pods.
            example: "glusterfs-node=pod" for filtering out only Gluster PODs.
        name (str): name of the pod to get details.

    Returns:
        dict : dict of pods info in the current project.
    """

    cmd = "oc get -o wide --no-headers=true pods "
    if selector:
        cmd += " --selector %s" % selector
    if name:
        cmd += name
    out = command.cmd_run(cmd, hostname=ocp_node)
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
    if not output.endswith('\n'):
        output = output + '\n'
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
    out = command.cmd_run(cmd, hostname=ocp_node)
    return yaml.load(out)


def get_ocp_gluster_pod_details(ocp_node):
    """Gets the gluster pod names in the current project.

    Args:
        ocp_node (str): Node in which ocp command will be executed.

    Returns:
        list: List of dicts, which consist of following key-value pairs:
            pod_name=<pod_name_value>,
            pod_host_ip=<host_ip_value>
            pod_ip=<pod_ip_vlaue>
            pod_hostname=<host_name_value>
            pod_status=<pod_status_value>
            pod_restarts=<pod_restart_value>
    """

    pod_columns = [
        ".:metadata.name", ".:status.hostIP", ".:status.podIP",
        ".:spec.nodeName", ".:status.phase",
        ".:status.containerStatuses[0].restartCount"]
    pod_selector = "glusterfs-node=pod"

    gluster_pods = oc_get_custom_resource(
        ocp_node, "pod", pod_columns, selector=pod_selector)

    if not gluster_pods[0]:
        return []

    gluster_pod_details = list(map(
        lambda pod: {
            "pod_name": pod[0],
            "pod_host_ip": pod[1],
            "pod_ip": pod[2],
            "pod_hostname": pod[3],
            "pod_status": pod[4],
            "pod_restarts": pod[5],
        }, gluster_pods
    ))

    return gluster_pod_details


def get_amount_of_gluster_nodes(ocp_node):
    """Calculate amount of Gluster nodes.

    Args:
        ocp_node (str): node to run 'oc' commands on.
    Returns:
        Integer value as amount of either GLuster PODs or Gluster nodes.
    """
    # Containerized Gluster
    gluster_pods = get_ocp_gluster_pod_details(ocp_node)
    if gluster_pods:
        return len(gluster_pods)

    # Standalone Gluster
    configured_gluster_nodes = len(g.config.get("gluster_servers", {}))
    if configured_gluster_nodes:
        return configured_gluster_nodes

    raise exceptions.ConfigError(
        "Haven't found neither Gluster PODs nor Gluster nodes.")


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
    command.cmd_run(cmd, hostname=ocp_node)
    return True


def oc_rsh(ocp_node, pod_name, cmd):
    """Run a command in the ocp pod using `oc rsh`.

    Args:
        ocp_node (str): Node on which oc rsh command will be executed.
        pod_name (str): Name of the pod on which the command will
            be executed.
        cmd (str|list): command to run.
    Returns:
        A tuple consisting of the command return code, stdout, and stderr.
    """
    prefix = ['oc', 'rsh', pod_name]
    if isinstance(cmd, six.string_types):
        cmd = ' '.join(prefix + [cmd])
    else:
        cmd = prefix + cmd
    stdout = command.cmd_run(cmd, hostname=ocp_node)
    return (0, stdout, '')


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
    command.cmd_run(cmd, hostname=ocp_node)
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
    cmd = ['oc', 'process', '-f', filename, params]
    out = command.cmd_run(cmd, hostname=ocp_node)
    g.log.info('Created resource from file (%s)', filename)
    return out


def oc_create_secret(hostname, secret_name_prefix="autotests-secret",
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
    data_key = data_key.encode('utf-8')
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
                 reclaim_policy="Delete", sc_name=None, **parameters):
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
        'hacount', 'clusterids', 'clusterid', 'chapauthenabled',
        'volumenameprefix', 'volumeoptions', 'volumetype'
    )
    for parameter in parameters.keys():
        if parameter.lower() not in allowed_parameters:
            parameters.pop(parameter)
    if not sc_name:
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
                  pvc_size=1, label=None):
    """Create PVC using data provided as stdin input.

    Args:
        hostname (str): Node on which 'oc create' command will be executed.
        sc_name (str): name of a storage class to create PVC in.
        pvc_name_prefix (str): PVC name will consist of this prefix and
                               random str.
        pvc_size (int/str): size of PVC in Gb
        label (dic): label for PVC.
    """
    pvc_name = "%s-%s" % (pvc_name_prefix, utils.get_random_str())
    metadata = {"name": pvc_name}
    if label:
        metadata["labels"] = label
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


def _oc_create_app_dc_with_io_image(hostname, pvc_name, dc_name_prefix,
                                    replicas, space_to_use, label, image):
    dc_name = "%s-%s" % (dc_name_prefix, utils.get_random_str())
    container_data = {
        "name": dc_name,
        "image": image,
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

    labels = {"name": dc_name}
    if label:
        label.pop("name", None)
        labels.update(label)

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
                "metadata": {"labels": labels},
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


def oc_create_app_dc_with_io(
        hostname, pvc_name, dc_name_prefix="autotests-dc-with-app-io",
        replicas=1, space_to_use=1048576, label=None, image="cirros"):
    """Create DC with app PODs and attached PVC, constantly running I/O.

    Args:
        hostname (str): Node on which 'oc create' command will be executed.
        pvc_name (str): name of the Persistent Volume Claim to attach to
                        the application PODs where constant I/O will run.
        dc_name_prefix (str): DC name will consist of this prefix and
                              random str.
        replicas (int): amount of application POD replicas.
        space_to_use (int): value in bytes which will be used for I/O.
        label (dict): dict of keys and values to add labels in DC.
        image (str): Container image for I/O.
    """
    return _oc_create_app_dc_with_io_image(
        hostname, pvc_name, dc_name_prefix, replicas, space_to_use,
        label, image=image)


def oc_create_tiny_pod_with_volume(hostname, pvc_name, pod_name_prefix='',
                                   mount_path='/mnt', image='cirros'):
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
                "image": image,  # noqa: 10 Mb! linux image
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


def oc_delete(
        ocp_node, rtype, name, raise_on_absence=True, collect_logs=False,
        skip_res_validation=True, is_force=False):
    """Delete an OCP resource by name

    Args:
        ocp_node (str): Node on which the ocp command will run.
        rtype (str): Name of the resource type (pod, storageClass, etc).
        name (str): Name of the resource to delete.
        raise_on_absence (bool): if resource absent raise
                                 exception if value is true,
                                 else return
                                 default value: True
        collect_logs (bool): Collect logs before deleting resource
        skip_res_validation(bool): To validate before deletion of resource.
        is_force (bool): True for deleting forcefully, default is False
    """
    if skip_res_validation and not oc_get_yaml(
            ocp_node, rtype, name, raise_on_error=raise_on_absence):
        return

    if rtype == "pod" and collect_logs:
        cmd = [
            'oc', 'logs', name,
            '&>', '{}_$(date +%Y-%m-%d-%H-%m-%s).log'.format(name)]
        command.cmd_run(cmd, hostname=ocp_node)

    cmd = ['oc', 'delete', rtype, name]
    if openshift_version.get_openshift_version() >= '3.11':
        cmd.append('--wait=false')

    # Forcefully delete
    if is_force:
        cmd.append("--grace-period 0 --force")

    command.cmd_run(cmd, hostname=ocp_node)


def oc_get_custom_resource(ocp_node, rtype, custom, name=None, selector=None,
                           field_selector=None):
    """Get an OCP resource by custom column names.

    Args:
        ocp_node (str): Node on which the ocp command will run.
        rtype (str): Name of the resource type (pod, storageClass, etc).
        custom (str): Name of the custom columm to fetch.
        name (str|None): Name of the resource to fetch.
        selector (str|list|None): Column Name or list of column
                                  names select to.
        field_selector (str|list|None): object field selector
            which looks like following: 'spec.nodeName=foo_node_1'
    Returns:
        list: List containting data about the resource custom column
    Raises:
        AssertionError: Raised when unable to get resource
    Example:
        Get all "pvc" with "metadata.name" parameter values:
            pvc_details = oc_get_custom_resource(
                ocp_node, "pvc", ":.metadata.name"
            )
    """
    cmd = ['oc', 'get', rtype, '--no-headers']

    if name:
        cmd.append(name)

    if selector:
        cmd.append('--selector %s' % (
            ','.join(selector) if isinstance(selector, list) else selector))

    if field_selector:
        # NOTE(Nitin Goyal): Add field-selector parameters to custom because it
        # is not supported in ocp 3.6 and 3.7 and filter them via python later
        custom = ','.join(custom) if isinstance(custom, list) else custom
        field_selector = (field_selector.split(',') if isinstance(
            field_selector, six.string_types) else field_selector)

        for fs in field_selector:
            custom += ',:' + re.split('=|!=', fs)[0]

    cmd.append('-o=custom-columns=%s' % (
        ','.join(custom) if isinstance(custom, list) else custom))

    out = command.cmd_run(cmd, hostname=ocp_node)

    if name:
        return list(filter(None, map(str.strip, (out.strip()).split(' '))))
    else:
        out_list = []
        for line in (out.strip()).split('\n'):
            out_list.append(
                list(filter(None, map(str.strip, line.split(' ')))))

    if not field_selector:
        return out_list
    # Filter out field-selector parameters
    for fs in field_selector[::-1]:
        fs_value = re.split('=|!=', fs)[1]
        found = fs.find('!=')
        for out in out_list[:]:
            # Not equalto in fs and value present in list then remove it
            if (found > 0 and out[-1] == fs_value
                    # Equalto in fs and value does not match to fs then remove
                    or found == -1 and out[-1] != fs_value):
                out_list.remove(out)
            out.pop()
    return out_list


def get_block_provisioner(ocp_node):
    return oc_get_custom_resource(
        ocp_node, 'dc', selector="glusterblock",
        custom=(r'":.spec.template.spec.containers[*].'
                r'env[?(@.name==\"PROVISIONER_NAME\")].value"'))[0][0]


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
    out = command.cmd_run(
        cmd, hostname=ocp_node, raise_on_error=raise_on_error)
    return yaml.load(out, Loader=yaml.FullLoader) if out else {}


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


def oc_label(hostname, rtype, rname, label, overwrite=False):
    """Add label for given resource
    Args:
        hostname (str): Node where we want to run our commands.
        rtype (str): Type of resource.
        rname (str): Name of resource.
        overwrite(bool): True if glusterfs already has a value.
            False by default.

    Raises:
        AssertionError: In case adding label to resource fails.
    """
    cmd = "oc label {} {} {}".format(rtype, rname, label)
    if overwrite:
        cmd += " --overwrite"
    out = command.cmd_run(cmd, hostname=hostname)

    return out


def wait_for_resources_absence(ocp_node, rtype, names,
                               interval=5, timeout=600):
    """Wait for an absence of any set of resources of one type.

    If provided resource type is 'pvc' then 'pv's are also checked.

    Args:
        ocp_node (str): OCP node to perform oc client operations on.
        rtype (str): type of a resource(s) such as 'pvc', 'pv' and 'pod'.
        names (iterable): any iterable with names of objects to wait for.
        interval (int): interval in seconds between waiting attempts.
        timeout (int): overall timeout for waiting.
    """
    _waiter = waiter.Waiter(timeout=timeout, interval=interval)
    if len(names[0]) == 1:
        names = (names, )
    resources = {name: {'resource': 'not_checked'} for name in names}
    for name in names:
        for w in _waiter:
            try:
                resources[name]['resource'] = oc_get_yaml(
                    ocp_node, rtype, name, raise_on_error=True)
            except AssertionError:
                # NOTE(vponomar): Reset attempts for waiter to avoid redundant
                # sleep equal to 'interval' on the next usage.
                _waiter._attempt = 0
                resources[name]['resource'] = 'absent'
                break
    if rtype == 'pvc':
        for name in names:
            resources[name]['pv_name'] = '?'
            for w in _waiter:
                try:
                    _pv_name = get_pv_name_from_pvc(ocp_node, name)
                    if resources[name]['pv_name'] == '?':
                        resources[name]['pv_name'] = _pv_name
                except AssertionError:
                    _waiter._attempt = 0
                    resources[name]['pv_name'] = 'absent'
                    break
    if _waiter.expired:
        # Gather more info for ease of debugging
        for name in names:
            try:
                r_events = get_events(ocp_node, obj_name=name)
            except Exception:
                r_events = '?'
            resources[name]['events'] = r_events
            if rtype == 'pvc' and resources[name]['pv_name'] != '?':
                try:
                    pv_events = get_events(
                        ocp_node, obj_name=resources[name]['pv_name'])
                except Exception:
                    pv_events = '?'
                resources[name]['pv_events'] = pv_events
        error_msg = (
            "Failed to wait %d seconds for some of the provided resources "
            "to be absent.\nResource type: '%s'\nResource names:  %s\n"
            "Resources info: \n%s" % (
                timeout, rtype, ' | '.join(names),
                '\n'.join([six.text_type(r) for r in resources.items()])))
        g.log.error(error_msg)
        raise exceptions.ExecutionError(error_msg)


def wait_for_resource_absence(ocp_node, rtype, name, interval=5, timeout=600):
    return wait_for_resources_absence(
        ocp_node, rtype, name, interval=interval, timeout=timeout)


def scale_dcs_pod_amount_and_wait(hostname, dc_names, pod_amount=1,
                                  namespace=None, timeout=600, wait_step=5):
    """Scale amount of PODs for a list of DCs.

    If pod_amount is 0, then wait for it's absence.
    If pod_amount => 1, then wait for all of a DC PODs to be ready.

    Args:
        hostname (str): Node on which the ocp command will run
        dc_name (str/list/set/tuple): one or more DC names to be scaled.
        pod_amount (int): Number of PODs to scale. Default is 1.
        namespace (str): Namespace of a DC.
        timeout (int): timeout value, default value is 600 seconds.
        wait_step( int): wait step, default value is 5 seconds.
    Returns: dictionary with following structure:
        {
            "dc_name_1": ["pod_name_1_1", "pod_name_1_2", ..., "pod_name_1_n"],
            "dc_name_2": ["pod_name_2_1", "pod_name_2_2", ..., "pod_name_2_n"],
            ...
            "dc_name_n": ["pod_name_n_1", "pod_name_n_2", ..., "pod_name_n_n"],
        }
    """
    dc_names = (
        [dc_names] if isinstance(dc_names, six.string_types) else dc_names)
    dc_and_pod_names = {}
    namespace_arg = "--namespace=%s" % namespace if namespace else ""
    scale_cmd = "oc scale %s --replicas=%d dc/%s" % (
        namespace_arg, pod_amount, " dc/".join(dc_names))

    command.cmd_run(scale_cmd, hostname=hostname)

    for dc_name in dc_names:
        dc_and_pod_names[dc_name] = get_pod_names_from_dc(hostname, dc_name)
    _start_time, _timeout = time.time(), timeout
    for pod_names in dc_and_pod_names.values():
        for pod_name in pod_names:
            if pod_amount == 0:
                wait_for_resource_absence(
                    hostname, 'pod', pod_name,
                    interval=wait_step, timeout=_timeout)
            else:
                wait_for_pod_be_ready(
                    hostname, pod_name, timeout=_timeout, wait_step=wait_step)
            _diff = time.time() - _start_time
            _timeout = wait_step if _diff > timeout else timeout - _diff
    return dc_and_pod_names


def scale_dc_pod_amount_and_wait(hostname, dc_name, pod_amount=1,
                                 namespace=None, timeout=600, wait_step=5):
    """Scale amount of PODs for a DC.

    If pod_amount is 0, then wait for it's absence.
    If pod_amount => 1, then wait for all of a DC PODs to be ready.

    Args:
        hostname (str): Node on which the ocp command will run
        dc_name (str): Name of heketi dc
        pod_amount (int): Number of PODs to scale. Default is 1.
        namespace (str): Namespace of a DC.
        timeout (int): timeout value, default value is 600 seconds.
        wait_step( int): wait step, default value is 5 seconds.
    Returns: List of POD names of a DC.
    """
    return scale_dcs_pod_amount_and_wait(
        hostname, dc_name, pod_amount, namespace=namespace,
        timeout=timeout, wait_step=wait_step)[dc_name]


def get_gluster_host_ips_by_pvc_name(ocp_node, pvc_name):
    """Get Gluster Host IPs, whose nodes store bricks for specified PVC.

    Args:
        ocp_node (str): Node to execute OCP commands on.
        pvc_name (str): Name of a PVC to get related Gluster Hosts.
    Returns:
        list: List of gluster host IPs.
    """
    # Check storage provisioner
    column = (
        r':.metadata.annotations.'
        r'"volume\.beta\.kubernetes\.io\/storage-provisioner"')
    sp = oc_get_custom_resource(ocp_node, "pvc", column, name=pvc_name)[0]

    # Get node IPs
    if sp == "kubernetes.io/glusterfs":
        pv_info = get_gluster_vol_info_by_pvc_name(ocp_node, pvc_name)
        gluster_host_ips = [
            brick["name"].split(":")[0]
            for brick in pv_info["bricks"]["brick"]
        ]
    elif sp.startswith("gluster.org/glusterblock"):
        get_gluster_pod_node_ip_cmd = (
            r"""oc get pv --template '{{range .items}}"""
            + r"""{{if eq .spec.claimRef.name "%s"}}"""
            + r"""{{.spec.iscsi.targetPortal}}{{" "}}"""
            + r"""{{.spec.iscsi.portals}}{{end}}{{end}}'""") % pvc_name
        node_ips_raw = command.cmd_run(
            get_gluster_pod_node_ip_cmd, hostname=ocp_node)
        node_ips_raw = node_ips_raw.replace(
            "[", " ").replace("]", " ").replace(",", " ")
        gluster_host_ips = [
            s.strip() for s in node_ips_raw.split(" ") if s.strip()
        ]
    else:
        assert False, "Unexpected storage provisioner: %s" % sp

    return gluster_host_ips


def get_gluster_pod_names_by_pvc_name(
        ocp_node, pvc_name, raise_on_error=True):
    """Get Gluster POD names, whose nodes store bricks for specified PVC.

    Args:
        ocp_node (str): Node to execute OCP commands on.
        pvc_name (str): Name of a PVC to get related Gluster PODs.
    Returns:
        list: List of dicts of gluster pods details.
    """
    gluster_host_ips = get_gluster_host_ips_by_pvc_name(ocp_node, pvc_name)

    # Get Gluster POD names which are located on the filtered nodes
    gluster_pods = get_ocp_gluster_pod_details(ocp_node)
    if gluster_pods:
        matched_gluster_pods = list(filter(
            lambda pod: (pod["pod_host_ip"] in gluster_host_ips), gluster_pods
        ))
        pod_count = len(matched_gluster_pods)
        err_msg = (
            "Expected 3 or more Gluster PODs to be found. "
            "Actual is '%s'" % (pod_count))
        assert (pod_count > 2), err_msg
        return matched_gluster_pods
    elif raise_on_error:
        raise exceptions.ExecutionError(
            "Haven't found Gluster PODs on the cluster.")
    else:
        return None


def wait_for_gluster_pod_be_ready_on_specific_node(
        ocp_client_node, gluster_hostname, selector='glusterfs=storage-pod',
        timeout=300, wait_step=10):
    """Wait for gluster pod to be ready on specific node.

    Args:
        ocp_client_node (str): Node to execute OCP commands on.
        gluster_hostname (str): Name of node hosting gluster pod.
        selector (str): Selector for gluster pod.

    Returns:
        None
    """
    g_pod_name = get_gluster_pod_name_for_specific_node(
        ocp_client_node, gluster_hostname, selector=selector)
    wait_for_pod_be_ready(
        ocp_client_node, g_pod_name, timeout=timeout, wait_step=wait_step)


def get_gluster_pod_name_for_specific_node(
        ocp_client_node, gluster_hostname, selector='glusterfs=storage-pod'):
    """Get gluster pod name on specific gluster node.

    Args:
        ocp_client_node (str): Node to execute OCP commands on.
        gluster_hostname (str): Name of node hosting gluster pod.
        selector (str): Selector for gluster pod.

    Returns:
        str: Name of the gluster pod
    """
    g_pod_name = oc_get_custom_resource(
        ocp_client_node, 'pod', ':.metadata.name', selector=selector,
        field_selector='spec.nodeName=%s' % gluster_hostname)
    if not g_pod_name:
        raise AssertionError(
            'Gluster pod was not found for node %s' % gluster_hostname)
    return g_pod_name[0][0]


def cmd_run_on_gluster_pod_or_node(
        ocp_client_node, cmd, gluster_node=None, raise_on_error=True):
    """Run shell command on either Gluster PODs or Gluster nodes.

    Args:
        ocp_client_node (str): Node to execute OCP commands on.
        cmd (str): shell command to run.
        gluster_node (str): optional. Allows to chose specific gluster node,
            keeping abstraction from deployment type. Can be either IP address
            or node name from "oc get nodes" command.
    Returns:
        Output of a shell command as string object.
    """
    # Containerized Glusterfs
    gluster_pods = oc_get_pods(ocp_client_node, selector="glusterfs-node=pod")
    err_msg = ""
    if gluster_pods:
        if gluster_node:
            for pod_name, pod_data in gluster_pods.items():
                if gluster_node in (pod_data["ip"], pod_data["node"]):
                    gluster_pod_names = [pod_name]
                    break
            else:
                raise exceptions.ExecutionError(
                    "Could not find Gluster PODs with node filter as "
                    "'%s'." % gluster_node)
        else:
            gluster_pod_names = list(gluster_pods.keys())

        for gluster_pod_name in gluster_pod_names:
            try:
                pod_cmd = "oc exec %s -- %s" % (gluster_pod_name, cmd)
                return command.cmd_run(
                    pod_cmd, hostname=ocp_client_node,
                    raise_on_error=raise_on_error)
            except Exception as e:
                err = ("Failed to run '%s' command on '%s' Gluster POD. "
                       "Error: %s\n" % (cmd, gluster_pod_name, e))
                err_msg += err
                g.log.error(err)
        raise exceptions.ExecutionError(err_msg)

    # Standalone Glusterfs
    if gluster_node:
        g_hosts = [gluster_node]
    else:
        g_hosts = list(g.config.get("gluster_servers", {}).keys())
    for g_host in g_hosts:
        try:
            return command.cmd_run(
                cmd, hostname=g_host, raise_on_error=raise_on_error)
        except Exception as e:
            err = ("Failed to run '%s' command on '%s' Gluster node. "
                   "Error: %s\n" % (cmd, g_host, e))
            err_msg += err
            g.log.error(err)

    if not err_msg:
        raise exceptions.ExecutionError(
            "Haven't found neither Gluster PODs nor Gluster nodes.")
    raise exceptions.ExecutionError(err_msg)


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
    parsed_out = list(filter(None, map(str.strip, out.split(" "))))
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
    vol_info_cmd = "gluster-block info %s/%s --json" % (
        block_hosting_vol_name, block_vol_name)
    vol_info = cmd_run_on_gluster_pod_or_node(ocp_node, vol_info_cmd)

    return json.loads(vol_info)


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
        out = command.cmd_run(cmd, hostname=hostname)
        output = out.split()

        # command to find if pod is ready
        if output[0] == "true" and output[1] == "Running":
            g.log.info("pod %s is in ready state and is "
                       "Running" % pod_name)
            return True
        elif output[1] in ["Error", "CrashBackOffLoop"]:
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


def wait_for_pods_be_ready(
        hostname, pod_count, selector=None, field_selector=None,
        timeout=600, wait_step=10):
    """Wait to 'pod_count' gluster pods be in Ready state.

    Args:
        hostname (str): Node where we want to run our commands.
        pod_count (int): No of pods to be waited for.
        selector (str): Selector to select pods of given label.
        field_selector (str): Selector to select pods.
        timeout (int): Seconds to wait for Node to be Ready.
        wait_step (int): Interval in seconds to wait before checking
                         status again.

    Raises:
        AssertionError: In case it fails to get pods.
        ExecutionError: In case pods won't get in ready state for given time.
    """
    if not selector and not field_selector:
        raise exceptions.ExecutionError(
            "Either selector or field-selector parameter should be provided")

    custom = (
        r':.metadata.name,":.status.conditions[?(@.type==\"Ready\")]".status')
    pod_status = None
    for w in waiter.Waiter(timeout, wait_step):
        pod_status = oc_get_custom_resource(
            hostname, "pod", custom, selector=selector,
            field_selector=field_selector)

        if not pod_status and pod_count != 0:
            selection_text = ''
            if selector and field_selector:
                selection_text += 'selector {} and field-selector {}'.format(
                    selector, field_selector)
            elif selector:
                selection_text += 'selector {}'.format(selector)
            else:
                selection_text += 'field-selector {}'.format(field_selector)
                raise exceptions.ExecutionError(
                    "Unable to find pods with mentioned {}".format(
                        selection_text))
        status = [status for _, status in pod_status]
        if len(status) == pod_count == status.count("True"):
            return
    try:
        pod_events = ""
        for pod_name, _ in pod_status:
            pod_events += (
                "\n" + get_events(hostname, obj_name=pod_name, obj_type="Pod"))
    except Exception:
        pod_events = "?"

    err_msg = (
        "Failed to wait %s sec for pods be in ready state.\n"
        "Events info: %s" % (timeout, pod_events))
    g.log.error(err_msg)
    raise exceptions.ExecutionError(err_msg)


def get_pod_names_from_dc_or_rc(
        hostname, rname, rtype='dc', timeout=180, wait_step=3):
    """Return list of POD names by their DC.

    Args:
        hostname (str): hostname on which 'oc' commands will be executed.
        rname (str): deployment_config name or replication_controller name
        rtype (str): resource type, 'dc' or 'rc', Default value is 'rc'
        timeout (int): timeout value. Default value is 180 sec.
        wait_step( int): Wait step, default value is 3 sec.
    Returns:
         list: list of strings which are POD names
    Raises: exceptions.ExecutionError
    """
    if rtype not in ('dc', 'rc'):
        raise NameError("Value of rtype should be either 'dc' or 'rc'.")
    get_replicas_amount_cmd = (
        "oc get %s --no-headers --all-namespaces "
        "-o=custom-columns=:.spec.replicas,:.metadata.name "
        "| grep '%s' | awk '{print $1}'" % (rtype, rname))
    get_pod_names_cmd = (
        "oc get pods --all-namespaces -o=custom-columns=:.metadata.name "
        "--no-headers=true --selector %s=%s" % (
            "deploymentconfig" if rtype == "dc" else "name", rname))
    replicas = int(command.cmd_run(get_replicas_amount_cmd, hostname=hostname))
    for w in waiter.Waiter(timeout, wait_step):
        out = command.cmd_run(get_pod_names_cmd, hostname=hostname)
        pod_names = [o.strip() for o in out.split('\n') if o.strip()]
        if len(pod_names) != replicas:
            continue
        g.log.info(
            "POD names for '%s %s' are '%s'. "
            "Expected amount of PODs is '%s'.", rname, rtype, out, replicas)
        return pod_names
    if w.expired:
        err_msg = ("Exceeded %s sec timeout waiting for PODs to appear "
                   "in amount of %s." % (timeout, replicas))
        g.log.error(err_msg)
        raise exceptions.ExecutionError(err_msg)


def get_pod_names_from_dc(hostname, rname, timeout=180, wait_step=3):
    return get_pod_names_from_dc_or_rc(
        hostname, rname, timeout=timeout, wait_step=wait_step)


def get_pod_name_from_dc(hostname, dc_name, timeout=180, wait_step=3):
    return get_pod_names_from_dc_or_rc(
        hostname, dc_name, timeout=timeout, wait_step=wait_step)[0]


def get_pod_name_from_rc(hostname, rc_name, timeout=180, wait_step=3):
    return get_pod_names_from_dc_or_rc(
        hostname, rc_name, rtype='rc', timeout=timeout, wait_step=wait_step)[0]


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
    out = command.cmd_run(cmd, hostname=hostname)
    output = out.split("\n")[0].strip()
    return output


def wait_for_pvcs_be_bound(hostname, pvc_names, timeout=300, wait_step=10):
    """Wait for bunch of PVCs to be in 'Bound' state.

    Args:
        hostname (str): hostname on which oc commands will be executed.
        pvc_names (iterable): bunch of PVC names to be waited for.
        timeout (int): total time in seconds we should wait for 'Bound' state.
        wait_step (int): seconds to sleep before checking PVCs again.
    Raises: exceptions.ExecutionError in case of errors.
    Returns: None
    """
    _waiter = waiter.Waiter(timeout=timeout, interval=wait_step)
    if len(pvc_names[0]) == 1:
        pvc_names = (pvc_names, )
    pvc_data = {pvc_name: {'state': 'not_checked'} for pvc_name in pvc_names}
    for pvc_name in pvc_names:
        pvc_not_found_counter = 0
        for w in _waiter:
            output = get_pvc_status(hostname, pvc_name)
            pvc_data[pvc_name]['state'] = output
            if not output:
                g.log.info("PVC '%s' not found, sleep for %ssec." % (
                    pvc_name, wait_step))
                pvc_data[pvc_name]['state'] = 'not_found'
                if pvc_not_found_counter > 0:
                    msg = ("PVC '%s' has not been found 2 times already. Make "
                           "sure you provided correct PVC name." % pvc_name)
                else:
                    pvc_not_found_counter += 1
                    continue
            elif output == "Pending":
                g.log.info("PVC '%s' is in Pending state, sleep for %ssec" % (
                    pvc_name, wait_step))
                continue
            elif output == "Bound":
                g.log.info("PVC '%s' is in Bound state." % pvc_name)
                _waiter._attempt = 0
                break
            elif output == "Error":
                msg = "PVC '%s' is in 'Error' state." % pvc_name
                g.log.error(msg)
            else:
                msg = "PVC %s has different state - %s" % (pvc_name, output)
                g.log.error(msg)
            if msg:
                raise AssertionError(msg)
    if _waiter.expired:
        # Gather more info for ease of debugging
        for pvc_name in pvc_names:
            try:
                pvc_events = get_events(hostname, obj_name=pvc_name)
            except Exception:
                pvc_events = '?'
            pvc_data[pvc_name]['events'] = pvc_events
        error_msg = (
            "Failed to wait %d seconds for some of the provided PVCs "
            "to be in 'Bound' state.\nPVC names: %s\nPVCs info: \n%s" % (
                timeout, ' | '.join(pvc_names),
                '\n'.join([six.text_type(pvc) for pvc in pvc_data.items()])))
        g.log.error(error_msg)
        raise exceptions.ExecutionError(error_msg)


def wait_for_pvc_be_bound(hostname, pvc_name, timeout=300, wait_step=10):
    return wait_for_pvcs_be_bound(
        hostname, pvc_name, timeout=timeout, wait_step=wait_step)


verify_pvc_status_is_bound = wait_for_pvc_be_bound


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
    cmd = ("oc patch pvc %s "
           "-p='{\"spec\": {\"resources\": {\"requests\": "
           "{\"storage\": \"%sGi\"}}}}'" % (pvc_name, size))
    out = command.cmd_run(cmd, hostname=hostname)
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
    # NOTE(vponomar): following command allows to get PV even if PVC is deleted
    cmd = ("oc get pv -o jsonpath='{.items[?(@.spec.claimRef.name==\"%s\")]"
           ".metadata.name}'" % pvc_name)
    pv_name = command.cmd_run(cmd, hostname=hostname)
    assert pv_name.strip(), (
        "Failed to find PV with PVC name '%s' as filter" % pvc_name)
    g.log.info("pv name is %s for pvc %s" % (pv_name, pvc_name))

    return pv_name


def get_vol_names_from_pv(hostname, pv_name, vol_type='file'):
    '''
     Returns the heketi and gluster
     vol names of the corresponding PV
     Args:
         hostname (str): hostname on which we want
                         to find vol names
         pv_name (str): pv_name for which we
                        want to find corresponding
                        vol names
         vol_type (str): volume type block or file
     Returns:
         volname (dict): dict if successful
                      {"heketi_vol": heketi_vol_name,
                       "gluster_vol": gluster_vol_name
                    ex: {"heketi_vol": " xxxx",
                         "gluster_vol": "vol_xxxx"]
                    otherwise raise Exception
    '''
    vol_dict = {}
    if vol_type == 'block':
        cmd = (r"oc get pv %s -o=custom-columns="
               r":.metadata.annotations.'gluster\.org\/volume-id',"
               r":.metadata.annotations.glusterBlockShare" % pv_name)
    else:
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
    cmd = "oc get events -o yaml"
    if openshift_version.get_openshift_version() >= '3.9':
        cmd += " --field-selector %s" % ",".join(field_selector or "''")
    get_objects = command.cmd_run(cmd, hostname=hostname)
    objects = yaml.load(get_objects, Loader=yaml.FullLoader)['items']
    if openshift_version.get_openshift_version() >= '3.9':
        return objects

    # Backup approach for OCP3.6 and OCP3.7 which do not have
    # '--field-selector' feature.
    filtered_objects = []
    for o in objects:
        if obj_name and o["involvedObject"]["name"] != obj_name:
            continue
        if obj_namespace and o["involvedObject"]["namespace"] != obj_namespace:
            continue
        if obj_type and o["involvedObject"]["kind"] != obj_type:
            continue
        if event_reason and o["reason"] != event_reason:
            continue
        if event_type and o["type"] != event_type:
            continue
        filtered_objects.append(o)
    return filtered_objects


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
    pvc_list = set([
        pvc[0]
        for pvc in oc_get_custom_resource(hostname, "pvc", ":.metadata.name")
        if pvc[0].startswith(prefix)
    ])

    pv_list = set([
        pv[0]
        for pv in oc_get_custom_resource(
            hostname, "pv", ":.spec.claimRef.name"
        )
        if pv[0].startswith(prefix)
    ])

    pvc_pv_diff = pvc_list ^ pv_list
    if pvc_pv_diff:
        err_msg = "PVC and PV list match failed"
        err_msg += "\nPVC list: %s, " % pvc_list
        err_msg += "\nPV list %s" % pv_list
        err_msg += "\nDifference: %s" % pvc_pv_diff
        raise AssertionError(err_msg)


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
    pv_block_volumes = set([
        pv[2]
        for pv in oc_get_custom_resource(hostname, "pv", custom_columns)
        if pv[0].startswith(pvc_prefix) and pv[1] == "gluster.org/glusterblock"
    ])

    vol_diff = pv_block_volumes ^ set(heketi_block_volumes)
    if vol_diff:
        err_msg = "PV block volumes and Heketi Block volume list match failed"
        err_msg += "\nPV Block Volumes: %s, " % pv_block_volumes
        err_msg += "\nHeketi Block volumes %s" % heketi_block_volumes
        err_msg += "\nDifference: %s" % vol_diff
        raise AssertionError(err_msg)


def check_service_status_on_pod(
        ocp_client, podname, service, status, state, timeout=180, wait_step=3):
    """Check a service state on a pod.

    Args:
        ocp_client (str): node with 'oc' client
        podname (str): pod name on which service needs to be checked
        service (str): service which needs to be checked
        status (str): status to be checked
            e.g. 'active', 'inactive', 'failed'
        state (str): state to be checked
            e.g. 'running', 'exited', 'dead'
        timeout (int): seconds to wait before service starts having
                       specified 'status'
        wait_step (int): interval in seconds to wait before checking
                         service again.
    """
    err_msg = ("Exceeded timeout of %s sec for verifying %s service to start "
               "having '%s' status" % (timeout, service, status))

    for w in waiter.Waiter(timeout, wait_step):
        ret, out, err = oc_rsh(ocp_client, podname, SERVICE_STATUS % service)
        if ret != 0:
            err_msg = ("failed to get service %s's status on pod %s" %
                       (service, podname))
            g.log.error(err_msg)
            raise AssertionError(err_msg)

        for line in out.splitlines():
            status_match = re.search(SERVICE_STATUS_REGEX, line)
            if (status_match and status_match.group(1) == status
                    and status_match.group(2) == state):
                return True

    if w.expired:
        g.log.error(err_msg)
        raise exceptions.ExecutionError(err_msg)


def wait_for_service_status_on_gluster_pod_or_node(
        ocp_client, service, status, state, gluster_node=None,
        raise_on_error=True, timeout=180, wait_step=3):
    """Wait for a service specific status on a Gluster POD or node.

    Args:
        ocp_client (str): hostname on which we want to check service
        service (str): target service to be checked
        status (str): service status which we wait for
            e.g. 'active', 'inactive', 'failed'
        state (str): service state which we wait for
            e.g. 'running', 'exited', 'dead'
        gluster_node (str): Gluster node IPv4 which stores either Gluster POD
            or Gluster services directly.
        timeout (int): seconds to wait before service starts having
                       specified 'status'
        wait_step (int): interval in seconds to wait before checking
                         service again.
    """
    err_msg = ("Exceeded timeout of %s sec for verifying %s service to start "
               "having '%s' status" % (timeout, service, status))

    for w in waiter.Waiter(timeout, wait_step):
        out = cmd_run_on_gluster_pod_or_node(
            ocp_client, SERVICE_STATUS % service, gluster_node,
            raise_on_error=raise_on_error)
        for line in out.splitlines():
            status_match = re.search(SERVICE_STATUS_REGEX, line)
            if (status_match and status_match.group(1) == status
                    and status_match.group(2) == state):
                return True
    if w.expired:
        g.log.error(err_msg)
        raise exceptions.ExecutionError(err_msg)


def restart_service_on_gluster_pod_or_node(
        ocp_client, service, gluster_node=None):
    """Restart service on Gluster either POD or node.

    Args:
        ocp_client (str): host on which we want to run 'oc' commands.
        service (str): service which needs to be restarted
        gluster_node (str): Gluster node IPv4 which stores either Gluster POD
            or Gluster services directly.
    Raises:
        AssertionError in case restart of a service fails.
    """
    cmd_run_on_gluster_pod_or_node(
        ocp_client, SERVICE_RESTART % service, gluster_node)


def kill_service_on_gluster_pod_or_node(ocp_client, service, gluster_node):
    """Kill service on Gluster pod or node.

    Args:
        ocp_client (str): host on which we want to run 'oc' commands.
        service (str): service which needs to be killed
        gluster_node (str): Gluster node IPv4 which stores either Gluster POD
            or Gluster services directly.
    Raises:
        AssertionError: In case killing of a service fails.
    """
    # Get the pid of the service which needs to be killed
    get_service_pids = cmd_run_on_gluster_pod_or_node(
        ocp_client, PGREP_SERVICE % service, gluster_node=gluster_node)
    service_pids = " ".join(get_service_pids.split("\n"))
    cmd_run_on_gluster_pod_or_node(
        ocp_client, KILL_SERVICE % service_pids, gluster_node=gluster_node)

    # Check the status of service
    try:
        cmd_run_on_gluster_pod_or_node(
            ocp_client, IS_ACTIVE_SERVICE % service, gluster_node=gluster_node)
    except exceptions.ExecutionError as err:
        if "command terminated with exit code 3" not in str(err):
            raise


def oc_adm_manage_node(
        ocp_client, operation, nodes=None, node_selector=None):
    """Manage common operations on nodes for administrators.

    Args:
        ocp_client (str): host on which we want to run 'oc' commands.
        operations (str):
            eg. --schedulable=true.
        nodes (list): list of nodes to manage.
        node_selector (str): selector to select the nodes.
            Note: 'nodes' and 'node_selector' are are mutually exclusive.
            Only either of them should be passed as parameter not both.
    Returns:
        str: In case of success.
    Raises:
        AssertionError: In case of any failures.
    """

    if (not nodes) == (not node_selector):
        raise AssertionError(
            "'nodes' and 'node_selector' are mutually exclusive. "
            "Only either of them should be passed as parameter not both.")

    cmd = "oc adm manage-node %s" % operation
    if node_selector:
        cmd += " --selector %s" % node_selector
    else:
        node = ' '.join(nodes)
        cmd += " " + node

    return command.cmd_run(cmd, ocp_client)


def oc_get_schedulable_nodes(ocp_client):
    """Get the list of schedulable nodes.

    Args:
        ocp_client (str): host on which we want to run 'oc' commands.

    Returns:
        list: list of nodes if present.
    Raises:
        AssertionError: In case of any failures.
    """
    cmd = ("oc get nodes --field-selector=spec.unschedulable!=true "
           "-o=custom-columns=:.metadata.name,:.spec.taints[*].effect "
           "--no-headers | awk '!/NoSchedule/{print $1}'")

    out = command.cmd_run(cmd, ocp_client)

    return out.split('\n') if out else out


def get_default_block_hosting_volume_size(hostname, heketi_dc_name):
    """Get the default size of block hosting volume.

    Args:
        hostname (str): Node where we want to run our commands.
        heketi_dc_name (str): Name of heketi DC.

    Raises:
        exceptions.ExecutionError: if command fails.

    Returns:
        integer: if successful
    """
    heketi_pod = get_pod_name_from_dc(
        hostname, heketi_dc_name, timeout=10)
    cmd = 'cat /etc/heketi/heketi.json'
    ret, out, err = oc_rsh(hostname, heketi_pod, cmd)
    if ret or not out:
        msg = ("Failed to get the default size of block hosting volume with"
               " err: '%s' and output: '%s'" % (err, out))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)

    try:
        out = json.loads(out)
    except ValueError:
        msg = "Not able to load data into json format \n data: %s" % out
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)

    if ('glusterfs' in out.keys()
            and 'block_hosting_volume_size' in out['glusterfs'].keys()):
        return int(out['glusterfs']['block_hosting_volume_size'])
    msg = ("Not able to get the value of "
           "out['glusterfs']['block_hosting_volume_size'] from out:\n" % out)
    g.log.error(msg)
    raise exceptions.ExecutionError(msg)


def wait_for_ocp_node_be_ready(
        hostname, node_name, timeout=300, wait_step=5):
    """Wait for OCP node to be in ready state.

    Args:
        hostname (str): Node where we want to run our commands.
        node_name (str): Name of ocp node.
        timeout (int): Seconds to wait for Node to be Ready.
        wait_step (int): Interval in seconds to wait before checking
                         status again.
    Raises:
        exceptions.ExecutionError.
    """
    custom = r'":.status.conditions[?(@.type==\"Ready\")]".status'

    for w in waiter.Waiter(timeout, wait_step):
        status = oc_get_custom_resource(hostname, 'node', custom, node_name)
        if status[0] == 'True':
            return

    err_msg = ("Exceeded timeout: %s for waiting for Node: %s "
               "to be in Ready state" % (timeout, node_name))
    g.log.error(err_msg)
    raise exceptions.ExecutionError(err_msg)


def restart_api_and_controller_services(hostname):
    """Restart master api & controller services.

    Args:
        hostname (str): hostname on which the service
                        restart commands needs to be
                        executed.
    """
    cmd_api_restart = '/usr/local/bin/master-restart api'
    cmd_ctrl_restart = '/usr/local/bin/master-restart controllers'
    version = openshift_version.get_openshift_version()
    if version <= "3.9":
        cmd_api_restart = (
            'systemctl restart atomic-openshift-master-api.service')
        cmd_ctrl_restart = (
            'systemctl restart atomic-openshift-master-controllers.service')
    command.cmd_run(cmd_api_restart, hostname=hostname)
    command.cmd_run(cmd_ctrl_restart, hostname=hostname)
    cmd = 'oc get pods'
    for w in waiter.Waiter(900, 10):
        out = command.cmd_run(cmd, hostname=hostname, raise_on_error=False)
        if out:
            g.log.info("Sucessfully restarted master & controller services")
            break
    if w.expired:
        err_msg = ("Could not restart api & controller services"
                   "in specified timeout")
        g.log.error(err_msg)
        raise exceptions.ExecutionError(err_msg)


def oc_annotate(hostname, rtype, rname, annotations):
    """Annotate any resource.

     Args:
         hostname (str): Node where we want to run our commands.
         rtype (str): Type of resource.
         rname (str): Name of resource.
         annotations (str/list): single annotations-str or list of
            annotations-strings.
            Examples:
                'k1=v1'
                'k1=v1 k2=v2'
                ['k1=v1', 'k2=v2']
    Raises:
        AssertionError: In case adding annotations to resource fails.
    """
    annotations = (
        [annotations]
        if isinstance(annotations, six.string_types)
        else annotations)
    for annotation in annotations:
        cmd = 'oc annotate %s %s %s --overwrite' % (rtype, rname, annotation)
        command.cmd_run(cmd, hostname=hostname)


def oc_adm_add_role_to_user(hostname, role, user,
                            project_name=None, config=None):
    """Add a role to a user using "oc adm" command.

       Args:
            hostname (str): Hostname where the "oc adm" command
                            is executed.
            role (str): Specify the role which  needs to be given to a user.
            user (str): Name of the user to be updated with a role.
            project_name (str): Openshift project inside which role and
                                user are defined.
            config (str): Name of the config file where the values are stored.
     Raises:
        AssertionError: In case adding role to the user fails.
    """
    cmd = "oc adm policy add-cluster-role-to-user %s %s" % (role, user)
    if config:
        cmd = cmd + " --config=%s" % config
    if project_name:
        cmd = cmd + " -n %s" % project_name
    command.cmd_run(cmd, hostname=hostname)


def oc_create_service_monitor(hostname, sm_name="heketi",
                              sm_namespace="openshift-monitoring",
                              sm_labels=None,
                              ep_service_name="heketi",
                              ep_namespace_selector_matchnames=None,
                              ep_matchlabels=None):
    """Create service monitor.

    Args:
        hostname (str): hostname on which 'oc create' command will run.
        sm_name (str): name to be set for new service monitor.
        sm_namespace (str): namespace where the new service monitor should
            be created.
        sm_labels (dict): key/value pairs which will be set to the new service
            monitor. Example: {"k8s-app": "heketi"}
        ep_service_name (str): name of the service which is going to be
            monitored by the new service monitor. Example: "heketi".
        ep_namespace_selector_matchnames (str/list): string or list of strings
            with namespaces to be matched searching the endpoint of the
            monitored service. Example: ['glusterfs']
        ep_matchlabels (dict): label key/value pairs to match the searched
            endpoint service by. Example: {"heketi": "storage-service"}
    """
    sm_labels = sm_labels if sm_labels else {"k8s-app": "heketi"}
    ep_namespace_selector_matchnames = (
        ep_namespace_selector_matchnames if ep_namespace_selector_matchnames
        else ['glusterfs'])
    ep_namespace_selector_matchnames = (
        [ep_namespace_selector_matchnames]
        if isinstance(ep_namespace_selector_matchnames, six.string_types)
        else ep_namespace_selector_matchnames)
    ep_matchlabels = (
        ep_matchlabels if ep_matchlabels else {"heketi": "storage-service"})
    sm_data = json.dumps({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "ServiceMonitor",
        "metadata": {
            "generation": 1,
            "labels": sm_labels,
            "name": sm_name,
            "namespace": sm_namespace,
        },
        "spec": {
            "endpoints": [{
                "bearerTokenFile":
                "/var/run/secrets/kubernetes.io/serviceaccount/token",
                "interval": "30s",
                "port": ep_service_name,
                "scheme": "http",
                "targetPort": 0
            }],
            "namespaceSelector": {
                "matchNames": ep_namespace_selector_matchnames,
            },
            "selector": {"matchLabels": ep_matchlabels}
        }
    })
    oc_create(hostname, sm_data, 'stdin')
    return sm_name


def oc_patch(ocp_node, rtype, rname, changes, raise_on_error=True):
    """Patch openshift resource with change

    Args:
        ocp_node (str): Node on which the ocp command will run.
        rtype (str): Name of the resource type (pod, storageClass, etc).
        rname (str): Name of the resource to fetch.
        changes (dict): Changes to be applied through patch.
        raise_on_error (bool): If set to true a failure to patch
            resource with changes will raise an error, otherwise
            an None will be returned.
    Returns:
        str : output of oc patch command
    Raises:
        exceptions.ExecutionError: Raise when invalid json is provided.
        AssertionError: Raised when unable to patch resource and
            'raise_on_error' is true.
    """
    try:
        changes = json.dumps(changes)
    except TypeError:
        raise exceptions.ExecutionError(
            "Json %s is not serializable to string")

    cmd = ['oc', 'patch', rtype, rname, '-p', '\'%s\'' % changes]
    out = command.cmd_run(
        cmd, hostname=ocp_node, raise_on_error=raise_on_error)
    return out or None


def match_pv_and_heketi_volumes(hostname, heketi_volumes, pvc_prefix):
    """Match heketi volumes and PVs

    Args:
        hostname (str): Hostname on which we want to check heketi
                        volumes and PVCs
        heketi_volumes (list): List of heketi volume names
        pvc_prefix (str): PVC name prefix given by user at the time
                          of pvc creation
    """

    custom_columns = [
        r':.spec.claimRef.name',
        r':.metadata.annotations."pv\.kubernetes\.io\/provisioned\-by"',
        r':.metadata.annotations."gluster\.kubernetes\.io\/heketi-volume\-id"'
    ]
    pv_volumes = set([
        pv[2]
        for pv in oc_get_custom_resource(hostname, "pv", custom_columns)
        if pv[0].startswith(pvc_prefix) and pv[1] == "kubernetes.io/glusterfs"
    ])

    vol_diff = pv_volumes ^ set(heketi_volumes)
    err_msg = ("PV and Heketi volume list match failed"
               "PV: {}, Heketi volumes {}, "
               "Difference: {}".format(pv_volumes, heketi_volumes, vol_diff))
    assert not vol_diff, err_msg


def oc_create_offline_block_volume_expand_job(
        hostname, pvc_name, job_name_prefix='block-expand-job',
        mount_path='/mnt'):
    """Create Block Volume Expand Job with block PVC mounted at /mnt

    Args:
        hostname (str): Hostname on which we want to run oc commands
        pvc_name (str): Name of a block PVC to attach to block expand job
        job_name_prefix (str): Job name prefix given by user at the time
                          of job creation
        mount_path (str): Where PVC should be mounted

    Returns:
        string: Name of the created job
    """

    # Find MOUNTPOINT on host node wrt to the mount_path on pod and run
    # xfs_growfs on host MOUNTPOINT
    command = [
        'sh', '-c', 'echo -e "# df -Th {0}" && df -Th {0} && '
        'DEVICE=$(df --output=source {0} | sed -e /^Filesystem/d) && '
        'MOUNTPOINT=$($EXEC_ON_HOST lsblk $DEVICE -n -o MOUNTPOINT) && '
        '$EXEC_ON_HOST xfs_growfs $MOUNTPOINT > /dev/null && '
        'echo -e "\n# df -Th {0}" && df -Th {0}'.format(mount_path)
    ]

    # This will be a privileged container be careful while playing with it
    job_name = "%s-%s" % (job_name_prefix, utils.get_random_str())
    job_data = json.dumps({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name},
        "spec": {
            "completions": 1,
            "template": {
                "spec": {
                    "containers": [{
                        "image": "rhel7",
                        "env": [
                            {
                                "name": "HOST_ROOTFS",
                                "value": "/rootfs"
                            },
                            {
                                "name": "EXEC_ON_HOST",
                                "value": "nsenter --root=$(HOST_ROOTFS) "
                                        "nsenter -t 1 -m"
                            }
                        ],
                        "command": command,
                        "name": "rhel7",
                        "volumeMounts": [
                            {"mountPath": mount_path, "name": "block-pvc"},
                            {"mountPath": "/dev", "name": "host-dev"},
                            {"mountPath": "/rootfs", "name": "host-rootfs"}
                        ],
                        "securityContext": {"privileged": True}
                    }],
                    "volumes": [
                        {
                            "name": "block-pvc", "persistentVolumeClaim": {
                                "claimName": pvc_name
                            }
                        },
                        {
                            "name": "host-dev", "hostPath": {
                                "path": "/dev"
                            }
                        },
                        {
                            "name": "host-rootfs", "hostPath": {
                                "path": "/"
                            }
                        }
                    ],
                    "restartPolicy": "Never"
                }
            }
        }
    })

    oc_create(hostname, job_data, 'stdin')
    return job_name


def is_job_complete(hostname, job_name, namespace=""):
    """Check job completion status

    Args:
        hostname (str): Hostname on which we want to run command
        job_name (str): k8s job name
        namespace (str): k8s namespace name
    Return:
        bool
    """

    cmd = ['oc', 'get', 'jobs', '-o=custom-columns=:.status.succeeded',
           '--no-headers', job_name]

    cmd += ['-n', namespace] if namespace else []

    out = command.cmd_run(cmd, hostname=hostname)
    return out == "1"
