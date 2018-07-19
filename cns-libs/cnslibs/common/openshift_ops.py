"""Library for openshift operations.

Various utility functions for interacting with OCP/OpenShift.
"""

import base64
import json
import re
import types

from glusto.core import Glusto as g
import yaml

from cnslibs.common import exceptions
from cnslibs.common import utils
from cnslibs.common import waiter


PODS_WIDE_RE = re.compile(
    '(\S+)\s+(\S+)\s+(\w+)\s+(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\n')


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
                 allow_volume_expansion=False, **parameters):
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
        'volumeoptions',
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
        "parameters": parameters,
        "allowVolumeExpansion": allow_volume_expansion,
    })
    oc_create(hostname, sc_data, 'stdin')
    return sc_name


def oc_create_pvc(hostname, sc_name, pvc_name_prefix="autotests-pvc",
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
    pvc_data = json.dumps({
        "kind": "PersistentVolumeClaim",
        "apiVersion": "v1",
        "metadata": {
            "name": pvc_name,
            "annotations": {
                "volume.beta.kubernetes.io/storage-class": sc_name,
            },
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "%sGi" % pvc_size}}
        },
    })
    oc_create(hostname, pvc_data, 'stdin')
    return pvc_name


def oc_delete(ocp_node, rtype, name):
    """Delete an OCP resource by name.

    Args:
        ocp_node (str): Node on which the ocp command will run.
        rtype (str): Name of the resource type (pod, storageClass, etc).
        name (str): Name of the resource to delete.
    Raises:
        AssertionError: Raised when resource fails to create.
    """
    ret, out, err = g.run(ocp_node, ['oc', 'delete', rtype, name])
    if ret != 0:
        g.log.error('Failed to delete resource: %s, %s: %r; %r',
                    rtype, name, out, err)
        raise AssertionError('failed to delete resource: %r; %r' % (out, err))
    g.log.info('Deleted resource: %r %r', rtype, name)
    return


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
                              interval=10, timeout=120):
    for w in waiter.Waiter(timeout=timeout, interval=interval):
        try:
            oc_get_yaml(ocp_node, rtype, name, raise_on_error=True)
        except AssertionError:
            return
    if w.expired:
        error_msg = "%s '%s' still exists after waiting for it %d seconds" % (
            rtype, name, timeout)
        g.log.error(error_msg)
        raise exceptions.ExecutionError(error_msg)
