"""Library for openshift operations.

Various utility functions for interacting with OCP/OpenShift.
"""

import re
import types

import yaml

from glusto.core import Glusto as g


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


def oc_create(ocp_node, filename):
    """Create a resource based on the contents of the given file name.

    Args:
        ocp_node (str): Node on which the ocp command will run
        filename (str): Filename (on remote) to be passed to oc create
            command
    Raises:
        AssertionError: Raised when resource fails to create.
    """
    ret, out, err = g.run(ocp_node, ['oc', 'create', '-f', filename])
    if ret != 0:
        g.log.error('Failed to create resource: %r; %r', out, err)
        raise AssertionError('failed to create resource: %r; %r' % (out, err))
    g.log.info('Created resource from file (%s)', filename)
    return


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
