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


def oc_rsh(ocp_node, pod_name, command):
    """Run a command in the ocp pod using `oc rsh`.

    Args:
        ocp_node (str): Node on which oc rsh command will be executed.
        pod_name (str): Name of the pod on which the command will
            be executed.
        command (str|list): command to run.

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
    ret, stdout, stderr = g.run(ocp_node, cmd)
    return (ret, stdout, stderr)
