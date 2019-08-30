"""
Use this module for any Heketi server and client packages versions comparisons.

Usage example:

    # Assume Heketi server version is '7.0.0-3' and client is '7.0.0-5'
    Then we have following:

    from openshiftstoragelibs import heketi_version
    version = heketi_version.get_heketi_version()
    if version < '7.0.0-4':
        # True
    if version < '7.0.0-2':
        # False
    if '7.0.0-2' < version <= '7.0.0-3':
        # True

    At first step, we compare requested version against the Heketi server
    version, making sure they are compatible. Then, we make sure, that
    existing heketi client package has either the same or newer version than
    server's one.
"""
import re

from glusto.core import Glusto as g
import six

from openshiftstoragelibs import command
from openshiftstoragelibs import exceptions


HEKETI_VERSION_RE = r"(\d+)(?:\.)(\d+)(?:\.)(\d+)(?:\-)(\d+)$"
HEKETI_CLIENT_VERSION = None
HEKETI_SERVER_VERSION = None


def _get_heketi_client_version_str(hostname=None):
    """Gets Heketi client package version from heketi client node.

    Args:
        hostname (str): Node on which the version check command should run.
    Returns:
        str : heketi version, i.e. '7.0.0-1'
    Raises: 'exceptions.ExecutionError' if failed to get version
    """
    if not hostname:
        openshift_config = g.config.get("cns", g.config.get("openshift"))
        heketi_config = openshift_config['heketi_config']
        hostname = heketi_config['heketi_client_node'].strip()
    cmd = ("rpm -q heketi-client --queryformat '%{version}-%{release}\n' | "
           "cut -d '.' -f 1,2,3")
    ret, out, err = g.run(hostname, cmd, "root")
    if ret != 0:
        msg = ("Failed to get heketi client version. "
               "\n'err': %s\n 'out': %s" % (err, out))
        g.log.error(msg)
        raise AssertionError(msg)
    out = out.strip()
    if not out:
        error_msg = "Empty output for '%s' cmd: '%s'" % (cmd, out)
        g.log.error(error_msg)
        raise exceptions.ExecutionError(error_msg)

    return out


def _get_heketi_server_version_str(ocp_client_node=None):
    """Gets Heketi server package version from Heketi POD.

    Args:
        ocp_client_node (str): Node on which the version check command should
                               run.
    Returns:
        str : heketi version, i.e. '7.0.0-1'
    Raises: 'exceptions.ExecutionError' if failed to get version
    """
    if not ocp_client_node:
        ocp_client_node = list(g.config["ocp_servers"]["client"].keys())[0]
    get_package_version_cmd = (
        "rpm -q heketi --queryformat '%{version}-%{release}\n' | "
        "cut -d '.' -f 1,2,3")

    # NOTE(vponomar): we implement Heketi POD call command here, not in common
    # module for OC commands just to avoid cross-reference imports.
    get_pods_cmd = "oc get -o wide --no-headers=true pods --selector heketi"
    heketi_pods = command.cmd_run(get_pods_cmd, hostname=ocp_client_node)

    err_msg = ""
    for heketi_pod_line in heketi_pods.split("\n"):
        heketi_pod_data = heketi_pod_line.split()
        if ("-deploy" in heketi_pod_data[0]
                or heketi_pod_data[1].lower() != "1/1"
                or heketi_pod_data[2].lower() != "running"):
            continue
        try:
            pod_cmd = "oc exec %s -- %s" % (
                heketi_pod_data[0], get_package_version_cmd)
            return command.cmd_run(pod_cmd, hostname=ocp_client_node)
        except Exception as e:
            err = ("Failed to run '%s' command on '%s' Heketi POD. "
                   "Error: %s\n" % (pod_cmd, heketi_pod_data[0], e))
            err_msg += err
            g.log.error(err)
    if not err_msg:
        err_msg += "Haven't found 'Running' and 'ready' (1/1) Heketi PODs.\n"
    err_msg += "Heketi PODs: %s" % heketi_pods
    raise exceptions.ExecutionError(err_msg)


def _parse_heketi_version(heketi_version_str):
    """Parses Heketi version str into tuple of 4 values.

    Args:
        heketi_version_str (str): Heketi version like '7.0.0-1'
    Returns:
        Tuple object of 4 values - major, minor, micro and build version parts.
    """
    groups = re.findall(HEKETI_VERSION_RE, heketi_version_str)
    err_msg = (
        "Failed to parse '%s' str into 4 Heketi version parts. "
        "Expected value like '7.0.0-1'" % heketi_version_str)
    assert groups, err_msg
    assert len(groups) == 1, err_msg
    assert len(groups[0]) == 4, err_msg
    return (int(groups[0][0]), int(groups[0][1]),
            int(groups[0][2]), int(groups[0][3]))


class HeketiVersion(object):
    """Eases Heketi versions comparison.

    Instance of this class can be used for comparison with other instance of
    it or to string-like objects.

    Input str version is required to have 4 version parts -
    'major', 'minor', 'micro' and 'build' versions. Example - '7.0.0-1'

    Usage example (1) - compare to string object:
        version_7_0_0_2 = HeketiVersion('7.0.0-2')
        cmp_result = '7.0.0-1' < version_7_0_0_2 <= '8.0.0-1'

    Usage example (2) - compare to the same type of an object:
        version_7_0_0_1 = HeketiVersion('7.0.0-1')
        version_7_0_0_2 = HeketiVersion('7.0.0-2')
        cmp_result = version_7_0_0_1 < version_7_0_0_2
    """
    def __init__(self, heketi_version_str):
        self.v = _parse_heketi_version(heketi_version_str)
        self.v_str = heketi_version_str
        self.major, self.minor, self.micro, self.build = self.v

    def __str__(self):
        return self.v_str

    def _adapt_other(self, other):
        if isinstance(other, six.string_types):
            return HeketiVersion(other)
        elif isinstance(other, HeketiVersion):
            return other
        else:
            raise NotImplementedError(
                "'%s' type is not supported for Heketi version "
                "comparison." % type(other))

    def _compare_client_and_server_versions(self, client_v, server_v):
        if client_v < server_v:
            raise Exception(
                "Client version (%s) is older than server's (%s)." % (
                    client_v, server_v))

    def __lt__(self, other):
        global HEKETI_CLIENT_VERSION
        global HEKETI_SERVER_VERSION
        self._compare_client_and_server_versions(
            HEKETI_CLIENT_VERSION.v, HEKETI_SERVER_VERSION.v)
        adapted_other = self._adapt_other(other)
        return self.v < adapted_other.v

    def __le__(self, other):
        global HEKETI_CLIENT_VERSION
        global HEKETI_SERVER_VERSION
        self._compare_client_and_server_versions(
            HEKETI_CLIENT_VERSION.v, HEKETI_SERVER_VERSION.v)
        adapted_other = self._adapt_other(other)
        return self.v <= adapted_other.v

    def __eq__(self, other):
        global HEKETI_CLIENT_VERSION
        global HEKETI_SERVER_VERSION
        self._compare_client_and_server_versions(
            HEKETI_CLIENT_VERSION.v, HEKETI_SERVER_VERSION.v)
        adapted_other = self._adapt_other(other)
        return self.v == adapted_other.v

    def __ge__(self, other):
        global HEKETI_CLIENT_VERSION
        global HEKETI_SERVER_VERSION
        self._compare_client_and_server_versions(
            HEKETI_CLIENT_VERSION.v, HEKETI_SERVER_VERSION.v)
        adapted_other = self._adapt_other(other)
        return self.v >= adapted_other.v

    def __gt__(self, other):
        global HEKETI_CLIENT_VERSION
        global HEKETI_SERVER_VERSION
        self._compare_client_and_server_versions(
            HEKETI_CLIENT_VERSION.v, HEKETI_SERVER_VERSION.v)
        adapted_other = self._adapt_other(other)
        return self.v > adapted_other.v

    def __ne__(self, other):
        global HEKETI_CLIENT_VERSION
        global HEKETI_SERVER_VERSION
        self._compare_client_and_server_versions(
            HEKETI_CLIENT_VERSION.v, HEKETI_SERVER_VERSION.v)
        adapted_other = self._adapt_other(other)
        return self.v != adapted_other.v


def get_heketi_version(hostname=None, ocp_client_node=None):
    """Cacher of the Heketi client package version.

    Version of Heketi client package is constant value. So, we call API just
    once and then reuse it's output.

    Args:
        hostname (str): a node with 'heketi' client where command should run on
            If not specified, then first key
            from 'openshift.heketi_config.heketi_client_node' config option
            will be picked up.
        ocp_client_node (str): a node with the 'oc' client,
            where Heketi POD command will run.
            If not specified, then first key
            from 'ocp_servers.client' config option will be picked up.
    Returns:
        HeketiVersion object instance.
    """
    global HEKETI_CLIENT_VERSION
    global HEKETI_SERVER_VERSION
    if not (HEKETI_SERVER_VERSION and HEKETI_CLIENT_VERSION):
        client_version_str = _get_heketi_client_version_str(hostname=hostname)
        server_version_str = _get_heketi_server_version_str(
            ocp_client_node=ocp_client_node)
        HEKETI_CLIENT_VERSION = HeketiVersion(client_version_str)
        HEKETI_SERVER_VERSION = HeketiVersion(server_version_str)
    return HEKETI_SERVER_VERSION
