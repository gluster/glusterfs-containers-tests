"""
Use this module for any OpenShift Storage version comparisons.

Usage example:

    # Assume OCS version is '3.11.3-12'. Then we have following:
    from openshiftstoragelibs import openshift_storage_version
    version = openshift_storage_version.get_openshift_storage_version()
    if version < '3.10':
        # False
    if version <= '3.11':
        # True
    if version < '3.11.3':
        # True
    if version < '3.11.1':
        # False
    if '3.9' < version <= '3.12':
        # True

Notes:
- If one of comparison operands has empty/zero-like 'micro' part of version,
  then it is ignored during comparison, where only 'major' and 'minor' parts of
  the OCS versions are used.

"""
import re

from glusto.core import Glusto as g
import six

from openshiftstoragelibs import command


OPENSHIFT_STORAGE_VERSION_RE = r"(?:v?)(\d+)(?:\.)(\d+)(?:\.(\d+))?.*"
BUILD_INFO_FILE_REGEX = r"Dockerfile-(:?.*)-rhgs-server-(:?.*)-"
OPENSHIFT_STORAGE_VERSION = None


def _get_openshift_storage_version_str(hostname=None):
    """Gets OpenShift Storage version from gluster pod's buildinfo directory.

    Args:
        hostname (str): Node on which the ocp command should run.
    Returns:
        str : Openshift Storage version, i.e. '3.11.3'
    Raises: 'NotImplementedError' if CRS setup is provided.
    """
    if not hostname:
        hostname = list(g.config['ocp_servers']['client'].keys())[0]
    get_gluster_pod_cmd = (
        "oc get --no-headers=true pods --selector glusterfs-node=pod "
        "-o=custom-columns=:.metadata.name | tail -1")
    gluster_pod = command.cmd_run(get_gluster_pod_cmd, hostname)
    if not gluster_pod:
        raise NotImplementedError(
            "OCS version check cannot be done on the standalone setup.")

    buildinfo_cmd = (
        "oc rsh %s ls -l /root/buildinfo/ | "
        "awk '{print $9}' | tail -1" % gluster_pod)
    out = command.cmd_run(buildinfo_cmd, hostname)

    return out


def _parse_openshift_storage_version(openshift_storage_version_str):
    """Parses OpenShift Storage version str into tuple of 3 values.

    Args:
        openshift_storage_version_str (str): OpenShift Storage version
        like '3.10' or '3.10.45'
    Returns:
        Tuple object of 3 values - major, minor and micro version parts.
    """
    groups = re.findall(
        OPENSHIFT_STORAGE_VERSION_RE, openshift_storage_version_str)
    err_msg = (
        "Failed to parse '%s' str into 3 OCS version parts - "
        "'major', 'minor' and 'micro'. "
        "Expected value like '3.10' or '3.10.45'"
        % openshift_storage_version_str)
    assert groups, err_msg
    assert len(groups) == 1, err_msg
    assert len(groups[0]) == 3, err_msg
    return (int(groups[0][0]), int(groups[0][1]), int(groups[0][2] or 0))


class OpenshiftStorageVersion(object):
    """Eases OpenShift Storage versions comparison.

    Instance of this class can be used for comparison with other instance of
    it or to string-like objects.

    Input str version is required to have, at least, 2 version parts -
    'major' and 'minor'. Third part is optional - 'micro' version.
    Examples: '3.10', 'v3.10', '3.10.45', 'v3.10.45'.

    Before each comparison, both operands are checked for zero value in 'micro'
    part. If one or both are false, then 'micro' part not used for comparison.

    Usage example (1) - compare to string object:
        version_3_10 = OpenshiftStorageVersion('3.10')
        cmp_result = '3.9' < version_3_10 <= '3.11'

    Usage example (2) - compare to the same type of an object:
        version_3_10 = OpenshiftStorageVersion('3.10')
        version_3_11 = OpenshiftStorageVersion('3.11')
        cmp_result = version_3_10 < version_3_11
    """
    def __init__(self, openshift_storage_version_str):
        self.v = _parse_openshift_storage_version(
            openshift_storage_version_str)
        self.major, self.minor, self.micro = self.v

    def _adapt_other(self, other):
        if isinstance(other, six.string_types):
            return OpenshiftStorageVersion(other)
        elif isinstance(other, OpenshiftStorageVersion):
            return other
        else:
            raise NotImplementedError(
                "'%s' type is not supported for OCS version "
                "comparison." % type(other))

    def __lt__(self, other):
        adapted_other = self._adapt_other(other)
        if not all((self.micro, adapted_other.micro)):
            return self.v[0:2] < adapted_other.v[0:2]
        return self.v < adapted_other.v

    def __le__(self, other):
        adapted_other = self._adapt_other(other)
        if not all((self.micro, adapted_other.micro)):
            return self.v[0:2] <= adapted_other.v[0:2]
        return self.v <= adapted_other.v

    def __eq__(self, other):
        adapted_other = self._adapt_other(other)
        if not all((self.micro, adapted_other.micro)):
            return self.v[0:2] == adapted_other.v[0:2]
        return self.v == adapted_other.v

    def __ge__(self, other):
        adapted_other = self._adapt_other(other)
        if not all((self.micro, adapted_other.micro)):
            return self.v[0:2] >= adapted_other.v[0:2]
        return self.v >= adapted_other.v

    def __gt__(self, other):
        adapted_other = self._adapt_other(other)
        if not all((self.micro, adapted_other.micro)):
            return self.v[0:2] > adapted_other.v[0:2]
        return self.v > adapted_other.v

    def __ne__(self, other):
        adapted_other = self._adapt_other(other)
        if not all((self.micro, adapted_other.micro)):
            return self.v[0:2] != adapted_other.v[0:2]
        return self.v != adapted_other.v


def get_openshift_storage_version(hostname=None):
    """Cacher of an OpenShift Storage version.

    Version of an OpenShift Storage cluster is constant value. So, we call
    API just once and then reuse it's output.

    Args:
        hostname (str): a node with 'oc' client where command should run on.
            If not specified, then first key
            from 'ocp_servers.client' config option will be picked up.
    Returns:
        OpenshiftStorageVersion object instance.
    """
    global OPENSHIFT_STORAGE_VERSION
    if not OPENSHIFT_STORAGE_VERSION:
        version_str = _get_openshift_storage_version_str(hostname=hostname)
        OPENSHIFT_STORAGE_VERSION = OpenshiftStorageVersion(version_str)
    return OPENSHIFT_STORAGE_VERSION
