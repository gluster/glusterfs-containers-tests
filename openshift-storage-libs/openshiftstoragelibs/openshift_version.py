"""
Use this module for any OpenShift version comparisons.

Usage example:

    # Assume OpenShift version is '3.10.45'. Then we have following:
    from openshiftstoragelibs import openshift_version
    version = openshift_version.get_openshift_version()
    if version < '3.10':
        # False
    if version <= '3.10':
        # True
    if version < '3.10.46':
        # True
    if version < '3.10.13':
        # False
    if '3.9' < version <= '3.11':
        # True

Notes:
- If one of comparison operands has empty/zero-like 'micro' part of version,
  then it is ignored during comparison, where only 'major' and 'minor' parts of
  the OpenShift versions are used.

"""
import re

from glusto.core import Glusto as g
import six

from openshiftstoragelibs import exceptions


OPENSHIFT_VERSION_RE = r"(?:v?)(\d+)(?:\.)(\d+)(?:\.(\d+))?$"
OPENSHIFT_VERSION = None


def _get_openshift_version_str(hostname=None):
    """Gets OpenShift version from 'oc version' command.

    Args:
        hostname (str): Node on which the ocp command should run.
    Returns:
        str : oc version, i.e. 'v3.10.47'
    Raises: 'exceptions.ExecutionError' if failed to get version
    """
    if not hostname:
        hostname = list(g.config['ocp_servers']['client'].keys())[0]
    cmd = "oc version | grep openshift | cut -d ' ' -f 2"
    ret, out, err = g.run(hostname, cmd, "root")
    if ret != 0:
        msg = "Failed to get oc version. \n'err': %s\n 'out': %s" % (err, out)
        g.log.error(msg)
        raise AssertionError(msg)
    out = out.strip()
    if not out:
        error_msg = "Empty output from 'oc version' command: '%s'" % out
        g.log.error(error_msg)
        raise exceptions.ExecutionError(error_msg)

    return out


def _parse_openshift_version(openshift_version_str):
    """Parses OpenShift version str into tuple of 3 values.

    Args:
        openshift_version_str (str): OpenShift version like '3.10' or '3.10.45'
    Returns:
        Tuple object of 3 values - major, minor and micro version parts.
    """
    groups = re.findall(OPENSHIFT_VERSION_RE, openshift_version_str)
    err_msg = (
        "Failed to parse '%s' str into 3 OpenShift version parts - "
        "'major', 'minor' and 'micro'. "
        "Expected value like '3.10' or '3.10.45'" % openshift_version_str)
    assert groups, err_msg
    assert len(groups) == 1, err_msg
    assert len(groups[0]) == 3, err_msg
    return (int(groups[0][0]), int(groups[0][1]), int(groups[0][2] or 0))


class OpenshiftVersion(object):
    """Eases OpenShift versions comparison.

    Instance of this class can be used for comparison with other instance of
    it or to string-like objects.

    Input str version is required to have, at least, 2 version parts -
    'major' and 'minor'. Third part is optional - 'micro' version.
    Examples: '3.10', 'v3.10', '3.10.45', 'v3.10.45'.

    Before each comparison, both operands are checked for zero value in 'micro'
    part. If one or both are false, then 'micro' part not used for comparison.

    Usage example (1) - compare to string object:
        version_3_10 = OpenshiftVersion('3.10')
        cmp_result = '3.9' < version_3_10 <= '3.11'

    Usage example (2) - compare to the same type of an object:
        version_3_10 = OpenshiftVersion('3.10')
        version_3_11 = OpenshiftVersion('3.11')
        cmp_result = version_3_10 < version_3_11
    """
    def __init__(self, openshift_version_str):
        self.v = _parse_openshift_version(openshift_version_str)
        self.major, self.minor, self.micro = self.v

    def _adapt_other(self, other):
        if isinstance(other, six.string_types):
            return OpenshiftVersion(other)
        elif isinstance(other, OpenshiftVersion):
            return other
        else:
            raise NotImplementedError(
                "'%s' type is not supported for OpenShift version "
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


def get_openshift_version(hostname=None):
    """Cacher of an OpenShift version.

    Version of an OpenShift cluster is constant value. So, we call API just
    once and then reuse it's output.

    Args:
        hostname (str): a node with 'oc' client where command should run on.
            If not specified, then first key
            from 'ocp_servers.client' config option will be picked up.
    Returns:
        OpenshiftVersion object instance.
    """
    global OPENSHIFT_VERSION
    if not OPENSHIFT_VERSION:
        version_str = _get_openshift_version_str(hostname=hostname)
        OPENSHIFT_VERSION = OpenshiftVersion(version_str)
    return OPENSHIFT_VERSION
