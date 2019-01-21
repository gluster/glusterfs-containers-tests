"""
Use this module for any Heketi client package version comparisons.

Usage example:

    # Assume Heketi version is '7.0.0-3'. Then we have following:
    from cnslibs.common import heketi_version
    version = heketi_version.get_heketi_version()
    if version < '7.0.0-4':
        # True
    if version < '7.0.0-2':
        # False
    if '7.0.0-2' < version <= '7.0.0-3':
        # True
"""
import re

from glusto.core import Glusto as g
import six

from cnslibs.common import exceptions


HEKETI_VERSION_RE = r"(\d+)(?:\.)(\d+)(?:\.)(\d+)(?:\-)(\d+)$"
HEKETI_VERSION = None


def _get_heketi_version_str(hostname=None):
    """Gets Heketi client package version.

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
        msg = "Failed to get heketi version. \n'err': %s\n 'out': %s" % (
            err, out)
        g.log.error(msg)
        raise AssertionError(msg)
    out = out.strip()
    if not out:
        error_msg = "Empty output for '%s' cmd: '%s'" % (cmd, out)
        g.log.error(error_msg)
        raise exceptions.ExecutionError(error_msg)

    return out


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

    def __lt__(self, other):
        adapted_other = self._adapt_other(other)
        return self.v < adapted_other.v

    def __le__(self, other):
        adapted_other = self._adapt_other(other)
        return self.v <= adapted_other.v

    def __eq__(self, other):
        adapted_other = self._adapt_other(other)
        return self.v == adapted_other.v

    def __ge__(self, other):
        adapted_other = self._adapt_other(other)
        return self.v >= adapted_other.v

    def __gt__(self, other):
        adapted_other = self._adapt_other(other)
        return self.v > adapted_other.v

    def __ne__(self, other):
        adapted_other = self._adapt_other(other)
        return self.v != adapted_other.v


def get_heketi_version(hostname=None):
    """Cacher of the Heketi client package version.

    Version of Heketi client package is constant value. So, we call API just
    once and then reuse it's output.

    Args:
        hostname (str): a node with 'heketi' client where command should run on
            If not specified, then first key
            from 'openshift.heketi_config.heketi_client_node' config option
            will be picked up.
    Returns:
        HeketiVersion object instance.
    """
    global HEKETI_VERSION
    if not HEKETI_VERSION:
        version_str = _get_heketi_version_str(hostname=hostname)
        HEKETI_VERSION = HeketiVersion(version_str)
    return HEKETI_VERSION
