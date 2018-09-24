"""Generic host utility functions.

Generic utility functions not specifc to a larger suite of tools.
For example, not specific to OCP, Gluster, Heketi, etc.
"""

import random
import re
import string

from glusto.core import Glusto as g

from prometheus_client.parser import text_string_to_metric_families

ONE_GB_BYTES = 1073741824.0


def get_device_size(host, device_name):
    """Gets device size for the given device name.

    Args:
        host (str): Node in command will be executed.
        device_name (str): device name for which the size has to
            be calculated.

    Returns:
        str : returns device size in GB on success
            False otherwise

    Example:
        get_device_size(host, device_name)
    """

    cmd = "fdisk -l %s " % device_name
    ret, out, _ = g.run(host, cmd)
    if ret != 0:
        g.log.error("Failed to execute fdisk -l command "
                    "on node %s" % host)
        return False

    regex = 'Disk\s' + device_name + '.*?,\s(\d+)\sbytes\,.*'
    match = re.search(regex, out)
    if match is None:
        g.log.error("Regex mismatch while parsing fdisk -l output")
        return False

    return str(int(int(match.group(1))/ONE_GB_BYTES))


def get_random_str(size=14):
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))


def parse_prometheus_data(text):
    """Parse prometheus-formatted text to the python objects

    Args:
        text (str): prometheus-formatted data

    Returns:
        dict: parsed data as python dictionary
    """
    metrics = {}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            key, data, val = (sample.name, sample.labels, sample.value)
            if data.keys():
                data['value'] = val
                if key in metrics.keys():
                    metrics[key].append(data)
                else:
                    metrics[key] = [data]
            else:
                metrics[key] = val

    return metrics
