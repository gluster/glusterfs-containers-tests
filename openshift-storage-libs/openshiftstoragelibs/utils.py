"""Generic host utility functions.

Generic utility functions not specifc to a larger suite of tools.
For example, not specific to OCP, Gluster, Heketi, etc.
"""

import random
import string

from prometheus_client.parser import text_string_to_metric_families


def get_random_str(size=14):
    """
    Gets the random string

    Args:
        size (int): size of the random string

    Returns:
        int: size of the string

    """
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
