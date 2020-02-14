"""Module for doing process related tasks such as ps info."""

from openshiftstoragelibs.openshift_ops import (
    cmd_run_on_gluster_pod_or_node,
)


def get_process_info_on_gluster_pod_or_node(
        master, g_node, process, fields):
    """Get the info of the running process in the gluster node or pod.

    Args:
        master (str): master node of ocp cluster.
        g_node (str): ip or hostname of gluster node.
        process (str): name of the process.
            e.g. 'glusterfsd'
        fields (list): field names of the process.
            e.g. ['pid', 'rss', 'vsz', '%cpu']

    Returns:
        list: [
            {field1: val, field2: val},
            {field1: val, field2: val}
        ]
    """

    cmd = "ps -C {} -o {} --no-headers".format(process, ','.join(fields))

    out = cmd_run_on_gluster_pod_or_node(master, cmd, g_node)

    return [
        dict(list(zip(fields, prc.strip().split()))) for prc in out.split('\n')
    ]
