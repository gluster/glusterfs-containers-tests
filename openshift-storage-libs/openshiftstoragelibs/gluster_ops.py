try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json
try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree
import re
import six
import time

from glusto.core import Glusto as g
from glustolibs.gluster.block_ops import block_list
from glustolibs.gluster.heal_libs import is_heal_complete
from glustolibs.gluster.volume_ops import (
    get_volume_status,
    get_volume_list,
    volume_status,
    volume_start,
    volume_stop,
)

from openshiftstoragelibs import exceptions
from openshiftstoragelibs.heketi_ops import heketi_blockvolume_info
from openshiftstoragelibs.openshift_ops import (
    cmd_run_on_gluster_pod_or_node,
    get_ocp_gluster_pod_details,
)
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import waiter


@podcmd.GlustoPod()
def wait_to_heal_complete(
        vol_name=None, g_node="auto_get_gluster_endpoint",
        timeout=300, wait_step=5):
    """Monitors heal for volumes on gluster

    Args:
        vol_name (str): Name of the gluster volume else default is None and
            will check for all the volumes
        g_node (str): Name of the gluster node else default is
            auto_get_gluster_endpoint
        timeout (int): Time to wait for heal check to complete default is 300
        wait_step (int): Time to trigger heal check command for next iteration
    Raises:
        AssertionError: In case heal is not complete
    """
    if not vol_name:
        gluster_vol_list = get_volume_list(g_node)
        if not gluster_vol_list:
            raise AssertionError("failed to get gluster volume list")
    else:
        gluster_vol_list = [vol_name]

    _waiter = waiter.Waiter(timeout=timeout, interval=wait_step)
    for gluster_vol in gluster_vol_list:
        for w in _waiter:
            if is_heal_complete(g_node, gluster_vol):
                # NOTE(vponomar): Reset attempts for waiter to avoid redundant
                # sleep equal to 'interval' on the next usage.
                _waiter._attempt = 0
                break

    if w.expired:
        err_msg = ("reached timeout waiting for all the gluster volumes "
                   "to reach the 'healed' state.")
        g.log.error(err_msg)
        raise AssertionError(err_msg)


@podcmd.GlustoPod()
def get_gluster_vol_status(file_vol, is_detail=False):
    """Get Gluster vol status.

    Args:
        file_vol (str): file volume name.
        is_detail (bool): True for detailed output else False
    """
    # Get Gluster vol info
    options = 'detail' if is_detail else ''
    gluster_volume_status = get_volume_status(
        "auto_get_gluster_endpoint", file_vol, options=options)
    if not gluster_volume_status:
        raise AssertionError("Failed to get volume status for gluster "
                             "volume '%s'" % file_vol)
    if file_vol in gluster_volume_status:
        gluster_volume_status = gluster_volume_status.get(file_vol)
    return gluster_volume_status


@podcmd.GlustoPod()
def get_gluster_vol_hosting_nodes(file_vol):
    """Get Gluster vol hosting nodes.

    Args:
        file_vol (str): file volume name.
    """
    vol_status = get_gluster_vol_status(file_vol)
    g_nodes = []
    for g_node, g_node_data in vol_status.items():
        for process_name, process_data in g_node_data.items():
            if not process_name.startswith("/var"):
                continue
            g_nodes.append(g_node)
    return g_nodes


@podcmd.GlustoPod()
def restart_gluster_vol_brick_processes(ocp_client_node, file_vol,
                                        gluster_nodes):
    """Restarts brick process of a file volume.

    Args:
        ocp_client_node (str): Node to execute OCP commands on.
        file_vol (str): file volume name.
        gluster_nodes (str/list): One or several IPv4 addresses of Gluster
            nodes, where 'file_vol' brick processes must be recreated.
    """
    if not isinstance(gluster_nodes, (list, set, tuple)):
        gluster_nodes = [gluster_nodes]

    # Get Gluster vol brick PIDs
    gluster_volume_status = get_gluster_vol_status(file_vol)
    pids = []
    for gluster_node in gluster_nodes:
        pid = None
        for g_node, g_node_data in gluster_volume_status.items():
            if g_node != gluster_node:
                continue
            for process_name, process_data in g_node_data.items():
                if not process_name.startswith("/var"):
                    continue
                pid = process_data["pid"]
                # When birck is down, pid of the brick is returned as -1.
                # Which is unexepeted situation. So, add appropriate assertion.
                assert pid != "-1", (
                    "Got unexpected PID (-1) for '%s' gluster vol on '%s' "
                    "node." % file_vol, gluster_node)
        assert pid, ("Could not find 'pid' in Gluster vol data for '%s' "
                     "Gluster node. Data: %s" % (
                         gluster_node, gluster_volume_status))
        pids.append((gluster_node, pid))

    # Restart Gluster vol brick processes using found PIDs
    for gluster_node, pid in pids:
        cmd = "kill -9 %s" % pid
        cmd_run_on_gluster_pod_or_node(ocp_client_node, cmd, gluster_node)

    # Wait for Gluster vol brick processes to be recreated
    for gluster_node, pid in pids:
        killed_pid_cmd = "ps -eaf | grep %s | grep -v grep | awk '{print $2}'"
        _waiter = waiter.Waiter(timeout=60, interval=2)
        for w in _waiter:
            result = cmd_run_on_gluster_pod_or_node(
                ocp_client_node, killed_pid_cmd, gluster_node)
            if result.strip() == pid:
                continue
            g.log.info("Brick process '%s' was killed successfully on '%s'" % (
                pid, gluster_node))
            break
        if w.expired:
            error_msg = ("Process ID '%s' still exists on '%s' after waiting "
                         "for it 60 seconds to get killed." % (
                             pid, gluster_node))
            g.log.error(error_msg)
            raise exceptions.ExecutionError(error_msg)

    # Start volume after gluster vol brick processes recreation
    ret, out, err = volume_start(
        "auto_get_gluster_endpoint", file_vol, force=True)
    if ret != 0:
        err_msg = "Failed to start gluster volume %s on %s. error: %s" % (
            file_vol, gluster_node, err)
        g.log.error(err_msg)
        raise AssertionError(err_msg)


@podcmd.GlustoPod()
def restart_file_volume(file_vol, sleep_time=120):
    """Restart file volume (stop and start volume).

    Args:
        file_vol (str): name of a file volume
    """
    gluster_volume_status = get_volume_status(
        "auto_get_gluster_endpoint", file_vol)
    if not gluster_volume_status:
        raise AssertionError("failed to get gluster volume status")

    g.log.info("Gluster volume %s status\n%s : " % (
        file_vol, gluster_volume_status)
    )

    ret, out, err = volume_stop("auto_get_gluster_endpoint", file_vol)
    if ret != 0:
        err_msg = "Failed to stop gluster volume %s. error: %s" % (
            file_vol, err)
        g.log.error(err_msg)
        raise AssertionError(err_msg)

    # Explicit wait to stop ios and pvc creation for 2 mins
    time.sleep(sleep_time)

    ret, out, err = volume_start(
        "auto_get_gluster_endpoint", file_vol, force=True)
    if ret != 0:
        err_msg = "failed to start gluster volume %s error: %s" % (
            file_vol, err)
        g.log.error(err_msg)
        raise AssertionError(err_msg)

    ret, out, err = volume_status("auto_get_gluster_endpoint", file_vol)
    if ret != 0:
        err_msg = ("Failed to get status for gluster volume %s error: %s" % (
            file_vol, err))
        g.log.error(err_msg)
        raise AssertionError(err_msg)


@podcmd.GlustoPod()
def match_heketi_and_gluster_block_volumes_by_prefix(
        heketi_block_volumes, block_vol_prefix):
    """Match block volumes from heketi and gluster. This function can't
       be used for block volumes with custom prefixes

    Args:
        heketi_block_volumes (list): list of heketi block volumes with
                                     which gluster block volumes need to
                                     be matched
        block_vol_prefix (str): block volume prefix by which the block
                                volumes needs to be filtered
    """
    gluster_vol_list = get_volume_list("auto_get_gluster_endpoint")

    gluster_vol_block_list = []
    for gluster_vol in gluster_vol_list[1:]:
        ret, out, err = block_list("auto_get_gluster_endpoint", gluster_vol)
        try:
            if ret != 0 and json.loads(out)["RESULT"] == "FAIL":
                msg = "failed to get block volume list with error: %s" % err
                g.log.error(msg)
                raise AssertionError(msg)
        except Exception as e:
            g.log.error(e)
            raise

        gluster_vol_block_list.extend([
            block_vol.replace(block_vol_prefix, "")
            for block_vol in json.loads(out)["blocks"]
            if block_vol.startswith(block_vol_prefix)
        ])

    vol_difference = set(gluster_vol_block_list) ^ set(heketi_block_volumes)
    if vol_difference:
        err_msg = "Gluster and Heketi Block volume list match failed"
        err_msg += "\nGluster Volumes: %s, " % gluster_vol_block_list
        err_msg += "\nBlock volumes %s" % heketi_block_volumes
        err_msg += "\nDifference: %s" % vol_difference
        raise AssertionError(err_msg)


@podcmd.GlustoPod()
def get_block_hosting_volume_name(heketi_client_node, heketi_server_url,
                                  block_volume, gluster_node=None,
                                  ocp_client_node=None):
    """Returns block hosting volume name of given block volume

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        block_volume (str): Block volume of which block hosting volume
                            returned
    Kwargs:
        gluster_node (str): gluster node/pod ip where gluster command can be
                            run
        ocp_client_node (str): OCP client node where oc commands can be run
    Returns:
        str : Name of the block hosting volume for given block volume
    """
    block_vol_info = heketi_blockvolume_info(
        heketi_client_node, heketi_server_url, block_volume
    )

    for line in block_vol_info.splitlines():
        block_hosting_vol_match = re.search(
            "^Block Hosting Volume: (.*)$", line
        )

        if not block_hosting_vol_match:
            continue

        if gluster_node and ocp_client_node:
            cmd = 'gluster volume list'
            gluster_vol_list = cmd_run_on_gluster_pod_or_node(
                ocp_client_node, cmd, gluster_node).split('\n')
        else:
            gluster_vol_list = get_volume_list('auto_get_gluster_endpoint')

        for vol in gluster_vol_list:
            if block_hosting_vol_match.group(1).strip() in vol:
                return vol


@podcmd.GlustoPod()
def match_heketi_and_gluster_volumes_by_prefix(heketi_volumes, prefix):
    """Match volumes from heketi and gluster using given volume name prefix

    Args:
        heketi_volumes (list): List of heketi volumes with which gluster
                               volumes need to be matched
        prefix (str): Volume prefix by which the volumes needs to be filtered
    """
    g_vol_list = get_volume_list("auto_get_gluster_endpoint")
    g_volumes = [
        g_vol.replace(prefix, "")
        for g_vol in g_vol_list if g_vol.startswith(prefix)]

    vol_difference = set(heketi_volumes) ^ set(g_volumes)
    err_msg = ("Heketi and Gluster volume list match failed"
               "Heketi volumes: {}, Gluster Volumes: {},"
               "Difference: {}"
               .format(heketi_volumes, g_volumes, vol_difference))
    assert not vol_difference, err_msg


@podcmd.GlustoPod()
def get_gluster_vol_free_inodes_with_hosts_of_bricks(vol_name):
    """Get the inodes of gluster volume

    Args:
        vol_name (str): Name of the gluster volume
    Returns:
        dict : Host ip mapped with dict of brick processes and free inodes
    Example:
        >>> get_gluster_vol_free_inodes_with_hosts_of_bricks('testvol')
            {   node_ip1:{
                    'brick_process1':'free_inodes',
                    'brick_process2':'free_inodes'},
                node_ip2:{
                    'brick_process1':'free_inodes',
                    'brick_process2':'free_inodes'},
            }
    """
    hosts_with_inodes_info = dict()

    # Get the detailed status of volume
    vol_status = get_gluster_vol_status(vol_name, is_detail=True)

    # Fetch the node ip, brick processes and free inodes from the status
    for g_node, g_node_data in vol_status.items():
        for brick_process, process_data in g_node_data.items():
            if not brick_process.startswith("/var"):
                continue
            if g_node not in hosts_with_inodes_info:
                hosts_with_inodes_info[g_node] = dict()
            inodes_info = {brick_process: process_data["inodesFree"]}
            hosts_with_inodes_info[g_node].update(inodes_info)
    return hosts_with_inodes_info


def _get_gluster_cmd(target, command):

    if isinstance(command, six.string_types):
        command = [command]
    ocp_client_node = list(g.config['ocp_servers']['client'].keys())[0]
    gluster_pods = get_ocp_gluster_pod_details(ocp_client_node)

    if target == 'auto_get_gluster_endpoint':
        if gluster_pods:
            target = podcmd.Pod(ocp_client_node, gluster_pods[0]["pod_name"])
        else:
            target = list(g.config.get("gluster_servers", {}).keys())[0]
    elif not isinstance(target, podcmd.Pod) and gluster_pods:
        for g_pod in gluster_pods:
            if target in (g_pod['pod_host_ip'], g_pod['pod_hostname']):
                target = podcmd.Pod(ocp_client_node, g_pod['pod_name'])
                break

    if isinstance(target, podcmd.Pod):
        return target.node, ' '.join(['oc', 'rsh', target.podname] + command)

    return target, ' '.join(command)


def get_peer_status(mnode):
    """Parse the output of command 'gluster peer status' using run_async.

    Args:
        mnode (str): Node on which command has to be executed.

    Returns:
        NoneType: None if command execution fails or parse errors.
        list: list of dicts on success.

    Examples:
        >>> get_peer_status(mnode = 'abc.lab.eng.xyz.com')
        [{'uuid': '77dc299a-32f7-43d8-9977-7345a344c398',
        'hostname': 'ijk.lab.eng.xyz.com',
        'state': '3',
        'hostnames' : ['ijk.lab.eng.xyz.com'],
        'connected': '1',
        'stateStr': 'Peer in Cluster'},

        {'uuid': 'b15b8337-9f8e-4ec3-8bdb-200d6a67ae12',
        'hostname': 'def.lab.eng.xyz.com',
        'state': '3',
        'hostnames': ['def.lab.eng.xyz.com'],
        'connected': '1',
        'stateStr': 'Peer in Cluster'}
        ]
    """
    mnode, cmd = _get_gluster_cmd(mnode, "gluster peer status --xml")
    obj = g.run_async(mnode, cmd, log_level='DEBUG')
    ret, out, err = obj.async_communicate()

    if ret:
        g.log.error(
            "Failed to execute peer status command on node {} with error "
            "{}".format(mnode, err))
        return None

    try:
        root = etree.XML(out)
    except etree.ParseError:
        g.log.error("Failed to parse the gluster peer status xml output.")
        return None

    peer_status_list = []
    for peer in root.findall("peerStatus/peer"):
        peer_dict = {}
        for element in peer.getchildren():
            if element.tag == "hostnames":
                hostnames_list = []
                for hostname in element.getchildren():
                    hostnames_list.append(hostname.text)
                element.text = hostnames_list
            peer_dict[element.tag] = element.text
        peer_status_list.append(peer_dict)
    return peer_status_list
