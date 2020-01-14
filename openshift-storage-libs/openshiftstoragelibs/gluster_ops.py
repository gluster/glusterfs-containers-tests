try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json
import re
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
from openshiftstoragelibs.openshift_ops import cmd_run_on_gluster_pod_or_node
from openshiftstoragelibs import podcmd
from openshiftstoragelibs import waiter


@podcmd.GlustoPod()
def wait_to_heal_complete(
        timeout=300, wait_step=5, g_node="auto_get_gluster_endpoint"):
    """Monitors heal for volumes on gluster"""
    gluster_vol_list = get_volume_list(g_node)
    if not gluster_vol_list:
        raise AssertionError("failed to get gluster volume list")

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
def get_gluster_vol_status(file_vol):
    """Get Gluster vol status.

    Args:
        file_vol (str): file volume name.
    """
    # Get Gluster vol info
    gluster_volume_status = get_volume_status(
        "auto_get_gluster_endpoint", file_vol)
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
                                  block_volume):
    """Returns block hosting volume name of given block volume

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        block_volume (str): Block volume of which block hosting volume
                            returned
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

        gluster_vol_list = get_volume_list("auto_get_gluster_endpoint")
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
