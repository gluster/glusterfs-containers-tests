import six
import time
import json
import re

from glusto.core import Glusto as g
from glustolibs.gluster.heal_libs import is_heal_complete
from glustolibs.gluster.volume_ops import (
    get_volume_status,
    get_volume_list,
    volume_status,
    volume_start,
    volume_stop
)
from glustolibs.gluster.block_ops import block_list
from cnslibs.common.openshift_ops import (
    oc_get_pods,
    oc_rsh,
    wait_for_process_to_kill_on_pod
)
from cnslibs.common.heketi_ops import heketi_blockvolume_info
from cnslibs.common import exceptions, podcmd
from cnslibs.common import waiter


def _get_gluster_pod(gluster_pod, hostname=None):
    """create glusto.podcmd object if gluster_pod is string and
       hostname is given else returns gluster_pod object given

    Args:
        gluster_pod (podcmd | str): gluster pod class object has gluster
                                    pod and ocp master node or gluster
                                    pod name
        hostname (str): master node on which gluster pod exists
    """
    if isinstance(gluster_pod, podcmd.Pod):
        return gluster_pod
    elif isinstance(gluster_pod, six.string_types):
        if hostname:
            return podcmd.Pod(hostname, gluster_pod)
        else:
            raise exceptions.ExecutionError(
                "gluster pod is string '%s' but hostname '%s' not valid" % (
                    gluster_pod, hostname)
            )
    else:
        raise exceptions.ExecutionError(
            "invalid gluster pod parameter '%s', '%s'" % (
                gluster_pod, type(gluster_pod))
        )


@podcmd.GlustoPod()
def wait_to_heal_complete(
        gluster_pod, hostname=None, timeout=300, wait_step=5):
    """Monitors heal for volumes on gluster
        gluster_pod (podcmd | str): gluster pod class object has gluster
                                    pod and ocp master node or gluster
                                    pod name
        hostname (str): master node on which gluster pod exists
    """
    gluster_pod = _get_gluster_pod(gluster_pod, hostname)

    gluster_vol_list = get_volume_list(gluster_pod)
    if not gluster_vol_list:
        raise AssertionError("failed to get gluster volume list")

    _waiter = waiter.Waiter(timeout=timeout, interval=wait_step)
    for gluster_vol in gluster_vol_list:
        for w in _waiter:
            if is_heal_complete(gluster_pod, gluster_vol):
                break

    if w.expired:
        err_msg = ("reached timeout waiting for all the gluster volumes "
                   "to reach the 'healed' state.")
        g.log.error(err_msg)
        raise AssertionError(err_msg)


@podcmd.GlustoPod()
def get_brick_pids(gluster_pod, block_hosting_vol, hostname=None):
    """gets brick pids from gluster pods

    Args:
        hostname (str): hostname on which gluster pod exists
        gluster_pod (podcmd | str): gluster pod class object has gluster
                                    pod and ocp master node or gluster
                                    pod name
        block_hosting_vol (str): Block hosting volume id
    """
    gluster_pod = _get_gluster_pod(gluster_pod, hostname)

    gluster_volume_status = get_volume_status(gluster_pod, block_hosting_vol)
    if not gluster_volume_status:
        raise AssertionError("failed to get volume status for gluster "
                             "volume '%s' on pod '%s'" % (
                                gluster_pod, block_hosting_vol))

    gluster_volume_status = gluster_volume_status.get(block_hosting_vol)
    assert gluster_volume_status, ("gluster volume %s not present" % (
                                        block_hosting_vol))

    pids = {}
    for parent_key, parent_val in gluster_volume_status.items():
        for child_key, child_val in parent_val.items():
            if not child_key.startswith("/var"):
                continue

            pid = child_val["pid"]
            # When birck is down, pid of the brick is returned as -1.
            # Which is unexepeted situation, hence raising error.
            if pid == "-1":
                raise AssertionError("Something went wrong brick pid is -1")

            pids[parent_key] = pid

    return pids


@podcmd.GlustoPod()
def restart_brick_process(hostname, gluster_pod, block_hosting_vol):
    """restarts brick process of block hosting volumes

    Args:
        hostname (str): hostname on which gluster pod exists
        gluster_pod (podcmd | str): gluster pod class object has gluster
                                    pod and ocp master node or gluster
                                    pod name
        block_hosting_vol (str): block hosting volume name
    """
    pids = get_brick_pids(gluster_pod, block_hosting_vol, hostname)

    # using count variable to limit the max pod process kill to 2
    count = 0
    killed_process = {}
    pid_keys = pids.keys()
    oc_pods = oc_get_pods(hostname)
    for pod in oc_pods.keys():
        if not (oc_pods[pod]["ip"] in pid_keys and count <= 1):
            continue

        ret, out, err = oc_rsh(
            hostname, pod, "kill -9 %s" % pids[oc_pods[pod]["ip"]]
        )
        if ret != 0:
            err_msg = "failed to kill process id %s error: %s" % (
                pids[oc_pods[pod]["ip"]], err)
            g.log.error(err_msg)
            raise AssertionError(err_msg)

        killed_process[pod] = pids[oc_pods[pod]["ip"]]
        count += 1

    for pod, pid in killed_process.items():
        wait_for_process_to_kill_on_pod(pod, pid, hostname)

    ret, out, err = volume_start(gluster_pod, block_hosting_vol, force=True)
    if ret != 0:
        err_msg = "failed to start gluster volume %s on pod %s error: %s" % (
            block_hosting_vol, gluster_pod, err)
        g.log.error(err_msg)
        raise AssertionError(err_msg)


@podcmd.GlustoPod()
def restart_block_hosting_volume(
        gluster_pod, block_hosting_vol, sleep_time=120, hostname=None):
    """restars block hosting volume service

    Args:
        hostname (str): hostname on which gluster pod exists
        gluster_pod (podcmd | str): gluster pod class object has gluster
                                    pod and ocp master node or gluster
                                    pod name
        block_hosting_vol (str): name of block hosting volume
    """
    gluster_pod = _get_gluster_pod(gluster_pod, hostname)

    gluster_volume_status = get_volume_status(gluster_pod, block_hosting_vol)
    if not gluster_volume_status:
        raise AssertionError("failed to get gluster volume status")

    g.log.info("Gluster volume %s status\n%s : " % (
        block_hosting_vol, gluster_volume_status)
    )

    ret, out, err = volume_stop(gluster_pod, block_hosting_vol)
    if ret != 0:
        err_msg = "failed to stop gluster volume %s on pod %s error: %s" % (
            block_hosting_vol, gluster_pod, err)
        g.log.error(err_msg)
        raise AssertionError(err_msg)

    # Explicit wait to stop ios and pvc creation for 2 mins
    time.sleep(sleep_time)
    ret, out, err = volume_start(gluster_pod, block_hosting_vol, force=True)
    if ret != 0:
        err_msg = "failed to start gluster volume %s on pod %s error: %s" % (
            block_hosting_vol, gluster_pod, err)
        g.log.error(err_msg)
        raise AssertionError(err_msg)

    ret, out, err = volume_status(gluster_pod, block_hosting_vol)
    if ret != 0:
        err_msg = ("failed to get status for gluster volume %s on pod %s "
                   "error: %s" % (block_hosting_vol, gluster_pod, err))
        g.log.error(err_msg)
        raise AssertionError(err_msg)


@podcmd.GlustoPod()
def match_heketi_and_gluster_block_volumes_by_prefix(
        gluster_pod, heketi_block_volumes, block_vol_prefix, hostname=None):
    """Match block volumes from heketi and gluster. This function can't
       be used for block volumes with custom prefixes

    Args:
        gluster_pod (podcmd | str): gluster pod class object has gluster
                                    pod and ocp master node or gluster
                                    pod name
        heketi_block_volumes (list): list of heketi block volumes with
                                     which gluster block volumes need to
                                     be matched
        block_vol_prefix (str): block volume prefix by which the block
                                volumes needs to be filtered
        hostname (str): ocp master node on which oc command gets executed

    """
    gluster_pod = _get_gluster_pod(gluster_pod, hostname)

    gluster_vol_list = get_volume_list(gluster_pod)

    gluster_vol_block_list = []
    for gluster_vol in gluster_vol_list[1:]:
        ret, out, err = block_list(gluster_pod, gluster_vol)
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

    if cmp(sorted(gluster_vol_block_list), heketi_block_volumes) != 0:
        err_msg = "Gluster and Heketi Block volume list match failed"
        err_msg += "\nGluster Volumes: %s, " % gluster_vol_block_list
        err_msg += "\nBlock volumes %s" % heketi_block_volumes
        err_msg += "\nDifference: %s" % (set(gluster_vol_block_list) ^
                                         set(heketi_block_volumes))
        raise AssertionError(err_msg)


@podcmd.GlustoPod()
def get_block_hosting_volume_name(heketi_client_node, heketi_server_url,
                                  block_volume, gluster_pod, hostname=None):
    """Returns block hosting volume name of given block volume

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        block_volume (str): Block volume of which block hosting volume
                            returned
        gluster_pod (podcmd | str): Gluster pod class object has gluster
                                    pod and ocp master node or gluster
                                    pod name
        hostname (str): OCP master node on which ocp commands get executed

    Returns:
        str : Name of the block hosting volume for given block volume
    """
    gluster_pod = _get_gluster_pod(gluster_pod, hostname)

    block_vol_info = heketi_blockvolume_info(
        heketi_client_node, heketi_server_url, block_volume
    )

    for line in block_vol_info.splitlines():
        block_hosting_vol_match = re.search(
            "^Block Hosting Volume: (.*)$", line
        )

        if not block_hosting_vol_match:
            continue

        gluster_vol_list = get_volume_list(gluster_pod)
        for vol in gluster_vol_list:
            if block_hosting_vol_match.group(1).strip() in vol:
                return vol
