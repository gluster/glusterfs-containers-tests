import os
import tempfile

from glusto.core import Glusto as g
import six
import yaml

from openshiftstoragelibs.command import cmd_run
from openshiftstoragelibs.exceptions import (
    ExecutionError,
    NotSupportedException,
)
from openshiftstoragelibs.openshift_version import get_openshift_version
from openshiftstoragelibs import waiter


MASTER_CONFIG_FILEPATH = "/etc/origin/master/master-config.yaml"


def validate_multipath_pod(hostname, podname, hacount, mpath):
    """Validate multipath for given app-pod.

     Args:
         hostname (str): ocp master node name
         podname (str): app-pod name for which we need to validate
                        multipath. ex : nginx1
         hacount (int): multipath count or HA count. ex: 3
         mpath (str): multipath value to check
     Returns:
         bool: True if successful, otherwise raises exception
    """

    cmd = "oc get pods -o wide | grep %s | awk '{print $7}'" % podname
    pod_nodename = cmd_run(cmd, hostname)

    active_node_count, enable_node_count = (1, hacount - 1)
    cmd = "multipath -ll %s | grep 'status=active' | wc -l" % mpath
    active_count = int(cmd_run(cmd, pod_nodename))
    assert active_node_count == active_count, (
        "Active node count on %s for %s is %s and not 1" % (
            pod_nodename, podname, active_count))

    cmd = "multipath -ll %s | grep 'status=enabled' | wc -l" % mpath
    enable_count = int(cmd_run(cmd, pod_nodename))
    assert enable_node_count == enable_count, (
        "Passive node count on %s for %s is %s and not %s" % (
            pod_nodename, podname, enable_count, enable_node_count))

    g.log.info("Validation of multipath for %s is successfull" % podname)
    return True


def enable_pvc_resize(master_node):
    '''
     This function edits the /etc/origin/master/master-config.yaml
     file - to enable pv_resize feature
     and restarts atomic-openshift service on master node
     Args:
         master_node (str): hostname of masternode  on which
                           want to edit the
                           master-config.yaml file
     Returns:
         bool: True if successful,
               otherwise raise Exception
    '''
    version = get_openshift_version()
    if version < "3.9":
        msg = ("pv resize is not available in openshift "
               "version %s " % version)
        g.log.error(msg)
        raise NotSupportedException(msg)

    with tempfile.NamedTemporaryFile(delete=False) as temp:
        temp_filename = temp.name

    try:
        g.download(master_node, MASTER_CONFIG_FILEPATH, temp_filename)
    except Exception as e:
        err_msg = (
            "Failed to download '{}' from master node '{}' due to"
            "exception\n{}".format(
                MASTER_CONFIG_FILEPATH, master_node, six.text_type(e)))
        raise ExecutionError(err_msg)

    with open(temp_filename, 'r') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        dict_add = data['admissionConfig']['pluginConfig']
        if "PersistentVolumeClaimResize" in dict_add:
            g.log.info("master-config.yaml file is already edited")
            return True
        dict_add['PersistentVolumeClaimResize'] = {
            'configuration': {
                'apiVersion': 'v1',
                'disable': 'false',
                'kind': 'DefaultAdmissionConfig'}}
        data['admissionConfig']['pluginConfig'] = dict_add
        kube_config = data['kubernetesMasterConfig']
        for key in ('apiServerArguments', 'controllerArguments'):
            kube_config[key] = (
                kube_config.get(key)
                if isinstance(kube_config.get(key), dict) else {})
            value = ['ExpandPersistentVolumes=true']
            kube_config[key]['feature-gates'] = value

    with open(temp_filename, 'w+') as f:
        yaml.dump(data, f, default_flow_style=False)

    try:
        g.upload(master_node, temp_filename, MASTER_CONFIG_FILEPATH)
    except Exception as e:
        err_msg = (
            "Failed to upload '{}' to master node '{}' due to"
            "exception\n{}".format(
                master_node, MASTER_CONFIG_FILEPATH, six.text_type(e)))
        raise ExecutionError(err_msg)
    os.unlink(temp_filename)

    if version == "3.9":
        cmd = ("systemctl restart atomic-openshift-master-api "
               "atomic-openshift-master-controllers")
    else:
        cmd = ("/usr/local/bin/master-restart api && "
               "/usr/local/bin/master-restart controllers")
    ret, out, err = g.run(master_node, cmd, "root")
    if ret != 0:
        err_msg = "Failed to execute cmd %s on %s\nout: %s\nerr: %s" % (
            cmd, master_node, out, err)
        g.log.error(err_msg)
        raise ExecutionError(err_msg)

    # Wait for API service to be ready after the restart
    for w in waiter.Waiter(timeout=120, interval=1):
        try:
            cmd_run("oc get nodes", master_node)
            return True
        except AssertionError:
            continue
    err_msg = "Exceeded 120s timeout waiting for OCP API to start responding."
    g.log.error(err_msg)
    raise ExecutionError(err_msg)


def get_iscsi_session(node, iqn=None, raise_on_error=True):
    """Get the list of ip's of iscsi sessions.

    Args:
        node (str): where we want to run the command.
        iqn (str): name of iqn.
    Returns:
        list: list of session ip's.
    raises:
        ExecutionError: In case of any failure if raise_on_error=True.
    """

    cmd = "set -o pipefail && ((iscsiadm -m session"
    if iqn:
        cmd += " | grep %s" % iqn
    cmd += ") | awk '{print $3}' | cut -d ':' -f 1)"

    out = cmd_run(cmd, node, raise_on_error=raise_on_error)

    return out.split("\n") if out else out


def get_iscsi_block_devices_by_path(node, iqn=None, raise_on_error=True):
    """Get list of iscsiadm block devices from path.

    Args:
        node (str): where we want to run the command.
        iqn (str): name of iqn.
    returns:
        dictionary: block devices and there ips.
    raises:
        ExecutionError: In case of any failure if raise_on_error=True.
    """
    cmd = "set -o pipefail && ((ls --format=context /dev/disk/by-path/ip*"
    if iqn:
        cmd += " | grep %s" % iqn
    cmd += ") | awk -F '/|:|-' '{print $10,$25}')"

    out = cmd_run(cmd, node, raise_on_error=raise_on_error)

    if not out:
        return out

    out_dic = {}
    for i in out.split("\n"):
        ip, device = i.strip().split(" ")
        out_dic[device] = ip

    return out_dic


def get_mpath_name_from_device_name(node, device, raise_on_error=True):
    """Get name of mpath device form block device

    Args:
        node (str): where we want to run the command.
        device (str): for which we have to find mpath.
    Returns:
        str: name of device
    Raises:
        ExecutionError: In case of any failure if raise_on_error=True.
    """
    cmd = ("set -o pipefail && ((lsblk -n --list --output=NAME /dev/%s)"
           " | tail -1)" % device)

    return cmd_run(cmd, node, raise_on_error=raise_on_error)


def get_active_and_enabled_devices_from_mpath(node, mpath):
    """Get active and enabled devices from mpath name.

    Args:
        node (str): where we want to run the command.
        mpath (str): name of mpath for which we have to find devices.
    Returns:
        dictionary: devices info
    Raises:
        ExecutionError: In case of any failure
    """

    cmd = ("set -o pipefail && ((multipath -ll %s | grep -A 1 status=%s)"
           r" | grep -v '\-\-' | cut -d ':' -f 4 | awk '{print $2}')")

    active = cmd_run(cmd % (mpath, 'active'), node).split('\n')[1::2]
    enabled = cmd_run(cmd % (mpath, 'enabled'), node).split('\n')[1::2]

    out_dic = {
        'active': active,
        'enabled': enabled}
    return out_dic
