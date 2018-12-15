from glusto.core import Glusto as g
import yaml

from cnslibs.common.exceptions import (
    ExecutionError,
    NotSupportedException)
from cnslibs.common.openshift_ops import oc_version


MASTER_CONFIG_FILEPATH = "/etc/origin/master/master-config.yaml"


def validate_multipath_pod(hostname, podname, hacount):
    '''
     This function validates multipath for given app-pod
     Args:
         hostname (str): ocp master node name
         podname (str): app-pod name for which we need to validate
                        multipath. ex : nginx1
         hacount (int): multipath count or HA count. ex: 3
     Returns:
         bool: True if successful,
               otherwise False
    '''
    cmd = "oc get pods -o wide | grep %s | awk '{print $7}'" % podname
    ret, out, err = g.run(hostname, cmd, "root")
    if ret != 0 or out == "":
        g.log.error("failed to exectute cmd %s on %s, err %s"
                    % (cmd, hostname, out))
        return False
    pod_nodename = out.strip()
    active_node_count = 1
    enable_node_count = hacount - 1
    cmd = "multipath -ll | grep 'status=active' | wc -l"
    ret, out, err = g.run(pod_nodename, cmd, "root")
    if ret != 0 or out == "":
        g.log.error("failed to exectute cmd %s on %s, err %s"
                    % (cmd, pod_nodename, out))
        return False
    active_count = int(out.strip())
    if active_node_count != active_count:
        g.log.error("active node count on %s for %s is %s and not 1"
                    % (pod_nodename, podname, active_count))
        return False
    cmd = "multipath -ll | grep 'status=enabled' | wc -l"
    ret, out, err = g.run(pod_nodename, cmd, "root")
    if ret != 0 or out == "":
        g.log.error("failed to exectute cmd %s on %s, err %s"
                    % (cmd, pod_nodename, out))
        return False
    enable_count = int(out.strip())
    if enable_node_count != enable_count:
        g.log.error("passive node count on %s for %s is %s "
                    "and not %s" % (
                        pod_nodename, podname, enable_count,
                        enable_node_count))
        return False

    g.log.info("validation of multipath for %s is successfull"
               % podname)
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
    version = oc_version(master_node)
    if any(v in version for v in ("3.6", "3.7", "3.8")):
        msg = ("pv resize is not available in openshift "
               "version %s " % version)
        g.log.error(msg)
        raise NotSupportedException(msg)

    try:
        conn = g.rpyc_get_connection(master_node, user="root")
        if conn is None:
            err_msg = ("Failed to get rpyc connection of node %s"
                       % master_node)
            g.log.error(err_msg)
            raise ExecutionError(err_msg)

        with conn.builtin.open(MASTER_CONFIG_FILEPATH, 'r') as f:
            data = yaml.load(f)
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
        with conn.builtin.open(MASTER_CONFIG_FILEPATH, 'w+') as f:
            yaml.dump(data, f, default_flow_style=False)
    except Exception as err:
        raise ExecutionError("failed to edit master-config.yaml file "
                             "%s on %s" % (err, master_node))
    finally:
        g.rpyc_close_connection(master_node, user="root")

    g.log.info("successfully edited master-config.yaml file "
               "%s" % master_node)
    if "3.9" in version:
        cmd = ("systemctl restart atomic-openshift-master-api "
               "atomic-openshift-master-controllers")
    else:
        cmd = ("/usr/local/bin/master-restart api && "
               "/usr/local/bin/master-restart controllers")
    ret, out, err = g.run(master_node, cmd, "root")
    if ret != 0 or out == "":
        err_msg = ("failed to execute cmd %s on %s, err %s"
                   % (cmd, master_node, out))
        g.log.error(err_msg)
        raise ExecutionError(err_msg)

    return True
