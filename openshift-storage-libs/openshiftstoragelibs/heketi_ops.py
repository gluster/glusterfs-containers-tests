try:
    # py2/3
    import simplejson as json
except ImportError:
    # py2
    import json

import re
import time

from glusto.core import Glusto as g
import six

from openshiftstoragelibs import command
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import heketi_version
from openshiftstoragelibs.utils import parse_prometheus_data
from openshiftstoragelibs import waiter


HEKETI_BHV = re.compile(r"Id:(\S+)\s+Cluster:(\S+)\s+Name:(\S+)\s\[block\]")
HEKETI_OPERATIONS = re.compile(r"Id:(\S+)\s+Type:(\S+)\s+Status:(\S+)")
HEKETI_COMMAND_TIMEOUT = g.config.get("common", {}).get(
    "heketi_command_timeout", 120)
TIMEOUT_PREFIX = "timeout %s " % HEKETI_COMMAND_TIMEOUT

MASTER_NODE = list(g.config["ocp_servers"]["master"].keys())[0]
HEKETI_DC = g.config.get("cns", g.config.get("openshift"))[
    "heketi_config"]["heketi_dc_name"]
GET_HEKETI_PODNAME_CMD = (
    "oc get pods -l deploymentconfig=%s -o=custom-columns=:.metadata.name "
    "--no-headers" % HEKETI_DC
)


def cmd_run_on_heketi_pod(cmd, raise_on_error=True):
    """Autodetect Heketi podname and run specified command on it."""
    heketi_podname = command.cmd_run(
        cmd=GET_HEKETI_PODNAME_CMD, hostname=MASTER_NODE).strip()
    # NOTE(vponomar): we redefine '--server' option which is provided
    # as part of the 'cmd' var.
    assert heketi_podname.strip(), (
        "Heketi POD not found on '%s' node using following command: \n%s" % (
            MASTER_NODE, GET_HEKETI_PODNAME_CMD))
    if '--server=' in cmd and 'heketi-cli' in cmd:
        cmd_with_podname_prefix = (
            "oc exec %s -- %s --server=http://localhost:8080" % (
                heketi_podname, cmd))
    else:
        cmd_with_podname_prefix = "oc exec %s -- %s" % (heketi_podname, cmd)
    result = command.cmd_run(
        cmd=cmd_with_podname_prefix, hostname=MASTER_NODE,
        raise_on_error=raise_on_error)
    return result


def heketi_cmd_run(hostname, cmd, raise_on_error=True):
    """Run Heketi client command from a node backing up with Heketi pod CLI."""
    try:
        out = command.cmd_run(
            cmd=cmd, hostname=hostname, raise_on_error=raise_on_error)
    except Exception as e:
        g.log.error(
            'Failed to run "%s" command on the "%s" host. '
            'Got following error:\n%s' % (cmd, hostname, e))
        if ('connection refused' in six.text_type(e).lower()
                or 'operation timed out' in six.text_type(e).lower()):
            time.sleep(1)
            out = cmd_run_on_heketi_pod(cmd, raise_on_error=raise_on_error)
        else:
            raise
    return out


def _set_heketi_global_flags(heketi_server_url, **kwargs):
    """Helper function to set heketi-cli global flags."""

    heketi_server_url = (
        heketi_server_url if heketi_server_url else (
            "http://heketi-storage-project.cloudapps.mystorage.com"))
    json = kwargs.get("json")
    secret = kwargs.get("secret")
    user = kwargs.get("user")
    json_arg = "--json" if json else ""
    secret_arg = "--secret %s" % secret if secret else ""
    user_arg = "--user %s" % user if user else ""
    if not user_arg:
        openshift_config = g.config.get("cns", g.config.get("openshift"))
        heketi_cli_user = openshift_config['heketi_config']['heketi_cli_user']
        if heketi_cli_user:
            user_arg = "--user %s" % heketi_cli_user
            heketi_cli_key = openshift_config[
                'heketi_config']['heketi_cli_key']
            if heketi_cli_key is not None:
                secret_arg = "--secret '%s'" % heketi_cli_key

    return (heketi_server_url, json_arg, secret_arg, user_arg)


def heketi_volume_create(heketi_client_node, heketi_server_url, size,
                         **kwargs):
    """Creates heketi volume with the given user options.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        size (str): Volume size

    Kwargs:
        The keys, values in kwargs are:
            - block : (bool)
            - clusters : (str)|None
            - disperse_data : (int)|None
            - durability : (str)|None
            - gid : (int)|None
            - gluster_volume_options : (str)|None
            - name : (str)|None
            - persistent_volume : (bool)
            - persistent_volume_endpoint : (str)|None
            - persistent_volume_file : (str)|None
            - redundancy : (int):None
            - replica : (int)|None
            - size : (int):None
            - snapshot-factor : (float)|None
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        dict: volume create info on success, only cli option is specified
            without --json option, then it returns raw string output.
    Raises:
        exceptions.ExecutionError when error occurs

    Example:
        heketi_volume_create(heketi_client_node, heketi_server_url, size)
    """

    if not kwargs.get('user'):
        openshift_config = g.config.get("cns", g.config.get("openshift"))
        heketi_cli_user = openshift_config['heketi_config']['heketi_cli_user']
        if heketi_cli_user:
            kwargs['user'] = heketi_cli_user
            heketi_cli_key = openshift_config[
                'heketi_config']['heketi_cli_key']
            if heketi_cli_key is not None:
                kwargs['secret'] = heketi_cli_key

    heketi_server_url = (
        heketi_server_url if heketi_server_url else (
            "http://heketi-storage-project.cloudapps.mystorage.com"))

    block_arg = "--block" if kwargs.get("block") else ""
    clusters_arg = ("--clusters %s" % kwargs.get("clusters")
                    if kwargs.get("clusters") else "")
    disperse_data_arg = ("--disperse-data %d" % kwargs.get("disperse_data")
                         if kwargs.get("disperse_data") else "")
    durability_arg = ("--durability %s" % kwargs.get("durability")
                      if kwargs.get("durability") else "")
    gid_arg = "--gid %d" % int(kwargs.get("gid")) if kwargs.get("gid") else ""
    gluster_volume_options_arg = ("--gluster-volume-options '%s'"
                                  % kwargs.get("gluster_volume_options")
                                  if kwargs.get("gluster_volume_options")
                                  else "")
    name_arg = "--name %s" % kwargs.get("name") if kwargs.get("name") else ""
    persistent_volume_arg = ("--persistent-volume %s"
                             % kwargs.get("persistent_volume")
                             if kwargs.get("persistent_volume") else "")
    persistent_volume_endpoint_arg = ("--persistent-volume-endpoint %s"
                                      % (kwargs.get(
                                         "persistent_volume_endpoint"))
                                      if (kwargs.get(
                                          "persistent_volume_endpoint"))
                                      else "")
    persistent_volume_file_arg = ("--persistent-volume-file %s"
                                  % kwargs.get("persistent_volume_file")
                                  if kwargs.get("persistent_volume_file")
                                  else "")
    redundancy_arg = ("--redundancy %d" % int(kwargs.get("redundancy"))
                      if kwargs.get("redundancy") else "")
    replica_arg = ("--replica %d" % int(kwargs.get("replica"))
                   if kwargs.get("replica") else "")
    snapshot_factor_arg = ("--snapshot-factor %f"
                           % float(kwargs.get("snapshot_factor"))
                           if kwargs.get("snapshot_factor") else "")
    json_arg = "--json" if kwargs.get("json") else ""
    secret_arg = (
        "--secret %s" % kwargs.get("secret") if kwargs.get("secret") else "")
    user_arg = "--user %s" % kwargs.get("user") if kwargs.get("user") else ""

    cmd = ("heketi-cli -s %s volume create --size=%s %s %s %s %s %s %s "
           "%s %s %s %s %s %s %s %s %s %s" % (
               heketi_server_url, str(size), block_arg, clusters_arg,
               disperse_data_arg, durability_arg, gid_arg,
               gluster_volume_options_arg, name_arg,
               persistent_volume_arg, persistent_volume_endpoint_arg,
               persistent_volume_file_arg, redundancy_arg, replica_arg,
               snapshot_factor_arg, json_arg, secret_arg, user_arg))
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if json_arg:
        return json.loads(out)
    return out


def heketi_volume_info(heketi_client_node, heketi_server_url, volume_id,
                       **kwargs):
    """Executes heketi volume info command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        volume_id (str): Volume ID

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        dict: volume info on success

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_volume_info(heketi_client_node, volume_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s volume info %s %s %s %s" % (
        heketi_server_url, volume_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if json_arg:
        return json.loads(out)
    return out


def heketi_volume_expand(heketi_client_node, heketi_server_url, volume_id,
                         expand_size, **kwargs):
    """Executes heketi volume expand command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        volume_id (str): Volume ID
        expand_size (str): volume expand size

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        dict: volume expand info on success, only cli option is specified
            without --json option, then it returns raw string output.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_volume_expand(heketi_client_node, heketi_server_url, volume_id,
                             expand_size)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = ("heketi-cli -s %s volume expand --volume=%s "
           "--expand-size=%s %s %s %s" % (
               heketi_server_url, volume_id, expand_size, json_arg,
               admin_key, user))
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if json_arg:
        return json.loads(out)
    return out


def heketi_volume_delete(heketi_client_node, heketi_server_url, volume_id,
                         raise_on_error=True, **kwargs):
    """Executes heketi volume delete command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        volume_id (str): Volume ID
        raise_on_error (bool): whether or not to raise exception
            in case of an error.

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        str: volume delete command output on success

    Raises:
        exceptions.ExecutionError when error occurs

    Example:
        heketi_volume_delete(heketi_client_node, heketi_server_url, volume_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s volume delete %s %s %s %s" % (
        heketi_server_url, volume_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(
        heketi_client_node, cmd, raise_on_error=raise_on_error)
    return out


def heketi_volume_list(heketi_client_node, heketi_server_url, **kwargs):
    """Executes heketi volume list command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        dict: volume list with --json on success, if cli option is specified
            without --json option or with url, it returns raw string output.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_volume_info(heketi_client_node, heketi_server_url)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s volume list %s %s %s" % (
        heketi_server_url, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if json_arg:
        return json.loads(out)
    return out


def heketi_topology_info(heketi_client_node, heketi_server_url, **kwargs):
    """Executes heketi topology info command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        dict: topology info if --json option is specified. If only cli option
            is specified, raw command output is returned on success.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_topology_info(heketi_client_node, heketi_server_url)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s topology info %s %s %s" % (
        heketi_server_url, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if json_arg:
        return json.loads(out)
    return out


def hello_heketi(heketi_client_node, heketi_server_url, **kwargs):
    """Executes curl command to check if heketi server is alive.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Kwargs:
        The keys, values in kwargs are:
            - secret : (str)|None
            - user : (str)|None

    Returns:
        bool: True, if heketi server is alive

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        hello_heketi(heketi_client_node, heketi_server_url)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "curl --max-time 10 %s/hello" % heketi_server_url

    try:
        command.cmd_run(cmd=cmd, hostname=heketi_client_node)
    except Exception as e:
        g.log.error(
            'Failed to run "%s" command on the "%s" host. '
            'Got following error:\n%s' % (cmd, heketi_client_node, e))
        if ('connection refused' in six.text_type(e).lower()
                or 'operation timed out' in six.text_type(e).lower()):
            time.sleep(1)
            cmd_run_on_heketi_pod(
                "curl --max-time 10 http://localhost:8080/hello")
        else:
            raise
    return True


def heketi_cluster_create(heketi_client_node, heketi_server_url, **kwargs):
    """Executes heketi cluster create command with provided options.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None
            - block : (bool)|None
            - file : (bool)|None

    Returns:
        str: cluster delete command output on success

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_cluster_create(
            heketi_client_node, heketi_server_url, block=True)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s cluster create %s %s %s" % (
        heketi_server_url, json_arg, admin_key, user)

    if not kwargs.get("block", True):
        cmd += " --block=false"
    if not kwargs.get("file", True):
        cmd += " --file=false"

    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if kwargs.get("json", False):
        return json.loads(out)
    return out


def heketi_cluster_delete(heketi_client_node, heketi_server_url, cluster_id,
                          **kwargs):
    """Executes heketi cluster delete command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        cluster_id (str): Cluster ID

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        str: cluster delete command output on success

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_cluster_delete(heketi_client_node, heketi_server_url,
                              cluster_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s cluster delete %s %s %s %s" % (
        heketi_server_url, cluster_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    return out


def heketi_cluster_info(heketi_client_node, heketi_server_url, cluster_id,
                        **kwargs):
    """Executes heketi cluster info command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        cluster_id (str): Volume ID

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        dict: cluster info on success
        False: in case of failure

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_cluster_info(heketi_client_node, volume_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s cluster info %s %s %s %s" % (
        heketi_server_url, cluster_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if json_arg:
        return json.loads(out)
    return out


def heketi_cluster_list(heketi_client_node, heketi_server_url, **kwargs):
    """Executes heketi cluster list command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        dict: cluster list with --json on success, if cli option is specified
            without --json option or with url, it returns raw string output.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_cluster_info(heketi_client_node, heketi_server_url)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s cluster list %s %s %s" % (
        heketi_server_url, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if json_arg:
        return json.loads(out)
    return out


def heketi_device_add(heketi_client_node, heketi_server_url, device_name,
                      node_id, **kwargs):
    """Executes heketi device add command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        device name (str): Device name to add
        node_id (str): Node id to add the device

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        str: heketi device add command output on success.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_device_add(heketi_client_node, heketi_server_url, device_name,
                          node_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s device add --name=%s --node=%s %s %s %s" % (
        heketi_server_url, device_name, node_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    return out


def heketi_device_delete(heketi_client_node, heketi_server_url, device_id,
                         **kwargs):
    """Executes heketi device delete command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        device id (str): Device id to delete

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        str: heketi device delete command output on success.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_device_delete(heketi_client_node, heketi_server_url, device_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s device delete %s %s %s %s" % (
        heketi_server_url, device_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    return out


def heketi_device_disable(heketi_client_node, heketi_server_url, device_id,
                          **kwargs):
    """Executes heketi device disable command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        device_id (str): Device id to disable device

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        str: heketi device disable command output on success.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_device_disable(heketi_client_node, heketi_server_url, device_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)
    cmd = "heketi-cli -s %s device disable %s %s %s %s" % (
        heketi_server_url, device_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    return out


def heketi_device_enable(heketi_client_node, heketi_server_url, device_id,
                         **kwargs):
    """Executes heketi device enable command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        device_id (str): Device id to enable device

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        str: heketi device enable command output on success.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_device_enable(heketi_client_node, heketi_server_url, device_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)
    cmd = "heketi-cli -s %s device enable %s %s %s %s" % (
        heketi_server_url, device_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    return out


def heketi_device_info(heketi_client_node, heketi_server_url, device_id,
                       **kwargs):
    """Executes heketi device info command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        device_id (str): Device ID

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        Str: device info as raw CLI output if "json" arg is not provided.
        Dict: device info parsed to dict if "json" arg is provided.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_device_info(heketi_client_node, heketi_server_url, device_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s device info %s %s %s %s" % (
        heketi_server_url, device_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if json_arg:
        device_info = json.loads(out)
        return device_info
    else:
        return out


def heketi_device_remove(heketi_client_node, heketi_server_url, device_id,
                         **kwargs):
    """Executes heketi device remove command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        device_id (str): Device id to remove device

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        str: heketi device remove command output on success.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_device_remove(heketi_client_node, heketi_server_url, device_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s device remove %s %s %s %s" % (
        heketi_server_url, device_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    return out


def heketi_node_delete(heketi_client_node, heketi_server_url, node_id,
                       **kwargs):
    """Executes heketi node delete command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url.
        node_id (str): Node id to delete

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        str: heketi node delete command output on success.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_node_delete(heketi_client_node, heketi_server_url, node_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s node delete %s %s %s %s" % (
        heketi_server_url, node_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    return out


def heketi_node_disable(heketi_client_node, heketi_server_url, node_id,
                        **kwargs):
    """Executes heketi node disable command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        node_id (str): Node id to disable node

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        str: heketi node disable command output on success.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_node_disable(heketi_client_node, heketi_server_url, node_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s node disable %s %s %s %s" % (
        heketi_server_url, node_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    return out


def heketi_node_enable(heketi_client_node, heketi_server_url, node_id,
                       **kwargs):
    """Executes heketi node enable command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        node_id (str): Node id to enable device

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        str: heketi node enable command output on success.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_node_enable(heketi_client_node, heketi_server_url, node_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s node enable %s %s %s %s" % (
        heketi_server_url, node_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    return out


def heketi_node_info(heketi_client_node, heketi_server_url, node_id, **kwargs):
    """Executes heketi node info command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        node_id (str): Node ID

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        dict: node info on success,
        str: raw output if 'json' arg is not provided.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_node_info(heketi_client_node, heketi_server_url, node_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s node info %s %s %s %s" % (
        heketi_server_url, node_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if json_arg:
        return json.loads(out)
    return out


def heketi_node_list(heketi_client_node, heketi_server_url,
                     heketi_user=None, heketi_secret=None):
    """Execute CLI 'heketi node list' command and parse its output.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed
        heketi_server_url (str): Heketi server url to perform request to
        heketi_user (str): Name of the user to perform request with
        heketi_secret (str): Secret for 'heketi_user'
    Returns:
        list of strings which are node IDs
    Raises: openshiftstoragelibs.exceptions.ExecutionError when command fails.
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, user=heketi_user, secret=heketi_secret)

    cmd = "heketi-cli -s %s node list %s %s %s" % (
        heketi_server_url, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)

    heketi_node_id_list = []
    for line in out.strip().split("\n"):
        # Line looks like this: 'Id:nodeIdString\tCluster:clusterIdString'
        heketi_node_id_list.append(
            line.strip().split("Cluster")[0].strip().split(":")[1])
    return heketi_node_id_list


def heketi_blockvolume_info(heketi_client_node, heketi_server_url,
                            block_volume_id, **kwargs):
    """Executes heketi blockvolume info command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        block_volume_id (str): block volume ID

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        dict: block volume info on success.
        str: raw output if 'json' arg is not provided.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_blockvolume_info(heketi_client_node, block_volume_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s blockvolume info %s %s %s %s" % (
        heketi_server_url, block_volume_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if json_arg:
        return json.loads(out)
    return out


def heketi_blockvolume_create(heketi_client_node, heketi_server_url, size,
                              **kwargs):
    """Executes heketi blockvolume create

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        size (int): blockvolume size

    Kwargs:
        The keys, values in kwargs are:
            - name : (str)|None
            - cluster : (str)|None
            - ha : (int)|None
            - auth : (bool)
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        dict: blockvolume create info on success, only cli option is specified
            without --json option, then it returns raw string output.

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_blockvolume_create(heketi_client_node, heketi_server_url, size)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    auth = clusters = ha = name = None
    if heketi_server_url is None:
        heketi_server_url = (
            "http://heketi-storage-project.cloudapps.mystorage.com")

    if 'auth' in kwargs:
        auth = kwargs['auth']
    if 'clusters' in kwargs:
        clusters = kwargs['clusters']
    if 'ha' in kwargs:
        ha = int(kwargs['ha'])
    if 'name' in kwargs:
        name = kwargs['name']

    auth_arg = clusters_arg = ha_arg = name_arg = ''

    if auth:
        auth_arg = "--auth"
    if clusters is not None:
        clusters_arg = "--clusters %s" % clusters
    if ha is not None:
        ha_arg = "--ha %d" % ha
    if name is not None:
        name_arg = "--name %s" % name

    cmd = ("heketi-cli -s %s blockvolume create --size=%s %s %s %s %s "
           "%s %s %s" % (heketi_server_url, str(size), auth_arg,
                         clusters_arg, ha_arg, name_arg,
                         admin_key, user, json_arg))
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if json_arg:
        return json.loads(out)
    return out


def heketi_blockvolume_delete(heketi_client_node, heketi_server_url,
                              block_volume_id, raise_on_error=True, **kwargs):
    """Executes heketi blockvolume delete command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        block_volume_id (str): block volume ID
        raise_on_error (bool): whether or not to raise exception
            in case of an error.

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        str: volume delete command output on success

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_blockvolume_delete(heketi_client_node, heketi_server_url,
                                  block_volume_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s blockvolume delete %s %s %s %s" % (
        heketi_server_url, block_volume_id, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(
        heketi_client_node, cmd, raise_on_error=raise_on_error)
    return out


def heketi_blockvolume_list(heketi_client_node, heketi_server_url, **kwargs):
    """Executes heketi blockvolume list command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        dict: volume list with --json on success, if cli option is specified
            without --json option or with url, it returns raw string output.
        False otherwise

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_volume_info(heketi_client_node, heketi_server_url)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s blockvolume list %s %s %s" % (
        heketi_server_url, json_arg, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if json_arg:
        return json.loads(out)
    return out


def verify_volume_name_prefix(hostname, prefix, namespace, pvc_name,
                              heketi_server_url, **kwargs):
    """Checks whether heketi voluem is present with volname prefix or not.

    Args:
        hostname (str): hostname on which we want
                        to check the heketi vol
        prefix (str): volnameprefix given in storageclass
        namespace (str): namespace
        pvc_name (str): name of the pvc
        heketi_server_url (str): Heketi server url

    Kwargs:
        The keys, values in kwargs are:
            - json : (bool)
            - secret : (str)|None
            - user : (str)|None

    Returns:
        bool: True if volume found.

    Raises:
        exceptions.ExecutionError: if command fails.
    """
    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    heketi_vol_name_prefix = "%s_%s_%s_" % (prefix, namespace, pvc_name)
    cmd = "heketi-cli -s %s volume list %s %s %s | grep %s" % (
        heketi_server_url, json_arg, admin_key, user, heketi_vol_name_prefix)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(hostname, cmd)
    output = out.strip()
    g.log.info("heketi volume with volnameprefix present %s" % output)
    return True


def set_tags(heketi_client_node, heketi_server_url, source, source_id, tag,
             **kwargs):
    """Set any tags on Heketi node or device.

    Args:
        - heketi_client_node (str) : Node where we want to run our commands.
            eg. "10.70.47.64"
        - heketi_server_url (str) : This is a heketi server url
            eg. "http://172.30.147.142:8080
        - source (str) : This var is for node or device whether we
                         want to set tag on node or device.
                         Allowed values are "node" and "device".
        - sorrce_id (str) : ID of node or device.
            eg. "4f9c0249834919dd372e8fb3344cd7bd"
        - tag (str) : This is a tag which we want to set
            eg. "arbiter:required"
    Kwargs:
        user (str) : username
        secret (str) : secret for that user
    Returns:
        True : if successful
    Raises:
        ValueError : when improper input data are provided.
        exceptions.ExecutionError : when command fails.
    """

    if source not in ('node', 'device'):
        msg = ("Incorrect value we can use 'node' or 'device' instead of %s."
               % source)
        g.log.error(msg)
        raise ValueError(msg)

    heketi_server_url, json_args, secret, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = ("heketi-cli -s %s %s settags %s %s %s %s" %
           (heketi_server_url, source, source_id, tag, user, secret))
    cmd = TIMEOUT_PREFIX + cmd
    heketi_cmd_run(heketi_client_node, cmd)
    g.log.info("Tagging of %s to %s is successful" % (source, tag))
    return True


def set_arbiter_tag(heketi_client_node, heketi_server_url, source,
                    source_id, arbiter_tag_value, **kwargs):
    """Set Arbiter tags on Heketi node or device.

    Args:
        - heketi_client_node (str) : node where we want to run our commands.
            eg. "10.70.47.64"
        - heketi_server_url (str) : This is a heketi server url
            eg. "http://172.30.147.142:8080
        - source (str) : This var is for node or device whether we
                         want to set tag on node or device.
                         Allowed values are "node" and "device".
        - source_id (str) : ID of Heketi node or device
            eg. "4f9c0249834919dd372e8fb3344cd7bd"
        - arbiter_tag_value (str) : This is a tag which we want to set
            Allowed values are "required", "disabled" and "supported".
    Kwargs:
        user (str) : username
        secret (str) : secret for that user
    Returns:
        True : if successful
    Raises:
        ValueError : when improper input data are provided.
        exceptions.ExecutionError : when command fails.
    """

    version = heketi_version.get_heketi_version(heketi_client_node)
    if version < '6.0.0-11':
        msg = ("heketi-client package %s does not support arbiter "
               "functionality" % version.v_str)
        g.log.error(msg)
        raise NotImplementedError(msg)

    if arbiter_tag_value in ('required', 'disabled', 'supported'):
        arbiter_tag_value = "arbiter:%s" % arbiter_tag_value
        return set_tags(heketi_client_node, heketi_server_url,
                        source, source_id, arbiter_tag_value, **kwargs)

    msg = ("Incorrect value we can use 'required', 'disabled', 'supported'"
           "instead of %s" % arbiter_tag_value)
    g.log.error(msg)
    raise ValueError(msg)


def rm_tags(heketi_client_node, heketi_server_url, source, source_id, tag,
            **kwargs):
    """Remove any kind of tags from Heketi node or device.

    Args:
        - heketi_client_node (str) : Node where we want to run our commands.
            eg. "10.70.47.64"
        - heketi_server_url (str) : This is a heketi server url
            eg. "http://172.30.147.142:8080
        - source (str) : This var is for node or device whether we
                         want to set tag on node or device.
                         Allowed values are "node" and "device".
        - sorrce_id (str) : id of node or device
            eg. "4f9c0249834919dd372e8fb3344cd7bd"
        - tag (str) : This is a tag which we want to remove.
    Kwargs:
        user (str) : username
        secret (str) : secret for that user
    Returns:
        True : if successful
    Raises:
        ValueError : when improper input data are provided.
        exceptions.ExecutionError : when command fails.
    """

    heketi_server_url, json_args, secret, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)
    if source not in ('node', 'device'):
        msg = ("Incorrect value we can use 'node' or 'device' instead of %s."
               % source)
        g.log.error(msg)
        raise ValueError(msg)

    cmd = ("heketi-cli -s %s %s rmtags %s %s %s %s" %
           (heketi_server_url, source, source_id, tag, user, secret))
    cmd = TIMEOUT_PREFIX + cmd
    heketi_cmd_run(heketi_client_node, cmd)
    g.log.info("Removal of %s tag from %s is successful." % (tag, source))
    return True


def rm_arbiter_tag(heketi_client_node, heketi_server_url, source, source_id,
                   **kwargs):
    """Remove Arbiter tag from Heketi node or device.

    Args:
        - heketi_client_node (str) : Node where we want to run our commands.
            eg. "10.70.47.64"
        - heketi_server_url (str) : This is a heketi server url
            eg. "http://172.30.147.142:8080
        - source (str) : This var is for node or device whether we
                         want to set tag on node or device.
                         Allowed values are "node" and "device".
        - source_id (str) : ID of Heketi node or device.
            eg. "4f9c0249834919dd372e8fb3344cd7bd"
    Kwargs:
        user (str) : username
        secret (str) : secret for that user
    Returns:
        True : if successful
    Raises:
        ValueError : when improper input data are provided.
        exceptions.ExecutionError : when command fails.
    """

    version = heketi_version.get_heketi_version(heketi_client_node)
    if version < '6.0.0-11':
        msg = ("heketi-client package %s does not support arbiter "
               "functionality" % version.v_str)
        g.log.error(msg)
        raise NotImplementedError(msg)

    return rm_tags(heketi_client_node, heketi_server_url,
                   source, source_id, 'arbiter', **kwargs)


def get_heketi_metrics(heketi_client_node, heketi_server_url,
                       prometheus_format=False):
    """Execute curl command to get metrics output.

    Args:
        - heketi_client_node (str) : Node where we want to run our commands.
        - heketi_server_url (str) : This is a heketi server url.
        - prometheus_format (bool) : control the format of output
            by default it is False, So it will parse prometheus format into
            python dict. If we need prometheus format we have to set it True.

    Raises:
        exceptions.ExecutionError: if command fails.

    Returns:
        Metrics output: if successful
    """

    version = heketi_version.get_heketi_version(heketi_client_node)
    if version < '6.0.0-14':
        msg = ("heketi-client package %s does not support heketi "
               "metrics functionality" % version.v_str)
        g.log.error(msg)
        raise NotImplementedError(msg)

    cmd = "curl --max-time 10 %s/metrics" % heketi_server_url

    try:
        out = command.cmd_run(cmd=cmd, hostname=heketi_client_node)
    except Exception as e:
        g.log.error(
            'Failed to run "%s" command on the "%s" host. '
            'Got following error:\n%s' % (cmd, heketi_client_node, e))
        if ('connection refused' in six.text_type(e).lower()
                or 'operation timed out' in six.text_type(e).lower()):
            time.sleep(1)
            out = cmd_run_on_heketi_pod(
                "curl --max-time 10 http://localhost:8080/metrics")
        else:
            raise

    if prometheus_format:
        return out.strip()
    return parse_prometheus_data(out)


def heketi_examine_gluster(heketi_client_node, heketi_server_url):
    """Execute heketi command to examine output from gluster servers.

    Args:
        - heketi_client_node (str): Node where we want to run our commands.
        - heketi_server_url (str): This is a heketi server url.

    Raises:
        NotImplementedError: if heketi version is not expected
        exceptions.ExecutionError: if command fails.

    Returns:
        dictionary: if successful
    """

    version = heketi_version.get_heketi_version(heketi_client_node)
    if version < '8.0.0-7':
        msg = ("heketi-client package %s does not support server state examine"
               " gluster" % version.v_str)
        g.log.error(msg)
        raise NotImplementedError(msg)

    heketi_server_url, json_arg, secret, user = _set_heketi_global_flags(
        heketi_server_url)
    # output is always json-like and we do not need to provide "--json" CLI arg
    cmd = ("heketi-cli server state examine gluster -s %s %s %s"
           % (heketi_server_url, user, secret))
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    return json.loads(out)


def get_block_hosting_volume_list(
        heketi_client_node, heketi_server_url, **kwargs):
    """Get heketi block hosting volume list.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Kwargs:
        The keys, values in kwargs are:
            - secret : (str)|None
            - user : (str)|None

    Returns:
        dict: block hosting volume list

    Raises:
        exceptions.ExecutionError: if command fails.
    """

    out = heketi_volume_list(
        heketi_client_node, heketi_server_url, **kwargs)

    BHV = {}

    for volume in HEKETI_BHV.findall(out.strip()):
        BHV[volume[0]] = {'Cluster': volume[1], 'Name': volume[2]}

    return BHV


def get_total_free_space(heketi_client_node, heketi_server_url):
    """
    Calculates free space across devices which are online
    and skips the ones which are offline.
    Args:
        - heketi_client_node (str): Node where we want to run our commands.
        - heketi_server_url (str): This is a heketi server url.

    Returns:
        int: if successful

    """
    device_free_spaces = []
    heketi_node_id_list = heketi_node_list(
        heketi_client_node, heketi_server_url)
    for node_id in heketi_node_id_list:
        total_device_free_space = 0
        node_info_dict = heketi_node_info(
            heketi_client_node, heketi_server_url,
            node_id, json=True)
        if (node_info_dict["state"].strip().lower() != "online"):
            continue
        for device in node_info_dict["devices"]:
            if (device["state"].strip().lower() != "online"):
                continue
            total_device_free_space += (device["storage"]["free"])
        device_free_spaces.append(total_device_free_space / 1024 ** 2)
    return int(sum(device_free_spaces)), len(device_free_spaces)


def heketi_server_operations_list(
        heketi_client_node, heketi_server_url, **kwargs):
    """Executes heketi server operations list command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Returns:
        list: list of server operations pending

    Raises:
        exceptions.ExecutionError: if command fails.
    """
    version = heketi_version.get_heketi_version(heketi_client_node)
    if version < '8.0.0-10':
        msg = (
            "heketi-client package %s does not support operations "
            "list functionality" % version.v_str)
        g.log.error(msg)
        raise NotImplementedError(msg)

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s %s %s server operations list" % (
        heketi_server_url, admin_key, user)
    cmd = TIMEOUT_PREFIX + cmd
    out = heketi_cmd_run(heketi_client_node, cmd)
    if out:
        operations = []
        for operation in HEKETI_OPERATIONS.findall(out.strip()):
            operations.append({
                'id': operation[0],
                'type': operation[1],
                'status': operation[2]
            })
        return operations
    else:
        g.log.info("No any pendig heketi server operation")
        return []


def heketi_server_operation_cleanup(
        heketi_client_node, heketi_server_url, operation_id=None,
        timeout=120, wait_time=5, **kwargs):
    """Executes heketi server operations cleanup command and wait until
    cleanup operations get completed for given timeout.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        operation_id (str): Operation Id needs to be cleaned.

    Raises:
        exceptions.ExecutionError: If cleanup not completed in given timeout.
    """
    version = heketi_version.get_heketi_version(heketi_client_node)
    if version < '8.0.0-10':
        msg = (
            "heketi-client package %s does not support operations "
            "cleanup functionality" % version.v_str)
        g.log.error(msg)
        raise NotImplementedError(msg)

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)
    cmd = "heketi-cli -s %s %s %s server operations cleanup" % (
        heketi_server_url, admin_key, user)
    if operation_id:
        cmd += " %s" % operation_id

    cmd = TIMEOUT_PREFIX + cmd
    heketi_cmd_run(heketi_client_node, cmd)
    for w in waiter.Waiter(timeout=timeout, interval=wait_time):
        cleanup_operations = heketi_server_operations_list(
            heketi_client_node, heketi_server_url, **kwargs)

        cleanup_operation = [
            operation["id"]
            for operation in cleanup_operations
            if operation["id"] == operation_id]
        if not cleanup_operation:
            break

    if w.expired:
        err_msg = (
            "Heketi server cleanup operation still pending even "
            "after %s second" % timeout)
        g.log.error(err_msg)
        raise exceptions.ExecutionError(err_msg)
