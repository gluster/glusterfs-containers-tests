import json

from glusto.core import Glusto as g

from cnslibs.common import exceptions
from cnslibs.common.utils import parse_prometheus_data


def _set_heketi_global_flags(heketi_server_url, **kwargs):
    """Helper function to set heketi-cli global flags."""

    heketi_server_url = (heketi_server_url if heketi_server_url else ("http:" +
                         "//heketi-storage-project.cloudapps.mystorage.com"))
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
                         raw_cli_output=False, **kwargs):
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
        Tuple (ret, out, err): if raw_cli_output is True.
    Raises:
        exceptions.ExecutionError when error occurs and raw_cli_output is False

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

    heketi_server_url = (heketi_server_url if heketi_server_url else ("http:" +
                         "//heketi-storage-project.cloudapps.mystorage.com"))

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

    err_msg = "Failed to create volume. "

    cmd = ("heketi-cli -s %s volume create --size=%s %s %s %s %s %s %s "
           "%s %s %s %s %s %s %s %s %s %s" % (
               heketi_server_url, str(size), block_arg, clusters_arg,
               disperse_data_arg, durability_arg, gid_arg,
               gluster_volume_options_arg, name_arg,
               persistent_volume_arg, persistent_volume_endpoint_arg,
               persistent_volume_file_arg, redundancy_arg, replica_arg,
               snapshot_factor_arg, json_arg, secret_arg, user_arg))
    ret, out, err = g.run(heketi_client_node, cmd)

    if raw_cli_output:
        return ret, out, err
    if ret != 0:
        err_msg += "Out: %s \n Err: %s" % (out, err)
        g.log.error(err_msg)
        raise exceptions.ExecutionError(err_msg)
    if json_arg:
        return json.loads(out)
    return out


def heketi_volume_info(heketi_client_node, heketi_server_url, volume_id,
                       raw_cli_output=False, **kwargs):
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
        False: in case of failure
        Tuple (ret, out, err): if raw_cli_output is True

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_volume_info(heketi_client_node, volume_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s volume info %s %s %s %s" % (
        heketi_server_url, volume_id, json_arg, admin_key, user)
    ret, out, err = g.run(heketi_client_node, cmd)

    if raw_cli_output:
        return ret, out, err
    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)

    if json_arg:
        return json.loads(out)
    return out


def heketi_volume_expand(heketi_client_node, heketi_server_url, volume_id,
                         expand_size, raw_cli_output=False, **kwargs):
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
        Tuple (ret, out, err): if raw_cli_output is True

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
    ret, out, err = g.run(heketi_client_node, cmd)

    if raw_cli_output:
        return ret, out, err

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
    if json_arg:
        return json.loads(out)
    return out


def heketi_volume_delete(heketi_client_node, heketi_server_url, volume_id,
                         raw_cli_output=False, raise_on_error=True, **kwargs):
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
        Tuple (ret, out, err): if raw_cli_output is True

    Raises:
        exceptions.ExecutionError when error occurs and raw_cli_output is False

    Example:
        heketi_volume_delete(heketi_client_node, heketi_server_url, volume_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)
    err_msg = "Failed to delete '%s' volume. " % volume_id

    cmd = "heketi-cli -s %s volume delete %s %s %s %s" % (
        heketi_server_url, volume_id, json_arg, admin_key, user)
    ret, out, err = g.run(heketi_client_node, cmd)

    if raw_cli_output:
        return ret, out, err
    if ret != 0:
        err_msg += "Out: %s, \nErr: %s" % (out, err)
        g.log.error(err_msg)
        if raise_on_error:
            raise exceptions.ExecutionError(err_msg)
    return out


def heketi_volume_list(heketi_client_node, heketi_server_url,
                       raw_cli_output=False, **kwargs):
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
        Tuple (ret, out, err): if raw_cli_output is True

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_volume_info(heketi_client_node, heketi_server_url)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s volume list %s %s %s" % (
        heketi_server_url, json_arg, admin_key, user)
    ret, out, err = g.run(heketi_client_node, cmd)

    if raw_cli_output:
        return ret, out, err
    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
    if json_arg:
        return json.loads(out)
    return out


def heketi_topology_info(heketi_client_node, heketi_server_url,
                         raw_cli_output=False, **kwargs):
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
        Tuple (ret, out, err): if raw_cli_output is True

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_topology_info(heketi_client_node, heketi_server_url)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s topology info %s %s %s" % (
        heketi_server_url, json_arg, admin_key, user)
    ret, out, err = g.run(heketi_client_node, cmd)

    if raw_cli_output:
        return ret, out, err
    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
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
    ret, out, err = g.run(heketi_client_node, cmd)

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
    return True


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
    ret, out, err = g.run(heketi_client_node, cmd)

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
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
    ret, out, err = g.run(heketi_client_node, cmd)

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
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
    ret, out, err = g.run(heketi_client_node, cmd)

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
    if json_arg:
        return json.loads(out)
    return out


def heketi_device_add(heketi_client_node, heketi_server_url, device_name,
                      node_id, raw_cli_output=False, **kwargs):
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
        Tuple (ret, out, err): if raw_cli_output is True

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
    ret, out, err = g.run(heketi_client_node, cmd)

    if raw_cli_output:
        return ret, out, err
    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
    return out


def heketi_device_delete(heketi_client_node, heketi_server_url, device_id,
                         raw_cli_output=False, **kwargs):
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
        Tuple (ret, out, err): if raw_cli_output is True

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_device_delete(heketi_client_node, heketi_server_url, device_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s device delete %s %s %s %s" % (
        heketi_server_url, device_id, json_arg, admin_key, user)
    ret, out, err = g.run(heketi_client_node, cmd)

    if raw_cli_output:
        return ret, out, err
    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
    return out


def heketi_device_disable(heketi_client_node, heketi_server_url, device_id,
                          raw_cli_output=False, **kwargs):
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
        Tuple (ret, out, err): if raw_cli_output is True

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_device_disable(heketi_client_node, heketi_server_url, device_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)
    cmd = "heketi-cli -s %s device disable %s %s %s %s" % (
        heketi_server_url, device_id, json_arg, admin_key, user)

    ret, out, err = g.run(heketi_client_node, cmd)

    if raw_cli_output:
        return ret, out, err
    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
    return out


def heketi_device_enable(heketi_client_node, heketi_server_url, device_id,
                         raw_cli_output=False, **kwargs):
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
        Tuple (ret, out, err): if raw_cli_output is True

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_device_enable(heketi_client_node, heketi_server_url, device_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)
    cmd = "heketi-cli -s %s device enable %s %s %s %s" % (
        heketi_server_url, device_id, json_arg, admin_key, user)

    ret, out, err = g.run(heketi_client_node, cmd)

    if raw_cli_output:
        return ret, out, err
    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
    return out


def heketi_device_info(heketi_client_node, heketi_server_url, device_id,
                       raw_cli_output=False, **kwargs):
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
        Tuple (ret, out, err): if raw_cli_output is True

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_device_info(heketi_client_node, heketi_server_url, device_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s device info %s %s %s %s" % (
        heketi_server_url, device_id, json_arg, admin_key, user)
    ret, out, err = g.run(heketi_client_node, cmd)

    if raw_cli_output:
        return ret, out, err
    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)

    if json_arg:
        device_info = json.loads(out)
        return device_info
    else:
        return out


def heketi_device_remove(heketi_client_node, heketi_server_url, device_id,
                         raw_cli_output=False, **kwargs):
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
        Tuple (ret, out, err): if raw_cli_output is True

    Raises:
        exceptions.ExecutionError: if command fails.

    Example:
        heketi_device_remove(heketi_client_node, heketi_server_url, device_id)
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, **kwargs)

    cmd = "heketi-cli -s %s device remove %s %s %s %s" % (
        heketi_server_url, device_id, json_arg, admin_key, user)
    ret, out, err = g.run(heketi_client_node, cmd)

    if raw_cli_output:
        return ret, out, err
    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)

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
    ret, out, err = g.run(heketi_client_node, cmd)

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
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
    ret, out, err = g.run(heketi_client_node, cmd)

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
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
    ret, out, err = g.run(heketi_client_node, cmd)

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
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
    ret, out, err = g.run(heketi_client_node, cmd)

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
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
    Raises: cnslibs.common.exceptions.ExecutionError when CLI command fails.
    """

    heketi_server_url, json_arg, admin_key, user = _set_heketi_global_flags(
        heketi_server_url, user=heketi_user, secret=heketi_secret)

    cmd = "heketi-cli -s %s node list %s %s %s" % (
        heketi_server_url, json_arg, admin_key, user)
    ret, out, err = g.run(heketi_client_node, cmd)

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)

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
    ret, out, err = g.run(heketi_client_node, cmd)

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
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
        heketi_server_url = ("http://" +
                             "heketi-storage-project.cloudapps.mystorage.com")

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
           "%s %s %s %s" % (heketi_server_url, str(size), auth_arg,
                            clusters_arg, ha_arg, name_arg, name_arg,
                            admin_key, user, json_arg))
    ret, out, err = g.run(heketi_client_node, cmd)

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
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
    err_msg = "Failed to delete '%s' volume. " % block_volume_id

    cmd = "heketi-cli -s %s blockvolume delete %s %s %s %s" % (
        heketi_server_url, block_volume_id, json_arg, admin_key, user)
    ret, out, err = g.run(heketi_client_node, cmd)

    if ret != 0:
        err_msg += "Out: %s, \nErr: %s" % (out, err)
        g.log.error(err_msg)
        if raise_on_error:
            raise exceptions.ExecutionError(err_msg)
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
    ret, out, err = g.run(heketi_client_node, cmd)

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, heketi_client_node, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
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

    heketi_vol_name_prefix = "%s_%s_%s_" % (prefix, namespace,  pvc_name)
    cmd = "heketi-cli -s %s volume list %s %s %s | grep %s" % (
        heketi_server_url, json_arg, admin_key, user, heketi_vol_name_prefix)
    ret, out, err = g.run(hostname, cmd, "root")

    if ret != 0:
        msg = (
            "Failed to execute '%s' command on '%s' node with following "
            "error: %s" % (cmd, hostname, err))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
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
    ret, out, err = g.run(heketi_client_node, cmd)

    if not ret:
        g.log.info("Tagging of %s to %s is successful" % (source, tag))
        return True

    g.log.error(err)
    raise exceptions.ExecutionError(err)


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
    ret, out, err = g.run(heketi_client_node, cmd)

    if not ret:
        g.log.info("Removal of %s tag from %s is successful." % (tag, source))
        return True

    g.log.error(err)
    raise exceptions.ExecutionError(err)


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

    cmd = "curl --max-time 10 %s/metrics" % heketi_server_url
    ret, out, err = g.run(heketi_client_node, cmd)
    if ret != 0:
        msg = "failed to get Heketi metrics with following error: %s" % err
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
    if prometheus_format:
        return out.strip()
    return parse_prometheus_data(out)
