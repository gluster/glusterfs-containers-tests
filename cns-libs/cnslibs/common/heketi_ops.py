"""
    Description: Library for heketi operations.
"""

import json
import six

from glusto.core import Glusto as g
from glustolibs.gluster.block_ops import block_list
from glustolibs.gluster.volume_ops import get_volume_list
from collections import OrderedDict
try:
    from heketi import HeketiClient
except ImportError:
    g.log.error("Please install python-client for heketi and re-run the test")

from cnslibs.common import exceptions, podcmd
from cnslibs.common.utils import parse_prometheus_data

HEKETI_SSH_KEY = "/etc/heketi/heketi_key"
HEKETI_CONFIG_FILE = "/etc/heketi/heketi.json"


def setup_heketi_ssh_key(heketi_client_node, gluster_servers,
                         heketi_ssh_key=HEKETI_SSH_KEY):
    """Creates heketi ssh key, sets up password-less SSH access between
    Heketi node and the Gluster server nodes, changes owner and group
    permissions of heketi keys

    Args:
        heketi_client_node (str): Node in which heketi is installed.
        gluster_servers (list): list of gluster servers in which
            heketi public key has to be copied.

    Kwargs:
        heketi_ssh_key (str): heketi ssh key file name. Defaults to
            /etc/heketi/heketi_key

    Returns:
        bool : True on successfully creating ssh keys for using heketi.
            False otherwise

    Example:
        setup_heketi_ssh_key(heketi_client_node, gluster_servers)
    """

    cmd = "ssh-keygen -f %s -t rsa -N ''" % heketi_ssh_key
    ret, _, _ = g.run(heketi_client_node, cmd)
    if ret != 0:
        g.log.error("Failed to generate ssh key for heketi "
                    "on node %s" % heketi_client_node)
        return False

    heketi_ssh_public_key = heketi_ssh_key
    for server in gluster_servers:
        cmd = "ssh-copy-id -i %s root@%s" % (heketi_ssh_public_key, server)
        ret, _, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to set up password-less SSH access between "
                        "Heketi node %s and the Gluster server node %s"
                        % (heketi_client_node, server))
            return False

    cmd = "chown heketi:heketi %s*" % heketi_ssh_key
    ret, _, _ = g.run(heketi_client_node, cmd)
    if ret != 0:
        g.log.error("Failed to change owner and group permissions of "
                    "heketi keys")
        return False

    g.log.info("ssh keys for using heketi setup is done")
    return True


def modify_heketi_executor(heketi_client_node, executor, keyfile, user, port,
                           sshexec_comment="SSH username and heketi key "
                                           "file information",
                           fstab="/etc/fstab",
                           heketi_config_file=HEKETI_CONFIG_FILE):
    """Modifies heketi executor section in /etc/heketi/heketi.json

    Args:
        heketi_client_node (str): Node in which heketi is installed.
        executor (str): executor name
        keyfile (str): heketi key file
        user (str): username
        port (str): portname

    Kwargs:
        sshexec_comment (str): ssh execution comment. Defaults to
            "SSH username and heketi key file information"
        fstab (str): Location of fstab file. Defaults to /etc/fstab.
        heketi_config_file (str): Heketi config file name. Defaults to
            /etc/heketi/heketi.json

    Returns:
        bool : True on successfully modifies heketi executor.
            False otherwise

    Example:
        heketi_client_node = "abc.com"
        executor = "ssh"
        keyfile = "/etc/heketi/heketi_key"
        user = "root"
        port = 22
        modify_heketi_executor(heketi_client_node, executor, keyfile,
                               user, port)
    """

    try:
        conn = g.rpyc_get_connection(heketi_client_node, user="root")
        if conn is None:
            g.log.error("Failed to get rpyc connection of heketi node %s"
                        % heketi_client_node)
            return False

        if not conn.modules.os.path.exists(heketi_config_file):
            g.log.error("Unable to locate %s in heketi node %s"
                        % heketi_config_file)
            return False

        with conn.builtin.open(heketi_config_file, 'r') as fh_read:
            config_data = conn.modules.json.load(fh_read,
                                                 object_pairs_hook=OrderedDict)
            config_data['glusterfs']['executor'] = executor
            config_data['glusterfs']['sshexec']['keyfile'] = keyfile
            config_data['glusterfs']['sshexec']['user'] = user
            config_data['glusterfs']['sshexec']['port'] = port
            config_data['glusterfs']['_sshexec_comment'] = sshexec_comment
            config_data['glusterfs']['sshexec']['fstab'] = fstab

        with conn.builtin.open(heketi_config_file, 'w') as fh_write:
            conn.modules.json.dump(config_data, fh_write, sort_keys=False,
                                   indent=4, ensure_ascii=False)
    except Exception:
        g.log.error("Failed to modify heketi executor in %s"
                    % heketi_config_file)
    finally:
        g.rpyc_close_connection(heketi_client_node, user="root")

    g.log.info("Successfully modified heketi executor in %s"
               % heketi_config_file)

    return True


def start_heketi_service(heketi_client_node):
    """Starts heketi service in given node.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        start_heketi_service(heketi_client_node)
    """

    cmd = "systemctl start heketi"
    return g.run(heketi_client_node, cmd)


def enable_heketi_service(heketi_client_node):
    """Enables heketi service in given node.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        enable_heketi_service(heketi_client_node)
    """

    cmd = "systemctl enable heketi"
    return g.run(heketi_client_node, cmd)


def export_heketi_cli_server(heketi_client_node,
                             heketi_cli_server=("http://heketi-storage-" +
                                                "project.cloudapps." +
                                                "mystorage.com"),
                             heketi_cli_user=None,
                             heketi_cli_key=None):
    """Exports given HEKETI_CLI_SERVER in given node.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.

    Kwargs:
        heketi_cli_server (str): url where heketi server is present.
            Defaults to,
            "http://heketi-storage-project.cloudapps.mystorage.com".

    Returns:
        tuple: Tuple containing three elements (ret, out, err).
            The first element 'ret' is of type 'int' and is the return value
            of command execution.

            The second element 'out' is of type 'str' and is the stdout value
            of the command execution.

            The third element 'err' is of type 'str' and is the stderr value
            of the command execution.

    Example:
        export_heketi_cli_server(heketi_client_node)
    """

    cmd = "export HEKETI_CLI_SERVER=%s" % heketi_cli_server
    if heketi_cli_user:
        cmd += "\nexport HEKETI_CLI_USER=%s" % heketi_cli_user
        if heketi_cli_key is not None:
            cmd += "\nexport HEKETI_CLI_KEY='%s'" % heketi_cli_key
    return g.run(heketi_client_node, cmd)


def heketi_create_topology(heketi_client_node, topology_info,
                           topology_file="/usr/share/heketi/topology.json"):
    """Creates topology json file dynamically by getting the topology info
    from user.

    Args:
        heketi_client_node (str): Node in which heketi is installed.
        topology_info (dict): Info to be populated in topology.json file

    Kwargs:
        topology_file (str): topology file name. Defaults to
            /usr/share/heketi/topology.json

    Returns:
        bool : True on successfully creating topology file.
            False otherwise

    Example:

        topology_info = OrderedDict([(
            'cluster1',
            OrderedDict([
                (
                    'gluster_node1',
                    {
                        'manage': 'dhcp115',
                        'storage': 'dhcp115',
                        'zone': '1',
                        'devices': ['/dev/vda', '/dev/vdb'],
                    },
                ), (
                    'gluster_node2',
                    {
                        'manage': 'dhcp116',
                        'storage': 'dhcp116',
                        'zone': '2',
                        'devices': ['/dev/vdc', '/dev/vdd'],
                    },
                ),
            ])
        ), (
            'cluster2',
            OrderedDict([
                (
                    'gluster_node3',
                    {
                        'manage': 'dhcp117',
                        'storage': 'dhcp117',
                        'zone': '1',
                        'devices': ['/dev/vda', '/dev/vdb'],
                    },
                ), (
                    'gluster_node4',
                    {
                        'manage': 'dhcp118',
                        'storage': 'dhcp118',
                        'zone': '2',
                        'devices': ['/dev/vdc', '/dev/vdd'],
                    },
                )
            ])
        )])

        heketi_create_topology(heketi_client_node, topology_info)
    """

    modified_topology_info = OrderedDict()
    modified_topology_info["clusters"] = []

    for each_cluster in topology_info.keys():
        each_cluster_dict = OrderedDict()
        each_cluster_dict['nodes'] = []
        for each_node in topology_info[each_cluster].keys():
            each_node_dict = OrderedDict()
            each_node_dict['node'] = OrderedDict()
            each_node_dict['devices'] = []
            each_node_dict['node']['hostnames'] = OrderedDict()
            for each_node_info in (topology_info[each_cluster]
                                   [each_node].keys()):
                if each_node_info in ("manage", "storage"):
                    (each_node_dict['node']['hostnames']
                     [each_node_info]) = [(topology_info[each_cluster]
                                          [each_node][each_node_info])]
                elif each_node_info == "zone":
                    each_node_dict['node']['zone'] = (int(
                                                      topology_info
                                                      [each_cluster]
                                                      [each_node]
                                                      [each_node_info]))
                elif each_node_info == "devices":
                    each_node_dict['devices'] = (topology_info[each_cluster]
                                                 [each_node][each_node_info])
            each_cluster_dict['nodes'].append(each_node_dict)
        modified_topology_info["clusters"].append(each_cluster_dict)

    try:
        conn = g.rpyc_get_connection(heketi_client_node, user="root")
        if conn is None:
            g.log.error("Failed to get rpyc connection of heketi node %s"
                        % heketi_client_node)
            return False

        with conn.builtin.open(topology_file, 'w') as fh_write:
            conn.modules.json.dump(modified_topology_info, fh_write, indent=4)
    except Exception:
        g.log.error("Failed to create topology file in %s"
                    % heketi_client_node)
    finally:
        g.rpyc_close_connection(heketi_client_node, user="root")

    g.log.info("Successfully created topology file in %s"
               % heketi_client_node)

    return True


def _set_heketi_global_flags(heketi_server_url, **kwargs):
    """
    Helper function to set heketi-cli global flags
    """

    heketi_server_url = (heketi_server_url if heketi_server_url else ("http:" +
                         "//heketi-storage-project.cloudapps.mystorage.com"))
    json = kwargs.get("json")
    secret = kwargs.get("secret")
    user = kwargs.get("user")
    json_arg = "--json" if json else ""
    secret_arg = "--secret %s" % secret if secret else ""
    user_arg = "--user %s" % user if user else ""
    if not user_arg:
        heketi_cli_user = g.config['cns']['heketi_config']['heketi_cli_user']
        if heketi_cli_user:
            user_arg = "--user %s" % heketi_cli_user
            heketi_cli_key = g.config['cns']['heketi_config']['heketi_cli_key']
            if heketi_cli_key is not None:
                secret_arg = "--secret '%s'" % heketi_cli_key

    return (heketi_server_url, json_arg, secret_arg, user_arg)


def heketi_topology_load(heketi_client_node, heketi_server_url,
                         topology_file="/usr/share/heketi/topology.json",
                         **kwargs):
    """Loads topology in given heketi node.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.

    Kwargs:
        topology_file (str): topology file name. Defaults to
            /usr/share/heketi/topology.json
        **kwargs
            The keys, values in kwargs are:
                - secret : (str)|None
                - user : (str)|None

    Returns:
        str: topology load output on success
        False: in case of failure

    Example:
        heketi_topology_load(heketi_client_node, heketi_server_url)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    cmd = ("heketi-cli -s %s topology load --json=%s %s %s"
           % (heketi_server_url, topology_file, admin_key, user))
    ret, out, _ = g.run(heketi_client_node, cmd)
    if ret != 0:
        g.log.error("Failed to execute heketi-cli topology load command")
        return False
    return out


def heketi_volume_create(heketi_client_node, heketi_server_url, size,
                         mode='cli', raw_cli_output=False, **kwargs):
    """Creates heketi volume with the given user options

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        size (str): Volume size

    Kwargs:
        mode (str): Mode in which heketi volume will be created.
            It can be cli|url. Defaults to cli.
        **kwargs
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
        Tuple (ret, out, err): if raw_cli_output is True
    Raises:
        exceptions.ExecutionError when error occurs and raw_cli_output is False

    Example:
        heketi_volume_create(heketi_client_node, heketi_server_url, size)
    """

    if not kwargs.get('user'):
        heketi_cli_user = g.config['cns']['heketi_config']['heketi_cli_user']
        if heketi_cli_user:
            kwargs['user'] = heketi_cli_user
            heketi_cli_key = g.config['cns']['heketi_config']['heketi_cli_key']
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
    gluster_volume_options_arg = ("--gluster-volume-options %s"
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
    if mode == 'cli':
        cmd = ("heketi-cli -s %s volume create --size=%s %s %s %s %s %s %s "
               "%s %s %s %s %s %s %s %s %s %s"
               % (heketi_server_url, str(size), block_arg, clusters_arg,
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
            vol_created_info = json.loads(out)
            return vol_created_info
        else:
            return out
    else:
        kwargs['size'] = int(size)
        try:
            user = kwargs.pop('user', 'admin')
            admin_key = kwargs.pop('secret', 'My Secret')
            conn = HeketiClient(heketi_server_url, user, admin_key)
            volume_create_info = conn.volume_create(kwargs)
        except Exception:
            g.log.error(err_msg)
            raise

        g.log.info("Volume creation is successful using heketi")
        return volume_create_info


def heketi_volume_info(heketi_client_node, heketi_server_url, volume_id,
                       mode='cli', raw_cli_output=False, **kwargs):
    """Executes heketi volume info command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        volume_id (str): Volume ID

    Kwargs:
        mode (str): Mode in which heketi volume will be created.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        dict: volume info on success
        False: in case of failure
        Tuple (ret, out, err): if raw_cli_output is True

    Example:
        heketi_volume_info(heketi_client_node, volume_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)
    if mode == 'cli':
        cmd = ("heketi-cli -s %s volume info %s %s %s %s"
               % (heketi_server_url, volume_id, json_arg, admin_key, user))
        ret, out, err = g.run(heketi_client_node, cmd)

        if raw_cli_output:
            return ret, out, err

        if ret != 0:
            g.log.error("Failed to execute heketi-cli volume info command")
            return False

        if json_arg:
            volume_info = json.loads(out)
            return volume_info
        else:
            return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            volume_info = conn.volume_info(volume_id)
            if volume_info is None:
                return False
        except Exception:
            g.log.error("Failed to get volume info using heketi")
            return False
        return volume_info


def heketi_volume_expand(heketi_client_node, heketi_server_url, volume_id,
                         expand_size, mode='cli', raw_cli_output=False,
                         **kwargs):

    """Executes heketi volume expand

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        volume_id (str): Volume ID
        expand_size (str): volume expand size

    Kwargs:
        mode (str): Mode in which heketi volume will be created.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        dict: volume expand info on success, only cli option is specified
            without --json option, then it returns raw string output.
        False otherwise
        Tuple (ret, out, err): if raw_cli_output is True

    Example:
        heketi_volume_expand(heketi_client_node, heketi_server_url, volume_id,
                             expand_size)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':
        cmd = ("heketi-cli -s %s volume expand --volume=%s "
               "--expand-size=%s %s %s %s"
               % (heketi_server_url, volume_id, expand_size, json_arg,
                  admin_key, user))

        ret, out, err = g.run(heketi_client_node, cmd)

        if raw_cli_output:
            return ret, out, err

        if ret != 0:
            g.log.error("Failed to execute heketi-cli volume expand command")
            return False

        if json_arg:
            volume_expand_info = json.loads(out)
            return volume_expand_info

        return out

    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            vol_req = {}
            vol_req['expand_size'] = int(expand_size)
            volume_expand_info = conn.volume_expand(volume_id, vol_req)
        except Exception:
            g.log.error("Failed to do volume expansion info using heketi")
            return False

        return volume_expand_info


def heketi_volume_delete(heketi_client_node, heketi_server_url, volume_id,
                         mode='cli', raw_cli_output=False,
                         raise_on_error=True, **kwargs):
    """Executes heketi volume delete command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        volume_id (str): Volume ID

    Kwargs:
        mode (str): Mode in which heketi volume will be created.
            It can be cli|url. Defaults to cli.
        **kwargs
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
    if mode == 'cli':
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
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            ret = conn.volume_delete(volume_id)
        except Exception:
            g.log.error(err_msg)
            if raise_on_error:
                raise
        return ret


def heketi_volume_list(heketi_client_node, heketi_server_url, mode='cli',
                       raw_cli_output=False, **kwargs):
    """Executes heketi volume list command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Kwargs:
        mode (str): Mode in which heketi volume will be created.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        dict: volume list with --json on success, if cli option is specified
            without --json option or with url, it returns raw string output.
        False otherwise
        Tuple (ret, out, err): if raw_cli_output is True

    Example:
        heketi_volume_info(heketi_client_node, heketi_server_url)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)
    if mode == 'cli':
        cmd = ("heketi-cli -s %s volume list %s %s %s"
               % (heketi_server_url, json_arg, admin_key, user))
        ret, out, err = g.run(heketi_client_node, cmd)

        if raw_cli_output:
            return ret, out, err

        if ret != 0:
            g.log.error("Failed to execute heketi-cli volume list command")
            return False

        if json_arg:
            volume_list = json.loads(out)
            return volume_list
        else:
            return out

    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            volume_list = conn.volume_list()
        except Exception:
            g.log.error("Failed to do volume list using heketi")
            return False
    return volume_list


def heketi_topology_info(heketi_client_node, heketi_server_url,
                         raw_cli_output=False, **kwargs):

    """Executes heketi topology info command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Kwargs:
        mode (str): Mode in which heketi volume will be created.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        dict: topology info if --json option is specified. If only cli option
            is specified, raw command output is returned on success.
        False, otherwise
        Tuple (ret, out, err): if raw_cli_output is True

    Example:
        heketi_topology_info(heketi_client_node, heketi_server_url)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)
    cmd = ("heketi-cli -s %s topology info %s %s %s"
           % (heketi_server_url, json_arg, admin_key, user))

    ret, out, err = g.run(heketi_client_node, cmd)

    if raw_cli_output:
        return ret, out, err

    if ret != 0:
        g.log.error("Failed to execute heketi-cli topology info command")
        return False

    if json_arg:
        topology_info = json.loads(out)
        return topology_info
    return out


def hello_heketi(heketi_client_node, heketi_server_url, mode='cli', **kwargs):
    """Executes curl command to check if heketi server is alive.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Kwargs:
        mode (str): Mode in which heketi volume will be created.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - secret : (str)|None
                - user : (str)|None

    Returns:
        bool: True, if heketi server is alive
            False otherwise

    Example:
        hello_heketi(heketi_client_node, heketi_server_url)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)
    if mode == 'cli':
        cmd = "curl %s/hello" % heketi_server_url
        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli command")
            return False

        if 'Hello from Heketi' in out:
            g.log.info("Heketi server %s alive" % heketi_server_url)
            return True
        return False
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            ret = conn.hello()
        except Exception:
            g.log.error("Failed to execute heketi hello command")
            return False
        return ret


def heketi_cluster_create(heketi_client_node, heketi_server_url,
                          block_arg=True,  file_arg=True, mode='cli',
                          **kwargs):
    """Executes heketi cluster create command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Kwargs:
        block_arg (bool): Control the possibility of creating block volumes
            on the cluster to be created. This is enabled by default.
            Set the option to False to disable creation of block volumes
            on this cluster. (default True)
        file_arg (bool): Control the possibility of creating regular file
            volumes on the cluster to be created. This is enabled by
            Set the option to True to disable creation of file
            volumes on this cluster. (default True)
        mode (str): Mode in which heketi volume will be created.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        dict: cluster create command output on success. If only cli option
            is specified without json, then it returns raw output as string.
        False on failure

    Example:
        heketi_cluster_create(heketi_client_node, heketi_server_url)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':

        block_param = file_param = ""
        if block_arg:
            block_param = '--block=true'
        else:
            block_param = '--block=false'

        if file_arg:
            file_param = '--file=true'
        else:
            file_param = '--file=false'

        cmd = ("heketi-cli -s %s cluster create %s %s %s %s %s"
               % (heketi_server_url, block_param, file_param, json_arg,
                  admin_key, user))

        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli cluster create command")
            return False

        if json_arg:
            cluster_create_info = json.loads(out)
            return cluster_create_info

        return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            cluster_req = {}
            cluster_req["block"] = block_arg
            cluster_req["file"] = file_arg
            cluster_create_info = conn.volume_create(cluster_req)
        except Exception:
            g.log.error("Failed to do cluster create using heketi")
            return False
        return cluster_create_info


def heketi_cluster_delete(heketi_client_node, heketi_server_url, cluster_id,
                          mode='cli', **kwargs):
    """Executes heketi volume delete command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        cluster_id (str): Cluster ID

    Kwargs:
        mode (str): Mode in which heketi volume will be created.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        str: cluster delete command output on success
        False on failure

    Example:
        heketi_cluster_delete(heketi_client_node, heketi_server_url,
                              cluster_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':
        cmd = ("heketi-cli -s %s cluster delete %s %s %s %s"
               % (heketi_server_url, cluster_id, json_arg, admin_key, user))

        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli cluster delete command")
            return False

        return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            ret = conn.cluster_delete(cluster_id)
        except Exception:
            g.log.error("Failed to do volume delete using heketi")
            return False
        return ret


def heketi_cluster_info(heketi_client_node, heketi_server_url, cluster_id,
                        mode='cli', **kwargs):
    """Executes heketi volume info command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        cluster_id (str): Volume ID

    Kwargs:
        mode (str): Mode to excecute the command.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        dict: cluster info on success
        False: in case of failure

    Example:
        heketi_cluster_info(heketi_client_node, volume_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)
    if mode == 'cli':
        cmd = ("heketi-cli -s %s cluster info %s %s %s %s"
               % (heketi_server_url, cluster_id, json_arg, admin_key, user))
        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli cluster info command")
            return False

        if json_arg:
            cluster_info = json.loads(out)
            return cluster_info
        else:
            return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            cluster_info = conn.cluster_info(cluster_id)
        except Exception:
            g.log.error("Failed to get cluster info using heketi")
            return False
        return cluster_info


def heketi_cluster_list(heketi_client_node, heketi_server_url, mode='cli',
                        **kwargs):
    """Executes heketi cluster list command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Kwargs:
        mode (str): Mode in which heketi volume will be created.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        dict: cluster list with --json on success, if cli option is specified
            without --json option or with url, it returns raw string output.
        False otherwise

    Example:
        heketi_cluster_info(heketi_client_node, heketi_server_url)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)
    if mode == 'cli':
        cmd = ("heketi-cli -s %s cluster list %s %s %s"
               % (heketi_server_url, json_arg, admin_key, user))
        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli cluster list command")
            return False

        if json_arg:
            cluster_list = json.loads(out)
            return cluster_list
        else:
            return out

    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            cluster_list = conn.cluster_list()
        except Exception:
            g.log.error("Failed to do cluster list using heketi")
            return False
    return cluster_list


def heketi_device_add(heketi_client_node, heketi_server_url, device_name,
                      node_id, mode='cli', raw_cli_output=False, **kwargs):

    """Executes heketi device add command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        device name (str): Device name to add
        node_id (str): Node id to add the device

    Kwargs:
        mode (str): Mode in which command will be executed.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        str: heketi device add command output on success. If mode='url'
            return True.
        False on failure
        Tuple (ret, out, err): if raw_cli_output is True

    Example:
        heketi_device_add(heketi_client_node, heketi_server_url, device_name,
                          node_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':

        cmd = ("heketi-cli -s %s device add --name=%s --node=%s %s %s %s"
               % (heketi_server_url, device_name, node_id, json_arg,
                  admin_key, user))

        ret, out, err = g.run(heketi_client_node, cmd)

        if raw_cli_output:
            return ret, out, err

        if ret != 0:
            g.log.error("Failed to execute heketi-cli device add command")
            return False

        return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            device_req = {}
            device_req["name"] = device_name
            device_req["node"] = node_id
            device = conn.device_add(device_req)
        except Exception:
            g.log.error("Failed to do device add using heketi")
            return False
        return device


def heketi_device_delete(heketi_client_node, heketi_server_url, device_id,
                         mode='cli', raw_cli_output=False, **kwargs):
    """Executes heketi device delete command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        device id (str): Device id to delete

    Kwargs:
        mode (str): Mode in which command will be executed.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        str: heketi device delete command output on success. If mode='url'
            return True.
        False on failure
        Tuple (ret, out, err): if raw_cli_output is True

    Example:
        heketi_device_delete(heketi_client_node, heketi_server_url, device_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':

        cmd = ("heketi-cli -s %s device delete %s %s %s %s"
               % (heketi_server_url, device_id, json_arg,
                  admin_key, user))

        ret, out, err = g.run(heketi_client_node, cmd)

        if raw_cli_output:
            return ret, out, err

        if ret != 0:
            g.log.error("Failed to execute heketi-cli device delete command")
            return False

        return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            device = conn.device_delete(device_id)
        except Exception:
            g.log.error("Failed to do device delete using heketi")
            return False
        return device


def heketi_device_disable(heketi_client_node, heketi_server_url, device_id,
                          mode='cli', raw_cli_output=False, **kwargs):
    """Executes heketi-cli device disable command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        device_id (str): Device id to disable device

    Kwargs:
        mode (str): Mode in which command will be executed.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        str: heketi device disable command output on success. If mode='url'
            return True.
        False on failure
        Tuple (ret, out, err): if raw_cli_output is True

    Example:
        heketi_device_disable(heketi_client_node, heketi_server_url, device_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':

        cmd = ("heketi-cli -s %s device disable %s %s %s %s"
               % (heketi_server_url, device_id, json_arg,
                  admin_key, user))

        ret, out, err = g.run(heketi_client_node, cmd)

        if raw_cli_output:
            return ret, out, err

        if ret != 0:
            g.log.error("Failed to execute heketi-cli device disable command")
            return False

        return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            device = conn.device_disable(device_id)
        except Exception:
            g.log.error("Failed to do device disable using heketi")
            return False
        return device


def heketi_device_enable(heketi_client_node, heketi_server_url, device_id,
                         mode='cli', raw_cli_output=False, **kwargs):
    """Executes heketi-cli device enable command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        device_id (str): Device id to enable device

    Kwargs:
        mode (str): Mode in which command will be executed.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        str: heketi device enable command output on success. If mode='url'
            return True.
        False on failure
        Tuple (ret, out, err): if raw_cli_output is True

    Example:
        heketi_device_enable(heketi_client_node, heketi_server_url, device_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':

        cmd = ("heketi-cli -s %s device enable %s %s %s %s"
               % (heketi_server_url, device_id, json_arg,
                  admin_key, user))

        ret, out, err = g.run(heketi_client_node, cmd)

        if raw_cli_output:
            return ret, out, err

        if ret != 0:
            g.log.error("Failed to execute heketi-cli device enable command")
            return False

        return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            device = conn.device_enable(device_id)
        except Exception:
            g.log.error("Failed to do device enable using heketi")
            return False
        return device


def heketi_device_info(heketi_client_node, heketi_server_url, device_id,
                       mode='cli', raw_cli_output=False, **kwargs):
    """Executes heketi device info command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        device_id (str): Device ID

    Kwargs:
        mode (str): Mode to excecute the command.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        dict: device info on success, if mode='cli' without json, then it
            returns raw output in string format.
        False: in case of failure
        Tuple (ret, out, err): if raw_cli_output is True

    Example:
        heketi_device_info(heketi_client_node, heketi_server_url, device_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)
    if mode == 'cli':
        cmd = ("heketi-cli -s %s device info %s %s %s %s"
               % (heketi_server_url, device_id, json_arg, admin_key, user))
        ret, out, err = g.run(heketi_client_node, cmd)

        if raw_cli_output:
            return ret, out, err

        if ret != 0:
            g.log.error("Failed to execute heketi-cli device info command")
            return False

        if json_arg:
            device_info = json.loads(out)
            return device_info
        else:
            return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            cluster_info = conn.device_info(device_id)
        except Exception:
            g.log.error("Failed to get device info using heketi")
            return False
        return cluster_info


def heketi_device_remove(heketi_client_node, heketi_server_url, device_id,
                         mode='cli', raw_cli_output=False, **kwargs):
    """Executes heketi-cli device remove command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        device_id (str): Device id to remove device

    Kwargs:
        mode (str): Mode in which command will be executed.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        str: heketi device remove command output on success. If mode='url'
            return True.
        False on failure
        Tuple (ret, out, err): if raw_cli_output is True

    Example:
        heketi_device_remove(heketi_client_node, heketi_server_url, device_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':

        cmd = ("heketi-cli -s %s device remove %s %s %s %s"
               % (heketi_server_url, device_id, json_arg,
                  admin_key, user))

        ret, out, err = g.run(heketi_client_node, cmd)

        if raw_cli_output:
            return ret, out, err

        if ret != 0:
            g.log.error("Failed to execute heketi-cli device remove command")
            return False

        return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            device = conn.device_remove(device_id)
        except Exception:
            g.log.error("Failed to do device remove using heketi")
            return False
        return device


def heketi_node_add(heketi_client_node, heketi_server_url, zone, cluster_id,
                    management_host_name, storage_host_name,
                    mode='cli', **kwargs):
    """Executes heketi cluster create command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url.
        zone (int): zone to add node
        cluster_id (str): cluster id to add the node.
        management_host_name (str): management host.
        storage_host_name (str): storage host.

    Kwargs:
        mode (str): Mode in which command will be executed.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        dict: heketi node add command output on success. If mode='cli' without
            json arg, then it return raw output in string format.
        False on failure

    Example:
        heketi_node_add(heketi_client_node, heketi_server_url, zone,
                        cluster_id, management_host_name, storage_host_name)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':

        cmd = ("heketi-cli -s %s node add --zone=%s --cluster=%s "
               "--management-host-name=%s --storage-host-name=%s %s %s %s"
               % (heketi_server_url, zone, cluster_id, management_host_name,
                  storage_host_name, json_arg, admin_key, user))

        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli node add command")
            return False

        if json_arg:
            node_add_info = json.loads(out)
            return node_add_info
        else:
            return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            node_req = {}
            node_req["zone"] = int(zone)
            node_req["cluster"] = cluster_id
            node_req['hostnames'] = {"manage": [management_host_name],
                                     "storage": [storage_host_name]}
            node_add_info = conn.node_add(node_req)
        except Exception:
            g.log.error("Failed to do node add using heketi")
            return False
        return node_add_info


def heketi_node_delete(heketi_client_node, heketi_server_url, node_id,
                       mode='cli', **kwargs):
    """Executes heketi cluster create command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url.
        node_id (str): Node id to delete

    Kwargs:
        mode (str): Mode in which command will be executed.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        str: heketi node delete command output on success. If mode='url'
            return True.
        False on failure

    Example:
        heketi_node_delete(heketi_client_node, heketi_server_url, node_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':

        cmd = ("heketi-cli -s %s node delete %s %s %s %s"
               % (heketi_server_url, node_id, json_arg,
                  admin_key, user))

        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli node delete command")
            return False

        return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            device = conn.node_delete(node_id)
        except Exception:
            g.log.error("Failed to do node delete using heketi")
            return False
        return device


def heketi_node_disable(heketi_client_node, heketi_server_url, node_id,
                        mode='cli', **kwargs):
    """Executes heketi-cli node disable command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        node_id (str): Node id to disable node

    Kwargs:
        mode (str): Mode in which command will be executed.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        str: heketi node disable command output on success. If mode='url'
            return True.
        False on failure

    Example:
        heketi_node_disable(heketi_client_node, heketi_server_url, node_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':

        cmd = ("heketi-cli -s %s node disable %s %s %s %s"
               % (heketi_server_url, node_id, json_arg,
                  admin_key, user))

        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli node disable command")
            return False

        return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            node = conn.device_disable(node_id)
        except Exception:
            g.log.error("Failed to do node disable using heketi")
            return False
        return node


def heketi_node_enable(heketi_client_node, heketi_server_url, node_id,
                       mode='cli', **kwargs):
    """Executes heketi-cli node enable command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        node_id (str): Node id to enable device

    Kwargs:
        mode (str): Mode in which command will be executed.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        str: heketi node enable command output on success. If mode='url'
            return True.
        False on failure

    Example:
        heketi_node_enable(heketi_client_node, heketi_server_url, node_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':

        cmd = ("heketi-cli -s %s node enable %s %s %s %s"
               % (heketi_server_url, node_id, json_arg,
                  admin_key, user))

        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli node enable command")
            return False

        return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            device = conn.node_enable(node_id)
        except Exception:
            g.log.error("Failed to do node enable using heketi")
            return False
        return device


def heketi_node_info(heketi_client_node, heketi_server_url, node_id,
                     mode='cli', **kwargs):
    """Executes heketi node info command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        node_id (str): Node ID

    Kwargs:
        mode (str): Mode to excecute the command.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        dict: node info on success, if mode='cli' without json, then it
            returns raw output in string format.
        False: in case of failure

    Example:
        heketi_node_info(heketi_client_node, heketi_server_url, node_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)
    if mode == 'cli':
        cmd = ("heketi-cli -s %s node info %s %s %s %s"
               % (heketi_server_url, node_id, json_arg, admin_key, user))
        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli node info command")
            return False

        if json_arg:
            node_info = json.loads(out)
            return node_info
        else:
            return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            node_info = conn.node_info(node_id)
        except Exception:
            g.log.error("Failed to get node info using heketi")
            return False
        return node_info


def heketi_node_remove(heketi_client_node, heketi_server_url, node_id,
                       mode='cli', **kwargs):
    """Executes heketi-cli device remove command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        node_id (str): Node id to remove node

    Kwargs:
        mode (str): Mode in which command will be executed.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        str: heketi node remove command output on success. If mode='url'
            return True.
        False on failure

    Example:
        heketi_node_remove(heketi_client_node, heketi_server_url, node_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':

        cmd = ("heketi-cli -s %s node remove %s %s %s %s"
               % (heketi_server_url, node_id, json_arg,
                  admin_key, user))

        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli node remove command")
            return False

        return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            node = conn.node_remove(node_id)
        except Exception:
            g.log.error("Failed to do node remove using heketi")
            return False
        return node


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

    cmd = ("heketi-cli -s %s node list %s %s %s"
           % (heketi_server_url, json_arg, admin_key, user))
    ret, out, _ = g.run(heketi_client_node, cmd)
    if ret != 0:
        msg = "Failed to get list of Heketi nodes."
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)

    heketi_node_id_list = []
    for line in out.strip().split("\n"):
        # Line looks like this: 'Id:nodeIdString\tCluster:clusterIdString'
        heketi_node_id_list.append(
            line.strip().split("Cluster")[0].strip().split(":")[1])
    return heketi_node_id_list


def heketi_blockvolume_info(heketi_client_node, heketi_server_url,
                            block_volume_id, mode='cli', **kwargs):
    """Executes heketi blockvolume info command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        block_volume_id (str): block volume ID

    Kwargs:
        mode (str): Mode in which heketi volume will be created.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        dict: block volume info on success, if mode='cli' without json, then
            returns raw output in string format.
        False: in case of failure

    Example:
        heketi_blockvolume_info(heketi_client_node, block_volume_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)
    if mode == 'cli':
        cmd = ("heketi-cli -s %s blockvolume info %s %s %s %s"
               % (heketi_server_url, block_volume_id, json_arg,
                  admin_key, user))
        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli blockvolume "
                        "info command")
            return False

        if json_arg:
            block_volume_info = json.loads(out)
            return block_volume_info
        else:
            return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            block_volume_info = conn.blockvolume_info(block_volume_id)
            if block_volume_info is None:
                return False
        except Exception:
            g.log.error("Failed to get blockvolume info using heketi")
            return False
        return block_volume_info


def heketi_blockvolume_create(heketi_client_node, heketi_server_url, size,
                              mode='cli', **kwargs):
    """Executes heketi blockvolume create

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        size (int): blockvolume size

    Kwargs:
        mode (str): Mode in which heketi volume will be created.
            It can be cli|url. Defaults to cli.
        **kwargs
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
        False otherwise

    Example:
        heketi_blockvolume_create(heketi_client_node, heketi_server_url, size)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

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

    if mode == 'cli':
        cmd = ("heketi-cli -s %s blockvolume create --size=%s %s %s %s %s "
               "%s %s %s %s" % (heketi_server_url, str(size), auth_arg,
                                clusters_arg, ha_arg, name_arg, name_arg,
                                admin_key, user, json_arg))

        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli blockvolume "
                        "create command")
            return False

        if json_arg:
            block_volume_create_info = json.loads(out)
            return block_volume_create_info

        return out

    else:
        kwargs['size'] = int(size)
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            block_volume_create_info = conn.blockvolume_create(**kwargs)
        except Exception:
            g.log.error("Failed to do blockvolume create using heketi")
            return False

        return block_volume_create_info


def heketi_blockvolume_delete(heketi_client_node, heketi_server_url,
                              block_volume_id, mode='cli', **kwargs):
    """Executes heketi blockvolume delete command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url
        block_volume_id (str): block volume ID

    Kwargs:
        mode (str): Mode in which heketi volume will be created.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        str: volume delete command output on success
        False on failure

    Example:
        heketi_blockvolume_delete(heketi_client_node, heketi_server_url,
                                  block_volume_id)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    if mode == 'cli':
        cmd = ("heketi-cli -s %s blockvolume delete %s %s %s %s"
               % (heketi_server_url, block_volume_id, json_arg,
                  admin_key, user))

        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli blockvolume "
                        "delete command")
            return False

        return out
    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            return conn.blockvolume_delete(block_volume_id)
        except Exception:
            g.log.error("Failed to do blockvolume delete using heketi")
            return False


def heketi_blockvolume_list(heketi_client_node, heketi_server_url, mode='cli',
                            **kwargs):
    """Executes heketi blockvolume list command.

    Args:
        heketi_client_node (str): Node on which cmd has to be executed.
        heketi_server_url (str): Heketi server url

    Kwargs:
        mode (str): Mode in which heketi command will be executed.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

    Returns:
        dict: volume list with --json on success, if cli option is specified
            without --json option or with url, it returns raw string output.
        False otherwise

    Example:
        heketi_volume_info(heketi_client_node, heketi_server_url)
    """

    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)
    if mode == 'cli':
        cmd = ("heketi-cli -s %s blockvolume list %s %s %s"
               % (heketi_server_url, json_arg, admin_key, user))
        ret, out, _ = g.run(heketi_client_node, cmd)
        if ret != 0:
            g.log.error("Failed to execute heketi-cli blockvolume "
                        "list command")
            return False

        if json_arg:
            block_volume_list = json.loads(out)
            return block_volume_list
        else:
            return out

    else:
        try:
            user = user.split(' ')[-1] if user else 'admin'
            admin_key = admin_key.split('t ')[-1] if admin_key else admin_key
            conn = HeketiClient(heketi_server_url, user, admin_key)
            return conn.blockvolume_list()
        except Exception:
            g.log.error("Failed to do blockvolume list using heketi")
            return False


def verify_volume_name_prefix(hostname, prefix, namespace, pvc_name,
                              heketi_server_url, **kwargs):
    '''
     This function checks if heketi voluem is present with
     volname prefix
     Args:
         hostname (str): hostname on which we want
                         to check the heketi vol
         prefix (str): volnameprefix given in storageclass
         namespace (str): namespace
         pvc_name (str): name of the pvc
         heketi_server_url (str): Heketi server url

     Kwargs:
        mode (str): Mode in which heketi command will be executed.
            It can be cli|url. Defaults to cli.
        **kwargs
            The keys, values in kwargs are:
                - json : (bool)
                - secret : (str)|None
                - user : (str)|None

     Returns:
         bool: True if volume found,
               Fasle otherwise.
    '''
    (heketi_server_url,
     json_arg, admin_key, user) = _set_heketi_global_flags(heketi_server_url,
                                                           **kwargs)

    heketi_vol_name_prefix = "%s_%s_%s_" % (prefix, namespace,
                                            pvc_name)
    cmd = ("heketi-cli -s %s volume list %s %s %s | grep %s" % (
               heketi_server_url, json_arg, admin_key, user,
               heketi_vol_name_prefix))
    ret, out, err = g.run(hostname, cmd, "root")
    if ret != 0:
        if not out:
            g.log.error("no heketi volume with volnameprefix - %s" % (
                            heketi_vol_name_prefix))
        else:
            g.log.error("failed to execute cmd %s" % cmd)
        return False
    output = out.strip()
    g.log.info("heketi volume with volnameprefix present %s" % (
                   output))
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


@podcmd.GlustoPod()
def match_heketi_and_gluster_block_volumes(
        gluster_pod, heketi_block_volumes, block_vol_prefix, hostname=None):
    """Match block volumes from heketi and gluster

    Args:
        gluster_pod (podcmd | str): gluster pod class object has gluster
                                    pod and ocp master node or gluster
                                    pod name
        heketi_block_volumes (list): list of heketi block volumes with
                                     which gluster block volumes need to
                                     be matched
        block_vol_prefix (str): block volume prefix by which the block
                                volumes needs to be filtered
        hostname (str): master node on which gluster pod exists

    """
    if isinstance(gluster_pod, podcmd.Pod):
        g.log.info("Recieved gluster pod object using same")
    elif isinstance(gluster_pod, six.string_types) and hostname:
        g.log.info("Recieved gluster pod name and hostname")
        gluster_pod = podcmd.Pod(hostname, gluster_pod)
    else:
        raise exceptions.ExecutionError("Invalid glsuter pod parameter")

    gluster_vol_list = get_volume_list(gluster_pod)

    gluster_vol_block_list = []
    for gluster_vol in gluster_vol_list[1:]:
        ret, out, err = block_list(gluster_pod, gluster_vol)
        gluster_vol_block_list.extend([
            block_vol.replace(block_vol_prefix, "")
            for block_vol in json.loads(out)["blocks"]
            if block_vol.startswith(block_vol_prefix)
        ])

    assert sorted(gluster_vol_block_list) == heketi_block_volumes, (
        "Gluster and Heketi Block volume list match failed")


def get_heketi_metrics(heketi_client_node, heketi_server_url,
                       prometheus_format=False):
    ''' Execute curl command to get metrics output

    Args:
        - heketi_client_node (str) : Node where we want to run our commands.
        - heketi_server_url (str) : This is a heketi server url
        - prometheus_format (bool) : control the format of output
            by default it is False, So it will parse prometheus format into
            python dict. If we need prometheus format we have to set it True.
    Returns:
        Metrics output: if successful
    Raises:
        err: if fails to run command

    '''

    cmd = "curl %s/metrics" % heketi_server_url
    ret, out, err = g.run(heketi_client_node, cmd)
    if ret != 0:
        msg = "failed to get Heketi metrics with following error: %s" % err
        g.log.error(msg)
        raise AssertionError(msg)
    if prometheus_format:
        return out.strip()
    return parse_prometheus_data(out)
