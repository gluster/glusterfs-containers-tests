from collections import OrderedDict
from cnslibs.common.exceptions import (
    ConfigError,
    ExecutionError)
from cnslibs.common.openshift_ops import (
    get_ocp_gluster_pod_names,
    oc_rsh)
from cnslibs.common.waiter import Waiter
import fileinput
from glusto.core import Glusto as g
import json
import rtyaml
import time
import yaml


MASTER_CONFIG_FILEPATH = "/etc/origin/master/master-config.yaml"


def edit_master_config_file(hostname, routingconfig_subdomain):
    '''
     This function edits the /etc/origin/master/master-config.yaml file
     Args:
         hostname (str): hostname on which want to edit
                         the master-config.yaml file
         routingconfig_subdomain (str): routing config subdomain url
                                        ex: cloudapps.mystorage.com
     Returns:
         bool: True if successful,
               otherwise False
    '''
    try:
        conn = g.rpyc_get_connection(hostname, user="root")
        if conn is None:
            g.log.error("Failed to get rpyc connection of node %s"
                        % hostname)
            return False
        with conn.builtin.open(MASTER_CONFIG_FILEPATH, 'r') as f:
            data = yaml.load(f)
            add_allow = 'AllowAllPasswordIdentityProvider'
            data['oauthConfig']['identityProviders'][0]['provider'][
                'kind'] = add_allow
            data['routingConfig']['subdomain'] = routingconfig_subdomain
        with conn.builtin.open(MASTER_CONFIG_FILEPATH, 'w+') as f:
            yaml.dump(data, f, default_flow_style=False)
    except Exception as err:
        raise ExecutionError("failed to edit master-config.yaml file "
                             "%s on %s" % (err, hostname))
    finally:
        g.rpyc_close_connection(hostname, user="root")

    g.log.info("successfully edited master-config.yaml file %s" % hostname)
    return True


def setup_router(hostname, router_name, timeout=1200, wait_step=60):
    '''
     This function sets up router
     Args:
         hostname (str): hostname on which we need to
                         setup router
         router_name (str): router name
         timeout (int): timeout value,
                        default value is 1200 sec
         wait_step( int): wait step,
                          default value is 60 sec
     Returns:
         bool: True if successful,
               otherwise False
    '''
    cmd = ("oc get pods | grep '%s'| grep -v deploy | "
           "awk '{print $3}'" % router_name)
    ret, out, err = g.run(hostname, cmd, "root")
    if ret != 0:
        g.log.error("failed to execute cmd %s" % cmd)
        return False
    output = out.strip().split("\n")[0]
    if "No resources found" in output or output == "":
        g.log.info("%s not present creating it" % router_name)
        cmd = "oadm policy add-scc-to-user privileged -z router"
        ret, out, err = g.run(hostname, cmd, "root")
        if ret != 0:
            g.log.error("failed to execute cmd %s" % cmd)
            return False
        cmd = "oadm policy add-scc-to-user privileged -z default"
        ret, out, err = g.run(hostname, cmd, "root")
        if ret != 0:
            g.log.error("failed to execute cmd %s" % cmd)
            return False
        cmd = "oadm router %s --replicas=1" % router_name
        ret, out, err = g.run(hostname, cmd, "root")
        if ret != 0:
            g.log.error("failed to execute cmd %s" % cmd)
            return False
        router_flag = False
        for w in Waiter(timeout, wait_step):
            cmd = "oc get pods | grep '%s'|  awk '{print $3}'" % router_name
            ret, out, err = g.run(hostname, cmd, "root")
            if ret != 0:
                g.log.error("failed to execute cmd %s" % cmd)
                break
            status = out.strip().split("\n")[0].strip()
            if status == "ContainerCreating" or status == "Pending":
                g.log.info("container creating for router %s sleeping for"
                           " %s seconds" % (router_name, wait_step))
                continue
            elif status == "Running":
                router_flag = True
                g.log.info("router %s is up and running" % router_name)
                return router_flag
            elif status == "Error":
                g.log.error("error while setting up router %s" % (
                                router_name))
                return router_flag
            else:
                g.log.error("%s router pod has different status - "
                            "%s" % (router_name, status))
                return router_flag
        if w.expired:
            g.log.error("failed to setup '%s' router in "
                        "%s seconds" % (router_name, timeout))
            return False
    else:
        g.log.info("%s already present" % router_name)
    return True


def update_router_ip_dnsmasq_conf(hostname, router_name, router_domain):
    '''
     This function updates the router-ip in /etc/dnsmasq.conf file
     Args:
         hostname (str): hostname on which we need to
                         edit dnsmaq.conf file
         router_name (str): router name to find its ip
     Returns:
         bool: True if successful,
               otherwise False
    '''
    cmd = ("oc get pods -o wide | grep '%s'| grep -v deploy | "
           "awk '{print $6}' | cut -d ':' -f 1") % router_name
    ret, out, err = g.run(hostname, cmd, "root")
    if ret != 0:
        g.log.error("failed to execute cmd %s" % cmd)
        return False
    router_ip = out.strip().split("\n")[0].strip()
    data_to_write = "address=/.%s/%s" % (router_domain, router_ip)
    try:
        conn = g.rpyc_get_connection(hostname, user="root")
        if conn is None:
            g.log.error("Failed to get rpyc connection of node %s"
                        % hostname)
            return False

        update_flag = False
        for line in conn.modules.fileinput.input(
                '/etc/dnsmasq.conf', inplace=True):
            if router_domain in line:
                conn.modules.sys.stdout.write(line.replace(line,
                                              data_to_write))
                update_flag = True
            else:
                conn.modules.sys.stdout.write(line)
        if not update_flag:
            with conn.builtin.open('/etc/dnsmasq.conf', 'a+') as f:
                f.write(data_to_write + '\n')
    except Exception as err:
        g.log.error("failed to update router-ip in dnsmasq.conf %s" % err)
        return False
    finally:
        g.rpyc_close_connection(hostname, user="root")
    g.log.info("sucessfully updated router-ip in dnsmasq.conf")
    return True


def update_nameserver_resolv_conf(hostname, position="first_line"):
    '''
     This function updates namserver 127.0.0.1
     at first line in  /etc/resolv.conf
     Args:
         hostname (str): hostname on which we need to
                         edit resolv.conf
         position (str): where to add nameserver
                         ex: EOF, it defaults to first line
     Returns:
         bool: True if successful,
               otherwise False
    '''
    try:
        conn = g.rpyc_get_connection(hostname, user="root")
        if conn is None:
            g.log.error("Failed to get rpyc connection of node %s"
                        % hostname)
            return False

        if position == "EOF":
            update_flag = False
            with conn.builtin.open("/etc/resolv.conf", "r+") as f:
                for line in f:
                    if "nameserver" in line and "127.0.0.1" in line:
                        update_flag = True
                        break
                if not update_flag:
                    f.write("nameserver 127.0.0.1\n")
        else:
            for linenum, line in enumerate(conn.modules.fileinput.input(
                    '/etc/resolv.conf', inplace=True)):
                if linenum == 0 and "127.0.0.1" not in line:
                    conn.modules.sys.stdout.write("nameserver 127.0.0.1\n")
                conn.modules.sys.stdout.write(line)
    except Exception as err:
        g.log.error("failed to update nameserver in resolv.conf %s" % err)
        return False
    finally:
        g.rpyc_close_connection(hostname, user="root")
    g.log.info("sucessfully updated namserver in resolv.conf")
    return True


def edit_multipath_conf_file(hostname):
    '''
     This function edits the /etc/multipath.conf
     Args:
         hostname (str): hostname on which we want to edit
                         the /etc/multipath.conf file
     Returns:
         bool: True if successful,
               otherwise False
    '''
    try:
        conn = g.rpyc_get_connection(hostname, user="root")
        if conn is None:
            g.log.error("Failed to get rpyc connection of node %s"
                        % hostname)
            return False

        edit_flag = False
        file1 = conn.builtin.open("/etc/multipath.conf", "r+")
        for line1 in file1.readlines():
            if "LIO iSCSI" in line1:
                g.log.info("/etc/multipath.conf file already "
                           "edited on %s" % hostname)
                edit_flag = True
        if not edit_flag:
            file1 = conn.builtin.open("/etc/multipath.conf", "a+")
            with open("cnslibs/common/sample-multipath.txt") as file2:
                for line2 in file2:
                    file1.write(line2)
    except Exception as err:
        g.log.error("failed to edit /etc/multipath.conf file %s on %s" %
                    (err, hostname))
        return False
    finally:
        g.rpyc_close_connection(hostname, user="root")
    g.log.info("successfully edited /etc/multipath.conf file %s" % hostname)
    return True


def edit_iptables_cns(hostname):
    '''
     This function edits the iptables file to open the ports
     Args:
         hostname (str): hostname on which we need to edit
                         the iptables
     Returns:
         bool: True if successful,
               otherwise False
    '''
    try:
        conn = g.rpyc_get_connection(hostname, user="root")
        if conn is None:
            g.log.error("Failed to get rpyc connection of node %s"
                        % hostname)
            return False

        edit_flag = False
        commit_count = 0
        with conn.builtin.open("/etc/sysconfig/iptables", "r+") as f:
            for line in f.readlines():
                if "--dport 3260" in line:
                    edit_flag = True
        data = [
            "-A OS_FIREWALL_ALLOW -p tcp -m state --state NEW -m %s" % line
            for line in ("tcp       --dport 24007        -j ACCEPT",
                         "tcp       --dport 24008        -j ACCEPT",
                         "tcp       --dport 2222         -j ACCEPT",
                         "multiport --dports 49152:49664 -j ACCEPT",
                         "tcp       --dport 24010        -j ACCEPT",
                         "tcp       --dport 3260         -j ACCEPT",
                         "tcp       --dport 111          -j ACCEPT")
        ]
        data_to_write = "\n".join(data) + "\n"
        filter_flag = False
        if not edit_flag:
            for line in conn.modules.fileinput.input('/etc/sysconfig/iptables',
                                                     inplace=True):
                if "*filter" in line:
                    filter_flag = True
                if "COMMIT" in line and filter_flag is True:
                    conn.modules.sys.stdout.write(data_to_write)
                    filter_flag = False
                conn.modules.sys.stdout.write(line)
        else:
            g.log.info("Iptables is already edited on %s" % hostname)
            return True

    except Exception as err:
        g.log.error("failed to edit iptables on %s err %s" % (hostname, err))
        return False
    finally:
        g.rpyc_close_connection(hostname, user="root")

    g.log.info("successfully edited iptables on %s" % hostname)
    return True


def enable_kernel_module(hostname, module_name):
    '''
     This function enables kernel modules required for CNS
     Args:
         hostname (str): hostname on which we want to
                         enable kernel modules
         module_name (str): name of the module
                            ex: dm_thin_pool
     Returns:
         bool: True if successfull or already running,
               False otherwise
    '''
    cmd = "lsmod | grep %s" % module_name
    ret, out, err = g.run(hostname, cmd, "root")
    if ret == 0:
        g.log.info("%s module is already enabled on %s"
                   % (module_name, hostname))
    else:
        cmd = "modprobe %s" % module_name
        ret, out, err = g.run(hostname, cmd, "root")
        if ret == 0:
            g.log.info("%s module enabled on %s"
                       % (module_name, hostname))
        else:
            g.log.error("failed to enable %s  module on %s"
                        % (module_name, hostname))
            return False
        cmd = "echo %s > /etc/modules-load.d/%s.conf" % (
                  module_name, module_name)
        ret, out, err = g.run(hostname, cmd, "root")
        if ret == 0:
            g.log.info("created %s.conf" % module_name)
        else:
            g.log.error("failed to %s.conf" % module_name)

    return True


def start_service(hostname, service):
    '''
     This function starts service by its name
     Args:
         hostname (str): hostname on which we want
                         to start service
     Returns:
         bool: True if successfull or already running,
               False otherwise
    '''
    cmd = "systemctl status %s" % service
    ret, out, err = g.run(hostname, cmd, "root")
    if ret == 0:
        g.log.info("%s service is already running on %s"
                   % (service, hostname))
        return True
    cmd = "systemctl start %s" % service
    ret, out, err = g.run(hostname, cmd, "root")
    if ret == 0:
        g.log.info("successfully started %s service on %s"
                   % (service, hostname))
        return True
    g.log.error("failed to start %s service on %s"
                % (service, hostname))
    return False


def start_rpcbind_service(hostname):
    '''
     This function starts the rpcbind service
     Args:
         hostname (str): hostname on which we want to start
                         rpcbind service
     Returns:
         bool: True if successfull or already running,
               False otherwise
    '''
    return start_service(hostname, 'rpcbind')


def start_gluster_blockd_service(hostname):
    '''
     This function starts the gluster-blockd service
     Args:
         hostname (str): hostname on which we want to start
                         gluster-blocks service
     Returns:
         bool: True if successfull or already running,
               False otherwise
    '''
    return start_service(hostname, 'gluster-blockd')


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
    ret, out, err = g.run(hostname, cmd, "root")
    if ret != 0 or out == "":
        g.log.error("failed to exectute cmd %s on %s, err %s"
                    % (cmd, pod_nodename, out))
        return False
    active_count = int(output.strip())
    if active_node_count != active_count:
        g.log.error("active node count on %s for %s is %s and not 1"
                    % (pod_nodename, podname, active_count))
        return False
    cmd = "multipath -ll | grep 'status=enabled' | wc -l"
    ret, out, err = g.run(hostname, cmd, "root")
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


def validate_gluster_blockd_service_gluster_pod(hostname):
    '''
     This function validates if gluster-blockd service is
     running on all gluster-pods
     Args:
         hostname (str): OCP master node name
     Returns:
         bool: True if service is running on all gluster-pods,
               otherwise False
    '''
    gluster_pod_list = get_ocp_gluster_pod_names(hostname)
    g.log.info("gluster_pod_list -> %s" % gluster_pod_list)
    for pod in gluster_pod_list:
        cmd = "systemctl status gluster-blockd"
        ret, out, err = oc_rsh(hostname, pod, cmd)
        if ret != 0:
            g.log.error("failed to execute cmd %s on %s out: "
                        "%s err: %s" % (
                            cmd, hostname, out, err))
            return False
    g.log.info("gluster-blockd service is running on all "
               "gluster-pods %s" % gluster_pod_list)
    return True
