import time

from glustolibs.gluster.exceptions import ExecutionError
from glusto.core import Glusto as g
import six

from openshiftstoragelibs.cloundproviders.vmware import VmWare
from openshiftstoragelibs import command
from openshiftstoragelibs import exceptions
from openshiftstoragelibs import waiter


CLOUD_PROVIDER = None


def wait_for_ssh_connection(hostname, timeout=600, interval=10):
    """Wait for ssh conection to be ready within given timeout.

    Args:
        hostname (str): hostname of a machine.
    Returns:
        None
    Raises:
        CloudProviderError: In case of any failures.
    """
    for w in waiter.Waiter(timeout, interval):
        try:
            # Run random command to verify ssh connection
            g.run(hostname, 'ls')
            return
        except (exceptions.ExecutionError, ExecutionError):
            g.log.info("Waiting for ssh connection on host '%s'" % hostname)

    msg = 'Not able to connect with the %s' % hostname
    g.log.error(msg)
    raise exceptions.CloudProviderError(msg)


def node_reboot_by_command(
        node, timeout=600, wait_step=10, wait_for_connection=True):
    """Reboot node and wait to start for given timeout.

    Args:
        node (str)          : Node which needs to be rebooted.
        timeout (int)       : Seconds to wait before node to be started.
        wait_step (int)     : Interval in seconds to wait before checking
                              status of node again.
        wait_for_connection : Flag to wait for to check SSH connection to node.
    Raises:
        CloudProviderError: In case of any failures.
    """
    cmd = "sleep 3; /sbin/shutdown -r now 'Reboot triggered by Glusto'"
    ret, out, err = g.run(node, cmd)
    if ret != 255:
        err_msg = "failed to reboot host '{}' error {}".format(node, err)
        g.log.error(err_msg)
        raise AssertionError(err_msg)

    # added sleep as node will restart after 3 sec
    time.sleep(3)

    if wait_for_connection:
        wait_for_ssh_connection(node, timeout=timeout, interval=wait_step)


def _get_cloud_provider():
    """Gather cloud provider facts"""

    global CLOUD_PROVIDER
    if CLOUD_PROVIDER:
        return CLOUD_PROVIDER

    try:
        cloud_provider_name = g.config['cloud_provider']['name']
    except KeyError:
        msg = "Incorrect config file. Cloud provider name is missing."
        g.log.error(msg)
        raise exceptions.ConfigError(msg)

    if cloud_provider_name == 'vmware':
        CLOUD_PROVIDER = VmWare()
    else:
        msg = "Cloud Provider %s is not supported." % cloud_provider_name
        g.log.error(msg)
        raise NotImplementedError(msg)

    return CLOUD_PROVIDER


def find_vm_name_by_ip_or_hostname(ip_or_hostname):
    """Find VM name from the ip or hostname.

    Args:
        ip_or_hostname (str): IP address or hostname of VM.
    Returns:
        str: Name of the VM.
    """
    cloudProvider = _get_cloud_provider()
    g.log.info('getting the name of vm for ip or hostname %s' % ip_or_hostname)
    return cloudProvider.find_vm_name_by_ip_or_hostname(ip_or_hostname)


def get_power_state_of_vm_by_name(name):
    """Get the power state of VM.

    Args:
        name (str): name of the VM for which state has to be find.
    Returns:
        str: Power state of the VM.
    """
    cloudProvider = _get_cloud_provider()
    g.log.info('getting the power state of vm "%s"' % name)
    return cloudProvider.get_power_state_of_vm_by_name(name)


def power_off_vm_by_name(name):
    """Power off the virtual machine.

    Args:
        name (str): name of the VM which needs to be powered off.
    Returns:
        None
    """
    cloudProvider = _get_cloud_provider()
    g.log.info('powering off the vm "%s"' % name)
    cloudProvider.power_off_vm_by_name(name)
    g.log.info('powered off the vm "%s" successfully' % name)


def power_on_vm_by_name(name, timeout=600, interval=10):
    """Power on the virtual machine and wait for SSH ready within given
    timeout.

    Args:
        name (str): name of the VM which needs to be powered on.
    Returns:
        None
    Raises:
        CloudProviderError: In case of any failures.
    """
    cloudProvider = _get_cloud_provider()
    g.log.info('powering on the VM "%s"' % name)
    cloudProvider.power_on_vm_by_name(name)
    g.log.info('Powered on the VM "%s" successfully' % name)

    # Wait for hostname to get assigned
    _waiter = waiter.Waiter(timeout, interval)
    for w in _waiter:
        try:
            hostname = cloudProvider.wait_for_hostname(name, 1, 1)
            # NOTE(vponomar): Reset attempts for waiter to avoid redundant
            # sleep equal to 'interval' on the next usage.
            _waiter._attempt = 0
            break
        except Exception as e:
            g.log.info(e)
    if w.expired:
        raise exceptions.CloudProviderError(e)

    # Wait for hostname to ssh connection ready
    for w in _waiter:
        try:
            wait_for_ssh_connection(hostname, 1, 1)
            break
        except Exception as e:
            g.log.info(e)
    if w.expired:
        raise exceptions.CloudProviderError(e)


def node_add_iptables_rules(node, chain, rules, raise_on_error=True):
    """Append iptables rules

    Args:
        node (str): Node on which iptables rules should be added.
        chain (str): iptables chain in which rule(s) need to be appended.
        rules (str|tuple|list): Rule(s) which need(s) to be added to a chain.
    Reuturns:
        None
    Exception:
        AssertionError: In case command fails to execute and
                        raise_on_error set to True
    """
    rules = [rules] if isinstance(rules, six.string_types) else rules

    add_iptables_rule_cmd = "iptables --append %s %s"
    check_iptables_rule_cmd = "iptables --check %s %s"
    for rule in rules:
        try:
            command.cmd_run(check_iptables_rule_cmd % (chain, rule), node)
        except AssertionError:
            command.cmd_run(
                add_iptables_rule_cmd % (chain, rule), node,
                raise_on_error=raise_on_error)


def node_delete_iptables_rules(node, chain, rules, raise_on_error=True):
    """Delete iptables rules

    Args:
        node (str): Node on which iptables rules should be deleted.
        chain (str): iptables chain from which rule(s) need to be deleted.
        rules (str|tuple|list): Rule(s) which need(s) to be deleted from
                                a chain.
    Reuturns:
        None
    Exception:
        AssertionError: In case command fails to execute and
                        raise_on_error set to True
    """
    rules = [rules] if isinstance(rules, six.string_types) else rules

    delete_iptables_rule_cmd = "iptables --delete %s %s"
    for rule in rules:
        command.cmd_run(
            delete_iptables_rule_cmd % (chain, rule), node,
            raise_on_error=raise_on_error)


def attach_disk_to_vm(name, disk_size, disk_type='thin'):
    """Add the disk specified to virtual machine.

    Args:
        name (str): name of the VM for which disk needs to be added.
        disk_size (int) : Specify disk size in KB
        disk_type (str) : Type of the disk, could be thick or thin.
            Default value is "thin".
    Returns:
        None
    """
    cloudProvider = _get_cloud_provider()

    vm_name = find_vm_name_by_ip_or_hostname(name)
    cloudProvider.attach_disk(vm_name, disk_size, disk_type)


def attach_existing_vmdk_from_vmstore(name, disk_path, vmdk_name):
    """Attach existing disk vmdk specified to virtual machine.

    Args:
        name (str): name of the VM for which disk needs to be added.
        vmdk_name (str) : name of the vmdk file which needs to be added.

    Returns:
        None
    """
    cloudProvider = _get_cloud_provider()

    vm_name = find_vm_name_by_ip_or_hostname(name)
    cloudProvider.attach_existing_vmdk(vm_name, disk_path, vmdk_name)


def detach_disk_from_vm(name, disk_name):
    """Remove the disk specified from virtual machine.

    Args:
        name (str): name of the VM from where the disk needs to be removed.
        disk_name (str) : name of the disk which needs to be removed.
            Example:
                '/dev/sdd'
                '/dev/sde'
    Returns:
        vdisk (str): vmdk filepath of removed disk
    """
    cloudProvider = _get_cloud_provider()

    vm_name = find_vm_name_by_ip_or_hostname(name)
    vdisk = cloudProvider.detach_disk(vm_name, disk_name)
    return vdisk


def get_disk_labels(name):
    """Remove the disk specified from virtual machine.

    Args:
        name (str) : name of the disk which needs to be removed.
            Example:
                '/dev/sdd'
                '/dev/sde'
    Returns:
        None
    """
    cloudProvider = _get_cloud_provider()
    vm_name = find_vm_name_by_ip_or_hostname(name)
    disk_labels = cloudProvider.get_all_disks(vm_name)
    return disk_labels
