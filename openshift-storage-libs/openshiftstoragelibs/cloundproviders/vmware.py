"""
Note: Do not use this module directly in the Test Cases. This module can be
used with the help of 'node_ops'
"""
import re

from glusto.core import Glusto as g
from pyVim import connect
from pyVmomi import vim, vmodl
import six

from openshiftstoragelibs import exceptions
from openshiftstoragelibs.waiter import Waiter


IP_REGEX = r"(^[12]?\d{1,2}\.[12]?\d{1,3}\.[12]?\d{1,2}\.[12]?\d{1,2}$)"


class VmWare(object):

    def __init__(self):
        try:
            self.hostname = g.config['cloud_provider']['vmware']['hostname']
            self.username = g.config['cloud_provider']['vmware']['username']
            self.password = g.config['cloud_provider']['vmware']['password']
            self.port = g.config['cloud_provider']['vmware'].get('port', 443)
        except KeyError:
            msg = ("Config file doesn't have values related to vmware Cloud"
                   " Provider.")
            g.log.error(msg)
            raise exceptions.ConfigError(msg)

        # Connect vsphere client
        try:
            self.vsphere_client = connect.ConnectNoSSL(
                self.hostname, self.port, self.username, self.password)
        except Exception as e:
            g.log.error(e)
            raise exceptions.CloudProviderError(e)

    def __del__(self):
        # Disconnect vsphere client
        try:
            connect.Disconnect(self.vsphere_client)
        except Exception as e:
            g.log.error(e)
            raise exceptions.CloudProviderError(e)

    def _wait_for_tasks(self, tasks, si):
        """Given the service instance si and tasks, it returns after all the
        tasks are complete.
        """

        pc = si.content.propertyCollector

        taskList = [six.text_type(task) for task in tasks]

        # Create filter
        objSpecs = [vmodl.query.PropertyCollector.ObjectSpec(obj=task)
                    for task in tasks]
        propSpec = vmodl.query.PropertyCollector.PropertySpec(
            type=vim.Task, pathSet=[], all=True)
        filterSpec = vmodl.query.PropertyCollector.FilterSpec()
        filterSpec.objectSet = objSpecs
        filterSpec.propSet = [propSpec]
        filterTask = pc.CreateFilter(filterSpec, True)

        try:
            version, state = None, None

            # Looking for updates till the state moves to a completed state.
            while len(taskList):
                update = pc.WaitForUpdates(version)
                for filterSet in update.filterSet:
                    for objSet in filterSet.objectSet:
                        task = objSet.obj
                        for change in objSet.changeSet:
                            if change.name == 'info':
                                state = change.val.state
                            elif change.name == 'info.state':
                                state = change.val
                            else:
                                continue

                        if not six.text_type(task) in taskList:
                            continue

                        if state == vim.TaskInfo.State.success:
                            # Remove task from taskList
                            taskList.remove(six.text_type(task))
                        elif state == vim.TaskInfo.State.error:
                            raise task.info.error
                # Move to next version
                version = update.version
        finally:
            if filterTask:
                filterTask.Destroy()

    def wait_for_hostname(self, vm_name, timeout=600, interval=10):
        """Wait for hostname to get assigned to a VM.

        Args:
            vm_name (str): Name of the VM.
        Returns:
            str: hostname of the VM.
        Raises:
            CloudProviderError: In case of any failures.
        """
        for w in Waiter(timeout, interval):
            vmlist = (
                self.vsphere_client.content.viewManager.CreateContainerView(
                    self.vsphere_client.content.rootFolder,
                    [vim.VirtualMachine], True))
            vm = [vm for vm in vmlist.view if vm.name == vm_name]
            hostname = vm[0].summary.guest.hostName
            if hostname:
                return hostname
        msg = 'VM %s did not got assigned hostname' % vm_name
        g.log.error(msg)
        raise exceptions.CloudProviderError(msg)

    def find_vm_name_by_ip_or_hostname(self, ip_or_hostname):
        """Find the name of VM by its IPv4 or HostName in vmware client.

        Args:
            ip_or_hostname (str): IPv4 or HostName of the VM.
        Returns:
            str: name of the VM.
        Raises:
            CloudProviderError: In case of any failures.
        Note:
            VM should be up and IP should be assigned to use this lib.
        """
        # Get a searchIndex object
        searcher = self.vsphere_client.content.searchIndex

        global IP_REGEX
        status_match = re.search(IP_REGEX, ip_or_hostname)

        if status_match:
            # Find a VM by IP
            vm = searcher.FindByIp(ip=ip_or_hostname, vmSearch=True)
        else:
            # Find a VM by hostname
            vm = searcher.FindByDnsName(dnsName=ip_or_hostname, vmSearch=True)

        if vm:
            return vm.name

        msg = 'IP or hostname %s is not assigned to any VM' % ip_or_hostname
        g.log.error(msg)
        raise exceptions.CloudProviderError(msg)

    def get_power_state_of_vm_by_name(self, vm_name):
        """Get the power state of VM by its name.

        Args:
            vm_name (str): name of the VM.
        Returns:
            str: power state of VM.
        Raises:
            CloudProviderError: In case of any failures.
        """
        # Get list of all VM's
        vmlist = self.vsphere_client.content.viewManager.CreateContainerView(
            self.vsphere_client.content.rootFolder, [vim.VirtualMachine], True)

        # Find VM
        vm = [vm for vm in vmlist.view if vm.name == vm_name]

        if vm:
            # Get VM power State
            return vm[0].summary.runtime.powerState

        msg = 'VM %s is not present in the cluster' % vm_name
        g.log.error(msg)
        raise exceptions.CloudProviderError(msg)

    def power_on_vm_by_name(self, vm_name):
        """Power on VM by its name.

        Args:
            vm_name (str): name of the VM.
        Returns:
            None
        Raises:
            CloudProviderError: In case of any failures.
        """
        # Get list of all VM's
        vmlist = self.vsphere_client.content.viewManager.CreateContainerView(
            self.vsphere_client.content.rootFolder, [vim.VirtualMachine], True)

        # Find VM
        vm = [vm for vm in vmlist.view if vm.name == vm_name]

        if not vm:
            msg = 'VM %s is not present in list' % vm_name
            g.log.error(msg)
            raise exceptions.CloudProviderError(msg)

        if vm[0].summary.runtime.powerState == 'poweredOn':
            msg = 'VM %s is already powered On' % vm_name
            g.log.error(msg)
            raise exceptions.CloudProviderError(msg)

        tasks = [vm[0].PowerOn()]
        self._wait_for_tasks(tasks, self.vsphere_client)

    def power_off_vm_by_name(self, vm_name):
        """Power off VM by its name.

        Args:
            vm_name (str): name of the VM.
        Returns:
            None
        Raises:
            CloudProviderError: In case of any failures.
        """
        # Get list of all VM's
        vmlist = self.vsphere_client.content.viewManager.CreateContainerView(
            self.vsphere_client.content.rootFolder, [vim.VirtualMachine], True)

        # Find VM
        vm = [vm for vm in vmlist.view if vm.name == vm_name]

        if not vm:
            msg = 'VM %s is not present in list' % vm_name
            g.log.error(msg)
            raise exceptions.CloudProviderError(msg)

        if vm[0].summary.runtime.powerState == 'poweredOff':
            msg = 'VM %s is already powered Off' % vm_name
            g.log.error(msg)
            raise exceptions.CloudProviderError(msg)

        tasks = [vm[0].PowerOff()]
        self._wait_for_tasks(tasks, self.vsphere_client)
