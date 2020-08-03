"""
Note: Do not use this module directly in the Test Cases. This module can be
used with the help of 'node_ops'
"""
import re
import string

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

        # TODO(Nitin Goyal): Need to raise exact same below exception in other
        # cloud providers as well in future e.g. AWS etc.
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

        # TODO(Nitin Goyal): Need to raise exact same below exception in other
        # cloud providers as well in future e.g. AWS etc.
        if vm[0].summary.runtime.powerState == 'poweredOff':
            msg = 'VM %s is already powered Off' % vm_name
            g.log.error(msg)
            raise exceptions.CloudProviderError(msg)

        tasks = [vm[0].PowerOff()]
        self._wait_for_tasks(tasks, self.vsphere_client)

    def get_obj(self, name, vimtype):
        """
        Retrieves the managed object for the name and type specified
        Args:
            name (str): Name of the VM.
            vimtype (str): Type of managed object
        Returns:
            obj (str): Object for specified vimtype and name
                Example:
                    'vim.VirtualMachine:vm-1268'
        Raises:
            CloudProviderError: In case of any failures.
        """
        obj = None
        content = self.vsphere_client.content.viewManager.CreateContainerView(
            self.vsphere_client.content.rootFolder, vimtype, True)
        for c in content.view:
            if c.name == name:
                obj = c
                break
        if not obj:
            msg = "Virtual machine with {} name not found.".format(name)
            g.log.error(msg)
            raise exceptions.CloudProviderError(msg)
        return obj

    def get_disk_labels(self, vm_name):
        """Retrieve disk labels which are attached to vm.

        Args:
            vm_name (str): Name of the VM.
        Returns:
            disk_labels (list): list of disks labels which are attached to vm.
                Example:
                    ['Hard disk 1', 'Hard disk 2', 'Hard disk 3']
        Raises:
            CloudProviderError: In case of any failures.
        """

        # Find vm
        vm = self.get_obj(vm_name, vimtype=[vim.VirtualMachine])

        disk_labels = []
        for dev in vm.config.hardware.device:
            disk_labels.append(dev.deviceInfo.label)
        return disk_labels

    def detach_disk(self, vm_name, disk_path):
        """Detach disk for given vmname by diskPath.

        Args:
            vm_name (str): Name of the VM.
            disk_path (str): Disk path which needs to be unplugged.
                Example:
                '/dev/sdd'
                '/dev/sde'
        Returns:
            vdisk_path (str): Path of vmdk file to be detached from vm.
        Raises:
            CloudProviderError: In case of any failures.
        """

        # Translate given disk to a disk label of vmware.
        letter = disk_path[-1]
        ucase = string.ascii_uppercase
        pos = ucase.find(letter.upper()) + 1
        if pos:
            disk_label = 'Hard disk {}'.format(str(pos))
        else:
            raise exceptions.CloudProviderError(
                "Hard disk '{}' missing from vm '{}'".format(pos, vm_name))

        # Find vm
        vm = self.get_obj(vm_name, vimtype=[vim.VirtualMachine])

        # Find if the given hard disk is attached to the system.
        virtual_hdd_device = None
        for dev in vm.config.hardware.device:
            if dev.deviceInfo.label == disk_label:
                virtual_hdd_device = dev
                vdisk_path = virtual_hdd_device.backing.fileName
                break

        if not virtual_hdd_device:
            raise exceptions.CloudProviderError(
                'Virtual disk label {} could not be found'.format(disk_label))
        disk_labels = self.get_disk_labels(vm_name)
        if disk_label in disk_labels:

            # Remove disk from the vm
            virtual_hdd_spec = vim.vm.device.VirtualDeviceSpec()
            virtual_hdd_spec.operation = (
                vim.vm.device.VirtualDeviceSpec.Operation.remove)
            virtual_hdd_spec.device = virtual_hdd_device

            # Wait for the task to be completed.
            spec = vim.vm.ConfigSpec()
            spec.deviceChange = [virtual_hdd_spec]
            task = vm.ReconfigVM_Task(spec=spec)
            self._wait_for_tasks([task], self.vsphere_client)
        else:
            msg = ("Could not find provided disk {} in list of disks {}"
                   " in vm {}".format(disk_label, disk_labels, vm_name))
            g.log.error(msg)
            raise exceptions.CloudProviderError(msg)
        return vdisk_path

    def attach_existing_vmdk(self, vm_name, disk_path, vmdk_name):
        """
        Attach already existing disk to vm
        Args:
            vm_name (str): Name of the VM.
            disk_path (str): Disk path which needs to be unplugged.
                Example:
                '/dev/sdd'
                '/dev/sde'
            vmdk_name (str): Path of vmdk file to attach in vm.
        Returns:
           None
        Raises:
            CloudProviderError: In case of any failures.
        """

        # Find vm
        vm = self.get_obj(vm_name, vimtype=[vim.VirtualMachine])

        # Translate given disk to a disk label of vmware.
        letter = disk_path[-1]
        ucase = string.ascii_uppercase
        pos = ucase.find(letter.upper()) + 1
        if pos:
            disk_label = 'Hard disk {}'.format(str(pos))
        else:
            raise exceptions.CloudProviderError(
                "Hard disk '{}' missing from vm '{}'".format(pos, vm_name))

        # Find if the given hard disk is not attached to the vm
        for dev in vm.config.hardware.device:
            if dev.deviceInfo.label == disk_label:
                raise exceptions.CloudProviderError(
                    'Virtual disk label {} already exists'.format(disk_label))

        # Find unit number for attaching vmdk
        unit_number = 0
        for dev in vm.config.hardware.device:
            if hasattr(dev.backing, 'fileName'):
                unit_number = int(dev.unitNumber) + 1

                # unit_number 7 reserved for scsi controller, max(16)
                if unit_number == 7:
                    unit_number += 1
                if unit_number >= 16:
                    raise Exception(
                        "SCSI controller is full. Cannot attach vmdk file")
            if isinstance(dev, vim.vm.device.VirtualSCSIController):
                controller = dev

        # Attach vmdk file to the disk and setting backings
        spec = vim.vm.ConfigSpec()
        disk_spec = vim.vm.device.VirtualDeviceSpec()
        disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        disk_spec.device = vim.vm.device.VirtualDisk()
        disk_spec.device.backing = (
            vim.vm.device.VirtualDisk.FlatVer2BackingInfo())
        disk_spec.device.backing.diskMode = 'persistent'
        disk_spec.device.backing.fileName = vmdk_name
        disk_spec.device.backing.thinProvisioned = True
        disk_spec.device.unitNumber = unit_number
        disk_spec.device.controllerKey = controller.key

        # creating the list
        dev_changes = []
        dev_changes.append(disk_spec)
        spec.deviceChange = dev_changes
        task = vm.ReconfigVM_Task(spec=spec)
        self._wait_for_tasks([task], self.vsphere_client)
