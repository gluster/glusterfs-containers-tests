#!/usr/bin/env python
# vim: sw=4 ts=4 et

import argparse
import click
import fileinput
import os
import re
import requests
import six
from six.moves import configparser
import sys
import yaml

try:
    import json
except ImportError:
    import simplejson as json


class VMWareAddNode(object):
    __name__ = 'VMWareAddNode'

    openshift_vers = None
    cluster_id = None
    vcenter_host = None
    vcenter_username = None
    vcenter_password = None
    vcenter_template_name = None
    vcenter_folder = None
    vcenter_datastore = None
    vcenter_datacenter = None
    vcenter_cluster = None
    vcenter_datacenter = None
    vcenter_resource_pool = None
    rhel_subscription_server = None
    openshift_sdn = None
    compute_nodes = None
    storage_nodes = None
    cns_automation_config_file_path = None
    ocp_hostname_prefix = None
    deployment_type = None
    console_port = 8443
    rhel_subscription_user = None
    rhel_subscription_pass = None
    rhel_subscription_pool = None
    dns_zone = None
    app_dns_prefix = None
    admin_key = None
    user_key = None
    wildcard_zone = None
    inventory_file = 'add-node.json'
    node_type = None
    node_number = None
    openshift_disable_check = None
    container_storage = None
    container_storage_disks = None
    container_storage_block_hosting_volume_size = None
    container_storage_disk_type = None
    additional_disks_to_storage_nodes = None
    container_storage_glusterfs_timeout = None
    heketi_admin_key = None
    heketi_user_key = None
    tag = None
    verbose = 0
    docker_registry_url = None
    docker_additional_registries = None
    docker_insecure_registries = None
    docker_image_tag = None
    ose_puddle_repo = None
    gluster_puddle_repo = None
    cns_glusterfs_image = None
    cns_glusterfs_version = None
    cns_glusterfs_block_image = None
    cns_glusterfs_block_version = None
    cns_glusterfs_heketi_image = None
    cns_glusterfs_heketi_version = None
    disable_yum_update_and_reboot = None
    openshift_use_crio = None

    def __init__(self):
        self.parse_cli_args()
        self.read_ini_settings()
        self.create_inventory_file()
        self.launch_refarch_env()

    def update_ini_file(self):
        """Update INI file with added number of nodes."""
        scriptbasename = "ocp-on-vmware"
        defaults = {'vmware': {
            'ini_path': os.path.join(
                os.path.dirname(__file__), '%s.ini' % scriptbasename),
            'storage_nodes': '3',
            'compute_nodes': '2',
        }}
        # where is the config?
        if six.PY3:
            config = configparser.ConfigParser()
        else:
            config = configparser.SafeConfigParser()

        vmware_ini_path = os.environ.get(
            'VMWARE_INI_PATH', defaults['vmware']['ini_path'])
        vmware_ini_path = os.path.expanduser(
            os.path.expandvars(vmware_ini_path))
        config.read(vmware_ini_path)

        if 'compute' in self.node_type:
            self.compute_nodes = (
                int(self.compute_nodes) + int(self.node_number))
            config.set('vmware', 'compute_nodes', str(self.compute_nodes))
            print "Updating %s file with %s compute_nodes" % (
                vmware_ini_path, self.compute_nodes)
        if 'storage' in self.node_type:
            self.storage_nodes = int(self.storage_nodes) or 3
            config.set('vmware', 'storage_nodes', str(self.storage_nodes))
            print "Updating %s file with %s storage_nodes" % (
                vmware_ini_path, self.storage_nodes)

        for line in fileinput.input(vmware_ini_path, inplace=True):
            if line.startswith("compute_nodes"):
                print "compute_nodes=" + str(self.compute_nodes)
            elif line.startswith("storage_nodes"):
                print "storage_nodes=" + str(self.storage_nodes)
            else:
                print line,

    def parse_cli_args(self):
        """Command line argument processing."""

        tag_help = """Skip to various parts of install valid tags include:
        - vms (create vms for adding nodes to cluster or CNS/CRS)
        - node-setup (install the proper packages on the CNS/CRS nodes)
        - clean (remove vms and unregister them from RHN
                 also remove storage classes or secrets"""
        parser = argparse.ArgumentParser(
            description='Add new nodes to an existing OCP deployment',
            formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument(
            '--node_type', action='store', default='compute',
            help='Specify the node label: compute, storage')
        parser.add_argument(
            '--node_number', action='store', default='3',
            help='Specify the number of nodes to add.')
        parser.add_argument(
            '--create_inventory', action='store_true',
            help=('Deprecated and not used option. '
                  'Everything that is needed gets autocreated.'))
        parser.add_argument(
            '--no_confirm', action='store_true',
            help='Skip confirmation prompt')
        parser.add_argument('--tag', default=None, help=tag_help)
        parser.add_argument(
            '--verbose', default=None, action='store_true',
            help='Verbosely display commands')
        self.args = parser.parse_args()
        self.verbose = self.args.verbose

    def _is_rpm_and_image_tag_compatible(self):
        if not (self.docker_image_tag and self.ose_puddle_repo):
            return True
        url = self.ose_puddle_repo
        if url[-1] == '/':
            url += 'Packages/'
        else:
            url += '/Packages/'
        resp = requests.get(url)
        if resp.ok:
            v = self.docker_image_tag.split('v')[-1].strip().split('-')[0]
            return (('atomic-openshift-%s' % v) in resp.text)
        raise Exception(
            "Failed to pull list of packages from '%s' url." % url)

    def read_ini_settings(self):
        """Read ini file settings."""

        scriptbasename = "ocp-on-vmware"
        defaults = {'vmware': {
            'ini_path': os.path.join(
                os.path.dirname(__file__), '%s.ini' % scriptbasename),
            'console_port': '8443',
            'container_storage': 'none',
            'container_storage_disks': '100,600',
            'container_storage_block_hosting_volume_size': '99',
            'additional_disks_to_storage_nodes': '100',
            'container_storage_disk_type': 'eagerZeroedThick',
            'container_storage_glusterfs_timeout': '',
            'heketi_admin_key': '',
            'heketi_user_key': '',
            'docker_registry_url': '',
            'docker_additional_registries': '',
            'docker_insecure_registries': '',
            'docker_image_tag': '',
            'ose_puddle_repo': '',
            'gluster_puddle_repo': '',
            'cns_glusterfs_image': 'rhgs3/rhgs-server-rhel7',
            'cns_glusterfs_version': 'latest',
            'cns_glusterfs_block_image': 'rhgs3/rhgs-gluster-block-prov-rhel7',
            'cns_glusterfs_block_version': 'latest',
            'cns_glusterfs_heketi_image': 'rhgs3/rhgs-volmanager-rhel7',
            'cns_glusterfs_heketi_version': 'latest',
            'deployment_type': 'openshift-enterprise',
            'openshift_vers': 'v3_11',
            'vcenter_username': 'administrator@vsphere.local',
            'vcenter_template_name': 'not-defined',
            'vcenter_folder': 'ocp',
            'vcenter_resource_pool': '/Resources/OCP3',
            'app_dns_prefix': 'apps',
            'vm_network': 'VM Network',
            'rhel_subscription_pool': 'Employee SKU',
            'openshift_sdn': 'redhat/openshift-ovs-subnet',
            'compute_nodes': '2',
            'storage_nodes': '3',
            'cns_automation_config_file_path': '',
            'ocp_hostname_prefix': 'openshift-on-vmware',
            'node_type': self.args.node_type,
            'node_number': self.args.node_number,
            'tag': self.args.tag,
            'openshift_disable_check': (
                'docker_storage,docker_image_availability,disk_availability'),
            'disable_yum_update_and_reboot': 'no',
            'openshift_use_crio': 'false',
        }}
        if six.PY3:
            config = configparser.ConfigParser()
        else:
            config = configparser.SafeConfigParser()

        # where is the config?
        vmware_ini_path = os.environ.get(
            'VMWARE_INI_PATH', defaults['vmware']['ini_path'])
        vmware_ini_path = os.path.expanduser(
            os.path.expandvars(vmware_ini_path))
        config.read(vmware_ini_path)

        # apply defaults
        for k, v in defaults['vmware'].items():
            if not config.has_option('vmware', k):
                config.set('vmware', k, str(v))

        self.console_port = config.get('vmware', 'console_port')
        self.cluster_id = config.get('vmware', 'cluster_id')
        self.container_storage = config.get('vmware', 'container_storage')
        self.container_storage_disks = config.get(
            'vmware', 'container_storage_disks')
        self.container_storage_block_hosting_volume_size = config.get(
            'vmware',
            'container_storage_block_hosting_volume_size').strip() or 99
        self.container_storage_disk_type = config.get(
            'vmware', 'container_storage_disk_type')
        self.additional_disks_to_storage_nodes = config.get(
            'vmware', 'additional_disks_to_storage_nodes')
        self.container_storage_glusterfs_timeout = config.get(
            'vmware', 'container_storage_glusterfs_timeout')
        self.heketi_admin_key = config.get('vmware', 'heketi_admin_key')
        self.heketi_user_key = config.get('vmware', 'heketi_user_key')
        self.docker_registry_url = config.get('vmware', 'docker_registry_url')
        self.docker_additional_registries = config.get(
            'vmware', 'docker_additional_registries')
        self.docker_insecure_registries = config.get(
            'vmware', 'docker_insecure_registries')
        self.docker_image_tag = (
            config.get('vmware', 'docker_image_tag') or '').strip()
        self.ose_puddle_repo = config.get('vmware', 'ose_puddle_repo')
        self.gluster_puddle_repo = config.get('vmware', 'gluster_puddle_repo')
        self.cns_glusterfs_image = (
            config.get('vmware', 'cns_glusterfs_image')).strip()
        self.cns_glusterfs_version = (
            config.get('vmware', 'cns_glusterfs_version')).strip()
        self.cns_glusterfs_block_image = (
            config.get('vmware', 'cns_glusterfs_block_image')).strip()
        self.cns_glusterfs_block_version = (
            config.get('vmware', 'cns_glusterfs_block_version')).strip()
        self.cns_glusterfs_heketi_image = (
            config.get('vmware', 'cns_glusterfs_heketi_image')).strip()
        self.cns_glusterfs_heketi_version = (
            config.get('vmware', 'cns_glusterfs_heketi_version')).strip()
        self.deployment_type = config.get('vmware', 'deployment_type')
        self.openshift_vers = config.get('vmware', 'openshift_vers')
        self.vcenter_host = config.get('vmware', 'vcenter_host')
        self.vcenter_username = config.get('vmware', 'vcenter_username')
        self.vcenter_password = config.get('vmware', 'vcenter_password')
        self.vcenter_template_name = config.get(
            'vmware', 'vcenter_template_name')
        self.vcenter_folder = config.get('vmware', 'vcenter_folder')
        self.vcenter_datastore = config.get('vmware', 'vcenter_datastore')
        self.vcenter_datacenter = config.get('vmware', 'vcenter_datacenter')
        self.vcenter_cluster = config.get('vmware', 'vcenter_cluster')
        self.vcenter_datacenter = config.get('vmware', 'vcenter_datacenter')
        self.vcenter_resource_pool = config.get(
            'vmware', 'vcenter_resource_pool')
        self.dns_zone = config.get('vmware', 'dns_zone')
        self.app_dns_prefix = config.get('vmware', 'app_dns_prefix')
        self.vm_network = config.get('vmware', 'vm_network')
        self.rhel_subscription_user = config.get(
            'vmware', 'rhel_subscription_user')
        self.rhel_subscription_pass = config.get(
            'vmware', 'rhel_subscription_pass')
        self.rhel_subscription_server = config.get(
            'vmware', 'rhel_subscription_server')
        self.rhel_subscription_pool = config.get(
            'vmware', 'rhel_subscription_pool')
        self.openshift_sdn = config.get('vmware', 'openshift_sdn')
        self.compute_nodes = int(config.get('vmware', 'compute_nodes')) or 2
        self.storage_nodes = int(config.get('vmware', 'storage_nodes')) or 3
        self.cns_automation_config_file_path = config.get(
            'vmware', 'cns_automation_config_file_path')
        self.ocp_hostname_prefix = config.get(
            'vmware', 'ocp_hostname_prefix') or 'ansible-on-vmware'
        self.lb_host = '%s-master-0' % self.ocp_hostname_prefix
        self.openshift_disable_check = config.get(
            'vmware', 'openshift_disable_check').strip() or (
                'docker_storage,docker_image_availability,disk_availability')
        self.disable_yum_update_and_reboot = config.get(
            'vmware', 'disable_yum_update_and_reboot').strip() or 'no'
        self.node_type = config.get('vmware', 'node_type')
        self.node_number = config.get('vmware', 'node_number')
        self.tag = config.get('vmware', 'tag')
        self.openshift_use_crio = (
            config.get('vmware', 'openshift_use_crio') or '').strip()
        err_count = 0

        if 'storage' in self.node_type:
            if self.node_number < 3:
                err_count += 1
                print ("Node number for CNS and CRS should be 3 or more.")
            if self.container_storage is None:
                err_count += 1
                print ("Please specify crs or cns in container_storage in "
                       "the %s." % vmware_ini_path)
            elif self.container_storage in ('cns', 'crs'):
                self.inventory_file = (
                    "%s-inventory.json" % self.container_storage)
        required_vars = {
            'cluster_id': self.cluster_id,
            'dns_zone': self.dns_zone,
            'vcenter_host': self.vcenter_host,
            'vcenter_password': self.vcenter_password,
            'vcenter_datacenter': self.vcenter_datacenter,
        }
        for k, v in required_vars.items():
            if v == '':
                err_count += 1
                print "Missing %s " % k
        if not (self.container_storage_disks
                and re.search(
                    r'^[0-9]*(,[0-9]*)*$', self.container_storage_disks)):
            err_count += 1
            print ("'container_storage_disks' has improper value - "
                   "'%s'. Only integers separated with comma are allowed." % (
                       self.container_storage_disks))
        if self.container_storage_block_hosting_volume_size:
            try:
                self.container_storage_block_hosting_volume_size = int(
                    self.container_storage_block_hosting_volume_size)
            except ValueError:
                err_count += 1
                print ("'container_storage_block_hosting_volume_size' can be "
                       "either empty or integer. Provided value is '%s'" % (
                           self.container_storage_block_hosting_volume_size))
        if (self.additional_disks_to_storage_nodes and not re.search(
                r'^[0-9]*(,[0-9]*)*$',
                self.additional_disks_to_storage_nodes)):
            err_count += 1
            print ("'additional_disks_to_storage_nodes' has improper "
                   "value - '%s'. Only integers separated with comma "
                   "are allowed." % self.additional_disks_to_storage_nodes)
        if self.container_storage_glusterfs_timeout:
            try:
                self.container_storage_glusterfs_timeout = int(
                    self.container_storage_glusterfs_timeout)
            except ValueError:
                err_count += 1
                print ("'container_storage_glusterfs_timeout' can be "
                       "either empty or integer. Provided value is '%s'" % (
                           self.container_storage_glusterfs_timeout))
        if (self.cns_automation_config_file_path
                and not os.path.exists(
                    os.path.abspath(self.cns_automation_config_file_path))):
            err_count += 1
            print ("Wrong value for 'cns_automation_config_file_path' "
                   "config option. It is expected to be either a relative "
                   "or an absolute file path.")
        else:
            self.cns_automation_config_file_path = os.path.abspath(
                self.cns_automation_config_file_path)
        if self.docker_image_tag and self.docker_registry_url:
            vers_from_reg = self.docker_registry_url.split(':')[-1].strip()
            if not vers_from_reg == self.docker_image_tag:
                err_count += 1
                print ("If 'docker_image_tag' and 'docker_registry_url' are "
                       "specified, then their image tags should match. "
                       "docker_image_tag='%s', docker_registry_url='%s'" % (
                           self.docker_image_tag, self.docker_registry_url))
        if not self._is_rpm_and_image_tag_compatible():
            err_count += 1
            print ("OCP RPM versions and docker image tag do not match. "
                   "Need either to change 'ose_puddle_repo' or "
                   "'docker_image_tag' config options.")
        for opt_name in ('cns_glusterfs_image', 'cns_glusterfs_block_image',
                         'cns_glusterfs_heketi_image'):
            if len(getattr(self, opt_name).split(':')) > 1:
                err_count += 1
                print ("'%s' option is expected to contain "
                       "only image name." % opt_name)
        allowed_disable_checks = (
            'disk_availability',
            'docker_image_availability',
            'docker_storage',
            'memory_availability',
            'package_availability',
            'package_version',
        )
        self.openshift_disable_check_data = [
            el.strip()
            for el in self.openshift_disable_check.strip().split(',')
            if el.strip()
        ]
        if not all([(s in allowed_disable_checks)
                    for s in self.openshift_disable_check_data]):
            err_count += 1
            print ("'openshift_disable_check' is allowed to have only "
                   "following values separated with comma: %s.\n "
                   "Got following value: %s" % (','.join(
                       allowed_disable_checks), self.openshift_disable_check))

        if err_count > 0:
            print "Please fill out the missing variables in %s " % (
                vmware_ini_path)
            exit(1)
        self.wildcard_zone = "%s.%s" % (self.app_dns_prefix, self.dns_zone)
        self.support_nodes = 0

        print 'Configured inventory values:'
        for each_section in config.sections():
            for (key, val) in config.items(each_section):
                if 'pass' in key:
                    print '\t %s:  ******' % key
                else:
                    print '\t %s:  %s' % (key, val)
        print '\n'

    def create_inventory_file(self):
        if not self.args.no_confirm:
            if not click.confirm(
                    'Continue creating the inventory file with these values?'):
                sys.exit(0)

        d = {'host_inventory': {}}
        for i in range(0, int(self.node_number)):
            # Determine node_number increment on the number of nodes
            if self.node_type == 'compute':
                guest_name = '%s-%s' % (self.node_type, i)
                guest_type = 'compute'
            elif (self.node_type == 'storage'
                    and self.container_storage == 'crs'):
                guest_name = '%s-%s' % (self.container_storage, i)
                guest_type = self.container_storage
            elif (self.node_type == 'storage'
                    and self.container_storage == 'cns'):
                guest_name = '%s-%s' % (self.container_storage, i)
                guest_type = self.container_storage
            else:
                raise Exception(
                    "Unexpected combination of 'node_type' (%s) and "
                    "'container_storage' (%s)." % (
                        self.node_type, self.container_storage))
            if self.ocp_hostname_prefix:
                guest_name = "%s-%s" % (self.ocp_hostname_prefix, guest_name)
            d['host_inventory'][guest_name] = {
                'guestname': guest_name,
                'guesttype': guest_type,
                'tag': str(self.cluster_id) + '-' + self.node_type,
            }

        with open(self.inventory_file, 'w') as outfile:
            json.dump(d, outfile, indent=4, sort_keys=True)
        print 'Inventory file created: %s' % self.inventory_file

    def launch_refarch_env(self):
        with open(self.inventory_file, 'r') as f:
            print yaml.safe_dump(json.load(f), default_flow_style=False)

        if not self.args.no_confirm:
            if not click.confirm('Continue adding nodes with these values?'):
                sys.exit(0)

        if (self.container_storage in ('cns', 'crs')
                and 'storage' in self.node_type):
            if 'None' in self.tag:
                # do the full install and config minus the cleanup
                self.tag = 'vms,node-setup'
            playbooks = ['playbooks/%s-storage.yaml' % self.container_storage]
        else:
            if 'None' in self.tag:
                # do the full install and config minus the cleanup
                self.tag = 'all'
            playbooks = ['playbooks/add-node.yaml']

        playbook_vars_dict = {
            'add_node': 'yes',
            'vcenter_host': self.vcenter_host,
            'vcenter_username': self.vcenter_username,
            'vcenter_password': self.vcenter_password,
            'vcenter_template_name': self.vcenter_template_name,
            'vcenter_folder': self.vcenter_folder,
            'vcenter_datastore': self.vcenter_datastore,
            'vcenter_cluster': self.vcenter_cluster,
            'vcenter_datacenter': self.vcenter_datacenter,
            'vcenter_resource_pool': self.vcenter_resource_pool,
            'dns_zone': self.dns_zone,
            'wildcard_zone': self.wildcard_zone,
            'app_dns_prefix': self.app_dns_prefix,
            'vm_network': self.vm_network,
            'cns_automation_config_file_path': (
                self.cns_automation_config_file_path),
            'console_port': self.console_port,
            'cluster_id': self.cluster_id,
            'container_storage': self.container_storage,
            'container_storage_disks': self.container_storage_disks,
            'container_storage_disk_type': self.container_storage_disk_type,
            'additional_disks_to_storage_nodes': (
                self.additional_disks_to_storage_nodes),
            'dp_tool_heketi_admin_key': self.heketi_admin_key,
            'dp_tool_heketi_user_key': self.heketi_user_key,
            'ose_puddle_repo': self.ose_puddle_repo,
            'gluster_puddle_repo': self.gluster_puddle_repo,
            'deployment_type': self.deployment_type,
            'openshift_deployment_type': self.deployment_type,
            'openshift_vers': self.openshift_vers,
            'admin_key': self.admin_key,
            'user_key': self.user_key,
            'rhel_subscription_user': self.rhel_subscription_user,
            'rhel_subscription_pass': self.rhel_subscription_pass,
            'rhsm_satellite': self.rhel_subscription_server,
            'rhsm_pool': self.rhel_subscription_pool,
            'openshift_sdn': self.openshift_sdn,
            'openshift_use_openshift_sdn': True,
            'lb_host': self.lb_host,
            'node_type': self.node_type,
            'ocp_hostname_prefix': self.ocp_hostname_prefix,
            'disable_yum_update_and_reboot': self.disable_yum_update_and_reboot
        }
        if self.openshift_disable_check_data:
            playbook_vars_dict["openshift_disable_check"] = (
                ','.join(self.openshift_disable_check_data))
        if self.container_storage_block_hosting_volume_size:
            playbook_vars_dict[
                'openshift_storage_glusterfs_block_host_vol_size'] = (
                    self.container_storage_block_hosting_volume_size)
        if self.container_storage_glusterfs_timeout:
            playbook_vars_dict['openshift_storage_glusterfs_timeout'] = (
                self.container_storage_glusterfs_timeout)
        if self.docker_registry_url:
            playbook_vars_dict['oreg_url'] = self.docker_registry_url
        if self.docker_additional_registries:
            playbook_vars_dict['openshift_docker_additional_registries'] = (
                self.docker_additional_registries)
            playbook_vars_dict['openshift_docker_ent_reg'] = ''
        if self.docker_insecure_registries:
            playbook_vars_dict['openshift_docker_insecure_registries'] = (
                self.docker_insecure_registries)
        if self.docker_image_tag:
            playbook_vars_dict['openshift_image_tag'] = self.docker_image_tag

        if self.openshift_vers == 'v3_11':
            if self.openshift_use_crio:
                playbook_vars_dict['openshift_use_crio'] = (
                    self.openshift_use_crio)
                playbook_vars_dict['openshift_use_crio_only'] = (
                    self.openshift_use_crio)
                playbook_vars_dict['openshift_crio_enable_docker_gc'] = (
                    self.openshift_use_crio)
            else:
                playbook_vars_dict['openshift_use_crio'] = 'false'
        if self.openshift_vers in ("v3_6", "v3_7", "v3_9"):
            for key in ('image', 'version',
                        'block_image', 'block_version',
                        'heketi_image', 'heketi_version'):
                value = getattr(self, 'cns_glusterfs_%s' % key)
                if not value:
                    continue
                playbook_vars_dict['openshift_storage_glusterfs_%s' % key] = (
                    value)
        if self.openshift_vers in ('v3_6', 'v3_7'):
            playbook_vars_dict['docker_version'] = '1.12.6'
        elif self.openshift_vers != "v3_9":
            if self.cns_glusterfs_version:
                playbook_vars_dict['openshift_storage_glusterfs_image'] = (
                    "%s:%s" % (
                        self.cns_glusterfs_image or 'rhgs3/rhgs-server-rhel7',
                        self.cns_glusterfs_version))
            elif self.cns_glusterfs_image:
                playbook_vars_dict['openshift_storage_glusterfs_image'] = (
                    "%s:latest" % self.cns_glusterfs_image)
            if self.cns_glusterfs_block_version:
                playbook_vars_dict[
                    'openshift_storage_glusterfs_block_image'] = (
                        "%s:%s" % (
                            self.cns_glusterfs_block_image
                            or 'rhgs3/rhgs-gluster-block-prov-rhel7',
                            self.cns_glusterfs_block_version))
            elif self.cns_glusterfs_block_image:
                playbook_vars_dict[
                    "openshift_storage_glusterfs_block_image"] = (
                        "%s:latest" % self.cns_glusterfs_block_image)
            if self.cns_glusterfs_heketi_version:
                playbook_vars_dict[
                    'openshift_storage_glusterfs_heketi_image'] = (
                        "%s:%s" % (
                            self.cns_glusterfs_heketi_image
                            or 'rhgs3/rhgs-volmanager-rhel7',
                            self.cns_glusterfs_heketi_version))
            elif self.cns_glusterfs_heketi_image:
                playbook_vars_dict[
                    "openshift_storage_glusterfs_heketi_image"] = (
                        "%s:latest" % self.cns_glusterfs_heketi_image)

        playbook_vars_str = ' '.join('%s=%s' % (k, v)
                                     for (k, v) in playbook_vars_dict.items())

        for playbook in playbooks:
            devnull = '' if self.verbose > 0 else '> /dev/null'

            # refresh the inventory cache to prevent stale hosts from
            # interferring with re-running
            command = 'inventory/vsphere/vms/vmware_inventory.py %s' % (
                devnull)
            os.system(command)

            # remove any cached facts to prevent stale data during a re-run
            command = 'rm -rf .ansible/cached_facts'
            os.system(command)

            command = (
                "ansible-playbook"
                " --extra-vars '@./%s'"
                " --tags %s"
                " -e '%s' %s" % (
                    self.inventory_file, self.tag, playbook_vars_str, playbook)
            )

            if self.verbose > 0:
                command += " -vvvvv"

            click.echo('We are running: %s' % command)
            status = os.system(command)
            if os.WIFEXITED(status) and os.WEXITSTATUS(status) != 0:
                sys.exit(os.WEXITSTATUS(status))

        command = (
            "ansible-playbook "
            "-i %smaster-0, playbooks/get_ocp_info.yaml") % (
                "%s-" % self.ocp_hostname_prefix
                if self.ocp_hostname_prefix else "")
        os.system(command)

        print "Successful run!"
        if click.confirm('Update INI?'):
            self.update_ini_file()
        if click.confirm('Delete inventory file?'):
            print "Removing the existing %s file" % self.inventory_file
            os.remove(self.inventory_file)
        sys.exit(0)


if __name__ == '__main__':
    VMWareAddNode()
