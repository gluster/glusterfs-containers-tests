#!/usr/bin/env python
# set ts=4 sw=4 et
import argparse
import click
import fileinput
import json
import os
import random
import requests
import six
from six.moves import configparser
import sys
import yaml


class OCPOnVMWare(object):

    __name__ = 'OCPOnVMWare'
    console_port=8443
    cluster_id=None
    deployment_type=None
    openshift_vers=None
    vcenter_host=None
    vcenter_username=None
    vcenter_password=None
    vcenter_template_name=None
    vcenter_folder=None
    vcenter_cluster=None
    vcenter_datacenter=None
    vcenter_datastore=None
    vcenter_resource_pool=None
    dns_zone=None
    app_dns_prefix=None
    vm_network=None
    rhel_subscription_user=None
    rhel_subscription_pass=None
    rhel_subscription_server=None
    rhel_subscription_pool=None
    no_confirm=False
    tag=None
    verbose=0
    create_inventory=None
    compute_nodes=None
    ocp_hostname_prefix=None
    create_ocp_vars=None
    openshift_sdn=None
    container_storage=None
    openshift_disable_check=None
    wildcard_zone=None
    inventory_file='infrastructure.json'
    vmware_ini_path=None
    clean=None
    cns_automation_config_file_path=None,
    docker_registry_url=None
    docker_additional_registries=None
    docker_insecure_registries=None
    docker_image_tag=None
    ose_puddle_repo=None
    gluster_puddle_repo=None
    web_console_install=None
    disable_yum_update_and_reboot=None

    def __init__(self):
        self._parse_cli_args()
        self._read_ini_settings()
        self._create_inventory_file()
        self._create_ocp_vars()
        self._launch_refarch_env()

    def _parse_cli_args(self):
        """Command line argument processing"""
        tag_help = '''Skip to various parts of install valid tags include:
        - setup                     create the vCenter folder and resource pool
        - prod                      create and setup the OCP cluster
        - ocp-install               install OCP on the prod VMs
        - ocp-config                configure OCP on the prod VMs
        - clean                     unsubscribe and remove all VMs'''
        parser = argparse.ArgumentParser(
            description='Deploy VMs to vSphere and install/configure OCP',
            formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument(
            '--clean', action='store_true',
            help='Delete all nodes and unregister from RHN')
        parser.add_argument(
            '--create_inventory', action='store_true',
            help=('Deprecated and not used option. '
                  'Everything that is needed gets autocreated.'))
        parser.add_argument(
            '--create_ocp_vars', action='store_true',
            help='Deprecated and not used option.')
        parser.add_argument(
            '--no_confirm', action='store_true',
            help='Skip confirmation prompt')
        parser.add_argument('--tag', default=None, help=tag_help)
        parser.add_argument(
            '--verbose', default=None, action='store_true',
            help='Verbosely display commands')
        self.args = parser.parse_args()
        self.verbose = self.args.verbose
        self.tag = self.args.tag
        self.no_confirm = self.args.no_confirm
        self.clean = self.args.clean

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
            v = self.docker_image_tag.split('v')[-1].strip()
            return (('atomic-openshift-%s' % v) in resp.text)
        raise Exception(
            "Failed to pull list of packages from '%s' url." % url)

    def _read_ini_settings(self):
        """Read ini file settings."""

        scriptbasename = "ocp-on-vmware"
        defaults = {'vmware': {
            'ini_path': os.path.join(
                os.path.dirname(__file__), '%s.ini' % scriptbasename),
            'console_port': '8443',
            'container_storage': 'none',
            'deployment_type': 'openshift-enterprise',
            'openshift_vers': 'v3_11',
            'vcenter_username': 'administrator@vsphere.local',
            'vcenter_template_name': 'not-defined',
            'vcenter_folder': 'ocp',
            'vcenter_resource_pool': '/Resources/OCP3',
            'app_dns_prefix': 'apps',
            'vm_network':'VM Network',
            'cns_automation_config_file_path': '',
            'docker_registry_url': '',
            'docker_additional_registries': '',
            'docker_insecure_registries': '',
            'docker_image_tag': '',
            'web_console_install': '',
            'ose_puddle_repo': '',
            'gluster_puddle_repo': '',
            'rhel_subscription_pool': 'Employee SKU',
            'openshift_sdn': 'redhat/openshift-ovs-subnet',
            'compute_nodes': '2',
            'ocp_hostname_prefix': 'openshift-on-vmware',
            'tag': self.tag,
            'openshift_disable_check': (
                'docker_storage,docker_image_availability,disk_availability'),
            'disable_yum_update_and_reboot': 'no',
        }}
        if six.PY3:
            config = configparser.ConfigParser()
        else:
            config = configparser.SafeConfigParser()

        # where is the config?
        self.vmware_ini_path = os.environ.get(
            'VMWARE_INI_PATH', defaults['vmware']['ini_path'])
        self.vmware_ini_path = os.path.expanduser(
            os.path.expandvars(self.vmware_ini_path))
        config.read(self.vmware_ini_path)

        # apply defaults
        for k,v in defaults['vmware'].iteritems():
            if not config.has_option('vmware', k):
                config.set('vmware', k, str(v))

        self.console_port = config.get('vmware', 'console_port')
        self.cluster_id = config.get('vmware', 'cluster_id')
        self.container_storage = config.get('vmware', 'container_storage')
        self.deployment_type = config.get('vmware','deployment_type')
        if os.environ.get('VIRTUAL_ENV'):
            self.openshift_vers = (
                'v3_%s' % os.environ['VIRTUAL_ENV'].split('_')[-1].split(
                    '.')[-1])
        else:
            self.openshift_vers = config.get('vmware','openshift_vers')
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
        self.dns_zone= config.get('vmware', 'dns_zone')
        self.app_dns_prefix = config.get('vmware', 'app_dns_prefix')
        self.vm_network = config.get('vmware', 'vm_network')
        self.ocp_hostname_prefix = config.get(
            'vmware', 'ocp_hostname_prefix') or 'ansible-on-vmware'
        self.lb_host = '%s-master-0' % self.ocp_hostname_prefix
        self.cns_automation_config_file_path = config.get(
            'vmware', 'cns_automation_config_file_path')
        self.docker_registry_url = (
            config.get('vmware', 'docker_registry_url') or '').strip()
        self.docker_additional_registries = config.get(
            'vmware', 'docker_additional_registries')
        self.docker_insecure_registries = config.get(
            'vmware', 'docker_insecure_registries')
        self.docker_image_tag = (
            config.get('vmware', 'docker_image_tag') or '').strip()
        self.web_console_install = (
            config.get('vmware', 'web_console_install') or '').strip()
        self.ose_puddle_repo = config.get('vmware', 'ose_puddle_repo')
        self.gluster_puddle_repo = config.get('vmware', 'gluster_puddle_repo')
        self.rhel_subscription_user = config.get(
            'vmware', 'rhel_subscription_user')
        self.rhel_subscription_pass = config.get(
            'vmware', 'rhel_subscription_pass')
        self.rhel_subscription_server = config.get(
            'vmware', 'rhel_subscription_server')
        self.rhel_subscription_pool = config.get(
            'vmware', 'rhel_subscription_pool')
        self.openshift_sdn = config.get('vmware', 'openshift_sdn')
        self.compute_nodes = config.get('vmware', 'compute_nodes')
        self.storage_nodes = config.get('vmware', 'storage_nodes')
        self.openshift_disable_check = config.get(
            'vmware', 'openshift_disable_check').strip() or (
                'docker_storage,docker_image_availability,disk_availability')
        self.disable_yum_update_and_reboot = config.get(
            'vmware', 'disable_yum_update_and_reboot').strip() or 'no'
        err_count=0

        required_vars = {
            'vcenter_datacenter': self.vcenter_datacenter,
            'vcenter_host': self.vcenter_host,
            'vcenter_password': self.vcenter_password,
            'vcenter_template_name': self.vcenter_template_name,
            'dns_zone': self.dns_zone,
        }

        for k, v in required_vars.items():
            if v == '':
                err_count += 1
                print "Missing %s " % k
        if (self.cns_automation_config_file_path and
                not os.path.exists(
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
                self.vmware_ini_path)
            exit (1)
        self.wildcard_zone="%s.%s" % (self.app_dns_prefix, self.dns_zone)

        if not self.cluster_id:
            # Create a unique cluster_id first
            self.cluster_id = ''.join(
                random.choice('0123456789abcdefghijklmnopqrstuvwxyz')
                for i in range(7))
            config.set('vmware', 'cluster_id', self.cluster_id)
            for line in fileinput.input(self.vmware_ini_path, inplace=True):
                if line.startswith('cluster_id'):
                    print "cluster_id=" + str(self.cluster_id)
                else:
                    print line,

        print 'Configured inventory values:'
        for each_section in config.sections():
            for (key, val) in config.items(each_section):
                if 'pass' in key:
                    print '\t %s:  ******' % ( key )
                else:
                    print '\t %s:  %s' % ( key,  val )
        print '\n'

    def _create_inventory_file(self):
        click.echo('Configured inventory values:')
        click.echo('\tcompute_nodes: %s' % self.compute_nodes)
        click.echo('\tdns_zone: %s' % self.dns_zone)
        click.echo('\tapp_dns_prefix: %s' % self.app_dns_prefix)
        click.echo('\tocp_hostname_prefix: %s' % self.ocp_hostname_prefix)
        click.echo('\tUsing values from: %s' % self.vmware_ini_path)
        click.echo("")
        if not self.no_confirm:
            click.confirm('Continue using these values?', abort=True)

        master_name = "%s-master-0" % self.ocp_hostname_prefix
        d = {'host_inventory': {master_name: {
            'guestname': master_name,
            'guesttype': 'master',
            'tag': str(self.cluster_id) + '-master',
        }}}
        for i in range(0, int(self.compute_nodes)):
            compute_name = "%s-compute-%d" % (self.ocp_hostname_prefix, i)
            d['host_inventory'][compute_name] = {
                'guestname': compute_name,
                'guesttype': 'compute',
                'tag': '%s-compute' % self.cluster_id,
            }

        with open(self.inventory_file, 'w') as outfile:
            json.dump(d, outfile, indent=4, sort_keys=True)

        if self.args.create_inventory:
            exit(0)

    def _create_ocp_vars(self):
        if self.args.create_ocp_vars:
            click.echo(
                "No-op run. '--create_ocp_vars' option is not used anymore. "
                "Ending execution.")
            exit(0)

    def _launch_refarch_env(self):
        with open(self.inventory_file, 'r') as f:
            print yaml.safe_dump(json.load(f), default_flow_style=False)

        if not self.args.no_confirm:
            if not click.confirm('Continue adding nodes with these values?'):
                sys.exit(0)

        # Add section here to modify inventory file based on input
        # from user check your vmmark scripts for parsing the file and
        # adding the values.
        for line in fileinput.input(
                "inventory/vsphere/vms/vmware_inventory.ini", inplace=True):
            if line.startswith("server="):
                print "server=" + self.vcenter_host
            elif line.startswith("password="):
                print "password=" + self.vcenter_password
            elif line.startswith("username="):
                print "username=" + self.vcenter_username
            else:
                print line,

        if self.clean is True:
            tags = 'clean'
        elif self.tag:
            tags = self.tag
        else:
            tags = [
                'setup',
                'prod',
                'ocp-install',
                'ocp-configure',
            ]
            tags = ",".join(tags)

        # remove any cached facts to prevent stale data during a re-run
        command='rm -rf .ansible/cached_facts'
        os.system(command)

        playbook_vars_dict = {
            'vcenter_host': self.vcenter_host,
            'vcenter_username': self.vcenter_username,
            'vcenter_password': self.vcenter_password,
            'vcenter_template_name': self.vcenter_template_name,
            'vcenter_folder': self.vcenter_folder,
            'vcenter_cluster': self.vcenter_cluster,
            'vcenter_datacenter': self.vcenter_datacenter,
            'vcenter_datastore': self.vcenter_datastore,
            'vcenter_resource_pool': self.vcenter_resource_pool,
            'dns_zone': self.dns_zone,
            'app_dns_prefix': self.app_dns_prefix,
            'vm_network': self.vm_network,
            'lb_host': self.lb_host,
            'cns_automation_config_file_path': (
                self.cns_automation_config_file_path),
            'ose_puddle_repo': self.ose_puddle_repo,
            'gluster_puddle_repo': self.gluster_puddle_repo,
            'wildcard_zone': self.wildcard_zone,
            'console_port': self.console_port,
            'cluster_id': self.cluster_id,
            'deployment_type': self.deployment_type,
            'openshift_vers': self.openshift_vers,
            'rhsm_user': self.rhel_subscription_user,
            'rhsm_password': self.rhel_subscription_pass,
            'rhsm_satellite': self.rhel_subscription_server,
            'rhsm_pool': self.rhel_subscription_pool,
            'openshift_sdn': self.openshift_sdn,
            'openshift_use_openshift_sdn': True,
            'container_storage': self.container_storage,
            'ocp_hostname_prefix': self.ocp_hostname_prefix,
            'disable_yum_update_and_reboot': self.disable_yum_update_and_reboot
        }
        if self.openshift_disable_check_data:
            playbook_vars_dict["openshift_disable_check"] = (
                ','.join(self.openshift_disable_check_data))
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
        if self.web_console_install:
            playbook_vars_dict['openshift_web_console_install'] = (
                self.web_console_install)
        if self.openshift_vers in ('v3_6', 'v3_7'):
            playbook_vars_dict['docker_version'] = '1.12.6'

        playbook_vars_str = ' '.join('%s=%s' % (k, v)
                                     for (k, v) in playbook_vars_dict.items())

        command = (
            "ansible-playbook"
            " --extra-vars '@./infrastructure.json'"
            " --tags %s"
            " -e '%s' playbooks/ocp-end-to-end.yaml"
        ) % (tags, playbook_vars_str)

        if self.verbose > 0:
            command += " -vvvvvv"

        click.echo('We are running: %s' % command)
        status = os.system(command)
        if os.WIFEXITED(status) and os.WEXITSTATUS(status) != 0:
            sys.exit(os.WEXITSTATUS(status))


if __name__ == '__main__':
    OCPOnVMWare()
