=====================================
Deployment of OpenShift 3.x On VMWare
=====================================

-----------
What is it?
-----------

It is end-to-end deployment tool for deployment of OpenShift 3.x and
OpenShift Container Storage (OCS) on top of the VMWare cloud platform.
It is set of `Ansible <https://github.com/ansible/ansible>`__ playbooks,
which wraps the
`openshift-ansible <https://github.com/openshift/openshift-ansible>`__ library.

This wrapper adds additional things, which are needed and not provided by
mentioned library. Such as following:

- Node provisioning.

- Node preparation.

- Gather info about newly provisioned nodes and installed OpenShift (including
  storage part). Providing, as an output, config file for automated test cases.

- Run post-config actions which do required, for automated testing, stuff.

--------------------------
What can it do? It can ...
--------------------------

- ... deploy OpenShift 3.6, 3.7, 3.9, 3.10 and 3.11 on top of
  the VMWare cloud platform.

- ... deploy containerized and standalone GlusterFS clusters.
  The GlusterFS versions are configurable and depend on the used repositories.

- ... use downstream repositories for package installation.

- ... use any docker registry for getting container images.

-------------------
VMWare requirements
-------------------

- DHCP configured for all the new VMs.

- New VMs get deployed from VMWare 'template'. So, should be created proper
  VMWare template. It can be bare RHEL7. Or somehow updated RHEL7.

- One OpenShift cluster is expected to be, at least, 5 VMs large. So,
  there should be enough resources for it.

-----
Usage
-----

1) Create VMWare template VM using RHEL7
----------------------------------------

- Add SSH public key(s) for password-less connection required by Ansible

.. code-block:: console

    $ ssh-copy-id username@ip_address_of_the_vm_which_will_be_used_as_template

- Make sure that default user SSH key pair is the same on the “Ansible” machine

2) Install dependencies
-----------------------

Install following dependencies on the machine where you are going to run
deployment.

- Install “pip”, “git” and “libselinux-python” if not installed yet:

.. code-block:: console

    $ yum install python-pip git libselinux-python

- Install “tox” if not installed yet:

.. code-block:: console

    $ pip install git+git://github.com/tox-dev/tox.git@2.9.1#egg=tox

Considering the fact that it is 'end-to'end' deployment tool,
deployment always will run on the separate machine compared to the machines
of deployed cluster.

3) Configure tool before starting deployment
--------------------------------------------

Open “ocp-on-vmware.ini” file with any text editor and provide correct values
for all the config options. All of the options have inline descriptions in
the same file.

5) Deploy OpenShift:
--------------------

OpenShift can be deployed using following command:

.. code-block:: console

    $ tox -e ocp3.X -- python ocp-on-vmware.py --no_confirm --verbose

Replace 'X' in the 'ocp3.X' part of the command to
the proper minor version of the OpenShift. Allowed are following values:
'ocp3.6', 'ocp3.7', 'ocp3.9', 'ocp3.10' and 'ocp3.11'. The same is true for
below commands too.


6) Install OpenShift Container Storage
--------------------------------------

Use following command for brownfield installation of
OpenShift Container Storage on top of the already
deployed (in previous step) OpenShift cluster:

.. code-block:: console

    $ tox -e ocp3.X -- python add-node.py \
      --verbose --node_type=storage --node_number=3 --no_confirm

Note that if “--node_number=Y” is not provided, then 3 nodes will be installed
by default. Type of storage (CNS or CRS) is defined in
“ocp-on-vmware.ini” file. Where "CNS" is containerized GlusterFS and
"CRS" is standalone GlusterFS installations.


7) Clean up deployed cluster
----------------------------

If deplyoed cluster is not needed anymore, it can be cleaned up using following
command:

.. code-block:: console

    $ tox -e ocp3.x -- python  ocp-on-vmware.py --clean


------------------------
History of the code base
------------------------

Originally, code base was forked from
`openshift-ansible-contrib <https://github.com/openshift/openshift-ansible-contrib>`__
project.
It supported only OpenShift 3.6 with restricted set of features at that moment.
Project was exactly 'forked', and not 'used directly', just because that
'restricted set of features' didn't satisfy
our (OpenShift Storage Quality Assurance team) needs and environments.
Our needs were usage of VMWare cloud platform with configured DHCP for new VMs.

So, for ability to have end-to-end deployment tool, we forked it and started
actively work on it. Not having time for long review process of the source
project PRs (Pull Requests).
Then this 'fork' envolved a lot. It started supporting bunch of OpenShift
versions in the single code base. In addition to the addon of
other new features.
And, finally, this code came to the repo with 'automated test cases' which are
used with this deployment tool in CI.
