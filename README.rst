#######################
Openshift Storage Tests
#######################

This repo contains `test cases` and `functions` for `storage` on OpenShift. The
functions can be installed as python package and used in any other python
project.

***************
Getting started
***************

Prerequisites
*************

Create setup with below system requirements and install required packages on
host from which this repo is used as library or tests are executed.

Minimum System Requirements
===================

* 1 Master node
* 3 Storage nodes
* Passwordless ssh from the host (machine from where the tests would be run)
  to all the nodes in the cluster
* Each node in the trusted storage pool should have 2 mandatory and 1 optional
  devices

    #. Device 1 (part of the topology)
    #. Device 2 (part of the topology)
    #. Device 3 (Optional additional device not part of the topology)

Recommended System Requirements
====================

* 1 Master node
* 1 Compute node (for node addition and removal tests)
* 3 Infra node (recommended by openshift)
* 3 Storage nodes
* 3 Registry storage nodes (for logging, metrics and monitoring tests)
* Passwordless ssh from the host (machine from where the tests would be run)
  to all the nodes in the cluster
* Each node in the trusted storage pool should have 2 mandatory and 1 optional
  devices

    #. Device 1 (part of the topology)
    #. Device 2 (part of the topology)
    #. Device 3 (Optional additional device not part of the topology)

Installing `tox` on host
========================

`tox` aims to automate and standardize testing in Python. It is generic
`virtualenv` management and test command line tool you can use for

* Checking your package installs correctly with different Python versions and
  interpreters
* Running your tests in each of the environments, configuring your test tool of
  choice
* Acting as frontend to Continuous integration servers, greatly reducing
  boilerplates and merging CI and shell-based testing

Refer `tox doc <https://tox.readthedocs.io/en/latest/#>`__ for more
information.

Below are the instructions to install `tox` on host

* Install below system packages

    .. code-block::

        $ yum install python-pip git gcc python-devel

* Install `tox` package

    .. code-block::

        $ pip install git+git://github.com/tox-dev/tox.git@2.9.1#egg=tox

Executing the test cases
************************

* Create a config file which lists out the OCP configurations like master and
  storage node details, heketi related configurations etc. Sample config file
  can be found under `tests` directory

    .. code-block::

        $ tests/glusterfs-containers-tests-config.yaml

* To run test cases in a virtual environment using py2:

    .. code-block::

        $ tox -e functional -- glusto -c <config_file> \
            '--pytest=-v -rsx <test_file_path_or_dir>'

    For example:
        * Execute single test case from test class file

        .. code-block::

            $ tox -e functional -- glusto -c \
                tests/glusterfs-containers-tests-config.yaml \
                '--pytest=-v -rsx \
                tests/functional/arbiter/test_arbiter.py \
                -k test_arbiter_pvc_create'

        * Execute all test cases from test class file

        .. code-block::

            $ tox -e functional -- glusto -c \
                tests/glusterfs-containers-tests-config.yaml \
                '--pytest=-v -rsx
                tests/functional/arbiter/test_arbiter.py'

        * Execute all test cases from test directory

        .. code-block::

            $ tox -e functional -- glusto -c \
                tests/glusterfs-containers-tests-config.yaml \
                '--pytest=-v -rsx tests'

* To run test cases in a virtual environment using py3:

    .. code-block::

        $ python3 -m tox -e functional3 -- glusto -c <config_file> \
            '--pytest=-v -rsx <test_file_path_or_dir>'

  Note, that "tox" and other python packages should be installed
  using pip3 - separate package installer than the one used for py2 (pip).

Writing tests in `glusterfs-containers-tests`
*********************************************

`tests` directory in `glusterfs-containers-tests` contains test cases. One
might want to create a directory with feature name as the name of test
directory under tests to add new test cases.

Similar to `glusto-tests <https://github.com/gluster/glusto-tests>`__, test
cases in `glusterfs-containers-tests` can be written using standard `PyUnit`,
`PyTest` or `Nose` methodologies as supported by `glusto` framework.

One can follow the `PyUnit <http://glusto.readthedocs.io/en/latest/userguide/
unittest.html>`__ docs to write `PyUnit` tests, or `PyTest <http://glusto.
readthedocs.io/en/latest/userguide/pytest.html>`__ docs to write `PyTest`
tests, or `Nose <http://glusto.readthedocs.io/en/latest/userguide/
nosetests.html>`__ docs to write `Nose` tests.

For more information on how to write test cases, refer `developing-guide
<https://github.com/gluster/glusto-tests/blob/master/docs/userguide/developer
-guide.rst>`__.

Validating `PEP 8` rules after adding new code
**********************************************

Refer `PEP 8 -- Style Guide for Python Code <https://www.python.org/dev/peps/
pep-0008/>`__ for more information on `PEP 8` rules.

* Run `PEP 8` checks for all files

    .. code-block::

        $ tox -e pep8

* Run `PEP 8` check for single file

    .. code-block::

        $ tox -e pep8 <absolute_or_relative_file_path>

       For Example:

        .. code-block::

             $ tox -e pep8 tests/functional/test_heketi_restart.py

Logging
*******

Log `file name` and log `level` can be passed as argument to `glusto` command
while running the `glusto-tests`.

For example:

    .. code-block::

        $ tox -e functional -- glusto -c 'config.yml' \
            -l /tmp/glustotests-ocp.log --log-level DEBUG \
            '--pytest=-v -x tests -m ocp'

One can configure log files, log levels in the test cases as well. For details
on how to use `glusto` framework for configuring logs in tests, refer `docs
<http://glusto.readthedocs.io/en/latest/userguide/loggable.html>`__

Default log location is `/tmp/glustomain.log`

.. Note::

    When using `glusto` via the `Python Interactive Interpreter`, the default
    log location is `/tmp/glustomain.log`
