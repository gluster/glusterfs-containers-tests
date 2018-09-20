
import os
import tempfile
import yaml

from shutil import copytree
from glusto.core import Glusto as g
from glustolibs.misc.misc_libs import upload_scripts

from openshift_ops import (
    oc_process,
    oc_create,
    oc_rsh,
    oc_rsync,
    oc_get_yaml,
    create_namespace,
    switch_oc_project
)

from dynamic_provisioning import (
    wait_for_pod_be_ready,
    get_pod_name_from_dc
)


CNS_TEMPLATE_PATH = os.path.join(
    os.getcwdu(),
    "cns-libs",
    "templates"
)
ROOT = os.sep

LOAD_TYPE_JENKINS = "jenkins"
JENKINS_TEMPLATE = "jenkins-persistent-template.yaml"
JJB_TEMPLATE = "jjb-template.yaml"
JENKINS_LOAD_SCRIPT = "start-load-on-jenkins.j2"
JENKINS_GROOVY_SCRIPTS = [
    "install_mvn339.groovy",
    "install_jdk8.groovy",
    "jenkins_setup_jdk_user.groovy.j2"
]


def deploy_pods(oc_master_node, load_type, instances, pod_parameters,
                load_parameters, namespace=None):
    """Deploy and configure pods of different types
    Args:
        load_type (str): load type jenkins, mongodb or postgres
        instances (int): number of pods to be deployed
        pod_parameters (str): pod parameters used to deploy pods
        load_parameters (str): load parameters to be used to start
                               load on pods
        namespace (str): namespace used to deploy jenkins and jjb pods
    Raises: AssertionError if invalid load type specified
    """
    err_msg = "invalid load typed %s"

    if namespace:
        create_and_switch_to_namespace(oc_master_node, namespace)

    templates_path = upload_templates(oc_master_node, load_type)

    pod_index = 0
    for index in range(int(instances)):
        if load_type == LOAD_TYPE_JENKINS:
            pod_index = get_available_pod_index(
                oc_master_node,
                pod_index,
                LOAD_TYPE_JENKINS
            )
            deploy_and_configure_jenkins(
                oc_master_node,
                pod_index,
                templates_path,
                pod_parameters,
                load_parameters
            )
        else:
            g.log.error(err_msg % load_type)
            raise AssertionError(err_msg % load_type)


def upload_templates(oc_master_node, load_type):
    """Upload templates in openshift master node in temporary
       directory.
    Args:
        load_type (str): load type of which template files need to be uploaded
    Return:
        directory path: Return temporary directory path where templates are
                        copied.
    Raises: AssertionError if failed to upload files on server
    """
    err_msg = "failed to %s templates from %s to %s"

    temp_dir = tempfile.mkdtemp(prefix="cns-tmp-")
    src = os.path.join(CNS_TEMPLATE_PATH, load_type)
    dst = os.path.join(temp_dir, load_type)
    try:
        copytree(src, dst)
    except Exception:
        g.log.error(err_msg % ("copy", src, dst))
        raise

    dest = os.path.join(ROOT, "tmp")
    if not upload_scripts(oc_master_node, temp_dir, dest):
        g.log.error(err_msg % ("upload", temp_dir, dest))
        raise AssertionError(err_msg % ("upload", temp_dir, dest))

    return os.path.join(temp_dir, load_type)


def create_and_switch_to_namespace(oc_master_node, namespace):
    """Create new namespace if not already exists and switch
       to new created namespace
    Args:
        namespace (str): namespace name to be created
    Raises: AssertionError if failed to create or switch project to namespace
    """
    err_msg = "failed to %s namespace %s"

    try:
        oc_get_yaml(oc_master_node, "namespace", namespace)
        g.log.info("%s namespace already present using existing one")
    except AssertionError:
        g.log.info("%s namespace not present creating new one")
        if not create_namespace(oc_master_node, namespace):
            g.log.error(err_msg % ("create", namespace))
            raise AssertionError(err_msg % ("create", namespace))

    g.log.info("switch to namespace %s'" % namespace)
    if not switch_oc_project(oc_master_node, namespace):
        g.log.error(err_msg % ("switch", namespace))
        raise AssertionError(err_msg % ("switch", namespace))


def create_pod(oc_master_node, templates_path, template, params=None):
    """Creates pod with template and prameters provides
    Args:
        template (str): template by using which pod to be created
        params (str): parameters used to create template
    """
    info_msg = "creating %s using template %s"

    template_path = os.path.join(templates_path, "templates", template)
    if params:
        g.log.info(info_msg % ("process", template_path))
        template = oc_process(oc_master_node, params, template_path)

        g.log.info(info_msg % ("app", template))
        oc_create(oc_master_node, template, "value")
    else:
        g.log.info(info_msg % ("app", template_path))
        oc_create(oc_master_node, template_path)


def get_available_pod_index(oc_master_node, start_index, prefix):
    """Checks availability of the name with prefix and start_index
    Args:
        prefix (str): prefix for the dc name used
    Returns:
        start_index (int): maximum no combined with prefix
    """
    dc_list = oc_get_yaml(oc_master_node, "dc")
    dc_names = [dc_name["metadata"]["name"] for dc_name in dc_list["items"]]

    while "%s-%s" % (prefix, start_index) not in dc_names:
        start_index += 1

    g.log.info("next available index is %s" % start_index)
    return start_index


def deploy_and_configure_jenkins(oc_master_node, index, templates_path,
                                 pod_parameters, load_parameters):
    """Deploys and configures jenkins and jjb pod
    Args:
        index (int): index no by which pod name need to be appened
        pod_parameters (str): user specified parameters used to deploy pod
    """
    jk_service_name = "%s-%s" % (LOAD_TYPE_JENKINS, index)
    jnlp_service_name = "jnlp-%s" % index
    jjb_service_name = "jjb-%s" % index

    deploy_jenkins_pod(
        oc_master_node,
        templates_path,
        jk_service_name,
        jnlp_service_name,
        pod_parameters
    )
    route_details = oc_get_yaml(
        oc_master_node,
        "route",
        jk_service_name
    )
    route = route_details['spec']['host']
    jk_url = "https://%s" % route

    jjb_pod_name = deploy_jjb_pod(
        oc_master_node,
        templates_path,
        jk_url,
        jjb_service_name
    )

    configure_jjb(
        oc_master_node,
        load_parameters,
        templates_path,
        jjb_pod_name
    )

    start_jenkins_load(oc_master_node, templates_path, jjb_pod_name, jk_url)


def deploy_jenkins_pod(oc_master_node, templates_path, jk_service_name,
                       jnlp_service_name, pod_parameters):
    """Deploys jenkins pod with specified parameters

    Args:
        jk_service_name (str): Jenkins service name to be used for deploying
                               pod
        jnlp_service_name (str): JNLP service name to be used for deploying
                                 pod
        pod_parameters (str): user specified parameters used to deploy pod
    Returns:
        jk_pod_name (str): deployed and ready state Jenkins pod name
    """
    params = " ".join(["-p {0}={1}".format(key, val)
                      for key, val in pod_parameters.items()])
    params += (" -p JENKINS_SERVICE_NAME=%s -p JNLP_SERVICE_NAME=%s"
               % (jk_service_name, jnlp_service_name))

    create_pod(oc_master_node, templates_path, JENKINS_TEMPLATE, params)
    jk_pod_name = get_pod_name_from_dc(oc_master_node, jk_service_name)
    g.log.info("Jenkins pod %s created successfully" % jk_pod_name)

    g.log.info("waiting pod %s to be in 'Running' state" % jk_pod_name)
    wait_for_pod_be_ready(oc_master_node, jk_pod_name)

    return jk_pod_name


def deploy_jjb_pod(oc_master_node, templates_path, jk_url, jjb_service_name):
    """Deploys jjb pods with configuring jenkins url jk_url

    Args:
        jk_url (str): jenkins url to executes groovy scripts and configure
                      jenkins jobs through jjb
        jjb_service_name (str) : jjb service used for pod name
    Returns:
        jjb_pod_name (str): Deployed and ready state JJB Pod name
    """
    params = ("-p JENKINS_URL=%s JJB_SERVICE_NAME=%s"
              % (jk_url, jjb_service_name))
    g.log.info("using jenkins url '%s' to deploy jjb pod" % jk_url)
    create_pod(oc_master_node, templates_path, JJB_TEMPLATE, params)

    jjb_pod_name = get_pod_name_from_dc(oc_master_node, jjb_service_name)
    g.log.info("deployed jjb pod %s successfully" % jjb_pod_name)

    g.log.info("waiting pod to be in 'Running' state" % jjb_pod_name)
    wait_for_pod_be_ready(oc_master_node, jjb_pod_name)

    return jjb_pod_name


def create_project_template(template_path, load_parameters):
    """Generates project.yaml file for jjb configuration
    Args:
        template_path: Path of template files where project.yml file to be
                       generated
        load_parameters: Contains the no of jobs to be generated
    """
    project_dict = [{"project": {"name": "svt-jobs", "jobs": []}}]

    for index in range(int(load_parameters["JOBS"])):
        job = {
            "{name}_job": {
                "name": "test-%s" % index,
                "get_url": "https://github.com/hongkailiu/gs-spring-boot.git"
            }
        }
        project_dict[0]["project"]["jobs"].append(job)
        project_yaml_path = os.path.join(template_path, "jjb", "project.yaml")
        with open(project_yaml_path, "w") as f:
            f.write(yaml.dump(project_dict, default_flow_style=False))


def configure_jjb(oc_master_node, load_parameters, templates_path, pod_name):
    """Copies jjb jobs and load scripts to jjb pod's /data
        location
    Args:
        pod_name (str): jjb pod name on which script to be executed
    """
    dest = os.path.join(ROOT, "data")

    create_project_template(templates_path, load_parameters)

    source = os.path.join(templates_path, "jjb", "")
    g.log.info("copying script files from %s to pod %s" % (source, pod_name))
    oc_rsync(oc_master_node, pod_name, source, dest)

    source = os.path.join(templates_path, "files", "")
    g.log.info("copying jenkins load scripts from %s to jenkins pod %s"
               % (source, pod_name))
    oc_rsync(oc_master_node, pod_name, source, dest)


def start_jenkins_load(oc_master_node, templates_path, pod_name, jk_url):
    """Configures jenkins settings and start load

    Args:
        pod_name (str): jjb pod name on which script to be executed
        jk_url (str): jenkins url to executes groovy scripts and configure
                      jenkins jobs through jjb
    Raises: AssertionError if groovy scripts failed to configure jenkins
    """
    err_msg = "failed to execute groovy script %s"

    for g_script in JENKINS_GROOVY_SCRIPTS:
        g.log.info("execute groovy script %s on jenkins" % g_script)

        g_script_path = os.path.join(templates_path, "groovy", g_script)
        cmd = " ".join([
            "curl",
            "-k",
            "--user", "admin:password",
            "--data-urlencode",
            "\"script=$(cat {0})\"".format(g_script_path),
            "-X", "POST",
            "{0}/scriptText".format(jk_url)
        ])
        ret, out, err = g.run(oc_master_node, cmd)
        if ret != 0:
            g.log.error(err_msg % g_script)
            raise AssertionError(err_msg % g_script)

    g.log.info("start jenkins load script on jjb pod")
    dest = os.path.join(ROOT, "data")
    cmd = ["bash", os.path.join(dest, JENKINS_LOAD_SCRIPT), "&"]
    oc_rsh(oc_master_node, pod_name, cmd)
