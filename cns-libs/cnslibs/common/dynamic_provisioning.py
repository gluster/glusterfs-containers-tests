from collections import OrderedDict
import json
import os
import tempfile

from glusto.core import Glusto as g
from glustolibs.misc.misc_libs import upload_scripts
import rtyaml

from cnslibs.common import exceptions
from cnslibs.common.waiter import Waiter


TEMPLATE_DIR = os.path.abspath(os.path.dirname(__file__))


def create_pvc_file(hostname, claim_name, storage_class, size):
    '''
     This function creates pvc file
     Args:
         hostname (str): hostname on which we need to
                         create pvc file
         claim_name (str): name of the claim
                          ex: storage-claim1
         storage_class(str): name of the storage class
         size (int): size of the claim in GB
                          ex: 10 (for 10GB claim)
     Returns:
         bool: True if successful,
               otherwise False
    '''
    with open(os.path.join(TEMPLATE_DIR,
                           "sample-glusterfs-pvc-claim.json")) as data_file:
        data = json.load(data_file, object_pairs_hook=OrderedDict)
    data['metadata']['annotations'][
        'volume.beta.kubernetes.io/storage-class'] = storage_class
    data['metadata']['name'] = claim_name
    data['spec']['resources']['requests']['storage'] = "%dGi" % size
    try:
        conn = g.rpyc_get_connection(hostname, user="root")
        if conn is None:
            g.log.error("Failed to get rpyc connection of node %s"
                        % hostname)
            return False

        with conn.builtin.open('/%s.json' % claim_name, 'w') as data_file:
            json.dump(data, data_file, sort_keys=False,
                      indent=4, ensure_ascii=False)
    except Exception as err:
        g.log.error("failed to create pvc file %s" % err)
        return False
    finally:
        g.rpyc_close_connection(hostname, user="root")
    g.log.info("creation of pvc file %s successful" % claim_name)
    return True


def create_app_pod_file(hostname, claim_name, app_name, sample_app_name):
    '''
     This function creates app_pod_name file
     Args:
         hostname (str): hostname on which we need to
                         create app pod file
         claim_name (str): name of the claim
                           ex: storage-claim1
         app_name (str): name of the app-pod to create
                         ex: nginx1
         sample_app_name (str): sample-app-pod-name
                                ex: nginx
     Returns:
         bool: True if successful,
               otherwise False
    '''
    data = rtyaml.load(open(
        os.path.join(TEMPLATE_DIR, "sample-%s-pod.yaml" % sample_app_name)))
    data['spec']['volumes'][0]['persistentVolumeClaim'][
        'claimName'] = claim_name
    data['metadata']['name'] = app_name
    data['spec']['containers'][0]['name'] = app_name
    try:
        conn = g.rpyc_get_connection(hostname, user="root")
        if conn is None:
            g.log.error("Failed to get rpyc connection of node %s"
                        % hostname)
            return False
        rtyaml.dump(data, conn.builtin.open('/%s.yaml' % app_name, "w"))
    except Exception as err:
        g.log.error("failed to create app file %s" % err)
        return False
    finally:
        g.rpyc_close_connection(hostname, user="root")
    g.log.info("creation of %s app file successful" % app_name)
    return True


def create_secret_file(hostname, secret_name, namespace,
                       data_key, secret_type):
    '''
     This function creates secret yaml file
     Args:
         hostname (str): hostname on which we need to create
                         secret yaml file
         sc_name (str): secret name ex: heketi-secret
         namespace (str): namespace ex: storage-project
         data_key (str): data-key ex: cGFzc3dvcmQ=
         secret_type (str): type ex: kubernetes.io/glusterfs
                                 or gluster.org/glusterblock
     Returns:
         bool: True if successful,
               otherwise False
    '''
    data = rtyaml.load(open(
        os.path.join(TEMPLATE_DIR, "sample-glusterfs-secret.yaml")))

    data['metadata']['name'] = secret_name
    data['data']['key'] = data_key
    data['metadata']['namespace'] = namespace
    data['type'] = secret_type
    try:
        conn = g.rpyc_get_connection(hostname, user="root")
        if conn is None:
            g.log.error("Failed to get rpyc connection of node %s"
                        % hostname)
            return False
        rtyaml.dump(data, conn.builtin.open('/%s.yaml' % secret_name, "w"))
    except Exception as err:
        g.log.error("failed to create %s.yaml file %s" % (secret_name, err))
        return False
    finally:
        g.rpyc_close_connection(hostname, user="root")
    g.log.info("creation of %s.yaml file successful" % secret_name)
    return True


def create_storage_class_file(hostname, sc_name, resturl,
                              provisioner, **kwargs):
    '''
     This function creates storageclass yaml file
     Args:
         hostname (str): hostname on which we need to create
                         stoargeclass yaml file
         sc_name (str): stoargeclass name ex: fast
         resturl (str): resturl
          ex: http://heketi-storage-project.cloudapps.mystorage.com
         provisioner (str): provisioner
                            ex:  kubernetes.io/glusterfs
                                or gluster.org/glusterblock
         auth (bool): Authorization
                      ex: True/False
     Kwargs:
         **kwargs
            The keys, values in kwargs are:
               restuser:str   ex: username: test-admin
               hacount:int ex: hacount:3
               clusterids:str
                ex: clusterids: "630372ccdc720a92c681fb928f27b53f"
               chapauthenabled:bool ex: chapauthenabled:True/False
               restauthenabled:bool ex: restauthenabled:True/False
               secretnamespace:str ex: secretnamespace:"storage-project"
               secretname:str ex: secretname:"heketi-secret"
               restsecretnamespace:str
                ex: restsecretnamespace:"storage-project"
               restsecretname:str ex: restsecretname:"heketi-secret"
               volumenameprefix:str ex: "dept_qe"
     Returns:
         bool: True if successful,
               otherwise False
    '''
    data = rtyaml.load(open(
        os.path.join(TEMPLATE_DIR, "sample-glusterfs-storageclass.yaml")))

    data['metadata']['name'] = sc_name
    data['parameters']['resturl'] = resturl
    data['provisioner'] = provisioner

    for key in ('secretnamespace', 'restuser', 'secretname',
                'restauthenabled', 'restsecretnamespace',
                'restsecretname', 'hacount', 'clusterids',
                'chapauthenabled', 'volumenameprefix'):
        if kwargs.get(key):
            data['parameters'][key] = kwargs.get(key)

    try:
        conn = g.rpyc_get_connection(hostname, user="root")
        if conn is None:
            g.log.error("Failed to get rpyc connection of node %s"
                        % hostname)
            return False
        provisioner_name = provisioner.split("/")
        file_path = ("/%s-%s-storage-class"
                     ".yaml" % (
                         sc_name, provisioner_name[1]))
        rtyaml.dump(data, conn.builtin.open(file_path, "w"))
    except Exception as err:
        g.log.error("failed to create storage-class file %s" % err)
        return False
    finally:
        g.rpyc_close_connection(hostname, user="root")
    g.log.info("creation of %s-storage-class file successful" % sc_name)
    return True


def verify_pod_status_running(hostname, pod_name,
                              timeout=1200, wait_step=60):
    '''
     MAkes sure pod is running
     Args:
         hostname (str): hostname on which we want to check the pod status
         pod_name (str): pod_name for which we need the status
         timeout (int): timeout value, if pod status is ContainerCreating,
                        checks the status after wait_step value till timeout
                        default value is 1200 sec
         wait_step( int): wait step,
                          default value is 60 sec
     Returns:
         bool: True if pod status is Running,
               otherwise False

    '''
    status_flag = False
    for w in Waiter(timeout, wait_step):
        cmd = ("oc get pods | grep '%s'| grep -v deploy | "
               "awk '{print $3}'") % pod_name
        ret, out, err = g.run(hostname, cmd, "root")
        if ret != 0:
            g.log.error("failed to execute cmd %s" % cmd)
            break
        output = out.strip().split("\n")[0].strip()
        if output == "":
            g.log.info("pod %s not found sleeping for %s "
                       "sec" % (pod_name, wait_step))
            continue
        elif output == "ContainerCreating":
            g.log.info("pod %s creating sleeping for %s "
                       "sec" % (pod_name, wait_step))
            continue
        elif output == "Running":
            status_flag = True
            g.log.info("pod %s is up and running" % pod_name)
            break
        elif output == "Error":
            g.log.error("pod %s status error" % pod_name)
            break
        elif output == "Terminating":
            g.log.info("pod %s is terminating state sleeping "
                       "for %s sec" % (pod_name, wait_step))
            continue
        elif output == "Pending":
            g.log.info("pod %s is pending state sleeping "
                       "for %s sec" % (pod_name, wait_step))
            continue
        else:
            g.log.error("pod %s has different status - %s "
                        "sleeping for %s sec" % (
                            pod_name, output, wait_step))
            continue
    if w.expired:
        g.log.error("exceeded timeout %s for verifying running "
                    "status of pod %s" % (timeout, pod_name))
        return False
    return status_flag


def create_mongodb_pod(hostname, pvc_name, pvc_size, sc_name):
    '''
     This function creates mongodb pod
     Args:
         hostname (str): hostname on which we want to create
                         mongodb pod
         pvc_name (str): name of the pvc
                         ex: pvc-claim1
         sc_name (str): name of the storage class
                        ex: fast
     Returns: True if successfull,
              False otherwise
    '''
    template_path = os.path.join(TEMPLATE_DIR, "mongodb-template.json")
    with open(template_path, 'r') as template_f:
        data = json.load(template_f, object_pairs_hook=OrderedDict)
    data['objects'][1]['metadata']['annotations'][
        'volume.beta.kubernetes.io/storage-class'] = sc_name

    tmp_fd, tmp_path = tempfile.mkstemp(
        prefix='cns-automation-mongodb-pvcname-%s-' % pvc_name, suffix='.json')
    dst_dir = '/tmp'
    dst_path = os.path.join(dst_dir, os.path.basename(tmp_path))
    try:
        with os.fdopen(tmp_fd, 'w') as tmp_f:
            json.dump(
                data, tmp_f, sort_keys=False, indent=4, ensure_ascii=False)
        if not upload_scripts(hostname, tmp_path, dst_dir, "root"):
            g.log.error("Failed to upload mongodp template to %s" % hostname)
            return False
    finally:
        os.remove(tmp_path)

    try:
        conn = g.rpyc_get_connection(hostname, user="root")
        if conn is None:
            g.log.error("Failed to get rpyc connection of node %s"
                        % hostname)
            return False
        cmd = ("oc new-app %s --param=DATABASE_SERVICE_NAME=%s "
               "--param=VOLUME_CAPACITY=%sGi") % (
                   dst_path, pvc_name, pvc_size)
        ret, out, err = g.run(hostname, cmd, "root")
        if ret != 0:
            g.log.error("failed to execute cmd %s on %s" % (cmd, hostname))
            return False
    except Exception as err:
        g.log.error("failed to create mongodb pod %s" % err)
        return False
    finally:
        g.rpyc_close_connection(hostname, user="root")
    g.log.info("creation of mongodb pod successfull")
    return True


def get_pvc_status(hostname, pvc_name):
    '''
     This function verifies the if pod is running
     Args:
         hostname (str): hostname on which we want
                         to check the pvc status
         pvc_name (str): pod_name for which we
                         need the status
     Returns:
         bool, status (str): True, status of pvc
               otherwise False, error message.
    '''
    cmd = "oc get pvc | grep %s | awk '{print $2}'" % pvc_name
    ret, out, err = g.run(hostname, cmd, "root")
    if ret != 0:
        g.log.error("failed to execute cmd %s" % cmd)
        return False, err
    output = out.strip().split("\n")[0].strip()
    return True, output


def verify_pvc_status_is_bound(hostname, pvc_name, timeout=120, wait_step=3):
    """Verify that PVC gets 'Bound' status in required time.

    Args:
        hostname (str): hostname on which we will execute oc commands
        pvc_name (str): name of PVC to check status of
        timeout (int): total time in seconds we are ok to wait
                       for 'Bound' status of a PVC
        wait_step (int): time in seconds we will sleep before checking a PVC
                         status again.
    Returns: None
    Raises: exceptions.ExecutionError in case of errors.
    """
    pvc_not_found_counter = 0
    for w in Waiter(timeout, wait_step):
        ret, output = get_pvc_status(hostname, pvc_name)
        if ret is not True:
            msg = ("Failed to execute 'get' command for '%s' PVC. "
                   "Got following responce: %s" % (pvc_name, output))
            g.log.error(msg)
            raise exceptions.ExecutionError(msg)
        if output == "":
            g.log.info("PVC '%s' not found, sleeping for %s "
                       "sec." % (pvc_name, wait_step))
            if pvc_not_found_counter > 0:
                msg = ("PVC '%s' has not been found 2 times already. "
                       "Make sure you provided correct PVC name." % pvc_name)
            else:
                pvc_not_found_counter += 1
                continue
        elif output == "Pending":
            g.log.info("PVC '%s' is in Pending state, sleeping for %s "
                       "sec" % (pvc_name, wait_step))
            continue
        elif output == "Bound":
            g.log.info("PVC '%s' is in Bound state." % pvc_name)
            return pvc_name
        elif output == "Error":
            msg = "PVC '%s' is in 'Error' state." % pvc_name
            g.log.error(msg)
        else:
            msg = "PVC %s has different status - %s" % (pvc_name, output)
            g.log.error(msg)
        if msg:
            raise exceptions.ExecutionError(msg)
    if w.expired:
        msg = ("Exceeded timeout of '%s' seconds for verifying PVC '%s' "
               "to reach the 'Bound' status." % (timeout, pvc_name))
        g.log.error(msg)
        raise exceptions.ExecutionError(msg)
