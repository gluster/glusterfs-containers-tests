from glusto.core import Glusto as g


DOCKER_FILE_PATH = "/etc/sysconfig/docker"


def _docker_update_registry(hostname, registry, registry_type):
    '''
      This function updates docker registry
      Args:
         hostname (str): hostname on which want to setup
                         the docker
         registry (str): add regsitry url that needs to be added
                         in docker file.
         ex: "ADD_REGISTRY='--add-registry registry.access.stage.redhat.com'"
         registry_type (str): type of registry
                              ex: add or insecure
    '''
    try:
        conn = g.rpyc_get_connection(hostname, user="root")
        if conn is None:
            g.log.error("Failed to get rpyc connection of node %s"
                        % hostname)
            return False

        if not conn.modules.os.path.exists(DOCKER_FILE_PATH):
            g.log.error("Unable to locate %s in node %s"
                        % (DOCKER_FILE_PATH, hostname))
            return False

        registry_flag = False
        lookup_str = "%s_REGISTRY=" % registry_type.upper()
        for line in conn.modules.fileinput.input(
                DOCKER_FILE_PATH, inplace=True):
            if lookup_str in line:
                registry_flag = True
                if registry not in line:
                    line = line if "#" in line else "#" + line
                    conn.modules.sys.stdout.write(line)
                    conn.modules.sys.stdout.write(registry)
                    continue
            conn.modules.sys.stdout.write(line)

        if not registry_flag:
            with conn.builtin.open(DOCKER_FILE_PATH, 'a+') as docker_file:
                docker_file.write(registry + '\n')

    except Exception as err:
        g.log.error("failed to edit docker file with %s-registry "
                    "%s on %s" % (registry_type, err, hostname))
        return False
    finally:
        g.rpyc_close_connection(hostname, user="root")

    g.log.info("Sucessfully edited docker file with %s-registry "
               "on %s" % (registry_type, hostname))
    return True


def docker_add_registry(hostname, registry_url):
    '''
     This function edits /etc/sysconfig/docker file with ADD_REGISTRY
     Args:
         hostname (str): hostname on which want to setup
                         the docker
         registry_url (str): add regsitry url that needs to be added
                             in docker file.
         ex: "ADD_REGISTRY='--add-registry registry.access.stage.redhat.com'"
     Returns:
         bool: True if successful,
               otherwise False
    '''
    return _docker_update_registry(hostname, registry_url, 'add')


def docker_insecure_registry(hostname, registry_url):
    '''
     This function edits /etc/sysconfig/docker file with INSECURE_REGISTRY
     Args:
         hostname (str): hostname on which want to setup
                         the docker
         registry_url (str): insecure registry url that needs to be added
                             in docker file.
         ex: "INSECURE_REGISTRY=
               '--insecure-registry registry.access.stage.redhat.com'"
     Returns:
         bool: True if successful,
               otherwise False

    '''
    return _docker_update_registry(hostname, registry_url, 'insecure')
