from glusto.core import Glusto
import six

from openshiftstoragelibs import exceptions


def monkeypatch_class(name, bases, namespace):
    assert len(bases) == 1, "Only 1 parent class is supported."
    base = bases[0]
    for name, value in namespace.items():
        if not name.startswith("__"):
            setattr(base, name, value)
    return base


@six.add_metaclass(monkeypatch_class)
class MonkeyPatchedGlusto(Glusto):
    @classmethod
    def _wrapper_for_get_ssh_connection(cls, host, user=None, recreate=False):
        if recreate and "%s@%s" % (user, host) in cls._ssh_connections:
            cls.ssh_close_connection(host=host, user=user)
        ssh = cls._get_ssh_connection(host, user)
        if not ssh:
            if "%s@%s" % (user, host) in cls._ssh_connections:
                cls.ssh_close_connection(host=host, user=user)
            ssh = cls._get_ssh_connection(host=host, user=user)
            if not ssh:
                raise exceptions.ExecutionError(
                    "Failed to establish SSH connection to the '%s' host "
                    "using '%s' user. Plese check availability of the "
                    "hostname and make sure it has passwordless "
                    "connection from your host." % (host, user))
        return ssh

    @classmethod
    def run(cls, host, command, user=None, log_level=None):
        """Wrapper for original "run" method fixing broken connections."""
        if not user:
            user = cls.user

        ctlpersist = ''
        if cls.use_controlpersist:
            ctlpersist = " (cp)"

        # output command
        cls.log.info("%s@%s%s: %s" % (user, host, ctlpersist, command))

        # run the command
        ssh = cls._wrapper_for_get_ssh_connection(host, user, False)
        try:
            proc = ssh.popen(command, universal_newlines=True)
        except Exception as e:
            err_msg = (
                "Failed to establish SSH connection: %s" % six.text_type(e))
            cls.log.error(err_msg)
            ssh = cls._wrapper_for_get_ssh_connection(host, user, True)
            proc = ssh.popen(command, universal_newlines=True)

        stdout, stderr = proc.communicate()
        retcode = proc.returncode

        # output command results
        identifier = "%s@%s" % (user, host)
        cls._log_results(identifier, retcode, stdout, stderr,
                         log_level=log_level)

        return (retcode, stdout, stderr)
