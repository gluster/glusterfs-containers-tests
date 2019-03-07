"""Convenience wrappers for running commands within a pod

The goal of this module is to support running glusto commands in pods
without a lot of boilerplate and hassle. The basic idea is that we
have our own run() function that can be addressed to a pod using the
Pod object (namedtuple). This run function will work like a normal
g.run() when not using the Pod object.

Example:
    >>> run("my-host.local", ["parted", "/dev/sda", "p"])
    0, "<...stdout...>", "<...stderr...>"

    >>> run(Pod("my-host.local", "my-pod-426ln"),
    ...     ["pvs"])
    0, "<...stdout...>", "<...stderr...>"

In addition, if there's a need to to use some higher level functions
that directly call into glusto run we can monkey-patch the glusto object
using the GlustoPod context manager. GlustoPod can also be used as a
decorator.

Imagine a function that direcly calls g.run:
    >>> def get_magic_number(host, ticket):
    ...     s, out, _ = g.run(host, ['magicall', '--ticket', ticket])
    ...     if s != 0:
    ...         return None
    ...     return out.strip()

If you want to have this operate within a pod you can use the GlustoPod
manager to enable the pod-aware run method and pass it a Pod object
as the first argument. Example:
    >>> def check_magic_number(ticket):
    ...     with GlustoPod():
    ...         m = get_magic_number(Pod('myhost', 'mypod'), ticket)
    ...     return m > 42

Similarly it can be used as a context manager:
    >>> @GlustoPod()
    ... def funky(x):
    ...     m = get_magic_number(Pod('myhost', 'mypod'), ticket)
    ...     return m > 42

Because the custom run fuction only runs commands in pods when passed
a Pod object it is fairly safe to enable the monkey-patch over the
lifetime of a function that addresses both hosts and pods.
"""

from collections import namedtuple
from functools import partial, wraps
import types

from glusto.core import Glusto as g

from openshiftstoragelibs import openshift_ops

# Define a namedtuple that allows us to address pods instead of just
# hosts,
Pod = namedtuple('Pod', 'node podname')


def run(target, command, log_level=None, orig_run=g.run):
    """Function that runs a command on a host or in a pod via a host.
    Wraps glusto's run function.

    Args:
        target (str|Pod): If target is str object and
            it equals to 'auto_get_gluster_endpoint', then
            Gluster endpoint gets autocalculated to be any of
            Gluster PODs or nodes depending on the deployment type of
            a Gluster cluster.
            If it is str object with other value, then it is considered to be
            an endpoint for command.
            If 'target' is of the 'Pod' type,
            then command will run on the specified POD.
        command (str|list): Command to run.
        log_level (str|None): log level to be passed on to glusto's
            run method
        orig_run (function): The default implementation of the
            run method. Will be used when target is not a pod.

    Returns:
        A tuple of the command's return code, stdout, and stderr.
    """
    # NOTE: orig_run captures the glusto run method at function
    # definition time in order to capture the method before
    # any additional monkeypatching by other code

    if target == 'auto_get_gluster_endpoint':
        ocp_client_node = list(g.config['ocp_servers']['client'].keys())[0]
        gluster_pods = openshift_ops.get_ocp_gluster_pod_names(ocp_client_node)
        if gluster_pods:
            target = Pod(ocp_client_node, gluster_pods[0])
        else:
            target = list(g.config.get("gluster_servers", {}).keys())[0]

    if isinstance(target, Pod):
        prefix = ['oc', 'rsh', target.podname]
        if isinstance(command, types.StringTypes):
            cmd = ' '.join(prefix + [command])
        else:
            cmd = prefix + command

        # unpack the tuple to make sure our return value exactly matches
        # our docstring
        return g.run(target.node, cmd, log_level=log_level)
    else:
        return orig_run(target, command, log_level=log_level)


class GlustoPod(object):
    """A context manager / decorator that monkeypatches the
    glusto object to support running commands in pods.
    """

    def __init__(self, glusto_obj=None):
        self.runfunc = None
        self._g = glusto_obj or g

    def __enter__(self):
        """Patch glusto to use the wrapped run method.
        """
        self.runfunc = self._g.run
        # we "capture" the prior glusto run method here in order to
        # stack on top of any previous monkeypatches if they exist
        self._g.run = partial(run, orig_run=self.runfunc)

    def __exit__(self, etype, value, tb):
        """Restore the orginal run method.
        """
        self._g.run = self.runfunc
        self.runfunc = None

    def __call__(self, func):
        """Allow GlustoPod to be used as a decorator.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self:
                result = func(*args, **kwargs)
            return result
        return wrapper
