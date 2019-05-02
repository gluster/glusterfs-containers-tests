import time

from glusto.core import Glusto as g

from openshiftstoragelibs import exceptions
from openshiftstoragelibs import waiter


def node_reboot_by_command(node, timeout=600, wait_step=10):
    """Reboot node and wait to start for given timeout.

    Args:
        node (str)     : Node which needs to be rebooted.
        timeout (int)  : Seconds to wait before node to be started.
        wait_step (int): Interval in seconds to wait before checking
                         status of node again.
    """
    cmd = "sleep 3; /sbin/shutdown -r now 'Reboot triggered by Glusto'"
    ret, out, err = g.run(node, cmd)
    if ret != 255:
        err_msg = "failed to reboot host '%s' error %s" % (node, err)
        g.log.error(err_msg)
        raise AssertionError(err_msg)

    try:
        g.ssh_close_connection(node)
    except Exception as e:
        g.log.error("failed to close connection with host %s "
                    "with error: %s" % (node, e))
        raise

    # added sleep as node will restart after 3 sec
    time.sleep(3)

    for w in waiter.Waiter(timeout=timeout, interval=wait_step):
        try:
            if g.rpyc_get_connection(node, user="root"):
                g.rpyc_close_connection(node, user="root")
                return
        except Exception as err:
            g.log.info("exception while getting connection: '%s'" % err)

    if w.expired:
        error_msg = ("exceeded timeout %s sec, node '%s' is "
                     "not reachable" % (timeout, node))
        g.log.error(error_msg)
        raise exceptions.ExecutionError(error_msg)
