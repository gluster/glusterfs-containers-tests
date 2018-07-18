from glusto.core import Glusto as g


def cmd_run(cmd, hostname, raise_on_error=True):
    """Glusto's command runner wrapper.

    Args:
        cmd (str): Shell command to run on the specified hostname.
        hostname (str): hostname where Glusto should run specified command.
        raise_on_error (bool): defines whether we should raise exception
                               in case command execution failed.
    Returns:
        str: Stripped shell command's stdout value.
    """
    ret, out, err = g.run(hostname, cmd, "root")
    if raise_on_error:
        msg = ("Failed to execute command '%s' on '%s' node. Got non-zero "
               "return code '%s'. Err: %s" % (cmd, hostname, ret, err))
        assert int(ret) == 0, msg
    return out.strip()
