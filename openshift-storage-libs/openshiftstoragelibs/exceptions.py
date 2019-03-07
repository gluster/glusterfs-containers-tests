class ConfigError(Exception):
    '''
    Custom exception thrown when there is an unrecoverable configuration error.
    For example, a required configuration key is not found.
    '''


class ExecutionError(Exception):
    '''
    Custom exception thrown when a command executed by Glusto results in an
    unrecoverable error.

    For example, all hosts are not in peer state or a volume cannot be setup.
    '''


class NotSupportedException(Exception):
    '''
    Custom exception thrown when we do not support a particular feature in
    particular product version

    For example, pv resize is not supported in OCP version < 3.9
    '''
