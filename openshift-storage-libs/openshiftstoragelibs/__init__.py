from glusto.core import Glusto
from six import add_metaclass


def monkeypatch_class(name, bases, namespace):
    assert len(bases) == 1, "Only 1 parent class is supported."
    base = bases[0]
    for name, value in namespace.items():
        if not name.startswith("__"):
            setattr(base, name, value)
    return base


@add_metaclass(monkeypatch_class)
class MonkeyPatchedGlusto(Glusto):
    @classmethod
    def _get_ssh_connection(cls, host, user=None):
        ssh = super(MonkeyPatchedGlusto, cls)._get_ssh_connection(
            host=host, user=user)
        if not ssh:
            super(MonkeyPatchedGlusto, cls).ssh_close_connection(
                host=host, user=user)
        ssh = super(MonkeyPatchedGlusto, cls)._get_ssh_connection(
            host=host, user=user)
        return ssh
