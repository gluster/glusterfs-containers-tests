"""Test cases that create and delete multiple volumes.
"""

import contextlib
import time

import yaml

from glusto.core import Glusto as g

from cnslibs.common.heketi_libs import HeketiClientSetupBaseClass
from cnslibs.common.heketi_ops import (
    heketi_volume_list)
from cnslibs.common.naming import (
    make_unique_label, extract_method_name)
from cnslibs.common.openshift_ops import (
    oc_create, oc_delete, oc_get_pvc, oc_get_pv, oc_get_all_pvs)
from cnslibs.common.waiter import Waiter


def build_storage_class(name, resturl, restuser='foo', restuserkey='foo'):
    """Build s simple structure for a storage class.
    """
    return {
        'apiVersion': 'storage.k8s.io/v1beta1',
        'kind': 'StorageClass',
        'provisioner': 'kubernetes.io/glusterfs',
        'metadata': {
            'name': name,
        },
        'parameters': {
            'resturl': resturl,
            'restuser': restuser,
            'restuserkey': restuserkey,
        }
    }


def build_pvc(name, storageclass, size, accessmodes=None):
    """Build a simple structture for a PVC defintion.
    """
    annotations = {
        'volume.beta.kubernetes.io/storage-class': storageclass,
    }
    accessmodes = accessmodes if accessmodes else ['ReadWriteOnce']
    if not isinstance(size, str):
        size = '%dGi' % size
    return {
        'apiVersion': 'v1',
        'kind': 'PersistentVolumeClaim',
        'metadata': {
            'name': name,
            'annotations': annotations,
        },
        'spec': {
            'accessModes': accessmodes,
            'resources': {
                'requests': {'storage': size},
            }
        }
    }


@contextlib.contextmanager
def temp_config(ocp_node, cfg):
    """Context manager to help define YAML files on the remote node
    that can be in turn fed to 'oc create'. Must be used as a context
    manager (with-statement).

    Example:
        >>> d = {'foo': True, 'bar': 22, 'baz': [1, 5, 9]}
        >>> with temp_config(node, d) as fpath:
        ...     func_that_takes_a_path(fpath)

        Here, the data dictionary `d` is serialized to YAML and written
        to a temporary file at `fpath`. Then, `fpath` can be used by
        a function that takes a file path. When the context manager exits
        the temporary file is automatically cleaned up.

    Args:
        ocp_node (str): The node to create the temp file on.
        cfg (dict): A data structure to be converted to YAML and
            saved in a tempfile on the node.
    Returns:
        str: A path to a temporary file.
    """
    conn = g.rpyc_get_connection(ocp_node, user="root")
    tmp = conn.modules.tempfile.NamedTemporaryFile()
    try:
        tmp.write(yaml.safe_dump(cfg))
        tmp.flush()
        filename = tmp.name
        yield filename
    finally:
        tmp.close()


def wait_for_claim(ocp_node, pvc_name, timeout=60, interval=2):
    """Wait for a claim to be created & bound up to the given timeout.
    """
    for w in Waiter(timeout, interval):
        sts = oc_get_pvc(ocp_node, pvc_name)
        if sts and sts.get('status', {}).get('phase') == 'Bound':
            return sts
    raise AssertionError('wait_for_claim on pvc %s timed out'
                         % (pvc_name,))


def wait_for_sc_unused(ocp_node, sc_name, timeout=60, interval=1):
    for w in Waiter(timeout, interval):
        sts = oc_get_all_pvs(ocp_node)
        items = (sts and sts.get('items')) or []
        if not any(i.get('spec', {}).get('storageClassName') == sc_name
                   for i in items):
            return
    raise AssertionError('wait_for_sc_unused on %s timed out'
                         % (sc_name,))


def delete_storageclass(ocp_node, sc_name, timeout=60):
    wait_for_sc_unused(ocp_node, sc_name, timeout)
    oc_delete(ocp_node, 'storageclass', sc_name)


class ClaimInfo(object):
    """Helper class to organize data as we go from PVC to PV to
    volume w/in heketi.
    """
    pvc_name = None
    vol_name = None
    vol_uuid = None
    sc_name = None
    req = None
    info = None
    pv_info = None

    def __init__(self, name, storageclass, size):
        self.pvc_name = name
        self.req = build_pvc(
            name=self.pvc_name,
            storageclass=storageclass,
            size=size)

    def create_pvc(self, ocp_node):
        assert self.req
        with temp_config(ocp_node, self.req) as tmpfn:
            oc_create(ocp_node, tmpfn)

    def update_pvc_info(self, ocp_node, timeout=60):
        self.info = wait_for_claim(ocp_node, self.pvc_name, timeout)

    def delete_pvc(self, ocp_node):
        oc_delete(ocp_node, 'pvc', self.pvc_name)

    def update_pv_info(self, ocp_node):
        self.pv_info = oc_get_pv(ocp_node, self.volumeName)

    @property
    def volumeName(self):
        return self.info.get('spec', {}).get('volumeName')

    @property
    def heketiVolumeName(self):
        return self.pv_info.get('spec', {}).get('glusterfs', {}).get('path')


def _heketi_vols(ocp_node, url):
    # Unfortunately, getting json from heketi-cli only gets the ids
    # To get a mapping of ids & volume names without a lot of
    # back and forth between the test and the ocp_node we end up having
    # to scrape the output of 'volume list'
    # TODO: This probably should be made into a utility function
    out = heketi_volume_list(ocp_node, url, json=False)
    res = []
    for line in out.splitlines():
        if not line.startswith('Id:'):
            continue
        row = {}
        for section in line.split():
            if ':' in section:
                key, value = section.split(':', 1)
                row[key.lower()] = value.strip()
        res.append(row)
    return res


def _heketi_name_id_map(vols):
    return {vol['name']: vol['id'] for vol in vols}


class TestVolumeMultiReq(HeketiClientSetupBaseClass):
    def setUp(self):
        super(TestVolumeMultiReq, self).setUp()
        self.volcount = self._count_vols()

    def wait_to_settle(self, timeout=120, interval=1):
        # This was originally going to be a tearDown, but oddly enough
        # tearDown is called *before* the cleanup functions, so it
        # could never succeed. This needs to be added as a cleanup
        # function first so that we run after our test's other cleanup
        # functions but before we go on to the next test in order
        # to prevent the async cleanups in kubernetes from steping
        # on the next test's "toes".
        for w in Waiter(timeout):
            nvols = self._count_vols()
            if nvols == self.volcount:
                return
        raise AssertionError(
            'wait for volume count to settle timed out')

    def _count_vols(self):
        ocp_node = g.config['ocp_servers']['master'].keys()[0]
        return len(_heketi_vols(ocp_node, self.heketi_server_url))

    def test_simple_serial_vol_create(self):
        """Test that serially creating PVCs causes heketi to add volumes.
        """
        self.addCleanup(self.wait_to_settle)
        # TODO A nice thing to add to this test would be to also verify
        # the gluster volumes also exist.
        tname = make_unique_label(extract_method_name(self.id()))
        ocp_node = g.config['ocp_servers']['master'].keys()[0]
        # deploy a temporary storage class
        sc = build_storage_class(
            name=tname,
            resturl=self.heketi_server_url)
        with temp_config(ocp_node, sc) as tmpfn:
            oc_create(ocp_node, tmpfn)
        self.addCleanup(delete_storageclass, ocp_node, tname)
        orig_vols = _heketi_name_id_map(
            _heketi_vols(ocp_node, self.heketi_server_url))

        # deploy a persistent volume claim
        c1 = ClaimInfo(
            name='-'.join((tname, 'pvc1')),
            storageclass=tname,
            size=2)
        c1.create_pvc(ocp_node)
        self.addCleanup(c1.delete_pvc, ocp_node)
        c1.update_pvc_info(ocp_node)
        # verify volume exists
        self.assertTrue(c1.volumeName)
        c1.update_pv_info(ocp_node)
        self.assertTrue(c1.heketiVolumeName)

        # verify this is a new volume to heketi
        now_vols = _heketi_name_id_map(
            _heketi_vols(ocp_node, self.heketi_server_url))
        self.assertEqual(len(orig_vols) + 1, len(now_vols))
        self.assertIn(c1.heketiVolumeName, now_vols)
        self.assertNotIn(c1.heketiVolumeName, orig_vols)

        # deploy a 2nd pvc
        c2 = ClaimInfo(
            name='-'.join((tname, 'pvc2')),
            storageclass=tname,
            size=2)
        c2.create_pvc(ocp_node)
        self.addCleanup(c2.delete_pvc, ocp_node)
        c2.update_pvc_info(ocp_node)
        # verify volume exists
        self.assertTrue(c2.volumeName)
        c2.update_pv_info(ocp_node)
        self.assertTrue(c2.heketiVolumeName)

        # verify this is a new volume to heketi
        now_vols = _heketi_name_id_map(
            _heketi_vols(ocp_node, self.heketi_server_url))
        self.assertEqual(len(orig_vols) + 2, len(now_vols))
        self.assertIn(c2.heketiVolumeName, now_vols)
        self.assertNotIn(c2.heketiVolumeName, orig_vols)

    def test_multiple_vol_create(self):
        """Test creating two volumes via PVCs with no waiting between
        the PVC requests.

        We do wait after all the PVCs are submitted to get statuses.
        """
        self.addCleanup(self.wait_to_settle)
        tname = make_unique_label(extract_method_name(self.id()))
        ocp_node = g.config['ocp_servers']['master'].keys()[0]
        # deploy a temporary storage class
        sc = build_storage_class(
            name=tname,
            resturl=self.heketi_server_url)
        with temp_config(ocp_node, sc) as tmpfn:
            oc_create(ocp_node, tmpfn)
        self.addCleanup(delete_storageclass, ocp_node, tname)

        # deploy two persistent volume claims
        c1 = ClaimInfo(
            name='-'.join((tname, 'pvc1')),
            storageclass=tname,
            size=2)
        c1.create_pvc(ocp_node)
        self.addCleanup(c1.delete_pvc, ocp_node)
        c2 = ClaimInfo(
            name='-'.join((tname, 'pvc2')),
            storageclass=tname,
            size=2)
        c2.create_pvc(ocp_node)
        self.addCleanup(c2.delete_pvc, ocp_node)

        # wait for pvcs/volumes to complete
        c1.update_pvc_info(ocp_node)
        c2.update_pvc_info(ocp_node)
        now_vols = _heketi_name_id_map(
            _heketi_vols(ocp_node, self.heketi_server_url))

        # verify first volume exists
        self.assertTrue(c1.volumeName)
        c1.update_pv_info(ocp_node)
        self.assertTrue(c1.heketiVolumeName)
        # verify this volume in heketi
        self.assertIn(c1.heketiVolumeName, now_vols)

        # verify second volume exists
        self.assertTrue(c2.volumeName)
        c2.update_pv_info(ocp_node)
        self.assertTrue(c2.heketiVolumeName)
        # verify this volume in heketi
        self.assertIn(c2.heketiVolumeName, now_vols)
