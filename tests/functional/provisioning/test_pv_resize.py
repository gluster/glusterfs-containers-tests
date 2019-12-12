from unittest import skip

import ddt
from glusto.core import Glusto as g

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.openshift_storage_libs import enable_pvc_resize
from openshiftstoragelibs.exceptions import ExecutionError
from openshiftstoragelibs import heketi_ops
from openshiftstoragelibs.openshift_ops import (
    get_gluster_vol_info_by_pvc_name,
    get_pod_name_from_dc,
    get_pv_name_from_pvc,
    oc_create_app_dc_with_io,
    oc_delete,
    oc_get_custom_resource,
    oc_rsh,
    resize_pvc,
    scale_dc_pod_amount_and_wait,
    verify_pv_size,
    verify_pvc_size,
    wait_for_events,
    wait_for_pod_be_ready,
    wait_for_resource_absence)
from openshiftstoragelibs.openshift_storage_version import (
    get_openshift_storage_version,
)
from openshiftstoragelibs.openshift_version import get_openshift_version
from openshiftstoragelibs import waiter


@ddt.ddt
class TestPvResizeClass(BaseClass):
    """Test cases for PV resize"""

    @classmethod
    def setUpClass(cls):
        super(TestPvResizeClass, cls).setUpClass()
        cls.node = cls.ocp_master_node[0]
        if get_openshift_version() < "3.9":
            cls.skip_me = True
            return
        enable_pvc_resize(cls.node)

    def setUp(self):
        super(TestPvResizeClass, self).setUp()
        if getattr(self, "skip_me", False):
            msg = ("pv resize is not available in openshift older than v3.9")
            g.log.error(msg)
            raise self.skipTest(msg)

    @ddt.data(
        (True, True),
        (False, True),
        (False, False),
    )
    @ddt.unpack
    def test_pv_resize_with_prefix_for_name_and_size(
            self, create_vol_name_prefix=False, valid_size=True):
        """Validate PV resize with and without name prefix"""
        dir_path = "/mnt/"
        node = self.ocp_client[0]

        # Create PVC
        self.create_storage_class(
            allow_volume_expansion=True,
            create_vol_name_prefix=create_vol_name_prefix)
        pvc_name = self.create_and_wait_for_pvc()

        # Create DC with POD and attached PVC to it.
        dc_name = oc_create_app_dc_with_io(node, pvc_name)
        self.addCleanup(oc_delete, node, 'dc', dc_name)
        self.addCleanup(scale_dc_pod_amount_and_wait,
                        node, dc_name, 0)

        pod_name = get_pod_name_from_dc(node, dc_name)
        wait_for_pod_be_ready(node, pod_name)
        if create_vol_name_prefix:
            ret = heketi_ops.verify_volume_name_prefix(
                node, self.sc['volumenameprefix'],
                self.sc['secretnamespace'],
                pvc_name, self.heketi_server_url)
            self.assertTrue(ret, "verify volnameprefix failed")
        cmd = ("dd if=/dev/urandom of=%sfile "
               "bs=100K count=1000") % dir_path
        ret, out, err = oc_rsh(node, pod_name, cmd)
        self.assertEqual(
            ret, 0, "Failed to execute command %s on %s" % (cmd, node))
        pv_name = get_pv_name_from_pvc(node, pvc_name)

        # If resize size is invalid then size should not change
        if valid_size:
            cmd = ("dd if=/dev/urandom of=%sfile2 "
                   "bs=100K count=10000") % dir_path
            with self.assertRaises(AssertionError):
                ret, out, err = oc_rsh(node, pod_name, cmd)
                msg = ("Command '%s' was expected to fail on '%s' node. "
                       "But it returned following: ret is '%s', err is '%s' "
                       "and out is '%s'" % (cmd, node, ret, err, out))
                raise ExecutionError(msg)
            pvc_size = 2
            resize_pvc(node, pvc_name, pvc_size)
            verify_pvc_size(node, pvc_name, pvc_size)
            verify_pv_size(node, pv_name, pvc_size)
        else:
            invalid_pvc_size = 'ten'
            with self.assertRaises(AssertionError):
                resize_pvc(node, pvc_name, invalid_pvc_size)
            verify_pvc_size(node, pvc_name, 1)
            verify_pv_size(node, pv_name, 1)

        oc_delete(node, 'pod', pod_name)
        wait_for_resource_absence(node, 'pod', pod_name)
        pod_name = get_pod_name_from_dc(node, dc_name)
        wait_for_pod_be_ready(node, pod_name)
        cmd = ("dd if=/dev/urandom of=%sfile_new "
               "bs=50K count=10000") % dir_path
        ret, out, err = oc_rsh(node, pod_name, cmd)
        self.assertEqual(
            ret, 0, "Failed to execute command %s on %s" % (cmd, node))

    def _available_disk_free_space(self):
        min_free_space_gb = 3
        # Get available free space disabling redundant devices and nodes
        heketi_url = self.heketi_server_url
        node_id_list = heketi_ops.heketi_node_list(
            self.heketi_client_node, heketi_url)
        self.assertTrue(node_id_list)
        nodes = {}
        min_free_space = min_free_space_gb * 1024**2
        for node_id in node_id_list:
            node_info = heketi_ops.heketi_node_info(
                self.heketi_client_node, heketi_url, node_id, json=True)
            if (node_info['state'].lower() != 'online'
                    or not node_info['devices']):
                continue
            if len(nodes) > 2:
                out = heketi_ops.heketi_node_disable(
                    self.heketi_client_node, heketi_url, node_id)
                self.assertTrue(out)
                self.addCleanup(
                    heketi_ops.heketi_node_enable,
                    self.heketi_client_node, heketi_url, node_id)
            for device in node_info['devices']:
                if device['state'].lower() != 'online':
                    continue
                free_space = device['storage']['free']
                if (node_id in nodes.keys() or free_space < min_free_space):
                    out = heketi_ops.heketi_device_disable(
                        self.heketi_client_node, heketi_url, device['id'])
                    self.assertTrue(out)
                    self.addCleanup(
                        heketi_ops.heketi_device_enable,
                        self.heketi_client_node, heketi_url, device['id'])
                    continue
                nodes[node_id] = free_space
        if len(nodes) < 3:
            raise self.skipTest(
                "Could not find 3 online nodes with, "
                "at least, 1 online device having free space "
                "bigger than %dGb." % min_free_space_gb)

        # Calculate maximum available size for PVC
        available_size_gb = int(min(nodes.values()) / (1024**2))
        return available_size_gb

    def _pv_resize(self, exceed_free_space):
        dir_path = "/mnt"
        pvc_size_gb = 1

        available_size_gb = self._available_disk_free_space()

        # Create PVC
        self.create_storage_class(allow_volume_expansion=True)
        pvc_name = self.create_and_wait_for_pvc(pvc_size=pvc_size_gb)

        # Create DC with POD and attached PVC to it
        dc_name = oc_create_app_dc_with_io(self.node, pvc_name)
        self.addCleanup(oc_delete, self.node, 'dc', dc_name)
        self.addCleanup(scale_dc_pod_amount_and_wait, self.node, dc_name, 0)
        pod_name = get_pod_name_from_dc(self.node, dc_name)
        wait_for_pod_be_ready(self.node, pod_name)

        if exceed_free_space:
            # Try to expand existing PVC exceeding free space
            resize_pvc(self.node, pvc_name, available_size_gb)
            wait_for_events(self.node, obj_name=pvc_name,
                            event_reason='VolumeResizeFailed')

            # Check that app POD is up and runnig then try to write data
            wait_for_pod_be_ready(self.node, pod_name)
            cmd = (
                "dd if=/dev/urandom of=%s/autotest bs=100K count=1" % dir_path)
            ret, out, err = oc_rsh(self.node, pod_name, cmd)
            self.assertEqual(
                ret, 0,
                "Failed to write data after failed attempt to expand PVC.")
        else:
            # Expand existing PVC using all the available free space
            expand_size_gb = available_size_gb - pvc_size_gb
            resize_pvc(self.node, pvc_name, expand_size_gb)
            verify_pvc_size(self.node, pvc_name, expand_size_gb)
            pv_name = get_pv_name_from_pvc(self.node, pvc_name)
            verify_pv_size(self.node, pv_name, expand_size_gb)
            wait_for_events(
                self.node, obj_name=pvc_name,
                event_reason='VolumeResizeSuccessful')

            # Recreate app POD
            oc_delete(self.node, 'pod', pod_name)
            wait_for_resource_absence(self.node, 'pod', pod_name)
            pod_name = get_pod_name_from_dc(self.node, dc_name)
            wait_for_pod_be_ready(self.node, pod_name)

            # Write data on the expanded PVC
            cmd = ("dd if=/dev/urandom of=%s/autotest "
                   "bs=1M count=1025" % dir_path)
            ret, out, err = oc_rsh(self.node, pod_name, cmd)
            self.assertEqual(
                ret, 0, "Failed to write data on the expanded PVC")

    def test_pv_resize_no_free_space(self):
        """Validate PVC resize fails if there is no free space available"""
        if get_openshift_storage_version() < "3.11.5":
            self.skipTest(
                "This test case is not supported for < OCS 3.11.5 builds due "
                "to bug BZ-1653567")

        self._pv_resize(exceed_free_space=True)

    def test_pv_resize_by_exact_free_space(self):
        """Validate PVC resize when resized by exact available free space"""
        self._pv_resize(exceed_free_space=False)

    def test_pv_resize_try_shrink_pv_size(self):
        """Validate whether reducing the PV size is allowed"""
        dir_path = "/mnt/"
        node = self.ocp_master_node[0]

        # Create PVC
        pv_size = 5
        self.create_storage_class(allow_volume_expansion=True)
        pvc_name = self.create_and_wait_for_pvc(pvc_size=pv_size)

        # Create DC with POD and attached PVC to it.
        dc_name = oc_create_app_dc_with_io(node, pvc_name)
        self.addCleanup(oc_delete, node, 'dc', dc_name)
        self.addCleanup(scale_dc_pod_amount_and_wait,
                        node, dc_name, 0)

        pod_name = get_pod_name_from_dc(node, dc_name)
        wait_for_pod_be_ready(node, pod_name)

        cmd = ("dd if=/dev/urandom of=%sfile "
               "bs=100K count=3000") % dir_path
        ret, out, err = oc_rsh(node, pod_name, cmd)
        self.assertEqual(
            ret, 0, "Failed to execute command %s on %s" % (cmd, node))
        pvc_resize = 2
        with self.assertRaises(AssertionError):
            resize_pvc(node, pvc_name, pvc_resize)
        verify_pvc_size(node, pvc_name, pv_size)
        pv_name = get_pv_name_from_pvc(node, pvc_name)
        verify_pv_size(node, pv_name, pv_size)
        cmd = ("dd if=/dev/urandom of=%sfile_new "
               "bs=100K count=2000") % dir_path
        ret, out, err = oc_rsh(node, pod_name, cmd)
        self.assertEqual(
            ret, 0, "Failed to execute command %s on %s" % (cmd, node))

    def test_pv_resize_when_heketi_down(self):
        """Create a PVC and try to expand it when heketi is down, It should
        fail. After heketi is up, expand PVC should work.
        """
        self.create_storage_class(allow_volume_expansion=True)
        pvc_name = self.create_and_wait_for_pvc()
        dc_name, pod_name = self.create_dc_with_pvc(pvc_name)

        pv_name = get_pv_name_from_pvc(self.node, pvc_name)
        custom = (r':metadata.annotations.'
                  r'"gluster\.kubernetes\.io\/heketi-volume-id"')
        vol_id = oc_get_custom_resource(self.node, 'pv', custom, pv_name)[0]

        h_vol_info = heketi_ops.heketi_volume_info(
            self.heketi_client_node, self.heketi_server_url, vol_id, json=True)

        # Bring the heketi POD down
        scale_dc_pod_amount_and_wait(
            self.node, self.heketi_dc_name, pod_amount=0)
        self.addCleanup(
            scale_dc_pod_amount_and_wait, self.node,
            self.heketi_dc_name, pod_amount=1)

        cmd = 'dd if=/dev/urandom of=/mnt/%s bs=614400k count=1'
        ret, out, err = oc_rsh(self.node, pod_name, cmd % 'file1')
        self.assertFalse(ret, 'Not able to write file with err: %s' % err)
        wait_for_pod_be_ready(self.node, pod_name, 10, 5)

        resize_pvc(self.node, pvc_name, 2)
        wait_for_events(
            self.node, pvc_name, obj_type='PersistentVolumeClaim',
            event_type='Warning', event_reason='VolumeResizeFailed')

        # Verify volume was not expanded
        vol_info = get_gluster_vol_info_by_pvc_name(self.node, pvc_name)
        self.assertEqual(vol_info['gluster_vol_id'], h_vol_info['name'])
        self.assertEqual(
            len(vol_info['bricks']['brick']), len(h_vol_info['bricks']))

        # Bring the heketi POD up
        scale_dc_pod_amount_and_wait(
            self.node, self.heketi_dc_name, pod_amount=1)

        # Verify volume expansion
        verify_pvc_size(self.node, pvc_name, 2)
        vol_info = get_gluster_vol_info_by_pvc_name(self.node, pvc_name)
        self.assertFalse(len(vol_info['bricks']['brick']) % 3)
        self.assertLess(
            len(h_vol_info['bricks']), len(vol_info['bricks']['brick']))

        # Wait for remount after expansion
        for w in waiter.Waiter(timeout=30, interval=5):
            ret, out, err = oc_rsh(
                self.node, pod_name,
                "df -Ph /mnt | awk '{print $2}' | tail -1")
            self.assertFalse(ret, 'Failed with err: %s and Output: %s' % (
                err, out))
            if out.strip() == '2.0G':
                break

        # Write data making sure we have more space than it was
        ret, out, err = oc_rsh(self.node, pod_name, cmd % 'file2')
        self.assertFalse(ret, 'Not able to write file with err: %s' % err)

        # Verify pod is running
        wait_for_pod_be_ready(self.node, pod_name, 10, 5)

    @skip("Blocked by BZ-1547069")
    def test_pvc_resize_size_greater_than_available_space(self):
        """Re-size PVC to greater value than available volume size and then
        expand volume to support maximum size.
        """
        sc_name = self.create_storage_class(
            create_vol_name_prefix=True, allow_volume_expansion=True)
        pvc_name = self.create_and_wait_for_pvc(sc_name=sc_name, pvc_size=1)
        dc_name, pod_name = self.create_dc_with_pvc(pvc_name)

        # Verify the size of volume mounted
        cmd_on_pod = "df -Ph /mnt | tail -1 | awk '{print $2}'"
        _, stdout, _ = oc_rsh(self.node, pod_name, cmd_on_pod)
        self.assertGreaterEqual(
            int(stdout.strip('.0M\n')), 1000,
            "Size of %s not equal to 1G" % pvc_name)
        self.assertLessEqual(
            int(stdout.strip('.0M\n')), 1024,
            "Size of %s not equal to 1G" % pvc_name)
        available_size_gb = self._available_disk_free_space()
        with self.assertRaises(AssertionError):
            resize_pvc(
                self.ocp_master_node[0], pvc_name, available_size_gb + 1)
        resize_pvc(self.ocp_master_node[0], pvc_name, available_size_gb)
        verify_pvc_size(self.ocp_master_node[0], pvc_name, available_size_gb)
