import time

from glusto.core import Glusto as g
import pytest

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs import command
from openshiftstoragelibs.exceptions import ExecutionError
from openshiftstoragelibs.heketi_ops import (
    heketi_node_info,
    heketi_node_list,
    heketi_volume_delete,
    heketi_volume_info,
    heketi_volume_list,
    verify_volume_name_prefix,
)
from openshiftstoragelibs.node_ops import (
    find_vm_name_by_ip_or_hostname,
    node_reboot_by_command,
    power_off_vm_by_name,
    power_on_vm_by_name
)
from openshiftstoragelibs.openshift_ops import (
    cmd_run_on_gluster_pod_or_node,
    get_gluster_host_ips_by_pvc_name,
    get_gluster_pod_names_by_pvc_name,
    get_pv_name_from_pvc,
    get_pod_name_from_dc,
    get_pod_names_from_dc,
    oc_create_secret,
    oc_create_sc,
    oc_create_app_dc_with_io,
    oc_create_pvc,
    oc_create_tiny_pod_with_volume,
    oc_delete,
    oc_get_custom_resource,
    oc_rsh,
    scale_dc_pod_amount_and_wait,
    verify_pvc_status_is_bound,
    wait_for_pod_be_ready,
    wait_for_resource_absence)
from openshiftstoragelibs.openshift_version import get_openshift_version
from openshiftstoragelibs.waiter import Waiter


class TestDynamicProvisioningP0(BaseClass):
    '''
     Class that contain P0 dynamic provisioning test cases for
     glusterfile volume
    '''

    def setUp(self):
        super(TestDynamicProvisioningP0, self).setUp()
        self.node = self.ocp_master_node[0]

    def dynamic_provisioning_glusterfile(self, create_vol_name_prefix):
        # Create secret and storage class
        self.create_storage_class(
            create_vol_name_prefix=create_vol_name_prefix)

        # Create PVC
        pvc_name = self.create_and_wait_for_pvc()

        # Create DC with POD and attached PVC to it.
        dc_name = oc_create_app_dc_with_io(
            self.node, pvc_name, image=self.io_container_image_cirros)
        self.addCleanup(oc_delete, self.node, 'dc', dc_name)
        self.addCleanup(scale_dc_pod_amount_and_wait, self.node, dc_name, 0)

        pod_name = get_pod_name_from_dc(self.node, dc_name)
        wait_for_pod_be_ready(self.node, pod_name)

        # Verify Heketi volume name for prefix presence if provided
        if create_vol_name_prefix:
            ret = verify_volume_name_prefix(self.node,
                                            self.sc['volumenameprefix'],
                                            self.sc['secretnamespace'],
                                            pvc_name, self.sc['resturl'])
            self.assertTrue(ret, "verify volnameprefix failed")
        else:
            # Get the volume name and volume id from PV
            pv_name = get_pv_name_from_pvc(self.ocp_client[0], pvc_name)
            custom = [
                r':spec.glusterfs.path',
                r':metadata.annotations.'
                r'"gluster\.kubernetes\.io\/heketi-volume-id"'
            ]
            pv_vol_name, vol_id = oc_get_custom_resource(
                self.ocp_client[0], 'pv', custom, pv_name)

            # check if the pv_volume_name is present in heketi
            # Check if volume name is "vol_"+volumeid or not
            heketi_vol_name = heketi_volume_info(
                self.ocp_client[0], self.heketi_server_url, vol_id,
                json=True)['name']
            self.assertEqual(pv_vol_name, heketi_vol_name,
                             'Volume with vol_id = %s not found'
                             'in heketidb' % vol_id)
            self.assertEqual(heketi_vol_name, 'vol_' + vol_id,
                             'Volume with vol_id = %s have a'
                             'custom perfix' % vol_id)
            out = cmd_run_on_gluster_pod_or_node(self.ocp_master_node[0],
                                                 "gluster volume list")
            self.assertIn(pv_vol_name, out,
                          "Volume with id %s does not exist" % vol_id)

        # Make sure we are able to work with files on the mounted volume
        filepath = "/mnt/file_for_testing_io.log"
        for cmd in ("dd if=/dev/urandom of=%s bs=1K count=100",
                    "ls -lrt %s",
                    "rm -rf %s"):
            cmd = cmd % filepath
            ret, out, err = oc_rsh(self.node, pod_name, cmd)
            self.assertEqual(
                ret, 0,
                "Failed to execute '%s' command on %s" % (cmd, self.node))

    @pytest.mark.tier1
    def test_dynamic_provisioning_glusterfile(self):
        """Validate dynamic provisioning for gluster file"""
        g.log.info("test_dynamic_provisioning_glusterfile")
        self.dynamic_provisioning_glusterfile(False)

    @pytest.mark.tier2
    def test_dynamic_provisioning_glusterfile_volname_prefix(self):
        """Validate dynamic provisioning for gluster file with vol name prefix
        """
        if get_openshift_version() < "3.9":
            self.skipTest(
                "'volumenameprefix' option for Heketi is not supported"
                " in OCP older than 3.9")

        g.log.info("test_dynamic_provisioning_glusterfile volname prefix")
        self.dynamic_provisioning_glusterfile(True)

    @pytest.mark.tier1
    def test_dynamic_provisioning_glusterfile_heketipod_failure(self):
        """Validate dynamic provisioning for gluster file when heketi pod down
        """
        mount_path = "/mnt"
        datafile_path = '%s/fake_file_for_%s' % (mount_path, self.id())

        # Create secret and storage class
        sc_name = self.create_storage_class()

        # Create PVC
        app_1_pvc_name = self.create_and_wait_for_pvc(
            pvc_name_prefix="autotest-file", sc_name=sc_name
        )

        # Create app POD with attached volume
        app_1_pod_name = oc_create_tiny_pod_with_volume(
            self.node, app_1_pvc_name, "test-pvc-mount-on-app-pod",
            mount_path=mount_path, image=self.io_container_image_cirros)
        self.addCleanup(
            wait_for_resource_absence, self.node, 'pod', app_1_pod_name)
        self.addCleanup(oc_delete, self.node, 'pod', app_1_pod_name)

        # Wait for app POD be up and running
        wait_for_pod_be_ready(
            self.node, app_1_pod_name, timeout=60, wait_step=2)

        # Write data to the app POD
        write_data_cmd = (
            "dd if=/dev/urandom of=%s bs=1K count=100" % datafile_path)
        ret, out, err = oc_rsh(self.node, app_1_pod_name, write_data_cmd)
        self.assertEqual(
            ret, 0,
            "Failed to execute command %s on %s" % (write_data_cmd, self.node))

        # Remove Heketi pod
        heketi_down_cmd = "oc scale --replicas=0 dc/%s --namespace %s" % (
            self.heketi_dc_name, self.storage_project_name)
        heketi_up_cmd = "oc scale --replicas=1 dc/%s --namespace %s" % (
            self.heketi_dc_name, self.storage_project_name)
        self.addCleanup(self.cmd_run, heketi_up_cmd)
        heketi_pod_name = get_pod_name_from_dc(
            self.node, self.heketi_dc_name, timeout=10, wait_step=3)
        self.cmd_run(heketi_down_cmd)
        wait_for_resource_absence(self.node, 'pod', heketi_pod_name)

        app_2_pvc_name = oc_create_pvc(
            self.node, pvc_name_prefix="autotest-file2", sc_name=sc_name
        )
        self.addCleanup(
            wait_for_resource_absence, self.node, 'pvc', app_2_pvc_name)
        self.addCleanup(
            oc_delete, self.node, 'pvc', app_2_pvc_name, raise_on_absence=False
        )

        # Create second app POD
        app_2_pod_name = oc_create_tiny_pod_with_volume(
            self.node, app_2_pvc_name, "test-pvc-mount-on-app-pod",
            mount_path=mount_path, image=self.io_container_image_cirros)
        self.addCleanup(
            wait_for_resource_absence, self.node, 'pod', app_2_pod_name)
        self.addCleanup(oc_delete, self.node, 'pod', app_2_pod_name)

        # Bring Heketi POD back
        self.cmd_run(heketi_up_cmd)

        # Wait for Heketi POD be up and running
        new_heketi_pod_name = get_pod_name_from_dc(
            self.node, self.heketi_dc_name, timeout=10, wait_step=2)
        wait_for_pod_be_ready(
            self.node, new_heketi_pod_name, wait_step=5, timeout=120)

        # Wait for second PVC and app POD be ready
        verify_pvc_status_is_bound(self.node, app_2_pvc_name)
        wait_for_pod_be_ready(
            self.node, app_2_pod_name, timeout=60, wait_step=2)

        # Verify that we are able to write data
        ret, out, err = oc_rsh(self.node, app_2_pod_name, write_data_cmd)
        self.assertEqual(
            ret, 0,
            "Failed to execute command %s on %s" % (write_data_cmd, self.node))

    @pytest.mark.tier4
    def test_dynamic_provisioning_glusterfile_gluster_pod_or_node_failure(
            self):
        """Create glusterblock PVC when gluster pod or node is down."""
        mount_path = "/mnt"
        datafile_path = '%s/fake_file_for_%s' % (mount_path, self.id())

        # Create secret and storage class
        self.create_storage_class()

        # Create PVC
        pvc_name = self.create_and_wait_for_pvc()

        # Create app POD with attached volume
        pod_name = oc_create_tiny_pod_with_volume(
            self.node, pvc_name, "test-pvc-mount-on-app-pod",
            mount_path=mount_path, image=self.io_container_image_cirros)
        self.addCleanup(
            wait_for_resource_absence, self.node, 'pod', pod_name)
        self.addCleanup(oc_delete, self.node, 'pod', pod_name)

        # Wait for app POD be up and running
        wait_for_pod_be_ready(
            self.node, pod_name, timeout=60, wait_step=2)

        # Run IO in background
        io_cmd = "oc rsh %s dd if=/dev/urandom of=%s bs=1000K count=900" % (
            pod_name, datafile_path)
        async_io = g.run_async(self.node, io_cmd, "root")

        # Check for containerized Gluster
        if self.is_containerized_gluster():
            # Pick up one of the hosts which stores PV brick (4+ nodes case)
            gluster_pod_data = get_gluster_pod_names_by_pvc_name(
                self.node, pvc_name)[0]

            # Delete glusterfs POD from chosen host and wait for
            # spawn of new one
            oc_delete(self.node, 'pod', gluster_pod_data["pod_name"])
            cmd = ("oc get pods -o wide | grep glusterfs | grep %s | "
                   "grep -v Terminating | awk '{print $1}'") % (
                       gluster_pod_data["pod_hostname"])
            for w in Waiter(600, 15):
                new_gluster_pod_name = self.cmd_run(cmd)
                if new_gluster_pod_name:
                    break
            if w.expired:
                error_msg = "exceeded timeout, new gluster pod not created"
                g.log.error(error_msg)
                raise AssertionError(error_msg)
            g.log.info("new gluster pod name is %s" % new_gluster_pod_name)
            wait_for_pod_be_ready(self.node, new_gluster_pod_name)
        else:
            pvc_hosting_node_ip = get_gluster_host_ips_by_pvc_name(
                self.node, pvc_name)[0]
            heketi_nodes = heketi_node_list(
                self.heketi_client_node, self.heketi_server_url)
            node_ip_for_reboot = None
            for heketi_node in heketi_nodes:
                heketi_node_ip = heketi_node_info(
                    self.heketi_client_node, self.heketi_server_url,
                    heketi_node, json=True)["hostnames"]["storage"][0]
                if heketi_node_ip == pvc_hosting_node_ip:
                    node_ip_for_reboot = heketi_node_ip
                    break

            if not node_ip_for_reboot:
                raise AssertionError(
                    "Gluster node IP %s not matched with heketi node %s" % (
                        pvc_hosting_node_ip, heketi_node_ip))

            node_reboot_by_command(node_ip_for_reboot)

        # Check that async IO was not interrupted
        ret, out, err = async_io.async_communicate()
        self.assertEqual(ret, 0, "IO %s failed on %s" % (io_cmd, self.node))

    @pytest.mark.tier1
    def test_storage_class_mandatory_params_glusterfile(self):
        """Validate storage-class creation with mandatory parameters"""

        # create secret
        self.secret_name = oc_create_secret(
            self.node,
            namespace=self.sc.get('secretnamespace', 'default'),
            data_key=self.heketi_cli_key,
            secret_type=self.sc.get('provisioner', 'kubernetes.io/glusterfs'))
        self.addCleanup(
            oc_delete, self.node, 'secret', self.secret_name)

        # create storage class with mandatory parameters only
        sc_name = oc_create_sc(
            self.node, provisioner='kubernetes.io/glusterfs',
            resturl=self.sc['resturl'], restuser=self.sc['restuser'],
            secretnamespace=self.sc['secretnamespace'],
            secretname=self.secret_name
        )
        self.addCleanup(oc_delete, self.node, 'sc', sc_name)

        # Create PVC
        pvc_name = self.create_and_wait_for_pvc(sc_name=sc_name)

        # Create DC with POD and attached PVC to it.
        dc_name = oc_create_app_dc_with_io(
            self.node, pvc_name, image=self.io_container_image_cirros)
        self.addCleanup(oc_delete, self.node, 'dc', dc_name)
        self.addCleanup(scale_dc_pod_amount_and_wait, self.node, dc_name, 0)

        pod_name = get_pod_name_from_dc(self.node, dc_name)
        wait_for_pod_be_ready(self.node, pod_name)

        # Make sure we are able to work with files on the mounted volume
        filepath = "/mnt/file_for_testing_sc.log"
        cmd = "dd if=/dev/urandom of=%s bs=1K count=100" % filepath
        ret, out, err = oc_rsh(self.node, pod_name, cmd)
        self.assertEqual(
            ret, 0, "Failed to execute command %s on %s" % (cmd, self.node))

        cmd = "ls -lrt %s" % filepath
        ret, out, err = oc_rsh(self.node, pod_name, cmd)
        self.assertEqual(
            ret, 0, "Failed to execute command %s on %s" % (cmd, self.node))

        cmd = "rm -rf %s" % filepath
        ret, out, err = oc_rsh(self.node, pod_name, cmd)
        self.assertEqual(
            ret, 0, "Failed to execute command %s on %s" % (cmd, self.node))

    @pytest.mark.tier1
    def test_dynamic_provisioning_glusterfile_heketidown_pvc_delete(self):
        """Validate deletion of PVC's when heketi is down"""

        # Create storage class, secret and PVCs
        self.create_storage_class()
        self.pvc_name_list = self.create_and_wait_for_pvcs(
            1, 'pvc-heketi-down', 3)

        # remove heketi-pod
        scale_dc_pod_amount_and_wait(self.ocp_client[0],
                                     self.heketi_dc_name,
                                     0,
                                     self.storage_project_name)
        try:
            # delete pvc
            for pvc in self.pvc_name_list:
                oc_delete(self.ocp_client[0], 'pvc', pvc)
            for pvc in self.pvc_name_list:
                with self.assertRaises(ExecutionError):
                    wait_for_resource_absence(
                        self.ocp_client[0], 'pvc', pvc, interval=3, timeout=30)
        finally:
            # bring back heketi-pod
            scale_dc_pod_amount_and_wait(self.ocp_client[0],
                                         self.heketi_dc_name,
                                         1,
                                         self.storage_project_name)

        # verify PVC's are deleted
        for pvc in self.pvc_name_list:
            wait_for_resource_absence(self.ocp_client[0], 'pvc',
                                      pvc,
                                      interval=1, timeout=120)

        # create a new PVC
        self.create_and_wait_for_pvc()

    @pytest.mark.tier1
    def test_validate_pvc_in_multiple_app_pods(self):
        """Validate the use of a same claim in multiple app pods"""
        replicas = 5

        # Create PVC
        sc_name = self.create_storage_class()
        pvc_name = self.create_and_wait_for_pvc(sc_name=sc_name)

        # Create DC with application PODs
        dc_name = oc_create_app_dc_with_io(
            self.node, pvc_name, replicas=replicas,
            image=self.io_container_image_cirros)
        self.addCleanup(oc_delete, self.node, 'dc', dc_name)
        self.addCleanup(scale_dc_pod_amount_and_wait, self.node, dc_name, 0)

        # Wait for all the PODs to be ready
        pod_names = get_pod_names_from_dc(self.node, dc_name)
        self.assertEqual(replicas, len(pod_names))
        for pod_name in pod_names:
            wait_for_pod_be_ready(self.node, pod_name)

        # Create files in each of the PODs
        for pod_name in pod_names:
            self.cmd_run("oc exec {0} -- touch /mnt/temp_{0}".format(pod_name))

        # Check that all the created files are available at once
        ls_out = self.cmd_run("oc exec %s -- ls /mnt" % pod_names[0]).split()
        for pod_name in pod_names:
            self.assertIn("temp_%s" % pod_name, ls_out)

    @pytest.mark.tier1
    def test_pvc_deletion_while_pod_is_running(self):
        """Validate PVC deletion while pod is running"""
        if get_openshift_version() <= "3.9":
            self.skipTest(
                "PVC deletion while pod is running is not supported"
                " in OCP older than 3.9")

        # Create DC with POD and attached PVC to it
        sc_name = self.create_storage_class()
        pvc_name = self.create_and_wait_for_pvc(sc_name=sc_name)
        dc_name, pod_name = self.create_dc_with_pvc(pvc_name)

        # Delete PVC
        oc_delete(self.node, 'pvc', self.pvc_name)

        with self.assertRaises(ExecutionError):
            wait_for_resource_absence(
                self.node, 'pvc', self.pvc_name, interval=3, timeout=30)

        # Make sure we are able to work with files on the mounted volume
        # after deleting pvc.
        filepath = "/mnt/file_for_testing_volume.log"
        cmd = "dd if=/dev/urandom of=%s bs=1K count=100" % filepath
        ret, out, err = oc_rsh(self.node, pod_name, cmd)
        self.assertEqual(
            ret, 0, "Failed to execute command %s on %s" % (cmd, self.node))

    @pytest.mark.tier1
    def test_dynamic_provisioning_glusterfile_reclaim_policy_retain(self):
        """Validate retain policy for glusterfs after deletion of pvc"""

        if get_openshift_version() < "3.9":
            self.skipTest(
                "'Reclaim' feature is not supported in OCP older than 3.9")

        self.create_storage_class(reclaim_policy='Retain')
        self.create_and_wait_for_pvc()

        # get the name of the volume
        pv_name = get_pv_name_from_pvc(self.node, self.pvc_name)
        custom = [r':.metadata.annotations.'
                  r'"gluster\.kubernetes\.io\/heketi\-volume\-id"',
                  r':.spec.persistentVolumeReclaimPolicy']

        vol_id, reclaim_policy = oc_get_custom_resource(
            self.node, 'pv', custom, pv_name)

        self.assertEqual(reclaim_policy, 'Retain')

        # Create DC with POD and attached PVC to it.
        try:
            dc_name = oc_create_app_dc_with_io(
                self.node, self.pvc_name, image=self.io_container_image_cirros)
            pod_name = get_pod_name_from_dc(self.node, dc_name)
            wait_for_pod_be_ready(self.node, pod_name)
        finally:
            scale_dc_pod_amount_and_wait(self.node, dc_name, 0)
            oc_delete(self.node, 'dc', dc_name)
            wait_for_resource_absence(self.node, 'pod', pod_name)

        oc_delete(self.node, 'pvc', self.pvc_name)

        with self.assertRaises(ExecutionError):
            wait_for_resource_absence(
                self.node, 'pvc', self.pvc_name, interval=3, timeout=30)

        heketi_volume_delete(self.heketi_client_node,
                             self.heketi_server_url, vol_id)

        vol_list = heketi_volume_list(self.heketi_client_node,
                                      self.heketi_server_url)

        self.assertNotIn(vol_id, vol_list)

        oc_delete(self.node, 'pv', pv_name)
        wait_for_resource_absence(self.node, 'pv', pv_name)

    @pytest.mark.tier2
    def test_usage_of_default_storage_class(self):
        """Validate PVs creation for SC with default custom volname prefix"""

        if get_openshift_version() < "3.9":
            self.skipTest(
                "'volumenameprefix' option for Heketi is not supported"
                " in OCP older than 3.9")

        # Unset 'default' option from all the existing Storage Classes
        unset_sc_annotation_cmd = (
            r"""oc annotate sc %s """
            r""""storageclass%s.kubernetes.io/is-default-class"-""")
        set_sc_annotation_cmd = (
            r"""oc patch storageclass %s -p'{"metadata": {"annotations": """
            r"""{"storageclass%s.kubernetes.io/is-default-class": "%s"}}}'""")
        get_sc_cmd = (
            r'oc get sc --no-headers '
            r'-o=custom-columns=:.metadata.name,'
            r':".metadata.annotations.storageclass\.'
            r'kubernetes\.io\/is-default-class",:".metadata.annotations.'
            r'storageclass\.beta\.kubernetes\.io\/is-default-class"')
        sc_list = self.cmd_run(get_sc_cmd)
        for sc in sc_list.split("\n"):
            sc = sc.split()
            if len(sc) != 3:
                self.skipTest(
                    "Unexpected output for list of storage classes. "
                    "Following is expected to contain 3 keys:: %s" % sc)
            for value, api_type in ((sc[1], ''), (sc[2], '.beta')):
                if value == '<none>':
                    continue
                self.cmd_run(unset_sc_annotation_cmd % (sc[0], api_type))
                self.addCleanup(
                    self.cmd_run,
                    set_sc_annotation_cmd % (sc[0], api_type, value))

        # Create new SC
        prefix = "autotests-default-sc"
        self.create_storage_class(sc_name_prefix=prefix)

        # Make new SC be the default one and sleep for 1 sec to avoid races
        self.cmd_run(set_sc_annotation_cmd % (self.sc_name, '', 'true'))
        self.cmd_run(set_sc_annotation_cmd % (self.sc_name, '.beta', 'true'))
        time.sleep(1)

        # Create PVC without specification of SC
        pvc_name = oc_create_pvc(
            self.node, sc_name=None, pvc_name_prefix=prefix)
        self.addCleanup(
            wait_for_resource_absence, self.node, 'pvc', pvc_name)
        self.addCleanup(oc_delete, self.node, 'pvc', pvc_name)

        # Wait for successful creation of PVC and check its SC
        verify_pvc_status_is_bound(self.node, pvc_name)
        get_sc_of_pvc_cmd = (
            "oc get pvc %s --no-headers "
            "-o=custom-columns=:.spec.storageClassName" % pvc_name)
        out = self.cmd_run(get_sc_of_pvc_cmd)
        self.assertEqual(out, self.sc_name)

    @pytest.mark.tier2
    def test_node_failure_pv_mounted(self):
        """Test node failure when PV is mounted with app pods running"""
        filepath = "/mnt/file_for_testing_volume.log"
        pvc_name = self.create_and_wait_for_pvc()

        dc_and_pod_names = self.create_dcs_with_pvc(pvc_name)
        dc_name, pod_name = dc_and_pod_names[pvc_name]

        mount_point = "df -kh /mnt -P | tail -1 | awk '{{print $1}}'"
        pod_cmd = "oc exec {} -- {}".format(pod_name, mount_point)
        hostname = command.cmd_run(pod_cmd, hostname=self.node)
        hostname = hostname.split(":")[0]

        vm_name = find_vm_name_by_ip_or_hostname(hostname)
        self.addCleanup(power_on_vm_by_name, vm_name)
        power_off_vm_by_name(vm_name)

        cmd = "dd if=/dev/urandom of={} bs=1K count=100".format(filepath)
        ret, _, err = oc_rsh(self.node, pod_name, cmd)
        self.assertFalse(
            ret, "Failed to execute command {} on {} with error {}"
            .format(cmd, self.node, err))

        oc_delete(self.node, 'pod', pod_name)
        wait_for_resource_absence(self.node, 'pod', pod_name)
        pod_name = get_pod_name_from_dc(self.node, dc_name)
        wait_for_pod_be_ready(self.node, pod_name)

        ret, _, err = oc_rsh(self.node, pod_name, cmd)
        self.assertFalse(
            ret, "Failed to execute command {} on {} with error {}"
            .format(cmd, self.node, err))
