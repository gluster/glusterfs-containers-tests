from cnslibs.cns import cns_baseclass
from cnslibs.common.dynamic_provisioning import (
    verify_pvc_status_is_bound,
)
from cnslibs.common import heketi_ops
from cnslibs.common.openshift_ops import (
    get_gluster_vol_info_by_pvc_name,
    oc_create_pvc,
    oc_create_sc,
    oc_create_secret,
    oc_delete,
    wait_for_resource_absence,
)


class TestArbiterVolumeCreateExpandDelete(cns_baseclass.CnsBaseClass):

    @classmethod
    def setUpClass(cls):
        super(TestArbiterVolumeCreateExpandDelete, cls).setUpClass()
        if cls.deployment_type != "cns":
            # Do nothing and switch to the step with test skip operations
            return

        # Mark one of the Heketi nodes as arbiter-supported if none of
        # existent nodes or devices already enabled to support it.
        heketi_server_url = cls.cns_storage_class['storage_class1']['resturl']
        arbiter_tags = ('required', 'supported')
        arbiter_already_supported = False

        node_id_list = heketi_ops.heketi_node_list(
            cls.heketi_client_node, heketi_server_url)

        for node_id in node_id_list[::-1]:
            node_info = heketi_ops.heketi_node_info(
                cls.heketi_client_node, heketi_server_url, node_id, json=True)
            if node_info.get('tags', {}).get('arbiter') in arbiter_tags:
                arbiter_already_supported = True
                break
            for device in node_info['devices'][::-1]:
                if device.get('tags', {}).get('arbiter') in arbiter_tags:
                    arbiter_already_supported = True
                    break
            else:
                continue
            break
        if not arbiter_already_supported:
            heketi_ops.set_arbiter_tag(
                cls.heketi_client_node, heketi_server_url,
                'node', node_id_list[0], 'supported')

    def setUp(self):
        super(TestArbiterVolumeCreateExpandDelete, self).setUp()

        # Skip test if it is not CNS deployment
        if self.deployment_type != "cns":
            raise self.skipTest("This test can run only on CNS deployment.")
        self.node = self.ocp_master_node[0]

    def _create_storage_class(self):
        sc = self.cns_storage_class['storage_class1']
        secret = self.cns_secret['secret1']

        # Create secret file for usage in storage class
        self.secret_name = oc_create_secret(
            self.node, namespace=secret['namespace'],
            data_key=self.heketi_cli_key, secret_type=secret['type'])
        self.addCleanup(
            oc_delete, self.node, 'secret', self.secret_name)

        # Create storage class
        self.sc_name = oc_create_sc(
            self.node, resturl=sc['resturl'],
            restuser=sc['restuser'], secretnamespace=sc['secretnamespace'],
            secretname=self.secret_name,
            volumeoptions="user.heketi.arbiter true",
        )
        self.addCleanup(oc_delete, self.node, 'sc', self.sc_name)

    def _create_and_wait_for_pvc(self, pvc_size=1):
        # Create PVC
        self.pvc_name = oc_create_pvc(
            self.node, self.sc_name, pvc_name_prefix='arbiter-pvc',
            pvc_size=pvc_size)
        self.addCleanup(
            wait_for_resource_absence, self.node, 'pvc', self.pvc_name)
        self.addCleanup(oc_delete, self.node, 'pvc', self.pvc_name)

        # Wait for PVC to be in bound state
        verify_pvc_status_is_bound(self.node, self.pvc_name)

    def test_arbiter_pvc_create(self):
        """Test case CNS-944"""

        # Create sc with gluster arbiter info
        self._create_storage_class()

        # Create PVC and wait for it to be in 'Bound' state
        self._create_and_wait_for_pvc()

        # Get vol info
        vol_info = get_gluster_vol_info_by_pvc_name(self.node, self.pvc_name)

        # Verify amount proportion of data and arbiter bricks
        bricks = vol_info['bricks']['brick']
        arbiter_brick_amount = sum([int(b['isArbiter']) for b in bricks])
        data_brick_amount = len(bricks) - arbiter_brick_amount
        self.assertGreater(
            data_brick_amount, 0,
            "Data brick amount is expected to be bigger than 0. "
            "Actual amount is '%s'." % arbiter_brick_amount)
        self.assertGreater(
            arbiter_brick_amount, 0,
            "Arbiter brick amount is expected to be bigger than 0. "
            "Actual amount is '%s'." % arbiter_brick_amount)
        self.assertEqual(
            data_brick_amount,
            (arbiter_brick_amount * 2),
            "Expected 1 arbiter brick per 2 data bricks. "
            "Arbiter brick amount is '%s', Data brick amount is '%s'." % (
                arbiter_brick_amount, data_brick_amount)
        )
