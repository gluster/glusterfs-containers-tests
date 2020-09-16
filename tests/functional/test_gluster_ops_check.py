from glustolibs.gluster.brickmux_ops import is_brick_mux_enabled
import pytest

from openshiftstoragelibs.baseclass import BaseClass
from openshiftstoragelibs.openshift_ops import cmd_run_on_gluster_pod_or_node
from openshiftstoragelibs import podcmd


class TestOpsCheck(BaseClass):

    @pytest.mark.tier1
    @podcmd.GlustoPod()
    def test_check_bmux_enabled(self):
        """Check if the brickmultiplexing is enalbed"""

        bmux_status = is_brick_mux_enabled('auto_get_gluster_endpoint')

        # Validate the result
        err_msg = ("Brick multiplex is not enabled")
        self.assertTrue(bmux_status, err_msg)

    @pytest.mark.tier1
    def test_check_max_brick_per_process(self):
        """Check if the max-brick process is set to 250"""

        cmd = ("gluster v get all all | grep cluster.max-bricks-per-process |"
               "awk '{print $2}'")

        # Get brick per process value
        bprocess_status = cmd_run_on_gluster_pod_or_node(
            self.ocp_master_node[0], cmd)

        # Validate the result
        err_msg = ("Got unexepeted max-brick process - '%s' "
                   "Expected max brick process is : 250") % bprocess_status
        self.assertIn(bprocess_status, '250', err_msg)
