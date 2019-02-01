import unittest

from glusto.core import Glusto as g


class BaseClass(unittest.TestCase):

    ERROR_OR_FAILURE_EXISTS = False
    STOP_ON_FIRST_FAILURE = bool(g.config.get("common", {}).get(
        "stop_on_first_failure", False))

    def setUp(self):
        if (BaseClass.STOP_ON_FIRST_FAILURE and
                BaseClass.ERROR_OR_FAILURE_EXISTS):
            self.skipTest("Test is skipped, because of the restriction "
                          "to one test case failure.")
        return super(BaseClass, self).setUp()

    def _is_error_or_failure_exists(self):
        if hasattr(self, '_outcome'):
            # Python 3.4+
            result = self.defaultTestResult()
            self._feedErrorsToResult(result, self._outcome.errors)
        else:
            # Python 2.7-3.3
            result = getattr(
                self, '_outcomeForDoCleanups', self._resultForDoCleanups)
        ok_result = True
        for attr in ('errors', 'failures'):
            if not hasattr(result, attr):
                continue
            exc_list = getattr(result, attr)
            if exc_list and exc_list[-1][0] is self:
                ok_result = ok_result and not exc_list[-1][1]
        if hasattr(result, '_excinfo'):
            ok_result = ok_result and not result._excinfo
        if ok_result:
            return False
        self.ERROR_OR_FAILURE_EXISTS = True
        BaseClass.ERROR_OR_FAILURE_EXISTS = True
        return True

    def doCleanups(self):
        if (BaseClass.STOP_ON_FIRST_FAILURE and (
                self.ERROR_OR_FAILURE_EXISTS or
                self._is_error_or_failure_exists())):
            while self._cleanups:
                (func, args, kwargs) = self._cleanups.pop()
                msg = ("Found test case failure. Avoiding run of scheduled "
                       "following cleanup:\nfunc = %s\nargs = %s\n"
                       "kwargs = %s" % (func, args, kwargs))
                g.log.warn(msg)
        return super(BaseClass, self).doCleanups()

    @classmethod
    def doClassCleanups(cls):
        if (BaseClass.STOP_ON_FIRST_FAILURE and
                BaseClass.ERROR_OR_FAILURE_EXISTS):
            while cls._class_cleanups:
                (func, args, kwargs) = cls._class_cleanups.pop()
                msg = ("Found test case failure. Avoiding run of scheduled "
                       "following cleanup:\nfunc = %s\nargs = %s\n"
                       "kwargs = %s" % (func, args, kwargs))
                g.log.warn(msg)
        return super(BaseClass, cls).doClassCleanups()
