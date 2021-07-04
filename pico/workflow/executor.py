#!/usr/bin/env python3
#
# pico.workflow.executor - manages the execution of workflows
#
# Background
#
#   The Workflow and Executor classes were factored out of BAP.py when its
#   execution logic became too unwieldy.  They are simple implementations
#   of a generic workflow definition language and execution engine.
#
#   This module defines the Executor and Execution classes.  Execution is
#   intended to be subclassed by each backend-specific shim.  This subclass
#   is returned by the shim's Service.execute() implementation.  The subclass
#   must implement virtual method report() to report its backend status.
#
# How it works (see pseudo-code below):
#
#   An Executor instance controls a single run of a pipeline from start to end.
#   It does this by starting services according to the state of a workflow,
#   encapsulated in the .logic.Workflow object passed to it.
#
#   At any time, the Workflow will indicate which services are 'runnable'. The
#   executor then invokes the execute() method on the service, passing in the
#   blackboard and job scheduler.  The service returns the Execution object
#   that the executor monitors.
#
#   The Executor polls the scheduler, which polls the backends, for their
#   updated status, and updates the Workflow accordingly when they complete
#   or fail.  The Workflow object will then present next runnable services,
#   until the execution as a whole has completed.
#
#   The implementation is poll-based because the legacy backends run as async
#   processes, and the Python docs recommend against combining threads and
#   processes (otherwise a thread & event model would have been more obvious).
#   The polling frequency is set in the JobScheduler.
#
# Conceptual mode of use:
#
#      blackboard = .blackboard.Blackboard()
#      blackboard['inputs'] = { 'contigs': '/path/to/inputs', ... }
#      workflow = .logic.Workflow(deps, inputs, targets)
#      executor = .executor.Executor(workflow, services, scheduler)
#
#      status = executor.execute(execution_id, blackboard)
#      results = blackboard.get(...)
#

import enum
from .logic import Workflow, Services as ServicesEnum


### class Execution
#
#   Base class for the service executions, i.e. objects returned from the shims'
#   execute() method.  Subclasses must implement its report() method to retrieve
#   the backend status, and wrangle its output on to the black board.

class Execution:
    '''Base class for a single service execution, maintains the execution state.'''

    class State(enum.Enum):
        STARTED = 'STARTED'
        COMPLETED = 'COMPLETED'
        FAILED = 'FAILED'

    _sid = None     # Service ID
    _xid = None     # Workflow execution ID
    _state = None   # Current state of the service execution
    _error = None   # Set to error string on execution failure

    def __init__(self, sid, xid = None):
        '''Construct an execution with the given service id and workflow id.
           Without xid, there can be only one workflow invocation, and hence
           each Service can be invoked only once (as in 1.1.x).'''
        self._sid = sid
        self._xid = xid

    @property
    def sid(self):
        '''Service ID for which this is an execution.'''
        return self._sid

    @property
    def xid(self):
        '''Workflow execution ID of which this service invocation is part.'''
        return self._xid

    @property
    def id(self):
        '''Convenience property that returns the tuple (sid,xid).'''
        return (self._sid, self._xid)

    @property
    def ident(self):
        '''Unique identifier of this service execution within the executor.
           Equal to sid if xid is None (as in 1.1.x) else string sid[xid].'''
        return self._sid if not self._xid else '%s[%s]' % self.id

    @property
    def state(self):
        '''Current state of the Execution, an Execution.State value.'''
        return self._state

    @property
    def error(self):
        '''May hold an error string if the execution failed.'''
        return self._error

    def report(self):
        '''Pure virtual, here to signal that subclasses must implement this.'''
        raise NotImplementedError()

    def fail(self, err_fmt, *args):
        '''Transition this execution to FAILED and set its error message.
           Invokes self._transition(Execution.State.FAILED, err_fmt % args),
           which will conveniently be the subclass method if overridden.'''
        return self._transition(Execution.State.FAILED, err_fmt % args)

    def done(self):
        '''Mark this execution COMPLETED.
           Invokes self._transition(Execution.State.COMPLETED), which will
           conveniently be the subclass method if overridden .'''
        return self._transition(Execution.State.COMPLETED)

    def _transition(self, new_state, error = None):
        '''Update execution state to new_state, setting the error iff the new
           state is FAILED, intended for subclasses to extend.'''

        if new_state == Execution.State.FAILED and not error:
            raise ValueError('FAILED execution %s must set its error' % self.ident)

        self._state = new_state
        self._error = error if new_state == Execution.State.FAILED else None

        return new_state


### class Executor
#
#  Executes a Workflow. 

class Executor:
    '''Runs a Workflow from start to end, using a list of Service implementations.'''

    _workflow = None
    _services = None
    _scheduler = None

    _blackboard = None          # The data exchange mechanism between the services
    _executions = dict()        # Holds the running and completed service executions


    def __init__(self, workflow, services, scheduler):
        '''Construct executor instance to execute the given workflow using the given
           services (a dict of id -> WorkflowService mappings).'''

        # Type check our arguments to avoid confusion
        assert isinstance (workflow, Workflow)
        for k,v in services.items():
            assert isinstance(k, ServicesEnum)
            assert hasattr(v, 'execute')

        self._workflow = workflow
        self._services = services
        self._scheduler = scheduler


    def execute(self, blackboard, wx_id = None):
        '''Execute the workflow, optionally with a workflow execution ID.'''

        wx_name = "workflow" if not wx_id else "workflow[%s]" % wx_id

        # Create the blackboard for communication between services
        self._blackboard = blackboard
        self._blackboard.log("executor starting %s", wx_name)

        # Obtain the status of the Workflow object to control our execution
        wx_status = self._workflow.status
        self._blackboard.log("%s status: %s", wx_name, wx_status.value)
        assert wx_status != Workflow.Status.WAITING, "no services were started yet"

        # We run as long as there are runnable or running services in the Workflow
        while wx_status in [ Workflow.Status.RUNNABLE, Workflow.Status.WAITING ]:

            # Check that the Workflow and our idea of runnable and running match
            self.assert_cross_check(wx_id)
            more_jobs = True

            # Pick the first runnable off the runnables list, if any
            runnable = self._workflow.list_runnable()
            if runnable:
                # Look up the service and start it
                svc_ident = runnable[0]
                self.start_service(svc_ident, wx_id)

            else:
                # Nothing runnable, wait on the scheduler for job to end
                more_jobs = self._scheduler.listen()

                # Update all started executions with job state
                for sx_id, execution in self._executions.items():
                    if execution.state == Execution.State.STARTED:
                        self.poll_service(sx_id)

            # Update our status by querying the Workflow
            old_wx_status, wx_status = wx_status, self._workflow.status
            if old_wx_status != wx_status:
                self._blackboard.log("%s status: %s", wx_name, wx_status.value)

            # Defensive programming: if scheduler has no more job but we think we
            # are still WAITING we would get into a tight infinite loop
            if not more_jobs and wx_status == Workflow.Status.WAITING:
                raise Exception('fatal inconsistency between %s and scheduler' % wx_name)

        # Workflow is done, log result
        str_done = ', '.join(map(lambda s: s.value, self._workflow.list_completed()))
        str_fail = ', '.join(map(lambda s: s.value, self._workflow.list_failed()))
        str_skip = ', '.join(map(lambda s: s.value, self._workflow.list_skipped()))
        self._blackboard.log("%s execution completed", wx_name)
        self._blackboard.log("- done: %s", str_done if str_done else "(none)")
        self._blackboard.log("- failed: %s", str_fail if str_fail else "(none)")
        self._blackboard.log("- skipped: %s", str_skip if str_skip else "(none)")

        return wx_status


    def start_service(self, svc_id, wx_id):
        '''Start the execution of a service within a workflow execution.
           Actual startup should be asynchronous, but the service shim will
           return a state we use to update our status.'''

        service = self._services.get(svc_id)
        if not service:
            raise ValueError("no implementation for service id: %s" % svc_id.value)

        sx_id = (svc_id, wx_id)
        sx_name = '%s[%s]' % (svc_id.value,wx_id) if wx_id else svc_id.value

        try:
            if not wx_id:  # backward compatible 1.1.x
                execution = service.execute(svc_id.value, self._blackboard, self._scheduler)
            else:
                execution = service.execute(svc_id.value, wx_id, self._blackboard, self._scheduler, dict())

            self._blackboard.log("service start: %s" % sx_name)
            self._executions[sx_id] = execution
            self.update_state(sx_id, execution.state)

        except Exception as e:
            self._blackboard.log("service skipped: %s: %s", sx_name, str(e))
            self._workflow.mark_skipped(sx_id[0])


    def poll_service(self, sx_id):
        '''Poll the service execution for its current status.  This is a
           non-blocking call on the execution to check the backend state.'''

        execution = self._executions.get(sx_id)
        if not execution:
            sx_name = '%s[%s]' % (sx_id[0].value,sx_id[1]) if sx_id[1] else sx_id[0].value
            raise ValueError("no such service execution: %s" % sx_name)

        old_state = execution.state
        new_state = execution.report()

        if new_state != old_state:
            self.update_state(sx_id, new_state)


    def update_state(self, sx_id, state):
        '''Update the executing/ed service and workflow with new state.'''

        svc_id, wx_id = sx_id
        sx_name = '%s[%s]' % (svc_id.value, wx_id) if wx_id else svc_id.value

        self._blackboard.log("service execution state: %s %s", sx_name, state.value)
        if state == Execution.State.STARTED:
            self._workflow.mark_started(svc_id)
        elif state == Execution.State.COMPLETED:
            self._workflow.mark_completed(svc_id)
        elif state == Execution.State.FAILED:
            self._workflow.mark_failed(svc_id)
        else:
            raise ValueError("invalid service state for %s: %s" % (sx_name, state))


    def assert_cross_check(self, wx_id):
        '''Cross check that the state maintained in Workflow matches the state of
           the services our executions.'''

        for r in self._workflow.list_runnable():
            assert (r,wx_id) not in self._executions

        for r in self._workflow.list_started():
            assert self._executions[(r,wx_id)].state == Execution.State.STARTED
        for r in self._workflow.list_failed():
            assert (r,wx_id) not in self._executions or self._executions[(r,wx_id)].state == Execution.State.FAILED
        for r in self._workflow.list_completed():
            assert (r,wx_id) not in self._executions or self._executions[(r,wx_id)].state == Execution.State.COMPLETED
        for r in self._workflow.list_skipped():
            assert (r,wx_id) not in self._executions

        for (i,j) in self._executions.keys():
            state = self._executions[(i,j)].state
            if state == Execution.State.STARTED:
                assert i in self._workflow.list_started()
            elif state == Execution.State.FAILED:
                assert i in self._workflow.list_failed()
            elif state == Execution.State.COMPLETED:
                assert i in self._workflow.list_completed()
            else:
                assert False, "not a valid state"

