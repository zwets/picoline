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
#   This module defines the Executor and Task classes.  A task is a single
#   Service execution within a Workflow execution.  A Task subclass is
#   returned by the Service.execute() implementation on the shim.
#
#   The Task subclass must implement virtual method report() to report its
#   backend status.
#
# How it works (see pseudo-code below):
#
#   An Executor instance controls a single run of a pipeline from start to end.
#   It does this by starting services according to the state of a workflow,
#   encapsulated in the .logic.Workflow object passed to it.
#
#   At any time, the Workflow will indicate which services are 'runnable'. The
#   executor then invokes the execute() method on the service, passing in the
#   blackboard and job scheduler.  The service returns the Task object that
#   the executor monitors.
#
#   The Executor polls the scheduler, which polls the backends, for their
#   updated status.  Each Task is then queried by the Executor for its current
#   state.  When a Task completes or fails, the executor updates the Workflow.
#   The Workflow object may then present next runnable services, until the
#   workflow execution as a whole has completed.
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
#      executor = .executor.Executor(services, scheduler)
#
#      status = executor.execute(workflow, blackboard, execution_id)
#      results = blackboard.get(...)
#

import enum
from .logic import Workflow, Services as ServicesEnum


### class Task
#
#   Base class for single service execution in a single workflow execution.
#   Is return by the service's shim's execute() method.
#   Subclasses must implement its report() method to retrieve the backend status,
#   and wrangle its output on to the black board.

class Task:
    '''Base class for a service execution within a workflow execution,
       maintains the execution state.'''

    class State(enum.Enum):
        STARTED = 'STARTED'
        COMPLETED = 'COMPLETED'
        FAILED = 'FAILED'

    _sid = None     # Service ID
    _xid = None     # Workflow execution ID
    _state = None   # Current state of the service execution
    _error = None   # Set to error string on execution failure

    def __init__(self, sid, xid):
        '''Construct a task of service sid within workflow execution xid.
           Xid is None when there can be only one workflow invocation.'''
        self._sid = sid
        self._xid = xid

    @property
    def sid(self):
        '''Service ID of which this is an execution.'''
        return self._sid

    @property
    def xid(self):
        '''Workflow execution ID of which this is a task.'''
        return self._xid

    @property
    def id(self):
        '''Unique ID of this Task within the current Executor run.  Is
           simply the tuple (sid,xid).'''
        return (self._sid, self._xid)

#    @property
#    def id_str(self):
#        '''Human representation of this task: the string sid[xid],
#           or if xid is None (legacy), then just sid.'''
#        return self._sid if not self._xid else '%s[%s]' % self.id

    @property
    def state(self):
        '''Current state of the Task, a Task.State value.'''
        return self._state

    @property
    def error(self):
        '''May hold an error string if the task failed.'''
        return self._error

    def report(self):
        '''Pure virtual, here to signal that subclasses must implement this.'''
        raise NotImplementedError()

    def fail(self, err_fmt, *args):
        '''Transition this task to FAILED and set its error message.
           Invokes self._transition(Task.State.FAILED, err_fmt % args),
           which will conveniently be the subclass method if overridden.'''
        return self._transition(Task.State.FAILED, err_fmt % args)

    def done(self):
        '''Mark this task COMPLETED.
           Invokes self._transition(Task.State.COMPLETED), which will
           conveniently be the subclass method if overridden .'''
        return self._transition(Task.State.COMPLETED)

    def _transition(self, new_state, error = None):
        '''Update task state to new_state, setting the error iff the new
           state is FAILED, intended for subclasses to extend.'''

        if new_state == Task.State.FAILED and not error:
            id_str = self._sid if not self._xid else '%s[%s]' % self.id
            raise ValueError('FAILED task %s must set its error' % id_str)

        self._state = new_state
        self._error = error if new_state == Task.State.FAILED else None

        return new_state


### class Executor
#
#  Executes a Workflow. 

class Executor:
    '''Runs a Workflow from start to end, using a list of Service implementations.'''

    _services = None
    _scheduler = None

    _tasks = dict()        # Holds the running and completed service executions


    def __init__(self, services, scheduler):
        '''Construct executor instance the with the given services
           (a dict of id -> WorkflowService mappings) and scheduler.'''

        # Type check our arguments to avoid confusion
        for k,v in services.items():
            assert isinstance(k, ServicesEnum)
            assert hasattr(v, 'execute')

        self._services = services
        self._scheduler = scheduler


    def execute(self, workflow, blackboard, xid = None):
        '''Execute the workflow, using the blackboard, with optional execution id.'''

        # Type check our arguments to avoid confusion
        assert isinstance (workflow, Workflow)

        wx_name = "workflow execution" if not xid else "workflow execution %s" % xid

        # Create the blackboard for communication between services
        blackboard.log("executor starting %s", wx_name)

        # Obtain the status of the Workflow object to control our execution
        wx_status = workflow.status
        blackboard.log("%s status: %s", wx_name, wx_status.value)
        assert wx_status != Workflow.Status.WAITING, "no services were started yet"

        # We run as long as there are runnable or running services in the Workflow
        while wx_status in [ Workflow.Status.RUNNABLE, Workflow.Status.WAITING ]:

            # Check that the Workflow and our idea of runnable and running match
            self.assert_cross_check(workflow, xid)
            more_jobs = True

            # Pick the first runnable off the runnables list, if any
            runnable = workflow.list_runnable()
            if runnable:
                # Look up the service and start it
                sid = runnable[0]
                self.start_task(sid, xid, workflow, blackboard)

            else:
                # Nothing runnable, wait on the scheduler for job to end
                more_jobs = self._scheduler.listen()

                # Update all started tasks with job state
                for tid, task in self._tasks.items():
                    if task.state == Task.State.STARTED:
                        self.poll_task(tid, workflow, blackboard)

            # Update our status by querying the Workflow
            old_wx_status, wx_status = wx_status, workflow.status
            if old_wx_status != wx_status:
                blackboard.log("%s status: %s", wx_name, wx_status.value)

            # Defensive programming: if scheduler has no more job but we think we
            # are still WAITING we would get into a tight infinite loop
            if not more_jobs and wx_status == Workflow.Status.WAITING:
                raise Exception('fatal inconsistency between %s and scheduler' % wx_name)

        # Workflow is done, log result
        str_done = ', '.join(map(lambda s: s.value, workflow.list_completed()))
        str_fail = ', '.join(map(lambda s: s.value, workflow.list_failed()))
        str_skip = ', '.join(map(lambda s: s.value, workflow.list_skipped()))
        blackboard.log("%s completed", wx_name)
        blackboard.log("- done: %s", str_done if str_done else "(none)")
        blackboard.log("- failed: %s", str_fail if str_fail else "(none)")
        blackboard.log("- skipped: %s", str_skip if str_skip else "(none)")

        return wx_status


    def start_task(self, sid, xid, wf, bb):
        '''Start the execution of a service within a workflow execution.
           Actual startup should be asynchronous, but the service shim will
           return a state we use to update our status.'''

        service = self._services.get(sid)
        if not service:
            raise ValueError("no implementation for service id: %s" % sid.value)

        tid = (sid, xid)
        tshow = '%s[%s]' % (sid.value,xid) if xid else sid.value

        try:
            task = service.execute(sid.value, xid, bb, self._scheduler)
            bb.log("task start: %s" % tshow)
            self._tasks[tid] = task
            self.update_state(wf, tid, task.state, bb)

        except Exception as e:
            bb.log("task skipped: %s: %s", tshow, str(e))
            wf.mark_skipped(tid[0])


    def poll_task(self, tid, wf, bb):
        '''Poll the task for its current status.  This is a non-blocking call on
           the task to check the backend state, then update wf if applicable.'''

        task = self._tasks.get(tid)
        tshow = '%s[%s]' % (tid[0].value,tid[1]) if tid[1] else tid[0].value
        if not task:
            raise ValueError("no such task: %s" % tshow)

        old_state = task.state
        new_state = task.report()

        if new_state != old_state:
            self.update_state(wf, tid, new_state, bb)


    def update_state(self, wf, tid, state, bb):
        '''Update the executing/ed task and workflow with new state.'''

        sid, xid = tid
        tshow = '%s[%s]' % (sid.value, xid) if xid else sid.value

        bb.log("task %s: %s", state.value.lower(), tshow)
        if state == Task.State.STARTED:
            wf.mark_started(sid)
        elif state == Task.State.COMPLETED:
            wf.mark_completed(sid)
        elif state == Task.State.FAILED:
            wf.mark_failed(sid)
        else:
            raise ValueError("invalid task state for %s: %s" % (tshow, state))


    def assert_cross_check(self, wf, xid):
        '''Cross check that the state maintained in wf matches the state of all tasks.'''

        for sid in wf.list_runnable():
            assert (sid,xid) not in self._tasks

        for sid in wf.list_started():
            assert self._tasks[(sid,xid)].state == Task.State.STARTED
        for sid in wf.list_failed():
            assert (sid,xid) not in self._tasks or self._tasks[(sid,xid)].state == Task.State.FAILED
        for sid in wf.list_completed():
            assert (sid,xid) not in self._tasks or self._tasks[(sid,xid)].state == Task.State.COMPLETED
        for sid in wf.list_skipped():
            assert (sid,xid) not in self._tasks

        for (sid,xid) in self._tasks.keys():
            state = self._tasks[(sid,xid)].state
            if state == Task.State.STARTED:
                assert sid in wf.list_started()
            elif state == Task.State.FAILED:
                assert sid in wf.list_failed()
            elif state == Task.State.COMPLETED:
                assert sid in wf.list_completed()
            else:
                assert False, "not a valid state"

