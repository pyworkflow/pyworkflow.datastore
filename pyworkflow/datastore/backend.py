__version__ = '0.1.0'
__author__ = 'Willem Bult'
__email__ = 'willem.bult@gmail.com'

import pickle
import time
import random

from itertools import ifilter, imap
from collections import deque
from datetime import datetime, timedelta
from uuid import uuid4

from datastore.core import Key, Query

from pyworkflow.backend import Backend
from pyworkflow.activity import *
from pyworkflow.exceptions import UnknownActivityException, UnknownDecisionException, UnknownProcessException
from pyworkflow.events import *
from pyworkflow.decision import *
from pyworkflow.process import *
from pyworkflow.task import *
from pyworkflow.signal import *
from pyworkflow.defaults import Defaults

class DatastoreBackend(Backend):
    '''
    Datastore backend. Not very efficient. Primarily for development purposes.
    '''

    KEY_RUNNING_PROCESSES = Key('/processes/running')
    KEY_LOCKED_PROCESSES = Key('/processes/locked')
    KEY_RUNNING_ACTIVITIES = Key('/activities/running')
    KEY_RUNNING_DECISIONS = Key('/decisions/running')

    KEY_SCHEDULED_DECISIONS = Key('/decisions/scheduled')
    KEY_SCHEDULED_ACTIVITIES = Key('/activities/scheduled')


    class SyncedProcess:
        def __init__(self, backend, pid, autosave):
            self.backend = backend
            self.pid = pid
            self.autosave = autosave

        def __enter__(self):
            self.backend._lock_process(self.pid)
            self.process = self.backend._managed_process(self.pid)
            return self.process

        def __exit__(self, *args):
            if self.autosave:
                self.backend._save_managed_process(self.process)
            self.backend._unlock_process(self.pid)            

    def synced_process(self, pid, autosave=True):
        return self.SyncedProcess(self, pid, autosave=autosave)

    def __init__(self, datastore):
        self.workflows = {}
        self.activities = {}

        self.datastore = datastore

    def _pickle_process(self, process):
        return {'pid': process['pid'], 'proc': pickle.dumps(process['proc'])}

    def _unpickle_process(self, process):
        return {'pid': process['pid'], 'proc': pickle.loads(process['proc'])}

    def _lock_process(self, pid):
        key = str(uuid4())
        lock_idx = self.KEY_LOCKED_PROCESSES.child(pid)

        while True:
            lock = self.datastore.get(lock_idx)
            if lock is None:
                self.datastore.put(lock_idx, key)
                lock = self.datastore.get(lock_idx)
            if lock == key:
                break
            time.sleep(random.random() * .5)

    def _unlock_process(self, pid):
        lock_idx = self.KEY_LOCKED_PROCESSES.child(pid)
        self.datastore.delete(lock_idx)

    def _managed_process(self, process_or_pid):
        pid = process_or_pid if isinstance(process_or_pid, basestring) else process_or_pid.id
        match = self.datastore.get(self.KEY_RUNNING_PROCESSES.child(pid))
        if not match:
            raise UnknownProcessException()
        
        unpickled = self._unpickle_process(match)
        return unpickled

    def _save_managed_process(self, process):
        pickled = self._pickle_process(process)
        self.datastore.put(self.KEY_RUNNING_PROCESSES.child(process['pid']), pickled)        

    def _schedule_activity(self, process, activity, id, input, category=None):
        expiration = datetime.now() + timedelta(seconds=self.activities[activity]['scheduled_timeout'])
        execution = ActivityExecution(activity, id, input=input)
        aid = str(uuid4())
        category = category or self.activities[activity]['category']
        key = self.KEY_SCHEDULED_ACTIVITIES.child(aid)
        obj = {'key': str(key), 'dt': time.time(), 'aid': aid, 'exec': pickle.dumps(execution), 'pid': process['pid'], 'exp': expiration, 'category': str(category)}
        self.datastore.put(key, obj)

    def _activity_by_id(self, id):
        activity = filter(lambda a: pickle.loads(a['exec']).id == id, self.datastore.query(Query(self.KEY_RUNNING_ACTIVITIES)))
        if not activity:
            activity = filter(lambda a: pickle.loads(a['exec']).id == id, self.datastore.query(Query(self.KEY_SCHEDULED_ACTIVITIES)))
        return (activity or [None])[0]        

    def _cancel_activity(self, id):
        to_cancel = filter(lambda a: pickle.loads(a['exec']).id == id, self.datastore.query(Query(self.KEY_SCHEDULED_ACTIVITIES)))
        for a in to_cancel:
            self.datastore.delete(self.KEY_SCHEDULED_ACTIVITIES.child(a['aid']))

        to_cancel = filter(lambda a: pickle.loads(a['exec']).id == id, self.datastore.query(Query(self.KEY_RUNNING_ACTIVITIES)))
        for a in to_cancel:
            self.datastore.delete(self.KEY_RUNNING_ACTIVITIES.child(a['run_id']))

    def _schedule_decision(self, process, start=None, timer=None):
        workflow = self.workflows[process['proc'].workflow]
        category = workflow['category']

        key = self.KEY_SCHEDULED_DECISIONS.child(process['pid'])
        val = self.datastore.get(key) or {'key': str(key), 'category': category, 'decisions': []}
        matching = filter(lambda a: not a['start'] or a['start'] <= (start or datetime.now()), val['decisions'])
        
        if not matching:
            expiration = datetime.now() + timedelta(seconds=workflow['decision_timeout'])
            timer = pickle.dumps(timer) if timer else None
            new_obj = {'dt': time.time(), 'pid': process['pid'], 'exp': expiration, 'start': start, 'timer': timer}
            val['decisions'].append(new_obj)            
            self.datastore.put(key, val)

    def _cancel_decision(self, process):
        self.datastore.delete(self.KEY_SCHEDULED_DECISIONS.child(process['pid']))

    def register_workflow(self, name, category=Defaults.DECISION_CATEGORY,
        timeout=Defaults.WORKFLOW_TIMEOUT, 
        decision_timeout=Defaults.DECISION_TIMEOUT):

        self.workflows[name] = {
            'timeout': timeout,
            'decision_timeout': decision_timeout,
            'category': category
        }

    def register_activity(self, name, category=Defaults.ACTIVITY_CATEGORY, 
        scheduled_timeout=Defaults.ACTIVITY_SCHEDULED_TIMEOUT, 
        execution_timeout=Defaults.ACTIVITY_EXECUTION_TIMEOUT, 
        heartbeat_timeout=Defaults.ACTIVITY_HEARTBEAT_TIMEOUT):

        self.activities[name] = {
            'category': category,
            'scheduled_timeout': scheduled_timeout,
            'execution_timeout': execution_timeout,
            'heartbeat_timeout': heartbeat_timeout
        }

    def start_process(self, process):
        # register the process
        if not process.id:
            process = process.copy_with_id(str(uuid4()))

        managed_process = {'pid': process.id, 'proc': process}
        self._save_managed_process(managed_process)

        # schedule a decision
        self._schedule_decision(managed_process)
        
    def signal_process(self, process, signal, data=None):
        # find the process as we know it
        with self.synced_process(getattr(process, 'id', process)) as managed_process:
            # append the signal event
            managed_process['proc'].history.append(SignalEvent(Signal(signal, data)))

        # schedule a decision (if needed)
        self._schedule_decision(managed_process)

    def cancel_process(self, process, details=None):
        # find the process as we know it
        with self.synced_process(getattr(process, 'id', process)) as managed_process:
            # append the cancelation event
            managed_process['proc'].history.append(DecisionEvent(CancelProcess(details=details)))
        
        # remove scheduled decision
        self._cancel_decision(managed_process)

        # process has already been removed on retrieve; don't re-save
        self.datastore.delete(self.KEY_RUNNING_PROCESSES.child(managed_process['pid']))


    def heartbeat_activity_task(self, task):
        self._time_out_activities()

        # find the process as we know it
        ae = self.datastore.get(self.KEY_RUNNING_ACTIVITIES.child(task.context['run_id']))
        (execution, pid, expiration, hb_expiration) = (pickle.loads(ae['exec']), ae['pid'], ae['exp'], ae['hb_exp'])

        # replace with new heartbeat timeout
        self.datastore.delete(self.KEY_RUNNING_ACTIVITIES.child(task.context['run_id']))
        expiration = datetime.now() + timedelta(seconds=self.activities[execution.activity]['heartbeat_timeout'])
        self.datastore.put(self.KEY_RUNNING_ACTIVITIES.child(task.context['run_id']), {'run_id': task.context['run_id'], 'exec': pickle.dumps(execution), 'pid': pid, 'exp': expiration, 'hb_exp': hb_expiration})

    def complete_decision_task(self, task, decisions):
        self._time_out_decisions()
        
        if not type(decisions) is list:
            decisions = [decisions]

        # find the process as we know it
        decision = self.datastore.get(self.KEY_RUNNING_DECISIONS.child(task.context['run_id']))
        if not decision:
            raise UnknownDecisionException()

        self.datastore.delete(self.KEY_RUNNING_DECISIONS.child(task.context['run_id']))
        (pid, expiration) = (decision['pid'], decision['exp'])

        with self.synced_process(pid, autosave=False) as managed_process:

            # append the decision events
            for decision in decisions:
                managed_process['proc'].history.append(DecisionEvent(decision))
                self._save_managed_process(managed_process)
                
                # schedule activity if needed
                if hasattr(decision, 'activity'):
                    self._schedule_activity(managed_process, decision.activity, decision.id, decision.input, category=decision.category)

                # cancel activity
                if isinstance(decision, CancelActivity):
                    activity = self._activity_by_id(decision.id)
                    self._cancel_activity(decision.id)
                    managed_process['proc'].history.append(ActivityEvent(pickle.loads(activity['exec']), ActivityCanceled()))
                    self._save_managed_process(managed_process)

                # complete process
                if isinstance(decision, CompleteProcess) or isinstance(decision, CancelProcess):
                    for mp in filter(lambda mp: mp['pid'] == pid, self.datastore.query(Query(self.KEY_RUNNING_PROCESSES))):
                        self.datastore.delete(self.KEY_RUNNING_PROCESSES.child(mp['pid']))
                    self._cancel_decision(managed_process)
                    if managed_process['proc'].parent:
                        parent = self._managed_process(managed_process['proc'].parent)
                        if decision.type == 'complete_process':
                            parent['proc'].history.append(ChildProcessEvent(managed_process['pid'], ProcessCompleted(result=decision.result), workflow=managed_process['proc'].workflow, tags=managed_process['proc'].tags))
                        elif decision.type == 'cancel_process':
                            parent['proc'].history.append(ChildProcessEvent(managed_process['pid'], ProcessCanceled(details=decision.details), workflow=managed_process['proc'].workflow, tags=managed_process['proc'].tags))

                        self._save_managed_process(parent)
                        self._schedule_decision(parent)

                # start child process
                if isinstance(decision, StartChildProcess):
                    process = Process(workflow=decision.process.workflow, id=decision.process.id or str(uuid4()), input=decision.process.input, tags=decision.process.tags, parent=task.process.id)
                    child_process = {'pid': process.id, 'proc': process}
                    self._save_managed_process(child_process)
                    # schedule a decision
                    self._schedule_decision(child_process)

                if isinstance(decision, Timer):
                    self._schedule_decision(managed_process, start=datetime.now() + timedelta(seconds=decision.delay), timer=decision)

        # decision finished
        self.datastore.delete(self.KEY_RUNNING_DECISIONS.child(task.context['run_id']))

    def complete_activity_task(self, task, result=None):
        self._time_out_activities()

        # find the process as we know it
        activity = self.datastore.get(self.KEY_RUNNING_ACTIVITIES.child(task.context['run_id']))
        if not activity:
            raise UnknownActivityException()

        self.datastore.delete(self.KEY_RUNNING_ACTIVITIES.child(task.context['run_id']))
        (execution, pid, expiration, heartbeat_expiration) = (pickle.loads(activity['exec']), activity['pid'], activity['exp'], activity['hb_exp'])
        
        with self.synced_process(pid) as managed_process:
            # append the activity event
            managed_process['proc'].history.append(ActivityEvent(execution, result))

        # schedule a decision (if needed)
        self._schedule_decision(managed_process)

        # activity finished
        self.datastore.delete(self.KEY_RUNNING_ACTIVITIES.child(task.context['run_id']))

    def process_by_id(self, pid):
        return self._managed_process(pid)['proc']

    def processes(self, workflow=None, tag=None):
        match = lambda p: (p['proc'].workflow == workflow or not workflow) and (tag in p['proc'].tags or not tag)
        return imap(lambda p: p['proc'], ifilter(match, imap(lambda x: self._unpickle_process(x), self.datastore.query(Query(self.KEY_RUNNING_PROCESSES)))))

    def _time_out_activities(self):
        # activities that are past expired scheduling date. they're in scheduled_activities
        for expired in filter(lambda a: a['exp'] < datetime.now(), self.datastore.query(Query(self.KEY_SCHEDULED_ACTIVITIES))):
            self.datastore.delete(self.KEY_SCHEDULED_ACTIVITIES.child(expired['aid']))

            pid = expired['pid']
            with self.synced_process(pid) as managed_process:
                managed_process['proc'].history.append(ActivityEvent(pickle.loads(expired['exec']), ActivityTimedOut()))
            
            self._schedule_decision(managed_process)
            
        # activities that are past expired execution date. they're in running_activities
        for expired in filter(lambda a: a['exp'] < datetime.now() or a['hb_exp'] < datetime.now(), self.datastore.query(Query(self.KEY_RUNNING_ACTIVITIES))):
            self.datastore.delete(self.KEY_RUNNING_ACTIVITIES.child(expired['run_id']))

            pid = expired['pid']
            with self.synced_process(pid) as managed_process:
                managed_process['proc'].history.append(ActivityEvent(pickle.loads(expired['exec']), ActivityTimedOut()))
            
            self._schedule_decision(managed_process)

    def _time_out_decisions(self):
        # decisions that are past expired execution date. they're in running_decisions
        for expired in filter(lambda a: a['exp'] < datetime.now(), self.datastore.query(Query(self.KEY_RUNNING_DECISIONS))):
            self.datastore.delete(self.KEY_RUNNING_DECISIONS.child(expired['run_id']))
            self._schedule_decision(self._managed_process(expired['pid']))

    def poll_activity_task(self, category=Defaults.ACTIVITY_CATEGORY, identity=None):
        # find queued activity tasks (that haven't timed out)
        self._time_out_activities()

        def next_scheduled():
            try:
                q = Query(self.KEY_SCHEDULED_ACTIVITIES).filter('category','=',str(category))
                sa = sorted(self.datastore.query(q), key=lambda sa: sa['dt'])
                if not sa:
                    return None

                self.datastore.delete(self.KEY_SCHEDULED_ACTIVITIES.child(sa[0]['aid']))
                return (sa[0]['pid'], pickle.loads(sa[0]['exec']), sa[0]['exp'])
            except:
                return None

        while True:
            scheduled = next_scheduled()
            if scheduled:
                (pid, activity_execution, expiration) = scheduled
                run_id = str(uuid4())
                try:
                    with self.synced_process(pid) as managed_process:
                        expiration = datetime.now() + timedelta(seconds=self.activities[activity_execution.activity]['execution_timeout'])
                        heartbeat_expiration = datetime.now() + timedelta(seconds=self.activities[activity_execution.activity]['heartbeat_timeout'])

                        managed_process['proc'].history.append(ActivityStartedEvent(activity_execution))

                        self.datastore.put(self.KEY_RUNNING_ACTIVITIES.child(run_id), {'run_id': run_id, 'exec': pickle.dumps(activity_execution), 'pid': pid, 'exp': expiration, 'hb_exp': heartbeat_expiration})
                        return ActivityTask(activity_execution, process_id=pid, context={'run_id': run_id})

                except UnknownProcessException:
                    pass
            else:
                return None


    def poll_decision_task(self, category=Defaults.DECISION_CATEGORY, identity=None):
        # time-out expired activities
        self._time_out_activities()
        self._time_out_decisions()

        # find queued decision tasks (that haven't timed out)
        try:
            q = Query(self.KEY_SCHEDULED_DECISIONS).filter('category','=',category)
            all_decisions = [x for obj in self.datastore.query(q) for x in obj['decisions']]
            sd = sorted(all_decisions, key=lambda d: d['dt'])
            sd = filter(lambda d: d['start'] is None or d['start'] <= datetime.now(), sd)
            if not sd:
                return None

            key = self.KEY_SCHEDULED_DECISIONS.child(sd[0]['pid'])
            p_decisions = [x for x in sd[1:] if x['pid'] == sd[0]['pid']]
            if not p_decisions:
                # no more decisions for this process
                self.datastore.delete(key)
            else:
                # some other decisions for this process left at later time
                updated = {'key': str(key), 'category': category, 'decisions': p_decisions}
                self.datastore.put(key, updated)

            (pid, expiration, timer) = (sd[0]['pid'], sd[0]['exp'], sd[0]['timer'])
        except:
            return None

        run_id = str(uuid4())
        with self.synced_process(pid) as managed_process:

            if timer:
                managed_process['proc'].history.append(TimerEvent(pickle.loads(timer)))
            
            managed_process['proc'].history.append(DecisionStartedEvent())

        process = managed_process['proc']
        
        expiration = datetime.now() + timedelta(seconds=self.workflows[process.workflow]['timeout'])
        self.datastore.put(self.KEY_RUNNING_DECISIONS.child(run_id), {'run_id': run_id, 'pid': pid, 'exp': expiration})
        
        return DecisionTask(process, context={'run_id': run_id})
        