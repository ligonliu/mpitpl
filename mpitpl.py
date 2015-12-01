__author__ = 'ligonliu'

import dill

from mpi4py import MPI

MPI._p_pickle.dumps = dill.dumps
MPI._p_pickle.loads = dill.loads

TPLDATA = 0x7FFFFFFF
TPLCMD = 0x7FFFFFFE
TPL_MAN_RANK = 0
TPL_NEW_TASK_LIST_NAME = 'b4925923-2145-46d6-8eed-bbd6d73ed3a0'

class Task: # Define a task
    # task_name = None # task name, unique among all tasks
    # input_names = []  # List of names of input data, each datum has unique name
    # output_names = [] # List of names of output data
    # function = None

    # The function should take input data and return output data as dict
    # and potentially generate new tasks as a list in output_data, with key: mpitpl.TPL_NEW_TASK_LIST_NAME

    # input_data = {}
    # output_data = {}

    # failable: boolean  -not used right now

    def isReady(self):
        return set(self.input_data.keys()).issuperset(self.input_names)

    def isCompleted(self):
        return set(self.output_data.keys()).issuperset(self.output_names)

    def setInputData(self, dataDict):
        assert isinstance(dataDict, dict)
        for name in self.input_names:
            if dataDict.has_key(name):
                self.input_data[name] = dataDict[name]

    def setOutputData(self,dataDict):
        assert isinstance(dataDict, dict)
        for name in self.output_names:
            if dataDict.has_key(name):
                self.output_data[name] = dataDict[name]

    def setData(self, dataDict):
        self.setInputData(dataDict)
        self.setOutputData(dataDict)


    def __init__(self, copyFrom=None):

        if copyFrom is not None:
            assert isinstance(copyFrom, Task)
            self.task_name = copyFrom.task_name
            self.input_names = list(copyFrom.input_names)
            self.output_names = list(copyFrom.output_names)
            self.function = copyFrom.function

            self.input_data = dict(copyFrom.input_data)
            self.output_data = dict(copyFrom.output_data)
        else:
            self.task_name = None
            self.input_names = []
            self.output_names = []
            self.function = None

            self.input_data = {}
            self.output_data = {}



    def __hash__(self):  # because we need to use a python set (hash set)
        return self.task_name.__hash__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


CMDSET = set(['abortTask','jobSuccess','jobFail'])

class TPLManager:
    # comm: MPI communicator

    # data: a dictionary
    # ready_tasks: set of tasks
    # pending_tasks: set of tasks
    # running_worker_tasks : dictionary worker:task
    # completed_tasks : list

    def _handleNewTask(self, new_task):
        # check if the task is Ready or not.
        # if ready, put them into ready tasks. Otherwise, put them into pending tasks.
        return

    def _handleMessage(self, message):
        return

    def _getFirstIdleWorker(self):
        return next((i for i in range(1, self.comm.Get_size()) if not self.running_worker_tasks.has_key(i)), None)

    # The process:
    # At any message cycle
    # First, we dispatch any ready tasks to any available workers, until we exhaust ready tasks or available workers
    # Then, wait to receive messages. At least one worker finishes its task,
    # and returns the tasks' output data.
    # Then we update the current available data, and check if any pending task become ready with the new data
    def execute(self):
        assert isinstance(self.comm, MPI.Comm)
        while len(self.pending_tasks) + len(self.ready_tasks) + len(self.running_worker_tasks) > 0: #Still has unfinished tasks
            # if there are ready tasks, pick up a ready task and dispatch to an idle worker
            # if there are no idle worker, skip this step
            while self._getFirstIdleWorker() != None and len(self.ready_tasks)>0:
                # dispatch task(s) to available worker

                task = self.ready_tasks.pop()

                assert task.isReady()

                worker = self._getFirstIdleWorker()
                # print pickle.dumps(task),worker
                self.comm.send(obj=task, dest=worker, tag=TPLDATA)
                self.running_worker_tasks[worker] = task

            # accept one message from worker about its completed task
            status1 = MPI.Status()

            # run a blocking receive
            completed_task = self.comm.recv(source=MPI.ANY_SOURCE, tag=TPLDATA, status=status1)
            # recv should have source=ANY_SOURCE as default parameter according to mpi4py docs
            # but it seems necessary to give this parameter explicitly

            completed_worker = status1.Get_source()

            # print "received completed task {0} from {1}".format(completed_task.task_name, completed_worker)

            if completed_task is None:
                # abort the entire job... we should add fault tolerance later
                # send terminate to all workers
                for worker in range(1, self.comm.Get_size()):
                    self.comm.send('jobFail', worker, TPLCMD)

                return MPI.ERR_OTHER

            assert isinstance(completed_task, Task)

            assert completed_task.isCompleted()

            self.completed_tasks.append(completed_task)

            del self.running_worker_tasks[completed_worker]

            # if completed_task has posed new tasks, put them into pending_tasks
            if completed_task.output_data.has_key(TPL_NEW_TASK_LIST_NAME):
                newTasks = completed_task.output_data[completed_task.output_data]
                for newTask in newTasks:
                    self.pending_tasks.add(newTask)
                del completed_task.output_data[TPL_NEW_TASK_LIST_NAME]


            # import the data from completedTask
            self.data.update(completed_task.output_data)

            # see what pending tasks have all data. This step is expensive, in a higher-throughput implementation,
            # it should be done lazily (when there are not enough ready_tasks) and after receiving a bunch of completedTasks
            avail_data_names = set(self.data.keys())
            new_ready_tasks = set()
            pending_task_to_remove = set()

            for pending_task in self.pending_tasks:
                assert isinstance(pending_task, Task)
                assert not pending_task.isReady()

                if avail_data_names.issuperset(pending_task.input_names):
                    ready_task = Task(pending_task)
                    ready_task.setInputData(self.data)
                    assert ready_task.isReady()
                    new_ready_tasks.add(ready_task)
                    pending_task_to_remove.add(pending_task)

            self.pending_tasks.difference_update(pending_task_to_remove)

            self.ready_tasks.update(new_ready_tasks)

        # send finish signal to all workers
        for worker in range(1, self.comm.Get_size()):
            self.comm.send('jobSuccess', worker, TPLCMD)

        return MPI.SUCCESS

    def __init__(self, comm, init_task_list, init_data_dict):
        self.comm = comm
        assert isinstance(init_task_list, list)
        assert isinstance(init_data_dict, dict)

        self.data = dict(init_data_dict)

        self.ready_tasks = set()
        self.pending_tasks = set()

        self.running_worker_tasks = {}

        self.completed_tasks = []

        # see what tasks are ready immediately, put them in readytasks, others in pending tasks
        avail_data_names = set(self.data.keys())

        for init_task in init_task_list:
            assert isinstance(init_task, Task)
            if avail_data_names.issuperset(init_task.input_names):
                init_task.setData(self.data)
                assert init_task.isReady()
                self.ready_tasks.add(init_task)
            else:
                self.pending_tasks.add(init_task)



class TPLWorker:
    # comm: MPI comm

    # task: Task (ready or complete)
    # function_thread: thread object
    # executing = False

    # thread safety: recv, irecv and send are thread safe, isend unknown, from mpich docs

    # we need to have periodic checking of control messages.
    # (to abort in time and return the error message)
    # we can have a separate thread to poll and probe messages with the TPLCMD tag.
    # However, it failed, because the separate thread cannot interrupt the main thread
    # why it fails to work? Because self.comm.recv is a C library call
    # during C library calls, python cannot catch signals
    # so if self.comm.recv for data never returns
    # it never catches the CMD ARRIVE signal
    # see: https://bytes.com/topic/python/answers/168365-keyboardinterrupt-vs-extension-written-c


    def execute(self):
        executing = True

        while executing:
            # receive Task or command from manager
            try:
                status1 = MPI.Status()
                cmdOrTask = self.comm.recv(source=TPL_MAN_RANK, tag=MPI.ANY_TAG, status = status1)

                if status1.Get_tag()==TPLDATA:
                    # execute the task:
                    assert isinstance(cmdOrTask, Task)

                    assert cmdOrTask.isReady()

                    # print "received task " + cmdOrTask.task_name

                    cmdOrTask.output_data = cmdOrTask.function(cmdOrTask.input_data)

                    # print "task completed " + cmdOrTask.task_name

                    assert cmdOrTask.isCompleted()

                    # send the completed task back to manager
                    self.comm.send(obj=cmdOrTask, dest= TPL_MAN_RANK, tag = TPLDATA)

                    # print "task sent to manager " + cmdOrTask.task_name

                elif status1.Get_tag() == TPLCMD:
                    # print "CMD {0} received".format(cmdOrTask)

                    if cmdOrTask == 'abortTask':
                        cmdOrTask = None  #clear up this cmd, since there may be more cmds.
                        # terminate the task thread
                        continue # give up the current task, and immediate get ready to receive new tasks
                    elif cmdOrTask == 'jobSuccess':
                        self.executing = False
                        return MPI.SUCCESS
                    elif self.recv_cmd == 'jobFail':
                        self.executing = False
                        return MPI.ERR_OTHER


            except:
                # other exceptions. Report to the manager that task failed
                self.comm.send(None, TPL_MAN_RANK, tag=TPLDATA)


    def __init__(self, comm):
        self.comm = comm

