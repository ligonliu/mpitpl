from mpi4py import MPI
import thread, threading

# The manager stores the content in a dictionary
# The manager forks out two threads, each of them use a different communicator

# The MPIDict object should fork out the new thread for handling send/request/receive
# Then there are two threads accessing the same object

class Query: # can be insert, delete, getvalue, has_key
    request = None
    key = None

class QueryResponse:
    answer = None
    value = None


class MPIDict:
    local_dict = {}
    comm = None
    stor_thread = None
    active_query = None  # this rank has queried for the value, and the content is not stored in this rank

    def isManager(self):
        return 0==self.comm.Get_rank()

    def _handleActiveQuery(self):
        if self.active_query is not None:

            # should send message to the manager



    def _workerThread(self):

        while True:

            # if active_query is not None (it comes from its own thread)

            operation = self.comm.recv()

        return

    def _managerThread(self):



        return

    def __init__(self, comm = MPI.COMM_WORLD):
        self.comm = comm.Clone()

        # start either the manager or worker thread
        if self.isManager():
            stor_thread = threading.Thread(target=self._managerThread())
            stor_thread.start()
        else:
            stor_thread = threading.Thread(target=self._workerThread())
            # Version 0.x: do not start this thread for workers.



    def has_key(self, key):
        # if localDict has it, return localDict result (if distributed storage, manager-based storage has no problem)
        if self.localDict.has_key(key):
            return True
        # else, query the manager, if the manager has it, return true, else return false
        return

    def __getitem__(self, index):
        return

    def __setitem__(self, key, value):
        return



