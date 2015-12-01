#!/usr/bin/python2

__author__ = 'ligonliu'

from mpi4py import MPI
import subprocess

# The result of this test shows that a clone of a communicator is a different communicator
# and cannot send/receive with the original communicator
def TestCommClone():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    comm2 = comm.Clone()

    if rank == 0:
        data = {'a': 7, 'b': 3.14}
        comm2.send(data, dest=1, tag=11)
    elif rank == 1:
        print comm2.Get_name()
        data = comm2.recv(source=0, tag=11)
        print data

# The result of this test shows that "test" of isend requests will always return (True, None)
# "test" of irecv requests will return (False, None) if receiving is not complete. It will return (True, receivedObj)
# if receiving is complete.

def TestIsendrecv():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    if rank==0:
        subprocess.call('sleep 1', shell=True)
        req = comm.isend({1:2,4:5},1, tag=0)
        print '0 ' + repr(req.test())

        print '0 ' + repr(req.test())
    elif rank==1:
        req = comm.irecv(dest=0, tag=0)

        print req.test()
        subprocess.call('sleep 2', shell=True)
        print req.test()

        # print req.wait()

import threading, time, thread

class TestTimer:

    bgThread = None

    def p(self):

        while True:
            print "Hello world! "

            time.sleep(2)

    def __init__(self):

        self.bgThread = threading.Thread(target = self.p)
        self.bgThread.daemon = True
        self.bgThread.start()


def testTimer():
    a = TestTimer()
    b = 0
    for i in range(0, 100000000):
        b += i

    print b


def testKeyboardInterrupt():
    try:
        while True:
            print 'a'
            b = 1/0
    except KeyboardInterrupt:
        print 'exit'
        exit(0)
    except:
        print "not a keyboard interrupt"


class TestInterruptMain:
    bgThread = None


    def p(self):

        time.sleep(2)
        self.cmd = 'leave'
        thread.interrupt_main()
        print "hahahaha"

    def __init__(self):
        self.cmd = 'stay'
        self.bgThread = threading.Thread(target = self.p)
        self.bgThread.daemon = True
        self.bgThread.start()

def testInterruptMain():

    a = TestInterruptMain()

    try:
        while True:
            print a.cmd
    except KeyboardInterrupt:
        print "SIGINT " + a.cmd


from math import factorial
def tplTestFunc(inputData):
    assert isinstance(inputData, dict)

    returnValue = {}

    for key in inputData.keys():
        returnValue[key+'!'] = factorial(inputData[key])

    return returnValue

from mpitpl import Task, TPLManager, TPLWorker


def testTPL():

    comm = MPI.COMM_WORLD

    if comm.Get_rank()==0:

        t = Task()
        t.function = tplTestFunc

        t.input_names = ['a', 'b', 'c', 'd', 'e']
        t.output_names = ['a!','b!','c!','d!','e!']

        input_data = {'a':0, 'b':1, 'c':2, 'd':3, 'e':4}

        t.task_name = 't'

        t1 = Task()

        t1.task_name = 't1'

        t1.input_names = ['a!', 'b!', 'c!', 'd!', 'e!']

        t1.output_names = ['a!b!c!d!e!']

        t1.function = lambda x:{reduce(lambda a,b:a+b, sorted(x.keys())):reduce(lambda a,b:a*b, x.values())}

        init_task_list = [t, t1]

        man = TPLManager(comm,init_task_list,input_data)

        man.execute()

        # print result
        print man.data
        print "manager execute completed"

    else:
        wor = TPLWorker(comm)
        wor.execute()

        print "worker execute completed"



def testTaskSendRecv():

    comm = MPI.COMM_WORLD

    if comm.Get_rank()==0:
        t = Task()
        t.task_name = 'test'
        input_data = {'a':0, 'b':1, 'c':2, 'd':3, 'e':4}

        t.function = lambda x:x+1

        t.input_names = ['a', 'b', 'c', 'd', 'e']
        t.output_names = ['a!','b!','c!','d!','e!']

        t.setInputData(input_data)

        print t.input_data

        comm.send(t, dest=1, tag=1)


    else:
        t = comm.recv(source=0, tag=1)
        assert isinstance(t,Task)
        print t.function
        print t.input_names
        print t.input_data



if __name__ == '__main__':
    testTPL()