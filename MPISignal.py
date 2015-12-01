__author__ = 'ligonliu'

from mpi4py import MPI

# Let us try SIGUSR1 and SIGUSR2 first
# Signal handler 

class MPISignalHandler:
    comm = None
    polling_thread = None

