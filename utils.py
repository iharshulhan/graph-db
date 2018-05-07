# number of threads should be bigger than actual number of tasks, elsewhere the pool will not work
from multiprocessing.pool import ThreadPool, Pool
from typing import List, Callable

NUM_OF_THREADS = 150
thread_pool = ThreadPool(NUM_OF_THREADS + 1)

NUM_OF_PROCESSES = 10
process_pool = Pool(NUM_OF_PROCESSES + 1)


def execute_function_in_parallel(func: Callable, list_args: List, processes: bool = False) -> List:
    """
    Execute a function in parallel using ThreadPool or ProcessPool
    :param processes: execute tasks in separate processes
    :param func: a func to call
    :param list_args: an array containing calling params
    :return: an array with results
    """

    results = []
    tmp_args = []
    pool = process_pool if processes else thread_pool
    num_instances = NUM_OF_PROCESSES if processes else NUM_OF_THREADS
    # ThreadPool works if number of tasks is less than number of threads, so we feed it with batches of tasks
    for args in list_args:
        tmp_args.append(args)
        if len(tmp_args) == num_instances:
            results_tmp = [pool.apply_async(func, args=(*args,))
                           for args in tmp_args]
            for result in results_tmp:
                res = result.get()
                if res:
                    results.append(res)
            tmp_args = []

    results_tmp = [pool.apply_async(func, args=(*args,))
                   for args in tmp_args]
    for result in results_tmp:
        res = result.get()
        if res:
            results.append(res)
    return results
