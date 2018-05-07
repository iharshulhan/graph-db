"""
A script to start database engines
"""
from typing import List, Dict

from multiprocessing import Process
from graph_engine.api import start as start_engine


def start_engines(engine_params: List[Dict] = []):
    """
    Start engine
    :param engine_params: a list of params to start engines
    :return: None
    """
    if not engine_params:
        engine_params = [{'port': port + 8081, 'db_name': str(port)} for port in range(10)]

    urls = []
    for engine_param in engine_params:
        port = engine_param.get('port', None)
        db_name = engine_param.get('db_name', None)
        if port and db_name:
            p = Process(target=start_engine, args=(port, db_name))
            p.start()
            urls.append(f'http://localhost:{port}')

    return urls


if __name__ == '__main__':
    start_engines()
