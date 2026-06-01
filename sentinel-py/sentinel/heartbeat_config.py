# Runtime config for heartbeat manager 
# heartbeat manager is not exposed, only used internally 
import threading
from .heartbeat import HeartbeatManager

_manager = None
_lock = threading.Lock()


def get_manager(get_conn=None, owns_connection=True):
    global _manager

    if _manager is None:
        with _lock:
            if _manager is None: 
                    if get_conn is None:
                        raise ValueError("get_conn required for first initialization")
                    
                    _manager = HeartbeatManager(get_conn=get_conn, owns_connection=owns_connection, num_threads=3)
                    _manager.start()

    return _manager


def shutdown_manager():
    global _manager

    with _lock:
        if _manager:
            _manager.stop()
            _manager = None