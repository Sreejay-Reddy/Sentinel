import threading
import time
from .logging import logger

class HeartbeatTask:
    def __init__(self, key, fn, args, ttl_ms, owns_connection=True):
        self.key = key
        self.fn = fn
        self.args = args
        self.owns_connection = owns_connection

        self.interval = (ttl_ms / 1000.0) / 3
        self.next_heartbeat_at = time.time() + self.interval


class HeartbeatManager:
    def __init__(self, get_conn, owns_connection=True, num_threads=3):
        self.get_conn = get_conn
        self.num_threads = num_threads
        self.buckets = {i: set() for i in range(num_threads)}
        self.owns_connection = owns_connection

        self.threads = []
        self.running = False
        self.lock = threading.Lock()

    def register(self, key, fn, args, ttl_ms):
        task = HeartbeatTask(key, fn, args, ttl_ms)
        bucket_id = hash(key) % self.num_threads

        with self.lock:
            self.buckets[bucket_id].add(task)

        return task 

    def deregister(self, task):
        bucket_id = hash(task.key) % self.num_threads

        with self.lock:
            if task in self.buckets[bucket_id]:
                self.buckets[bucket_id].remove(task)

    def start(self):
        if self.running:
            return

        self.running = True

        for bucket_id in range(self.num_threads):
            t = threading.Thread(
                target=self._worker,
                args=(bucket_id,),
                daemon=True
            )
            t.start()
            self.threads.append(t)

    def stop(self):
        self.running = False

        for t in self.threads:
            t.join()

        self.threads = []
    
    def debug_dump(self):
        for i, bucket in self.buckets.items():
            print(f"Bucket {i}: {len(bucket)} tasks")

    def _worker(self, bucket_id):
        conn = self.get_conn()

        try:
            while self.running:
                now = time.time()

                with self.lock:
                    tasks = list(self.buckets[bucket_id])

                for task in tasks:
                    try:
                        if task.next_heartbeat_at <= now:
                            result = task.fn(conn, *task.args)

                            if result.success:
                                task.next_heartbeat_at = time.time() + task.interval
                            else:
                                self.deregister(task)

                    except Exception:
                        logger.exception("Heartbeat task failed")
                        try:
                            if self.owns_connection:
                                conn.close()
                        except Exception:
                            logger.exception("Could not close db connection")

                        conn = self.get_conn()

                        self.deregister(task)

                time.sleep(0.05)

        finally:
            try:
                if self.owns_connection:
                    conn.close()
            except Exception:
                logger.exception("Could not close db connection")