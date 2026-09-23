import os
import signal
import redis
import time
from rqueue.job_queue import JobQueue, JobStatus

class Worker:
    def __init__(self, queue: JobQueue, consumer_name: str):
        self.queue = queue
        self.consumer = consumer_name
        self.is_running = True

        # Register signal handlers for graceful shutdown.
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        print(f"\n[Worker '{self.consumer}'] Shutdown signal received. Exiting gracefully...")
        self.is_running = False

    def start(self):
        print(f"[Worker '{self.consumer}'] Start listening stream '{self.queue.STREAM}'...")
        last_scheduler_check = 0
        last_reclaim_check = 0

        while self.is_running:
            try:
                now = time.time()
                # fetch scheduled delays job
                if now - last_scheduler_check > 5:
                    self.queue.enqueue_scheduled_jobs()
                    last_scheduler_check = now

                # 2. Reclaim stale/pending jobs from dead workers
                if now - last_reclaim_check > 30:
                    self._check_stale_jobs()
                    last_reclaim_check = now

                jobs_to_process = self.queue.fetch_jobs(
                    self.consumer, count=1, block_ms=2000
                )
                for msg_id, job_id in jobs_to_process:
                    self._process_message(msg_id, job_id)

            except redis.ConnectionError:
                print("[Worker] Lost connection to Redis. Retrying in 5 seconds...")
                time.sleep(5)
            except KeyboardInterrupt:
                # Handle Ctrl+C if it arrives while xreadgroup is blocking.
                self._handle_shutdown(None, None)
                break

    def _check_stale_jobs(self):
        stale_jobs = self.queue.reclaim_stale(self.consumer, min_idle_time=30000)
            
        for msg_id, job_id in stale_jobs:
            print(f"[Worker '{self.consumer}'] Reclaimed stale job: {job_id}")
            self._process_message(msg_id, job_id)
    
    def _process_message(self, message_id: str, job_id: str):
        job = self.queue.get_job(job_id)

        if not job:
            print(f"Job {job_id} not found")
            self.queue.ack(message_id)
            return

        self.queue.update_status(job, JobStatus.PROCESSING)

        # Isolate job execution in a child process.
        pid = os.fork()

        if pid == 0:
            # --- CHILD PROCESS ---
            try:
                func, args, kwargs = job.unpack_payload()
                res = func(*args, **kwargs)
                print(res)

                self.queue.update_status(job, JobStatus.COMPLETED)
                self.queue.ack(message_id)
                print(f"[Child PID {os.getpid()}] completed job={job_id}")
                os._exit(0)

            except Exception as e:
                print(f"[Child PID {os.getpid()}] Job failed, job={job_id}: {e}")
                self.queue.update_status(job, JobStatus.RETRYING)
                self.queue.retry_job(job, message_id, error=str(e))
                os._exit(1)
        else:
            # --- PARENT PROCESS ---
            # Wait for the child process to finish.
            os.waitpid(pid, 0)
