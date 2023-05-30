# -*- coding: utf-8 -*-

#    EOCIS data-processor
#    Copyright (C) 2023  National Centre for Earth Observation (NCEO)
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

import threading
import time
import logging

from eocis_data_manager.store import Store
from eocis_data_manager.job_operations import JobOperations
from eocis_data_manager.job_manager import JobManager

from .task_runner import TaskRunner

class Daemon(threading.Thread):

    daemons = {}

    def __init__(self, id, store, queue_poll_interval=2, max_retries=2):
        super().__init__()
        self.id = id
        self.store = store
        self.queue_poll_interval = queue_poll_interval
        self.max_retries = max_retries
        self.logger = logging.getLogger(f"daemon{self.id}")
        self.job_manager = JobManager(self.store)

    @staticmethod
    def start_daemons(store,nr_threads=2):
        for thread_nr in range(nr_threads):
            id = f"daemon{thread_nr}"
            daemon = Daemon(id,store)
            Daemon.daemons[id] = daemon
            daemon.start()

    def run(self):
        while True:
            time.sleep(self.queue_poll_interval)

            with JobOperations(self.store) as jo:
                task = jo.get_next_task()
                ran_ok = False
                if task:
                    task.set_running()
                    jo.update_task(task)
                    ran_ok = self.run_task(task, jo)

            if ran_ok:
                self.job_manager.zip_results(task)

            if task:
                self.job_manager.update_job(task.get_job_id())

    def run_task(self,task, jo):
        self.logger.info(f"Running task: {task.get_task_name()} in job: {task.get_job_id()}")
        ts = TaskRunner()
        ok = ts.run(task)
        self.logger.info(f"Completed task (success:{ok}): {task.get_task_name()} in job: {task.get_job_id()}")

        if ok:
            task.set_completed()
            jo.update_task(task)
            return True
        else:
            retry_count = task.get_retry_count()
            if retry_count < self.max_retries:
                task.retry()
                jo.queue_task(task.get_job_id(), task.get_task_name())
            else:
                task.set_failed("Unknown error")
            jo.update_task(task)
            return False




if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--nr-threads", type=int, default=1)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    store = Store()
    Daemon.start_daemons(store, nr_threads=args.nr_threads)


