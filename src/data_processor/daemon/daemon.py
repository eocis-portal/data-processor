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
from eocis_data_manager.task import Task


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
                if task:
                    task.setRunning()
                    jo.updateTask(task)

            if task is not None:
                self.logger.info(f"Running task: {task.getTaskName()} in job: {task.getJobId()}")
                ts = TaskRunner()
                ok = ts.run(task)
                self.logger.info(f"Completed task (success:{ok}): {task.getTaskName()} in job: {task.getJobId()}")

                with JobOperations(self.store) as jo:
                    if ok:
                        task.setCompleted()
                    else:
                        retry_count = task.getRetryCount()
                        if retry_count < self.max_retries:
                            task.retry()
                            jo.queue_task(task.getJobId(),task.getTaskName())
                        else:
                            task.setFailed("Unknown error")
                    jo.updateTask(task)



if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--nr-threads", type=int, default=1)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    store = Store()
    Daemon.start_daemons(store, nr_threads=args.nr_threads)


