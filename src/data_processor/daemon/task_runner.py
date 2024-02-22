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

import os
from .process_runner import ProcessRunner
from eocis_data_manager.task import Task

thisdir = os.path.split(__file__)[0]


class TaskRunner:
    """
    Manage the execution of a task in a sub-process
    """

    def __init__(self):
        pass

    def run(self, task:Task) -> bool:
        """
        Run the task in a sub-process
        :param task: task to run
        :return: True iff the task succeeded
        """
        name = task.get_task_name()
        conda_path = "/home/dev/miniconda3/bin/conda"
        if not os.path.exists(conda_path):
            conda_path = "/home/dev/anaconda3/bin/conda"
        env = {
            "PYTHONPATH": "/home/dev/github/data-processor/src",
            "CONDA_PATH": conda_path,
            "OUT_PATH": "/tmp"
        }
        for (k,v) in task.get_spec().items():
            if isinstance(v,list):
                v= ",".join(v)
            elif isinstance(v,int) or isinstance(v,float):
                v = str(v)
            env[k] = v

        if task.get_task_type() == "regrid":
            script = os.path.join(thisdir, "regrid_task_runner.sh")
        else:
            script = os.path.join(thisdir, "subset_task_runner.sh")
        cmd = script
        pr = ProcessRunner(cmd, name=name, env_vars=env)
        (retcode, timedout) = pr.run()
        return retcode == 0 and not timedout



