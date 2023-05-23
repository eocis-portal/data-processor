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

thisdir = os.path.split(__file__)[0]

class TaskRunner:

    def __init__(self):
        pass

    def run(self, task):
        name = task.getTaskName()
        env = {
            "PYTHONPATH": "/home/dev/github/data-processor/src",
            "CONDA_PATH": "/home/dev/anaconda3/bin/conda",
            "OUT_PATH": "/tmp"
        }
        for (k,v) in task.getSpec().items():
            print(k,v)
            if isinstance(v,list):
                v= ",".join(v)
            elif isinstance(v,int) or isinstance(v,float):
                v = str(v)
            env[k] = v

        script = os.path.join(thisdir,"task_runner.sh")
        cmd = script
        pr = ProcessRunner(cmd, name=name, env_vars=env)
        (retcode, timedout) = pr.run()
        return retcode == 0 and not timedout



