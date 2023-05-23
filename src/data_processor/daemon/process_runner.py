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


import subprocess
import threading
import os
import signal
import time

class ProcessMonitor(threading.Thread):

    def __init__(self, timeout, runner):
        threading.Thread.__init__(self)
        self.timeout = timeout
        self.runner = runner
        self.run_lock = threading.RLock()
        self.run_lock.acquire()

    def set_done(self):
        self.run_lock.release()

    def run(self):
        acquired = self.run_lock.acquire(timeout=self.timeout)
        if not acquired:
            # timed out
            try:
                self.runner.timeout_if_not_complete()
            except Exception as ex:
                print("ProcessMonitor Exception: "+str(ex))


class ProcessRunner:

    def __init__(self, cmd, env_vars, name, echo_stdout=True, log_path=None,
                 working_dir=None, timeout=-1, output_handler=None):
        self.cmd = cmd
        self.env_vars = env_vars
        self.name = name
        self.return_code = None
        self.sub = None
        self.echo_stdout = echo_stdout
        self.log_path = log_path
        self.log_file = None
        self.working_dir = working_dir
        self.timeout = timeout
        self.monitor = None
        self.timed_out = False
        self.output_handler = output_handler

    def run(self):
        if self.log_path:
            self.log_file = open(self.log_path,"w")
        if self.timeout > 0:
            self.monitor = ProcessMonitor(timeout=self.timeout,runner=self)
            self.monitor.start()

        self.sub = subprocess.Popen(self.cmd, env=self.env_vars, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    text=True, cwd=self.working_dir, preexec_fn=os.setsid)
        while self.return_code is None:
            self.handle_output(self.sub.stdout.readline())
            self.return_code = self.sub.poll()

        self.handle_output(self.sub.stdout.read())
        if self.log_path:
            self.log_file.close()

        if self.monitor is not None:
            self.monitor.set_done()
            self.monitor.join()

        return (self.sub.returncode, self.timed_out)

    def get_return_code(self):
        return self.return_code

    def timeout_if_not_complete(self):
        if self.return_code is None:
            self.timed_out = True
            os.killpg(os.getpgid(self.sub.pid), signal.SIGTERM)
            time.sleep(5)
            # make sure?
            try:
                os.killpg(os.getpgid(self.sub.pid), signal.SIGKILL)
            except:
                pass

    def handle_output(self,output):
        if output:
            if output.endswith("\n"):
                output = output[:-1]
            if self.echo_stdout:
                print("[%s:%s]: %s" % (self.name,str(self.sub.pid), output))
            if self.log_file:
                self.log_file.write(output+"\n")
            if self.output_handler:
                self.output_handler(output)


