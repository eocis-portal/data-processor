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

from eocis_data_manager.store import Store
from eocis_data_manager.job_operations import JobOperations
from eocis_data_manager.job_manager import JobManager
from eocis_data_manager.job import Job

if __name__ == '__main__':
    store = Store()

    with JobOperations(store) as jo:
        jo.clear_task_queue()
        jo.remove_tasks_for_job("j0")
        jo.remove_job("j0")
        spec = {
            "LON_MAX": "5", "LON_MIN": "-12", "LAT_MIN": "45", "LAT_MAX": "65",
            "IN_PATH": "/home/dev/data/regrid/sst/{YEAR}/*/*/*.nc", "OUT_PATH": "/tmp",
            "SPATIAL_RESOLUTION": "1", "TEMPORAL_RESOLUTION": "monthly",
            "START_YEAR": "2022", "START_MONTH": "1", "START_DAY": "1",
            "END_YEAR": "2022", "END_MONTH": "12", "END_DAY": "31", "VARIABLES": ["analysed_sst"]
        }
        job = Job.create(spec, job_id="j0")
        jo.create_job(job)
    jm = JobManager(store)
    jm.create_tasks(job.get_job_id())
