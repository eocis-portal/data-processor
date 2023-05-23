from eocis_data_manager.store import Store
from eocis_data_manager.job_operations import JobOperations
from eocis_data_manager.job_manager import JobManager
from eocis_data_manager.task import Task
from eocis_data_manager.job import Job

if __name__ == '__main__':
    store = Store()

    with JobOperations(store) as jo:
        jo.clear_task_queue()
        jo.removeTasksForJob("j0")
        jo.removeJob("j0")
        spec = {
            "LON_MAX": "5", "LON_MIN": "-12", "LAT_MIN": "45", "LAT_MAX": "65",
            "IN_PATH": "/home/dev/data/regrid/sst/{YEAR}/*/*/*.nc", "OUT_PATH": "/tmp",
            "SPATIAL_RESOLUTION": "1", "TEMPORAL_RESOLUTION": "monthly",
            "START_YEAR": "2022", "START_MONTH": "1", "START_DAY": "1",
            "END_YEAR": "2022", "END_MONTH": "12", "END_DAY": "31", "VARIABLES": ["analysed_sst"]
        }
        job = Job.create(spec, job_id="j0")
        jo.createJob(job)
    jm = JobManager(store)
    jm.create_tasks(job.getJobId())
