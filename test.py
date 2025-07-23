# --- tests/test_scheduler.py ---
import unittest
from unittest.mock import patch, MagicMock, ANY
from scheduler_app.core.scheduler import SchedulerManager, task_wrapper

@patch('scheduler_app.core.scheduler.MongoDBAdapter')
@patch('scheduler_app.core.scheduler.pymongo.MongoClient')  # <-- add this
@patch('apscheduler.schedulers.background.BackgroundScheduler')
class TestScheduler(unittest.TestCase):

    def test_scheduler_manager_initialization(self, mock_scheduler, mock_mongo_client, mock_mongo_adapter):
        mock_scheduler.return_value = MagicMock()
        mock_mongo_adapter.return_value = MagicMock()
        manager = SchedulerManager()
        self.assertIsNotNone(manager.scheduler)

    def test_schedule_all_jobs(self, mock_scheduler, mock_mongo_client, mock_mongo_adapter):
        scheduler_instance = mock_scheduler.return_value
        mock_mongo_adapter.return_value = MagicMock()
        manager = SchedulerManager()
        manager.schedule_all_jobs()
        scheduler_instance.add_job.assert_called()

    @patch('scheduler_app.core.scheduler.get_scheduler_manager')
    @patch('scheduler_app.jobs.functions.JOB_FUNCTIONS')
    def test_task_wrapper(self, mock_job_functions, mock_get_mgr, mock_scheduler, mock_mongo_client, mock_mongo_adapter):
        mock_mgr = MagicMock()
        mock_get_mgr.return_value = mock_mgr
        mock_job_functions.get.return_value.return_value = True
        task_wrapper('fetch_labels')
        mock_mgr.schedule_on_start_dependents.assert_called_once_with('fetch_labels', ANY)
