JOB_DEFINITIONS = [
    {
        "id": "daily_report_job",
        "trigger": "cron",
        "trigger_args": {"hour": 8, "minute": 0},
        "executor": "default",
        "dependencies": [],
        "function": "daily_report_job"
    },
    {
        "id": "frequent_health_check",
        "trigger": "interval",
        "trigger_args": {"seconds": 60},
        "executor": "default",
        "dependencies": [],
        "function": "frequent_health_check"
    },
    {
        "id": "data_ingestion_parent",
        "trigger": "cron",
        "trigger_args": {"hour": 9, "minute": 0},
        "executor": "default",
        "dependencies": [],
        "function": "data_ingestion_parent"
    },
    {
        "id": "data_processing_child",
        "trigger": "cron",
        "trigger_args": {"hour": 9, "minute": 5},
        "executor": "default",
        "dependencies": ["data_ingestion_parent"],
        "function": "data_processing_child"
    }
]