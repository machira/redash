from redash.worker import celery
from redash.version_check import run_version_check
from redash import models
from .base import BaseTask


@celery.task(name="redash.tasks.record_event", base=BaseTask)
def record_event(event):
    models.Event.record(event)


@celery.task(name="redash.tasks.version_check", base=BaseTask)
def version_check():
    run_version_check()

