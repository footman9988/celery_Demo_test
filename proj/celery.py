from __future__ import absolute_import
from celery import Celery


from celery.utils.log import get_task_logger


app1 = Celery('proj', include=['proj.tasks'])
app1.config_from_object('proj.config')

logger = get_task_logger('myapp')








