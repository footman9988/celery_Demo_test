from __future__ import absolute_import
import logging.config
import redis
from kombu import Exchange, Queue

from celery.schedules import crontab

from datetime import timedelta


BROKER_URL = 'redis://192.168.118.71:6379/1'
CELERY_RESULT_BACKEND = 'redis://192.168.118.71:6379/2'

CELERY_TIMEZONE='Asia/Shanghai'


CELERY_QUEUES = (
    Queue('default', Exchange('default'), routing_key = "default"),
    Queue('for_task_A', Exchange('for_task_A'), routing_key= "for_task_A"),
    Queue('for_task_B', Exchange('for_task_B'), routing_key= 'for_task_B'),
)
CELERY_ROUTES = {
    'proj.tasks.TaskA':{"queue" : "for_task_A", "routing_key":"for_task_A"},
    'proj.tasks.TaskB':{"queue" : "for_task_B", "routing_key":"for_task_B"},
}


#crontab(hour=10,minute=56)  timedelta(seconds=5)

# schedules
CELERYBEAT_SCHEDULE = {
    'multiply-at-some-time': {
        'task': 'proj.tasks.TaskA',
        'schedule': crontab(hour=10,minute=15),   # 每天早上 6 点 00 分执行一次
        'args': ()                                  # 任务函数参数
    }
}


LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            # 'datefmt': '%m-%d-%Y %H:%M:%S'
            'format': '%(asctime)s  [%(levelname)s]- %(message)s'
        }
    },
    'handlers': {
        'celery': {
            # 'level': 'INFO',
            # 'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': 'my_Log.log',
            'when': 'midnight',
            'encoding': 'utf-8',
        },
    },
    'loggers': {
        'myapp': {
            'handlers': ['celery'],
            'level': 'INFO',
            'propagate': True,
        }
    }
}

logging.config.dictConfig(LOG_CONFIG)


CELERYD_CONCURRENCY = 10
CELERY_DEFAULT_QUEUE = 'default'
CELERY_DEFAULT_EXCHANGE = 'default'
CELERY_DEFAULT_ROUTING_KEY = 'default'