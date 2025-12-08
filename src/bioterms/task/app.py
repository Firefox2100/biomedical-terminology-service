from celery import Celery


celery_app = Celery(
    'bioterms_worker',
    broker='redis://localhost:6379/1',
    backend='redis://localhost:6379/2',
)


celery_app.conf.update(
    imports=[
        'bioterms.task.cache',
    ]
)
