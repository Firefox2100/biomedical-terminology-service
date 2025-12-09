from celery import Celery

from bioterms.etc.consts import CONFIG


celery_app = Celery(
    'bioterms_worker',
    broker=CONFIG.celery_broker,
    backend=CONFIG.celery_backend,
)


celery_app.conf.update(
    imports=[
        'bioterms.task.cache',
    ]
)
