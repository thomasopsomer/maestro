# -*- coding: utf-8 -*-
# @Author: ThomasO

from flask import current_app
from maestro.api.factory import create_celery_app
# from maestro.api.common import get_mm


celery = create_celery_app(current_app)


@celery.task()
def create_cluster_async(maestro_instance, **kargs):
    maestro_instance.add_spark_cluster(**kargs)
