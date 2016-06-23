# -*- coding: utf-8 -*-
# @Author: ThomasO
"""
Based on http://stackoverflow.com/questions/25360136/flask-with-create-app-sqlalchemy-and-celery
"""
from celery import Celery
from flask import Blueprint, Flask
from flask_restful import Api
from maestro.api.common import ERRORS

BROKER_URL = 'amqp://guest:guest@localhost:5672//'


def create_api():
    blueprint = Blueprint('maestro_blueprint_v1', __name__)
    api = Api(blueprint, errors=ERRORS, prefix="/api/v1")
    return blueprint, api


def create_celery_app(app):
    app = app or create_app()
    celery = Celery(__name__, broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    celery.Task = ContextTask
    return celery


def create_app(maestro_instance=None):
    """ """
    app = Flask(__name__)
    app.config['CELERY_BROKER_URL'] = BROKER_URL
    app.config['CELERY_RESULT_BACKEND'] = BROKER_URL
    if maestro_instance is not None:
        app.config["maestro"] = maestro_instance
    return app
