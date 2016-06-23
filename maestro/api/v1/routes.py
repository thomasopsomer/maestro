# -*- coding: utf-8 -*-
# @Author: thomasopsomer
from flask import Blueprint
from flask_restful import Api
from maestro.api.v1 import resources
from maestro.api.common import ERRORS
# from gevent import monkey
# monkey.patch_all()


def create_api():
    blueprint = Blueprint('maestro_blueprint_v1', __name__)
    api = Api(blueprint, errors=ERRORS, prefix="/api/v1")
    return blueprint, api


blueprint_v1, api = create_api()

# Simple Docker Machine API resources
api.add_resource(resources.MachineList, "/machines", endpoint="machines")
api.add_resource(resources.Machine, "/machines/<machine_name>", endpoint="machine")

# Maestro Cluster API resources
api.add_resource(resources.ClustersList, "/clusters", endpoint="clusters")
api.add_resource(resources.Clusters, "/clusters/<cluster_name>", endpoint="cluster")
