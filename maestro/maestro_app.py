# -*- coding: utf-8 -*-
# @Author: thomasopsomer
"""
Expose the Maestro class as a RESTful API.

URI:
    ### Simple Docker Machine api
    * **/machines**
        - Get(list of machines)
    * **/machines/<machine_name>**
        - GET (info on machine)
        - POST (create machine)
        - DELETE (remove machine)

    ### Clusters API
    * **/clusters**
        - GET (list of clusters)
    * **/clusters/<cluster_name>**
        - GET (info on cluster)
        - POST (create)
        - DELETE (remove)
        - PUT (update)

    ### Tasks API
    * **/tasks**
        - GET (list of tasks: finished, ongoing, waiting)
    * **/tasks/<task_id>**
        - GET (info)
        - POST (scheduled a task)
        - DELETE (cancel a taks)

Note:
    - using the spark ui API : http://spark.apache.org/docs/latest/monitoring.html

"""
import logging

from flask import Flask, request
from flask_restful import Resource, Api, abort
# from flask_restful import reqparse, fields, marshal_with
# from collections import OrderedDict

from maestro import Maestro

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


ROOT_URI = "maestro/api/v1/"


def create_app(aws_access_key=None, aws_secret_key=None):
    global mm

    mm = Maestro(aws_access_key=None, aws_secret_key=None)

    app = Flask(__name__)
    api = Api(app)
    return app, api


class MachineList(Resource):
    """ """
    def get(self):
        ls = mm.dm.ls(pprint=False)
        return ls


class Machine(Resource):
    """ """
    def get(self, machine_name):
        info = mm.dm.inspect(machine_name)
        return info

    def post(self):
        # todo:
        # parse data
        # create machine
        raise NotImplementedError

    def delete(self, machine_name):
        mm.dm.rm(machine_name)
        return {"message": "Removed machine %s" % machine_name}


if __name__ == "__main__":
    # init app and api
    app, api = create_app()

    # Set up the Api resource roting here
    api.add_resource(MachineList, "/machines")
    api.add_resource(Machine, "/machines/<machine_name>")

    app.run(debug=True)
