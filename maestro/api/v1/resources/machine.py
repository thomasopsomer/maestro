# -*- coding: utf-8 -*-
# @Author: ThomasO
"""
Resources for the Simple Docker Machine API

    ### Simple Docker Machine api

        * **/machines**
            - Get(list of machines)

        * **/machines/<machine_name>**
            - GET (info on machine)
            - POST (create machine)
            - DELETE (remove machine)
"""

from flask import current_app
from flask_restful import Resource, reqparse
from maestro.api.common import get_mm


class MachineList(Resource):
    """ """
    def __init__(self, *args, **kwargs):
        """ """
        super(MachineList, self).__init__(*args, **kwargs)
        self.parser = reqparse.RequestParser()
        self.parser.add_argument("name", type=str, location="form", required=True)
        self.parser.add_argument("driver_name", type=str, location="form", required=True)
        self.parser.add_argument("driver_config", type=dict, location="form", default={})
        self.parser.add_argument("engine_opts", type=list, location="form")
        self.parser.add_argument("engine_labels", type=list, location="form")
        self.parser.add_argument("swarm", type=bool, location="form")
        self.parser.add_argument("swarm_options", type=dict, location="form", default={})

    def get(self):
        ls = get_mm(current_app).dm.ls(pprint=False, timeout=100)
        return ls

    def post(self):
        """
        """
        get_mm(current_app).dm.create(**self.parser.parse_args())
        return {"message": "Machine created"}


class Machine(Resource):
    """ """
    def get(self, machine_name):
        info = get_mm(current_app).dm.inspect(machine_name)
        return info

    def delete(self, machine_name):
        get_mm(current_app).dm.rm(machine_name)
        return {"message": "Removed machine %s" % machine_name}

