# -*- coding: utf-8 -*-
# @Author: ThomasO
"""
Resources for the Spark Cluster API

    ### Clusters API

        * **/clusters**
            - GET (list of clusters)
            - POST (create)

        * **/clusters/<cluster_name>**
            - GET (info on cluster)
            - DELETE (remove)
            - PUT (update)
"""

from flask import current_app
from flask_restful import Resource, reqparse
from maestro.api.common import get_mm
from flask_restful import fields, marshal_with
from maestro.api.task import create_cluster_async


cluster_fields = {
    'cluster_name': fields.String,
    'n_workers': fields.Integer,
    'state': fields.String,
    'spark_image': fields.String,
    'cloud_provider': fields.String(attribute="provider"),
    #'machine_names': fields.List(fields.Url(endpoint="machine", absolute=True))
}


class ClustersList(Resource):
    """ """
    def __init__(self, *args, **kwargs):
        """ """
        super(ClustersList, self).__init__(*args, **kwargs)
        self.parser = reqparse.RequestParser()
        self.parser.add_argument("cluster_name", type=str, location="form", required=True)
        self.parser.add_argument("spark_image", type=str, location="form",
                                 default="gettyimages/spark:1.6.0-hadoop-2.6")
        self.parser.add_argument("n_workers", type=int, location="form", default=1)
        self.parser.add_argument("provider", type=str, location="form", default="amazonec2")
        self.parser.add_argument("master_driver_conf", type=dict, location="form", default={})
        self.parser.add_argument("worker_driver_conf", type=dict, location="form", default={})

    def get(self):
        cl = get_mm(current_app).list_clusters(verbose=False)
        return cl

    def post(self):
        # parse input post data
        # get_mm(current_app).add_spark_cluster(**self.parser.parse_args())
        maestro = get_mm(current_app)
        create_cluster_async.delay(maestro, **self.parser.parse_args())
        return {"message": "Cluster created"}


class Clusters(Resource):
    """ """
    def __init__(self, *args, **kwargs):
        """ """
        super(Clusters, self).__init__(*args, **kwargs)
        self.parser = reqparse.RequestParser()
        self.parser.add_argument("cluster_name", type=str, required=True)
        self.parser.add_argument("n_workers", type=int, required=True)

    @marshal_with(cluster_fields)
    def get(self, cluster_name):
        ssc = get_mm(current_app).get_cluster(cluster_name)
        return ssc

    def delete(self, cluster_name):
        get_mm(current_app).remove_cluster(cluster_name)
        return {"message": "Cluster %s removed" % cluster_name}

    def put(self, cluster_name, action=None):
        mm = get_mm(current_app)
        args = self.parser.parse_args()
        if action == "stop":
            mm.stop(cluster_name)
            return {"message": "Cluster %s has been stopped" % cluster_name}
        elif action == "start":
            mm.start(cluster_name)
            return {"message": "Cluster %s has been started" % cluster_name}
        elif action == "add_workers":
            mm.add_workers_to_cluster(args["cluster_name"], args["n_workers"])
            return {"message": "Added %s workers to cluster %s" %
                    (args["n_workers"], cluster_name)}
        elif action == "remove_workers":
            mm.remove_workers_from_cluster(args["cluster_name"], args["n_workers"])
            return {"message": "Removed %s workers from cluster %s" %
                    (args["n_workers"], cluster_name)}

