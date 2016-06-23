# -*- coding: utf-8 -*-
# @Author: ThomasO
from maestro import Maestro
import requests
import os

###################################################
# Test Core
###################################################

m = Maestro()


MASTER_DRIVER_CONF = {
    "security_group": "default",
    "access_key": os.environ["AWS_ACCESS_KEY_ID"],
    "secret_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    "instance_type": "r4.large",
    "region": "eu-west-1",
    "ssh_keypath": "/Users/thomasopsomer/AWS/asgard.pem",
    "request_spot_instance": True,
    "spot_price": 0.2,
    "zone": "a",
    "iam_instance_profile": "asgard",
    "root_size": "50"
}

WORKER_DRIVER_CONF = {
    "security_group": "default",
    "access_key": os.environ["AWS_ACCESS_KEY_ID"],
    "secret_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    # "instance_type": "m4.large",
    "region": "eu-west-1",
    "instance_type": "r4.4xlarge",
    "ssh_keypath": "/Users/thomasopsomer/AWS/asgard.pem",
    "zone": "a",
    "iam_instance_profile": "asgard",
    "root_size": "50",
    "request_spot_instance": True,
    "spot_price": 0.4
}

cluster_name = "ac"
spark_image = "asgard/asgard:develop"
spark_image = "gettyimages/spark:1.6.0-hadoop-2.6"

m.add_spark_cluster(cluster_name,
					spark_image=spark_image,
                    provider="amazonec2",
                    n_workers=4,
                    master_driver_conf=MASTER_DRIVER_CONF,
                    worker_driver_conf=WORKER_DRIVER_CONF)


cmd = "spark-submit --master spark://master:7077 --class org.apache.spark.examples.SparkPi /usr/spark/lib/spark-examples-1.6.0-hadoop2.6.0.jar"

m.run_on_cluster(cluster_name=cluster_name,
                 docker_image=spark_image,
                 command=cmd)


###################################################
# Test Machines API
###################################################
root_url = "http://127.0.0.1:7777/api/v1"


# GET
requests.get(root_url + "/machines").json()

requests.get(root_url + "/machines/jimi")
requests.get(root_url + "/machines/nothere")

# POST
data = {
    "name": "jimi",
    "driver_name": "virtualbox"
}
r = requests.post(root_url + "/machines", data)

# DELETE
requests.delete(root_url + "/machines/jimi")


###################################################
# Test Cluster API
###################################################

# GET
requests.get(root_url + "/clusters").json()

# POST
data = {
    "cluster_name": "mc",
    "spark_image": "gettyimages/spark:1.6.2-hadoop-2.6",
    "n_workers": 2,
}
r = requests.post(root_url + "/clusters", data)


# DELETE
requests.delete(root_url + "/clusters/mycluster2")

