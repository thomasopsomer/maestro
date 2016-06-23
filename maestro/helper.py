# -*- coding: utf-8 -*-
# @Author: thomasopsomer

"""
Helper functions for maestro
"""
import os
# from machine.helper import format_as_table


def get_machine_by_name(dm, name):
    if name in [x["Name"] for x in dm.ls()]:
        # ip = dm.inspect(name)["Driver"]["PrivateIPAddress"]
        # return ip
        print "oog"
    else:
        return False


def check_aws_credentials():
    if (os.environ.get("AWS_ACCESS_KEY_ID", None) is not None and
       os.environ.get("AWS_SECRET_ACCESS_KEY") is not None):
        return True
    else:
        raise ValueError("AWS credentials are not in env variable")


def manage_aws_credentials(aws_access_key_id, aws_secret_access_key):
    """
    Make sure credential are in the OS ENV
    """

    # if user asks to change credential in our env
    if aws_access_key_id is not None:
        os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
    if aws_secret_access_key is not None:
        os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key

    # check
    check_aws_credentials()
    # # if we don't have credentials in our env we ask for them
    # if "AWS_ACCESS_KEY_ID" not in os.environ:
    #     msg_KEY_ID = "AWS_ACCESS_KEY_ID :"
    #     os.environ["AWS_ACCESS_KEY_ID"] = input(msg_KEY_ID)
    # if "AWS_SECRET_ACCESS_KEY" not in os.environ:
    #     msg_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY :"
    #     os.environ["AWS_SECRET_ACCESS_KEY"] = input(msg_ACCESS_KEY)


DOCKER_COMPOSE_YML = \
"""version: "2"
services:
    master:
        container_name: master
        image: {image}
        command: /usr/spark/bin/spark-class org.apache.spark.deploy.master.Master --host master {args}
        hostname: master
        environment:
            - constraint:role==master
            - constraint:cluster=={cluster_name}
        ports:
            - 4040:4040
            - 6066:6066
            - 7077:7077
            - 8080:8080
        expose:
            - "8081-8095"
        networks:
            - {network_name}
    worker:
        image: {image}
        command: /usr/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://master:7077 {args}
        environment:
            - constraint:role!=master
            - constraint:cluster=={cluster_name}
        ports:
            - 8081:8081
        expose:
            - "8081-8095"
        networks:
            - {network_name}

networks:
    {network_name}:
        driver: overlay
"""


def make_docker_compose_yml(cluster_name, network_name="default",
                            spark_image="gettyimages/spark:1.6.0-hadoop-2.6",
                            path="./docker-compose.yml", args=""):
    """
    Generate a compose file with custom image, cluster name and network
    """
    # fill the compose file
    dc_yml = DOCKER_COMPOSE_YML.format(
        image=spark_image, cluster_name=cluster_name,
        network_name=network_name, args=args)
    # write the compose file to disk
    with open(path, "w") as fout:
        fout.write(dc_yml)
    return True



