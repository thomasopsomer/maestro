# -*- coding: utf-8 -*-
# @Author: thomasopsomer
"""
For now it only run with Amazon EC2 instance.

"""
from __future__ import absolute_import
import logging
import json

import machine
import docker

from .helper import manage_aws_credentials
from .swarm_spark_cluster import SwarmSparkCluster
from .config import *
from .errors import ClusterNameAlreadyExists, ClusterNotFoundError

logger = logging.getLogger('maestro')


class Maestro(object):
    """
    Maestro is the object that handled spark cluster creation
    using docker swarm.

    Maestro handle
        - Cluster management through SwarmSparkCluster creation
        - Tasks management ...

    """

    def __init__(self, aws_access_key=None, aws_secret_key=None, ssh_keypath=None,
                 config_path="./maestro.json"):
        """
        When a maestro in initiated, we create a t2.nano instance
        to run a consul db, that will keep up with connection between
        node, and deal with master recreation for instance (like zookeeper)
        """
        # grab config
        with open(config_path, 'r') as f:
            self.config = json.loads(f.read())

        # docker-machine client
        self.dm = machine.Machine(path="docker-machine")
        # key path if you want to be able to connect to ec2 instance on your own
        self.ssh_keypath = ssh_keypath

        # check aws credentials
        manage_aws_credentials(aws_access_key, aws_secret_key)

        if self.config["consul"]["driver_name"] == "local":
            logger.info("Running the consul container on host machine")
            # create docker client
            dc = docker.Client()
            # run the container on the consul locally
            run_consul_container(dc)
            logger.info("Consul container running locally on port 8500")
            self.ks_private_ip = get_my_ip()
        else:
            KS_NAME = self.config["consul"]["name"]
            # Get or create a small machine for the consul containers
            if KS_NAME in self.dm:
                self.ks_private_ip = self.dm.inspect(KS_NAME)["Driver"]["PrivateIPAddress"]
                logger.info("A consul machine has been found :)")
            else:
                logger.info("No consul machine found. Creating one...")
                # Create the machine for the KS machine
                self.dm.create(name=KS_NAME, driver_name=self.config["consul"]["driver_name"],
                               driver_config=self.config["consul"]["driver_config"],
                               verbose=True)

                logger.info("Machine created (t2.nano)")
                # run consul on that machine
                # connect to the docker client of the consul machine
                dc = docker.Client(**self.dm.config(machine=KS_NAME))
                # run the container on the consul machine
                run_consul_container(dc)
                # record consul machine information
                self.ks_name = KS_NAME
                self.ks_private_ip = self.dm.inspect(KS_NAME)["Driver"]["PrivateIPAddress"]
                logger.info("Consul container running on the `ks` on %s:8500" % self.ks_private_ip)

        # empty list of cluster
        self.clusters = {}

        # empty list of tasks
        self.tasks = []

    def __getitem__(self, item):
        """ """
        return self.get_cluster(item)

    def add_spark_cluster(self, cluster_name, spark_image="gettyimages/spark:1.6.0-hadoop-2.6",
                          provider="amazonec2", n_workers=2, master_driver_conf={},
                          worker_driver_conf={}, img_args=""):
        """ """
        if cluster_name not in self.clusters:
            ssc = SwarmSparkCluster(ks_private_ip=self.ks_private_ip,
                                    cluster_name=cluster_name,
                                    spark_image=spark_image,
                                    n_workers=n_workers,
                                    master_driver_conf=master_driver_conf,
                                    worker_driver_conf=worker_driver_conf,
                                    img_args="")
            # Launch the cluster
            ssc.launch_cluster()
            # Register cluster to maestro
            self.clusters[cluster_name] = ssc
        else:
            raise ClusterNameAlreadyExists("A cluster with this name already exists")

    def get_cluster(self, cluster_name):
        """ Return a SwarmSparkCluster given its name
        """
        try:
            return self.clusters[cluster_name]
        except:
            raise ClusterNotFoundError("No cluster named: %s" % cluster_name)

    def stop_cluster(self, cluster_name):
        """ """
        self.get_cluster(cluster_name).stop()

    def start_cluster(self, cluster_name):
        """ """
        self.get_cluster(cluster_name).start()

    def remove_cluster(self, cluster_name):
        """ """
        self.get_cluster(cluster_name).kill()
        # delete cluster from list of cluster in maestro
        del self.clusters[cluster_name]

    def list_clusters(self, verbose=True):
        """ """
        l = []
        LS_FIELDS = ["cluster_name", "state", "master_ip", "n_workers"]
        for cluster_name, ssc in self.clusters.iteritems():
            info = {}
            info["cluster_name"] = cluster_name
            info["state"] = ssc.state
            info["master_ip"] = ssc.master_ip
            info["n_workers"] = ssc.n_workers
            l.append(info)
        if verbose:
            print(machine.helper.format_as_table(data=l,
                                                 keys=LS_FIELDS,
                                                 header=LS_FIELDS,
                                                 sort_by_key=None,
                                                 sort_order_reverse=False))
        return l

    def add_workers_to_cluster(self, cluster_name, n_workers):
        """ """
        ssc = self.get_cluster(cluster_name)
        worker_conf = ssc.worker_driver_conf
        ssc.add_workers(n_workers, worker_driver_options=worker_conf)

    def remove_workers_from_cluster(self, cluster_name, n_workers):
        """ """
        ssc = self.get_cluster(cluster_name)
        ssc.remove(n_workers)

    def run_on_cluster(self, cluster_name, docker_image, command,
                       options={}, container_name=None):
        """ """
        ssc = self.get_cluster(cluster_name)
        cont_id = ssc.run(docker_image=docker_image, command=command,
                          container_name=container_name, options=options)
        self.tasks.append(cont_id)


def run_consul_container(dc):
    # pull the image
    dc.pull(repository="progrium/consul")
    # create the container with all options
    host_config = dc.create_host_config(port_bindings={8500: 8500})
    ks_container = dc.create_container(image="progrium/consul",
                                       command="-server -bootstrap",
                                       hostname="consul",
                                       ports=[8500],
                                       host_config=host_config,
                                       name="ks")
    # run the container
    dc.start(ks_container["Id"])
    return True


def get_my_ip():
    url = "http://checkip.amazonaws.com/"
    r = requests.get(url)
    return r.content[:-1]


if __name__ == "__main__":
    """ """
    from maestro import maestro
    ssh_keypath = "/Users/thomasopsomer/Dropbox/TheAssets_BI/AWS/mykp.pem"
    mm = maestro.Maestro()

    mm.add_spark_cluster(cluster_name="test", n_workers=1)



