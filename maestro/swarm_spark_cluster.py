# -*- coding: utf-8 -*-
# @Author: thomasopsomer
import logging
from functools import partial
import multiprocessing as mp
import machine
import docker

from helper import make_docker_compose_yml
from amazonec2_helper import find_cheapest_zone
from bridge import get_project
import config


logger = logging.getLogger('maestro')

NET_ETH = "eth0"


class SwarmSparkCluster(object):
    """
    """
    def __init__(self, ks_private_ip, cluster_name="MyCluster",
                 spark_image="theassets/spark-python",
                 provider="amazonec2", n_workers=2,
                 master_driver_conf={}, worker_driver_conf={}):
        """
        """
        # instantiate a docker machine client
        self.dm = machine.Machine()
        self.ks_private_ip = ks_private_ip

        # cluster state
        self.state = "not_created"

        # Docker image to use for spark
        self.spark_image = spark_image
        self.cluster_name = cluster_name
        # make docker-compose.yml file
        make_docker_compose_yml(path="./docker-compose.yml", spark_image=spark_image)

        # ensure driver is amazon for now
        if provider != "amazonec2":
            raise NotImplementedError("For now only works with aws")

        # master & worker machine configuration
        self.master_driver_conf = master_driver_conf or config.MASTER_DRIVER_CONF
        self.worker_driver_conf = worker_driver_conf or config.WORKER_DRIVER_CONF

        # TODO: cheapest zone in case of spot instance

        self.n_workers = n_workers

        # to record all machine names that belong to the cluster :)
        self.machine_names = []

    def launch_cluster(self):
        """ """
        # Make master for Swarm & Spark
        self.create_master(driver_options=self.master_driver_conf)

        # Make workers
        self.create_workers(self.n_workers)

        # update cluster state
        self.state = "running"
        logger.info("Cluster %s created. Visit %s:8080 to see Spark Master UI" %
                    (self.cluster_name, self.master_ip))

    def create_master(self, driver_name="amazonec2", driver_options={}):
        """
        # 1. Create a machine on aws to handle swarm and spark containers
        # 2. Run the Spark master containers on the node
        """
        # master name
        self.master_name = self.cluster_name + "-master"

        # check if a master of this name already exists
        if self.master_name in self.dm:
            self.master_ip = self.dm.ip(machine=self.master_name)
            self.machine_names.append(self.master_name)
            logger.info("A master: %s has been found" % self.master_name)
        else:
            logger.info("Creating master: %s ..." % self.master_name)
            # defined option for swarm master machine
            swarm_options = {"swarm_discovery": "consul://{}:8500".format(self.ks_private_ip),
                             "swarm_master": True}
            engine_opts = ["cluster-store=consul://{}:8500".format(self.ks_private_ip),
                           "cluster-advertise={}:2376".format(NET_ETH)]
            engine_labels = ["role=master"]
            master_driver_name = driver_name
            master_driver_options = driver_options or self.master_driver_conf

            # create the machine
            self.dm.create(name=self.master_name,
                           driver_name=master_driver_name, driver_config=master_driver_options,
                           engine_opts=engine_opts, engine_labels=engine_labels,
                           swarm=True, swarm_options=swarm_options,
                           verbose=True)
            self.machine_names.append(self.master_name)
            self.master_ip = self.dm.ip(machine=self.master_name)

            # Run the spark master container
            # Use the docker-compose.yml file. Modify it if needed
            # launch with docker-compose
            self.dm.eval_env(machine=self.master_name, swarm=True)
            docker.Client(**self.dm.config(machine=self.master_name))
            project = get_project(".")
            project.up(service_names=["master"])

    def create_workers(self, n_workers, driver_name="amazonec2", driver_options={}, offset=0):
        """ """
        cluster_name = self.cluster_name

        # Create the machines
        worker_swarm_options = {"swarm_discovery": "consul://{}:8500".format(self.ks_private_ip)}
        worker_driver_name = driver_name
        worker_driver_options = driver_options or self.worker_driver_conf
        engine_opts = ["cluster-store=consul://{}:8500".format(self.ks_private_ip),
                       "cluster-advertise={}:2376".format(NET_ETH)]
        engine_labels = ["role=worker"]

        kargs = {"worker_driver_name": worker_driver_name,
                 "worker_driver_options": worker_driver_options,
                 "engine_opts": engine_opts,
                 "worker_swarm_options": worker_swarm_options,
                 "engine_labels": engine_labels}

        partial_create_worker = partial(create_workers_mp, **kargs)

        # Launch workers in parallel
        pool = mp.Pool(processes=min(mp.cpu_count(), n_workers))
        pool.map(partial_create_worker,
                 map(lambda x: make_worker_name(cluster_name, x, offset=offset), range(n_workers)))
        pool.close()
        pool.join()

        # record worker names
        self.machine_names.extend([cluster_name + "-worker-%s" % x for x in range(n_workers)])

        # Run Spark Worker Container on them
        self.dm.eval_env(machine=self.master_name, swarm=True)
        docker.Client(**self.dm.config(machine=self.master_name))
        project = get_project(".")
        project.get_service("master").scale(desired_num=1)
        project.get_service("worker").scale(desired_num=n_workers)

    def add_workers(self, n_workers, worker_driver_options={}):
        """ """
        self.create_workers(n_workers, worker_driver_options, offset=self.n_workers)
        self.n_workers += n_workers

    def remove_workers(self, n_workers, force=False):
        """ """
        if n_workers > self.n_workers:
            if not force:
                raise ValueError("Too many worker to remove. Cluster have %s" % self.n_workers)
            else:
                logger.info("Removing all workers.")
                for k in range(self.n_workers):
                    self.dm.remove(machine=make_worker_name(self.cluster_name, k))
                logger.info("Removed %s workers. Remaining %s workers" % (self.n_workers, 0))

        else:
            logger.info("Removing %s workers." % n_workers)
            for k in range(n_workers):
                self.dm.remove(machine=make_worker_name(self.cluster_name, k))
            logger.info("Removed %s workers. Remaining %s workers" %
                        (n_workers, self.n_workers - n_workers))

    def get_master_ip(self):
        """ """
        if hasattr(self, "master_ip"):
            return self.master_ip
        else:
            raise AttributeError("Master not yet created")

    def stop(self):
        """ """
        for m in self.machine_names:
            self.dm.stop(m)
            logger.info("Machine: %s has been stoped" % m)
        # update cluster state
        self.state = "stopped"

    def start(self):
        """ """
        for m in self.machine_names:
            logger.info("Starting machine: %s ..." % m)
            self.dm.start(m)
            logger.info("Regenerating certificate for machine %s ..." % m)
            self.dm.regenerate(m)

    def kill(self):
        """ """
        for m in self.machine_names:
            logger.info("Removing machine: %s ..." % m)
            self.dm.rm(m)
        logger.info("Cluster: %s has been removed" % self.cluster_name)

    def run(self, container_name=None, docker_image=None, command=None, options={}):
        """ """
        self.dm.eval_env(machine=self.master_name, swarm=False)
        dc = docker.Client(**self.dm.config(machine=self.master_name))
        #
        command = command or "spark-submit --master spark://master:7077 --class org.apache.spark.examples.SparkPi /usr/spark/lib/spark-examples-1.6.0-hadoop2.6.0.jar"
        image = docker_image or "gettyimages/spark:1.6.0-hadoop-2.6"
        host_config = dc.create_host_config(network_mode="container:master")

        cont_task = dc.create_container(name=container_name, image=image, command=command,
                                        host_config=host_config, **options)
        dc.start(cont_task["Id"])
        dc.start()


def create_workers_mp(name, worker_driver_name, worker_driver_options,
                      engine_opts, worker_swarm_options, engine_labels):
    """ """
    m = machine.Machine()
    m.create(name, **{"driver_name": worker_driver_name,
                      "driver_config": worker_driver_options,
                      "engine_opts": engine_opts,
                      "engine_labels": engine_labels,
                      "swarm": True,
                      "swarm_options": worker_swarm_options,
                      "verbose": True})


def make_worker_name(cluster_name, i, offset=0):
    """ """
    if i < offset:
        raise ValueError("Index needs to be greater than offset")
    else:
        return cluster_name + "-worker-{}".format(i + offset)


def check_for_cheapest(driver_options):
    """ """
    cond = ("request_spot_instance" in driver_options and
            "region" in driver_options and
            "instance_type" in driver_options and
            "zone" not in driver_options)
    if cond:
        zone, price = find_cheapest_zone(region_name=driver_options["region"],
                                         instance_type=driver_options["instance_type"])
        driver_options["zone"] = zone
        return driver_options

