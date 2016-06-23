# -*- coding: utf-8 -*-
# @Author: thomasopsomer

"""
TODO:
    - use thread instead of mp for creating several workers in //
    - get error when running docker compose  :/

"""
import logging
from functools import partial
import multiprocessing as mp
from multiprocessing.dummy import Pool as ThreadPool
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
                 spark_image="gettyimages/spark:1.6.0-hadoop-2.6",
                 provider="amazonec2", n_workers=2,
                 master_driver_conf={}, worker_driver_conf={}, img_args=""):
        """
        """
        # instantiate a docker machine client
        self.dm = machine.Machine()
        self.ks_private_ip = ks_private_ip

        # cluster state
        self.state = "not_created"

        # Docker image to use for spark
        self.spark_image = spark_image
        self.img_args = img_args
        self.cluster_name = cluster_name
        self.network_name = "%s-network" % cluster_name

        # make docker-compose.yml file
        make_docker_compose_yml(
            cluster_name=cluster_name, spark_image=spark_image,
            network_name=self.network_name,
            path="./docker-compose.yml", args=img_args)

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
        """
        Create a Cluster according to conf provided in constructor
        """
        # Create machines for master and workers
        self.create_machines(self.master_driver_conf, self.worker_driver_conf,
                             n_workers=self.n_workers)
        # Run containers
        self.run_containers(self.n_workers)
        # update cluster state
        self.state = "running"
        logger.info("Cluster %s created. Visit %s:8080 to see Spark Master UI" %
                    (self.cluster_name, self.master_ip))

    def create_machines(self,  master_driver_options={}, worker_driver_options={},
                        worker_driver_name="amazonec2", master_driver_name="amazonec2",
                        n_workers=2):
        """
        Create host machines for master and workers in parrallel using docker-machine
        """
        # master & workers name
        tasks = []
        # add master creation to task to execute
        tasks.extend(self._schedule_master(master_driver_name, master_driver_options))
        # add worker creation to tasks
        tasks.extend(self._schedule_workers(n_workers, worker_driver_name,
                                            worker_driver_options, offset=0))
        #
        n_node = len(tasks)
        # create all machine in //
        if n_node == 0:
            logger.info("No machine to create for cluster %s" % self.cluster_name)
        else:
            logger.info("Creating %s machines..." % n_node)
            # pool = mp.Pool(processes=min(mp.cpu_count(), n_workers))
            pool = ThreadPool(n_node)
            pool.map(create_node_wp, tasks)
            pool.close()
            pool.join()
            logger.info("%s machine were successfully created" % n_node)
        # record name of machien created
        self.machine_names.extend([x["name"] for x in tasks])
        self.master_ip = self.dm.ip(machine=self.master_name)
        return True

    def run_containers(self, n_workers, spark_image=None):
        """ docker-compose up """
        # if spark_image is provided, generate the compose file
        if spark_image is not None:
            make_docker_compose_yml(self.cluster_name,
                                    path="./docker-compose.yml",
                                    spark_image=spark_image)

        # ~ log in swarm master
        self.dm.eval_env(machine=self.master_name, swarm=True)
        docker.Client(**self.dm.config(machine=self.master_name))
        # get docker compose project from file
        project = get_project(".")

        # Run the spark master container
        project.up(service_names=["master"])

        # Run Spark Worker Container on the workers
        try:
            # project.get_service("master").scale(desired_num=1)
            project.get_service("worker").scale(desired_num=n_workers)
        except:
            self.restart_containers()

    def stop_containers(self):
        """ docker-compose stop """
        # ~ log in swarm master
        self.dm.eval_env(machine=self.master_name, swarm=True)
        docker.Client(**self.dm.config(machine=self.master_name))
        # get docker compose project from file
        project = get_project(".")
        # stop the project
        project.stop()

    def restart_containers(self):
        """ docker-compose restart """
        self.dm.eval_env(machine=self.master_name, swarm=True)
        docker.Client(**self.dm.config(machine=self.master_name))
        project = get_project(".")
        project.stop()
        project.get_service("master").scale(desired_num=1)
        project.get_service("worker").scale(desired_num=self.n_workers)

    def _schedule_master(self, driver_name, driver_options):
        """ """
        tasks = []
        #
        self.master_name = self.cluster_name + "-master"
        # 
        if self.master_name in self.dm:
            self.master_ip = self.dm.ip(machine=self.master_name)
            self.machine_names.append(self.master_name)
            logger.info("A master: %s has been found" % self.master_name)
        else:
            swarm_options = {"swarm_discovery": "consul://{}:8500".format(self.ks_private_ip),
                             "swarm_master": True}
            engine_opts = ["cluster-store=consul://{}:8500".format(self.ks_private_ip),
                           "cluster-advertise={}:2376".format(NET_ETH)]
            engine_labels = ["role=master", "cluster=%s" % self.cluster_name]
            master_driver_name = driver_name
            master_driver_options = driver_options or self.master_driver_conf

            c = {"name": self.master_name,
                 "driver_name": master_driver_name,
                 "driver_config": master_driver_options,
                 "engine_opts": engine_opts,
                 "engine_labels": engine_labels,
                 "swarm": True,
                 "swarm_options": swarm_options,
                 "verbose": True}
            tasks.append(c)
        #
        return tasks

    def _schedule_workers(self, n_workers, driver_name, driver_options, offset=0):
        """ """
        #
        tasks = []
        # Check for existing workers
        k = 0
        ms = [x["name"] for x in self.dm.ls(pprint=False)]
        for wn in map(lambda x: make_worker_name(self.cluster_name, x, offset=offset),
                      range(n_workers)):
            if wn in ms:
                self.machine_names.append(wn)
                logger.info("A worker: %s has been found" % wn)
                k += 1
            # else:
            #     break
        n_workers = n_workers - k
        offset = k

        # Create the machines needed
        worker_swarm_options = {"swarm_discovery": "consul://{}:8500".format(self.ks_private_ip)}
        worker_driver_name = driver_name
        worker_driver_options = driver_options or self.worker_driver_conf
        engine_opts = ["cluster-store=consul://{}:8500".format(self.ks_private_ip),
                       "cluster-advertise={}:2376".format(NET_ETH)]
        # engine_labels = ["role=worker"]
        engine_labels = ["cluster=%s" % self.cluster_name]

        for i in range(n_workers):
            c = {"name": make_worker_name(self.cluster_name, i, offset=offset),
                 "driver_name": worker_driver_name,
                 "driver_config": worker_driver_options,
                 "engine_opts": engine_opts,
                 "swarm": True,
                 "swarm_options": worker_swarm_options,
                 "engine_labels": engine_labels,
                 "verbose": True}
            tasks.append(c)
        #
        return tasks

    def add_workers(self, n_workers, worker_driver_options={}):
        """ """
        raise NotImplementedError

    def remove_workers(self, n_workers, force=False):
        """ """
        if n_workers > self.n_workers:
            if not force:
                raise ValueError("Too many worker to remove. Cluster have %s" % self.n_workers)
            else:
                logger.info("Removing all workers.")
                for k in range(self.n_workers):
                    self.dm.remove(machine=make_worker_name(self.cluster_name, k))
                logger.info("Removed all workers.")

        else:
            logger.info("Removing %s workers." % n_workers)
            for k in reversed(range(n_workers)):
                self.dm.remove(machine=make_worker_name(self.cluster_name, k))
            logger.info("Removed %s workers. %s workers remaining." %
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
        self.state = "terminated"
        logger.info("Cluster: %s has been removed" % self.cluster_name)

    def run(self, command, docker_image, container_name=None, options={}):
        """ """
        self.dm.eval_env(machine=self.master_name, swarm=False)
        dc = docker.Client(**self.dm.config(machine=self.master_name))
        #
        host_config = dc.create_host_config(network_mode="container:master")
        # create container
        cont_task = dc.create_container(name=container_name, image=docker_image,
                                        command=command, host_config=host_config,
                                        **options)
        # run the container
        dc.start(cont_task["Id"])
        return cont_task["Id"]

    def install_on_cluster(self, bash_cmd, master=True):
        """
        Allow to run a bach_cmd on each container of the cluster.
        For instance if you need to install a package, or download
        something on all machine, you can use this method.
        """
        for machine_name in self.machine_names:
            exec(dm_client, machine, cmd)
        dm = self.dm
        tasks = [(dm, machine, cmd) for machine in self.machine_names]
        #
        logger.info('Running cmd: %s on each machine of the cluseter...')
        # run all cmd on each machine in //
        pool = ThreadPool(n_node)
        pool.map(exec_wp, tasks)
        pool.close()
        pool.join()


def create_node(name, driver_name, driver_config, engine_opts, engine_labels,
                swarm, swarm_options, verbose=True):
    """ """
    m = machine.Machine()
    m.create(name=name, driver_name=driver_name, driver_config=driver_config,
             engine_opts=engine_opts, engine_labels=engine_labels, swarm=swarm,
             swarm_options=swarm_options, verbose=verbose)
    return name


def create_node_wp(kwargs):
    return create_node(**kwargs)


def make_worker_name(cluster_name, i, offset=0):
    """ """
    # if i < offset:
        # raise ValueError("Index needs to be greater than offset")
    # else:
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


def exec_wp(args):
    return docker_exec(*args)


def docker_exec(dm_client, machine_name, cmd):
    """ """
    # docker eval env to use "machine"
    dm_client.eval_env(machine=machine_name, swarm=False)
    dc = docker.Client(**dm_client.config(machine=machine_name))
    # get spark worker or master container name
    r = dc.containers(filters={'name': 'maestro_'})
    if r:
        container_names = r.get("Names", None)
    # exec a command
    if container_names:
        ex = dc.exec_create(container=container_names[0], cmd=cmd)
        exec_start(ex)
        return True
    return False




