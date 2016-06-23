# Maestro


## Python API Usage

```python

from maestro import Maestro
import os


# init maestro
m = Maestro()


# set conf for driver
MASTER_DRIVER_CONF = {
    "security_group": "default",
    "access_key": os.environ["AWS_ACCESS_KEY_ID"],
    "secret_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    "instance_type": "r4.xlarge",
    "region": "eu-west-1",
    "ssh_keypath": "/Users/thomasopsomer/AWS/asgard.pem",
    "request_spot_instance": True,
    "spot_price": 0.2
    "zone": "a",
    "iam_instance_profile": "asgard",
    "root_size": "50"
}

# set conf for workers
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
    "root_size": "50"
    "request_spot_instance": True,
    "spot_price": 0.4
}


# cluster name and docker image
cluster_name = "ac"
spark_image = "asgard/asgard:develop"


# launch a spark cluster
m.add_spark_cluster(cluster_name,
					spark_image=spark_image,
                    provider="amazonec2",
                    n_workers=4,
                    master_driver_conf=MASTER_DRIVER_CONF,
                    worker_driver_conf=WORKER_DRIVER_CONF)


# run a task on the cluster
cmd = "spark-submit --master spark://master:7077 --class org.apache.spark.examples.SparkPi /usr/spark/lib/spark-examples-1.6.0-hadoop2.6.0.jar"

m.run_on_cluster(cluster_name=cluster_name,
                 docker_image=spark_image,
                 command=cmd)


```


## REST API Usage

- Start Rabbitmq and Celery
```bash
celery -A maestro.api.task worker --loglevel=info
```

- start maestro server
```
python maestro/api/server.py
```



## TODO

- Allow to install stuff on each machine on request

- Try flask + celery

- Add database layer with sqlite for cluster data persistence and finding all created cluster ...

- Issue with pydm: be sure that when machine is delete, the created keypair is also deleted

- Specify network for each cluster base on cluster name
