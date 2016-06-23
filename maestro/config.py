# -*- coding: utf-8 -*-
# @Author: thomasopsomer
import os


######################################################
# Default configuration for the consul machine
######################################################

KS_NAME = "ks"
KS_DRIVER = "amazonec2"
KS_DRIVER_CONFIG = {
    "security_group": "default",
    "access_key": os.environ["AWS_ACCESS_KEY_ID"],
    "secret_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    "instance_type": "t2.nano",
    # "vpc_id": "default",
    "region": "eu-west-1",
    "root_size": 8
    # "zone": "a",
    # "subnet_id": "subnet-46932b30"
}


######################################################
# Default configuration for Spark master & workers
######################################################

# MASTER_DRIVER_CONF = {
#     "security_group": "default",
#     "access_key": os.environ["AWS_ACCESS_KEY_ID"],
#     "secret_key": os.environ["AWS_SECRET_ACCESS_KEY"],
#     "instance_type": "t2.micro",
#     "region": "eu-west-1",
#     # "zone": "a",
#     # "subnet_id": "subnet-46932b30"
#     # "vpc_id": "default"
# }
MASTER_DRIVER_CONF = {
    "security_group": "default",
    "access_key": os.environ["AWS_ACCESS_KEY_ID"],
    "secret_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    "instance_type": "m4.large",
    "region": "eu-west-1",
    "request_spot_instance": True,
    "spot_price": 0.2
    # "zone": "a",
    # "subnet_id": "subnet-46932b30"
    # "vpc_id": "default"
}

WORKER_DRIVER_CONF = {
    "security_group": "default",
    "access_key": os.environ["AWS_ACCESS_KEY_ID"],
    "secret_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    "instance_type": "m4.large",
    "region": "eu-west-1",
    # "zone": "a",
    # "subnet_id": "subnet-46932b30"
    "request_spot_instance": True,
    "spot_price": 0.2
    # "vpc_id": "default"
}

