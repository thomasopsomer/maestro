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
    "vpc_id": "default"
}


######################################################
# Default configuration for Spark master & workers
######################################################

MASTER_DRIVER_CONF = {
    "security_group": "default",
    "access_key": os.environ["AWS_ACCESS_KEY_ID"],
    "secret_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    "instance_type": "t2.micro"
    # "vpc_id": "default"
}

WORKER_DRIVER_CONF = {
    "security_group": "default",
    "access_key": os.environ["AWS_ACCESS_KEY_ID"],
    "secret_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    "instance_type": "t2.micro",
    # "request_spot_instance": True,
    # "spot_price": 0.2
    # "vpc_id": "default"
}

