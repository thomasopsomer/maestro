# -*- coding: utf-8 -*-
# @Author: thomasopsomer
from __future__ import absolute_import
import logging


# 1. logger
logger = logging.getLogger("maestro")
logger.setLevel(logging.INFO)

# 2. Add handler
steam_handler = logging.StreamHandler()
steam_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
steam_handler.setFormatter(formatter)
logger.addHandler(steam_handler)
