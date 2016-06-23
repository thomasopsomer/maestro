# -*- coding: utf-8 -*-
# @Author: ThomasO


# Custom error handling
ERRORS = {
    'MachineNotFoundError': {
        'message': "No machine found with this name",
        'status': 409
    },
    'MachineAlreadyExistsError': {
        'message': "A machine already exists with this name",
        'status': 410
    },
    'ClusterNameAlreadyExists': {
        'message': "A cluster already exists with this name",
        'status': 410
    },
    'ClusterNotFoundError': {
        'message': "No cluster found with this name",
        'status': 409
    }
}


def get_mm(current_app):
    """return a maestro client given the current flask app"""
    return current_app.config["maestro"]
