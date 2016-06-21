# Maestro API design


# Start up of maestro

On the machine where you want maestro to be running, run start_maestro.sh
    It will create a container with consul
    Start the UI of maestro on port 8000


# Use case

Maestro orchestrate the deployment of cluster to execute a task contained in a docker

Maestro receive an call for
```json
    {
        "image": "asgard/spark-python",
        "cmd": "/home/myscript.sh"
        "cluster_spec": {
            "instance_type": 
                {
                    "master": "r3.large",
                    "worker": "r3.4xlarge"
                }
            "spot_price": "0.5",
            "size": "4"
        }
    }
```

Then it launch a park cluster with one master and 3 worker and run the spark task contained in "asgard/spark-python" with the command "/home/myscript.sh" 


# API Design

### Simple Docker Machine api
* **/machines**
    - Get(list of machines)
* **/machines/<machine_name>**
    - GET (info on machine)
    - POST (create machine)
    - DELETE (remove machine)

### Clusters API
* **/clusters**
    - GET (list of clusters)
* **/clusters/<cluster_name>**
    - GET (info on cluster)
    - POST (create)
    - DELETE (remove)
    - PUT (update)

### Tasks API
* **/tasks**
    - GET (list of tasks: finished, ongoing, waiting)
* **/tasks/<task_id>**
    - GET (info)
    - POST (scheduled a task)
    - DELETE (cancel a taks)
