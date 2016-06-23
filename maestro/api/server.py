# -*- coding: utf-8 -*-
# @Author: ThomasO
"""
Expose the Maestro class as a RESTful API.

URI:
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

Note:
    - using the spark ui API : http://spark.apache.org/docs/latest/monitoring.html

"""
from maestro import Maestro
from maestro.api.v1 import blueprint_v1
from maestro.api.factory import create_app
# from maestro.api.v2 import blueprint_v1 as blueprint_v2
from gevent.pywsgi import WSGIServer
# from gevent import monkey
import begin
# monkey.patch_all()


import logging
# logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', level=logging.INFO)
logger = logging.getLogger('maestro.api')


@begin.start
def main(aws_access_key=None, aws_secret_key=None):
    mm = Maestro(aws_access_key, aws_secret_key)
    app = create_app(mm)

    # register version through blueprint
    app.register_blueprint(blueprint_v1)
    # app.register_blueprint(blueprint_v2)

    # use gevent WSGI server instead of the Flask
    http = WSGIServer(('', 7777), app.wsgi_app)
    # TODO gracefully handle shutdown
    http.serve_forever()
    # app.run(debug=False, port=7777)
