##################################################################################
### Module : genes.py
### Description : Gene API 
###
###
###
### Written by : scalphunter@gmail.com ,  2021/08/22
### Copyrights reserved by AtlasXomics
##################################################################################

### API
from flask import request, Response , send_from_directory
# import miscellaneous modules
import traceback
import json
from pathlib import Path
from . import utils 
from flask_cors import CORS

from celery import Celery
from celery.result import AsyncResult

class TaskAPI:
    def __init__(self,app,**kwargs):

        self.app=app
        CORS(self.app)
        self.tempDirectory=Path(self.app.config['TEMP_DIRECTORY'])
        self.broker="amqp://{}:{}@{}".format(self.app.config['RABBITMQ_USERNAME'],
                                              self.app.config['RABBITMQ_PASSWORD'],
                                              self.app.config['RABBITMQ_HOST'])
        self.backend="redis://:{}@{}".format( self.app.config['REDIS_PASSWORD'],
                                              self.app.config['REDIS_HOST'])
        self.celery=Celery('tasks', backend=self.backend, broker=self.broker)
        self.initialize()
        self.initEndpoints()

    def initialize(self):
        pass 

##### Endpoints

    def initEndpoints(self):


#### Task posting
        @self.app.route('/api/v1/task',methods=['POST'])
        def _runTask():
            sc=200
            res=None
            req=request.get_json()
            try:
                res=self.runTask(req)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("{} {}".format(str(e),exc),status_code=sc)
                self.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                # self.app.logger.info(utils.log(str(sc)))
                return resp

#### Task status check and retrieve the result if out
        @self.app.route('/api/v1/task/<task_id>',methods=['GET'])
        def _getTaskStatus(task_id):
            sc=200
            res=None
            try:
                res=self.getTask(task_id)
            except Exception as e:
                sc=500
                exc=traceback.format_exc()
                res=utils.error_message("{} {}".format(str(e),exc),status_code=sc)
                self.app.logger.exception(res['msg'])
            finally:
                resp=Response(json.dumps(res),status=sc)
                resp.headers['Content-Type']='application/json'
                # self.app.logger.info(utils.log(str(sc)))
                return resp  

    def createTaskObject(self, task_id, task_name, task_args, task_kwargs, queue, meta={}):
        task_obj={
            "_id": task_id,
            "name":task_name,
            "args":task_args,
            "kwargs":task_kwargs,
            "queue": queue,
            "requested_by": 'latch',
            "requested_at": utils.get_timestamp()
        } 
        task_obj.update(meta)
        return task_obj

    def runTask(self, req):
        kwargs = req.get('kwargs',{})
        kwargs['username'] = 'user'
        kwargs['group'] = 'latch'
        r=self.celery.send_task(req['task'],args=req['args'],kwargs=req['kwargs'],queue=req['queue'])
        task_object=self.createTaskObject(r.id, req['task'], req['args'], req['kwargs'], req['queue'], {})
        return task_object

    def runTaskSync(self, req):
        r=self.celery.send_task(req['task'],args=req['args'],kwargs=req['kwargs'],queue=req['queue'])
        task_object=self.createTaskObject(r.id, req['task'], req['args'], req['kwargs'], req['queue'], {})
        r.wait()
        return r.get()

    def getTask(self, task_id):
        task= AsyncResult(task_id, backend=self.celery.backend)
        try:
            task.ready()
            res={
                "task_id":task.id,
                "status":task.state
            }
            if task.state=="PROGRESS":
                res['progress'] = task.info.get('progress')
                res['position'] = task.info.get('position')
            if task.state=="SUCCESS":
                result=task.get()
                res['result']=result
        except Exception as e:
            raise Exception({"message": "Task has been failed","detail": str(e)})
        return res

    def get_workers(self):
        workers = []
        res = self.celery.control.inspect()

    def get_worker_tasks(self):
        worker_tasks = {}
        res = self.celery.control.inspect()
        active = res.active()
        reserved = res.reserved()
        for worker_name in active.keys():
            worker_tasks[worker_name] = []
            active_lis = active[worker_name]
            scheduled_lis = reserved[worker_name]
            worker_tasks[worker_name] = active_lis + scheduled_lis
        return worker_tasks


    def query_task(self, task_list):
        res = self.celery.control.inspect().query_task(task_list)
        print(res)
        return res

