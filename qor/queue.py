#!/usr/bin/env python

import redis
import time

class Queue:
    def __init__(self, queue='default', redishost='localhost', redisport=6379, redisdb=0):
        self.pool = redis.ConnectionPool(host=redishost, port=redisport, db=redisdb)
        self.redis = redis.Redis(connection_pool=self.pool)
        self.queue = queue
        self.qkey = 'q:%s' % queue

    def _jkey(self, id):
        "Format Job Redis Key"
        return '%s:j:%s' % (self.qkey, id)

    def size(self):
        "Queue Size (ready state)"
        return self.redis.llen('%s:ready' % self.qkey)

    def stats(self):
        "Stats about Queue"
        data = {
            'ready': self.size(),
            'working': self.redis.scard('%s:working' % self.qkey),
            'worked': self.redis.get('%s:worked' % self.qkey),
            'failed': self.redis.get('%s:failed' % self.qkey),
            'completed': self.redis.get('%s:completed' % self.qkey)
        }

        # avoid None values
        for k in data:
            data[k] = data[k] if data[k] != None else 0

        return data

    def addjob(self, data=[]):
        "Put a new Job in Queue"

        # JobID autoincrement
        jobid = self.redis.incr('%s:lastid' % self.qkey)
        
        # Job Redis Key
        jkey = self._jkey(jobid)

        pipe = self.redis.pipeline()
        pipe.hmset(jkey, {
            'data': data,
            'started_at': None,
            'created_at': time.time(),
            'finished_at': None,
            'state': 'ready',
            'worker': '',
            'message': ''
        })

        # put job in last position of queue
        pipe.rpush('%s:ready' % self.qkey, jobid)
        pipe.execute()

        return jobid

    def jobdata(self, jobid):
        "Return a dict with Job Details"
        return self.redis.hgetall(self._jkey(jobid))

    def nextjob(self):
        "Retrieve the next job (first) in queue"
        job = None

        jobid = self.redis.lpop('%s:ready' % self.qkey)

        # If really found any job
        if jobid:
            pipe = self.redis.pipeline()

            # add it into working list
            pipe.sadd('%s:working' % self.qkey, jobid)

            # counts worked jobs
            pipe.incr('%s:worked' % self.qkey)

            jkey = self._jkey(jobid)

            # updates job state
            pipe.hmset(jkey, {
                'state': 'working',
                'started_at': time.time()
            })
            pipe.execute()
        
        return jobid

    def finishjob(self, jobid, successful=True, message=''):
        "Finish successful and failed jobs"

        pipe = self.redis.pipeline()

        # remove from working list
        pipe.srem('%s:working' % self.qkey, jobid)

        # put it into finished list
        pipe.rpush('%s:finished' % self.qkey, jobid)
        
        jkey = self._jkey(jobid)
        state = 'completed' if successful else 'failed'

        # counts failed/completed jobs
        pipe.incr('%s:%s' % (self.qkey, state))

        # updates job
        pipe.hmset(jkey, {
            'state': state,
            'finished_at': time.time(),
            'message': message}
        )
        pipe.execute()
