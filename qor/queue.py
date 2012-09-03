#!/usr/bin/env python

import redis
import json
import time

class Queue:
    def __init__(self, queue='default', redishost='localhost', redisport=6379, redisdb=0):
        self.pool = redis.ConnectionPool(host=redishost, port=redisport, db=redisdb)
        self.redis = redis.Redis(connection_pool=self.pool)
        self.queue = queue
        self.qkey = 'q:%s' % queue

    def _encode(self, data):
        return json.dumps(data)

    def _decode(self, data):
        return json.loads(data)

    def _jkey(self, jobid):
        "Format Job Redis Key"
        return '%s:j:%s' % (self.qkey, jobid)

    def _deletejob(self, jobid):
        "Delete Job"
        # Job Redis Key
        jkey = self._jkey(jobid)
        return self.redis.delete(jkey)

    def _jobexists(self, jobid):
        "Check if a job exists"
        jkey = self._jkey(jobid)
        return self.redis.exists(jkey)

    def _timenow(self):
        return int(time.time())

    def size(self):
        "Queue Size (ready state)"
        return self.redis.llen('%s:ready' % self.qkey)

    def purgesize(self):
        "Purge Queue Size (complete/failed state)"
        return self.redis.llen('%s:finished' % self.qkey)

    def stats(self):
        "Stats about Queue"
        data = {
            'ready': self.size(),
            'working': self.redis.scard('%s:working' % self.qkey),
            'worked': self.redis.get('%s:worked' % self.qkey),
            'failed': self.redis.get('%s:failed' % self.qkey),
            'completed': self.redis.get('%s:completed' % self.qkey),
            'purge': self.purgesize()
        }

        # avoid None values
        for k in data:
            data[k] = int(data[k]) if data[k] != None else 0

        return data

    def addjob(self, data=[], expiretime=0):
        "Put a new Job in Queue"

        # JobID autoincrement
        jobid = self.redis.incr('%s:lastid' % self.qkey)
        
        # Job Redis Key
        jkey = self._jkey(jobid)

        # creation timestamp
        created_at = self._timenow()

        # expiretime
        expires_at = None
        if expiretime:
            expires_at = created_at + expiretime

        pipe = self.redis.pipeline()
        pipe.hmset(jkey, {
            'data': self._encode(data),
            'started_at': None,
            'created_at': created_at,
            'expires_at': expires_at,
            'finished_at': None,
            'state': 'ready',
            'worker': '',
            'message': ''
        })

        # put job in last position of queue
        pipe.rpush('%s:ready' % self.qkey, jobid)
        if pipe.execute():
            return jobid
        else:
            return False

    def jobdata(self, jobid):
        "Return a dict with Job Details"
        data = None

        if self._jobexists(jobid):
           data = self.redis.hgetall(self._jkey(jobid))
           data['data'] = self._decode(data['data'])
           for k in ['started_at', 'created_at', 'expires_at', 'finished_at']:
               data[k] = int(data[k]) if data[k] and data[k] != 'None' else None

        return data

    def nextjob(self):
        "Retrieve the next job (first) in queue"
        job = None

        # find the next non-expired job
        while True:
            jobid = self.redis.lpop('%s:ready' % self.qkey)
            if jobid == None:
                # no jobs
                break
            else:
                jobid = int(jobid)
                jobdata = self.jobdata(jobid)
                if jobdata:
                    # found job
                    if jobdata['expires_at'] != None and jobdata['expires_at'] <= self._timenow():
                        # job expired - delete it
                        self._deletejob(jobid)
                    else:
                        # job not expired - go ahead
                        break

        # If really found any job
        if jobid:
            pipe = self.redis.pipeline()

            # add it into working list
            pipe.sadd('%s:working' % self.qkey, jobid)

            # counts worked jobs
            pipe.incr('%s:worked' % self.qkey)

            # start timestamp
            started_at = self._timenow()

            jkey = self._jkey(jobid)

            # updates job state
            pipe.hmset(jkey, {
                'state': 'working',
                'started_at': started_at
            })
            pipe.execute()

        return jobid

    def finishjob(self, jobid, successful=True, message=''):
        "Finish successful and failed jobs"

        # check if job exists and state is 'working'
        if self._jobexists(jobid) and self.redis.sismember('%s:working' % self.qkey, jobid):
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
                'finished_at': self._timenow(),
                'message': message}
            )
            return pipe.execute()
        else:
            return False

    def nextjobpurge(self):
        "Retrieve the next job in finished queue"
        jobdata = None

        # find the next existent job
        while True:
            jobid = self.redis.lpop('%s:finished' % self.qkey)
            if jobid == None:
                # no jobs
                break
            else:
                jobid = int(jobid)
                jobdata = self.jobdata(jobid)
                if jobdata:
                    break

        return jobdata

if __name__ == '__main__':

    q = Queue('testing')
    
    # show stats
    print 'Queue Stats: %s' % q.stats()

    # add 10 jobs
    for i in range(10):
        jobid = q.addjob('Testing Job %d' % i)
        print "Added Job with ID %d" % jobid

    # show stats
    print 'Queue Stats: %s' % q.stats()

    # work until finish all jobs
    while True:
        jobid = q.nextjob()
        if jobid:
            print "Working Job ID %d" % jobid
            q.finishjob(jobid)
        else:
            print "No jobs left to work"
            break

    # show stats
    print 'Queue Stats: %s' % q.stats()

    # purge all finish jobs
    while True:
        jobdata = q.nextjobpurge()
        if jobdata:
            print "Purge Job %s" % jobdata
        else:
            print "No jobs left to purge"
            break

    # show stats
    print 'Queue Stats: %s' % q.stats()
