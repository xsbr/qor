QoR - Queue over Redis
======================

The main goal is provide a simple way to control a FIFO queue backed by a Redis Server.

This code still under development and not ready for production.

Job Flow
--------

Each Queue has three stages (lists):
* ready - jobs waiting to running
* working - jobs are running
* finished - jobs were finished (completed or failed)

```

                           *-> get next process job         return oldest finished <-*
                           |          and finish it <-*          job and delete it   |
           ,-------.       |       ,---------.        |        ,----------.          |
 addjob -> | ready | -> nextjob -> | working | -> finishjob -> | finished | -> nextjobpurge
   |       `-------'               `---------'                 `----------'
   |                                                                 |
   *-> producer add job               at finished list job can has <-*
                                    two states: completed or failed
```

Code Sample
-----------

Create Jobs on Producers:

```python
import qor

queue = qor.Queue('production')

# create first job
jobid1 = queue.addjob('First Job')

# you can send any type of value (expires in 10 seconds)
jobid2 = queue.addjob({'description': 'Second Job', 'systemid': 'a0b1c2d3e4'}, 10)

# show Queue Statistics
print queue.stats()
```

Running Jobs on Workers:

```python
import qor
import time

queue = qor.Queue('production')

# Run forever
while True:

    # get next job in queue
    jobid = queue.nextjob()

    if jobid == None:
        print "Waiting for new jobs..."
        time.sleep(1)
    else:
        # retrieve job data
        job = queue.jobdata(jobid)

        print "JobID %s - %s" % (jobid, job)

        # finish job
        queue.finishjob(jobid)
```
