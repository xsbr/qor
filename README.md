QoR - Queue over Redis
======================

The main goal is provide a simple way to control a FIFO queue backed by a Redis Server.

This code still under development and not ready for production.

Job Flow
--------

```

             ,-------.                 ,---------.                     ,----------.
 addjob() -> | ready | -> nextjob() -> | working | -> finishjob() ---> | complete |
             `-------'                 `---------'                 |   `----------'
                                                                   |
                                                                   `-> ,----------.
                                                                       |  failed  |
                                                                       `----------'

```

Code Sample
-----------

Create Jobs on Producers:

```python
queue = qor.Queue('production')

# create first job
jobid1 = queue.addjob('First Job')

# you can send any type of value
jobid2 = queue.addjob({'description': 'Second Job', 'systemid': 'a0b1c2d3e4'})

# show Queue Statistics
print queue.stats()
```

Running jobs on Workers:

```python
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
