node-htcondor contains various nodejs hooks for htcondor.

To install:

```bash
npm install htcondor
```

## Submit jobs

You can submit a condor job and watch for joblog.

```
var htcondor = require('htcondor');

//create condor submit object
var submit_options = {
    universe: "vanilla",
    executable: "test.sh",
    arguments: "hello",
    notification: "never",

    //transfer_output_files: 'bogus',

    requirements: '(Arch == "INTEL") || (Arch == "X86_64") && (machine == "some_ce")',

    shouldtransferfiles: "yes",
    when_to_transfer_output: "ON_EXIT",
    output: "stdout.txt",
    error: "stderr.txt",
    queue: 1
};

//submit to start the job
htcondor.submit(submit_options).then(function(job) {
    console.log("Submitted");

    //you can dump the condor submit property for your new job
    //console.dir(job.props);

    //job.id contains cluster/proc ids
    //console.dir(job.id);

    //you can *watch* job log
    job.log.onevent(function(event) {
        switch(event.MyType) {

        //normal status type events (just display content)
        case "SubmitEvent":
        case "ExecuteEvent":
        case "JobImageSizeEvent":
            console.log(event.MyType);
            break;

        //critical events
        case "ShadowExceptionEvent":
            console.log(event.MyType);
            console.dir(event);

            //I now stop the watcher (ends this submission)
            job.log.unwatch();
            break;

        //job ended normally
        case "JobTerminatedEvent":
            console.log(event.MyType);
            console.dir(event);

            //Do something based on the ReturnValue (resubmit, submit different job, etc..)
            console.log("return value:"+event.ReturnValue);
            job.log.unwatch();
            break;

        //you might want to provide default in case of other events..
        default:
            console.log(event.MyType);
            console.log("unknown event type.. stop watching");
            job.log.unwatch();
        }
    });
});

```

You can do the usual condor stuff.

```
htcondor.remove(job).then(function() {
    console.log("successfully removed job");
});
```

```
htcondor.hold(job).then(function() {
    console.log("successfully held job");
});
```

```
htcondor.release(job).then(function() {
    console.log("successfully released job");
});
```

## Query Jobs

Wrapper for condor_q

```javascript
htcondor = require("htcondor");
htcondor.q({id: "59794891.0"},function(err, job) {
    console.log(JSON.stringify(job, null, 4));
});
```

Outputs job entry..

```
{
    "MATCH_EXP_JOB_GLIDEIN_Entry_Name": "CMS_T2_US_Caltech_cit",
    "MaxHosts": 1,
    "MemoryUsage": "expression:( ( ResidentSetSize + 1023 ) / 1024 )",
    "MATCH_EXP_JOBGLIDEIN_ResourceName": "CIT_CMS_T2",
    "MATCH_EXP_JOB_GLIDECLIENT_Name": "osg-flock-grid-iu-edu_OSG_gWMSFrontend.main",
    "AccountingGroup": "group_xsedehigh.psiders",
    "User": "psiders@osg-xsede.grid.iu.edu",
    "NumJobReconnects": 1,
    "OnExitHold": "expression:( ExitBySignal == true ) || ( ExitCode isnt 0 )",
    "MATCH_GLIDEIN_ClusterId": 1998341,
    "CoreSize": 0,
...
}

```

You can set various condor_q options.. like constraint, or attributes.

```javascript
htcondor = require("htcondor");
htcondor.q({
    constraint: "JobStatus==5", 
    attributes: ["Iwd", "Owner", "JobStatus"]
}, 
function(err, jobs) {
    console.log(JSON.stringify(jobs, null, 4));
}
);
```

You can also use then() to receive all job entries in a single array.

```
htcondor = require("htcondor");
htcondor.q({constraint: "JobStatus==5"}).then(function(jobs) {
    console.log(JSON.stringify(jobs, null, 4));
});
```

## eventlog watcher

This module allows you to subscribe to condor event log (usually at /var/log/condor/EventLog), and receive callbacks so that you can monitor job status or any other attribute changes.

If you are monitoring jobs that you submit, then you can just watch the job log instead(See above). eventlog watcher is to monitor the entire cluster.

Obviously though.. you need to have EventLog enabled on your condor installation. You need to have something like following in your condor config.d if you don't see your EventLog generated already.

[/etc/condor/config.d/90eventlog.config]
```
EVENT_LOG=$(LOG)/EventLog
EVENT_LOG_JOB_AD_INFORMATION_ATTRS=Owner,CurrentHosts,x509userproxysubject,AccountingGroup,GlobalJobId,QDate,JobStartDate,JobCurrentStartDate,JobFinishedHookDone,MATCH_EXP_JOBGLIDEIN_Site,RemoteHost
EVENT_LOG_MAX_SIZE = 1000000
EVENT_LOG_MAX_ROTATIONS = 5
```

```javascript
var eventlog = require('htcondor').eventlog

//you can start watching on your htcondor eventlog
eventlog.watch("/var/log/condor/EventLog");

//and receive events
eventlog.on(function(ads) {
    console.dir(ads);
});
````

eventlog.on() will be called for each classads posted. ads will look like following.

Why didn't I implement this more like fs.watch()? 2 reasons... You often need to start watch before you know which job id to watch (see submit sample below). Another reason is that, you usually don't have more than 1 EventLog, so there is no point of having multiple watcher watching the same log.

```javascript
{ _jobid: '49563264.000.000',
  _timestamp: '12/15 19:12:25',
  _updatetime: Sun Dec 15 2013 19:12:25 GMT+0000 (UTC),
  Proc: 0,
  EventTime: '2013-12-15T19:12:25',
  TriggerEventTypeName: 'ULOG_SUBMIT',
  SubmitHost: '<129.79.53.21:9615?sock=8287_a430_1068600>',
  QDate: 1387134745,
  TriggerEventTypeNumber: 0,
  MyType: 'SubmitEvent',
  Owner: 'donkri',
  CurrentHosts: 0,
  GlobalJobId: 'osg-xsede.grid.iu.edu#49563264.0#1387134745',
  Cluster: 49563264,
  AccountingGroup: 'group_xsedelow.donkri',
  Subproc: 0,
  EventTypeNumber: 28,
  CurrentTime: 'time()' }
```

Call unwatch() to stop watchin on eventlog

```javascript
eventlog.unwatch()
```

## dump condor config

(This is a prototype feature)

You can dump all condor configs (in key/value dictionary).

```javascript
htcondor.dumpconfig().then(function(configs) {
    console.dir(configs);
}).catch(function(err) {
    console.log("error occured");
    console.dir(err);
});
```

## Configuring the module

You may optionally configure the module by setting `config` variable if HTCondor binaries or configuration are located in a non-standard location.  Current options are:

<dl>
  <dt>CondorLocation</dt>
  <dd>The location of the HTCondor install.  The directory that contains htcondor's `bin`, `sbin`... directories.  This will be used when issuing commands by prepending the command with the full path.</dd>

  <dt>CondorConfig</dt>
  <dd>Location of the Condor configuration file.</dd>
</dl>


## Warning

This module watches job log for all jobs you submit using Tail - which uses inotify kernel hook). Don't submit
too many jobs at once (instead, throttle it - around 4000 - 5000 jobs each). You can see your current limit imposed by your OS by
looking at /proc/sys/fs/inotify/max_user_watches

I might implement such throttling capability built into this module in the future..

#License
MIT. Please see License file for more details.
