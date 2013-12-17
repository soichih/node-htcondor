node-htcondor contains various nodejs hooks for htcondor.

Right now, the only module I am publishing is the htcondor eventlog watcher... (I will be publishing other classes soon!)

To install:

```bash
npm install htcondor
```

#eventlog:

This module allows you to subscribe to condor event log (usually at /var/log/condor/EventLog), and receive callbacks so that you can monitor job status or any other attribute changes.

You probably need to have something like following in your condor config.d if you don't see your EventLog generated.

[/etc/condor/config.d/90eventlog.config]
```
EVENT_LOG=$(LOG)/EventLog
EVENT_LOG_JOB_AD_INFORMATION_ATTRS=Owner,CurrentHosts,x509userproxysubject,AccountingGroup,GlobalJobId,QDate,JobStartDate,JobCurrentStartDate,JobFinishedHookDone,MATCH_EXP_JOBGLIDEIN_Site,RemoteHost
EVENT_LOG_MAX_SIZE = 1000000
EVENT_LOG_MAX_ROTATIONS = 5
```

```javascript
var eventlog = require('htcondor').eventlog

//you can start listening on your htcondor eventlog
eventlog.listen("/var/log/condor/EventLog");

//and receive events
eventlog.on(function(event) {
    console.dir(event);
});
````

Currently, on() will capture all classad update event (eventid 28). You will receive an object that looks like

```
{ _eventid: 28,
  _jobid: '49563264.000.000',
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

#License
MIT. Please see License file for more details.
