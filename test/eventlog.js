var eventlog = require('../index').eventlog

//you can start watching on your htcondor eventlog
eventlog.watch("/var/log/condor/EventLog");

//and receive events
eventlog.on(function(ads) {
    console.dir(ads);
});
