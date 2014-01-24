var htcondor = require('../index.js');

var submit_options = {
    universe: "vanilla",
    executable: "test.sh",
    arguments: "hello",
    notification: "never",

    //transfer_output_files: 'bogus',

    shouldtransferfiles: "yes",
    when_to_transfer_output: "ON_EXIT",
    output: "stdout.txt",
    error: "stderr.txt",
    queue: 1
};

/*
http://pages.cs.wisc.edu/~adesmet/status.html
event.EventTypeNumber will be set to one of following
Submit  0
Execute 1
Executable error        2
Checkpointed    3
Job evicted     4
Job terminated  5
Image size      6
Shadow exception        7
Generic 8
Job aborted     9
Job suspended   10
Job unsuspended 11
Job held        12
Job released    13
Node execute    14
Node terminated 15
Post script terminated  16
Globus submit   17
Globus submit failed    18
Globus resource up      19
Globus resource down    20
Remote error    21
*/

htcondor.submit(submit_options).then(function(job) {
    console.log("Submitted");
    console.dir(job);

    var joblog = job.log;
    joblog.watch(function(event) {
        //console.dir(event);
        switch(event.MyType) {

        //normal status type events
        case "SubmitEvent":
        case "ExecuteEvent":
        case "JobImageSizeEvent":
            console.log(event.MyType);
            break;

        //critical events
        case "ShadowExceptionEvent":
            console.log(event.MyType);
            console.dir(event);
            joblog.unwatch();
            break;

        //job ended
        case "JobTerminatedEvent":
            console.log(event.MyType);
            console.dir(event);

            //do something based on the ReturnValue
            console.log("returnvalue:"+event.ReturnValue);
            joblog.unwatch();
            break;

        default:
            console.log(event.MyType);
            console.log("unknown event type.. stop watching");
            joblog.unwatch();
        }
    });
});
