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

//for list of event.EventTypeNumber http://pages.cs.wisc.edu/~adesmet/status.html

htcondor.submit(submit_options).then(function(job) {
    console.log("Submitted");
    console.dir(job);

    job.log.onevent(function(event) {
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
            job.log.unwatch();
            break;

        //job ended
        case "JobTerminatedEvent":
            console.log(event.MyType);
            console.dir(event);

            //do something based on the ReturnValue
            console.log("returnvalue:"+event.ReturnValue);
            job.log.unwatch();
            break;

        default:
            console.log(event.MyType);
            console.log("unknown event type.. stop watching");
            job.log.unwatch();
        }
    });
}).catch(function(err) {
    console.log("rejected:"+err);
}).done();
