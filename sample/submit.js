var htcondor = require('../index.js');

var submit_options = {
    universe: "vanilla",
    executable: "test.sh",
    arguments: "hello",
    notification: "never",

    //transfer_output_files: 'bogus',

    //requirements: '(Arch == "INTEL") || (Arch == "X86_64") && (machine == "grid-client-1")',
    requirements: '(Arch == "INTEL") || (Arch == "X86_64")',

    shouldtransferfiles: "yes",
    when_to_transfer_output: "ON_EXIT",
    output: "stdout.txt",
    error: "stderr.txt",

    queue: 5
};

//for list of event.EventTypeNumber http://pages.cs.wisc.edu/~adesmet/status.html

var terminated = 0;

htcondor.submit(submit_options).then(function(job) {
    console.log("Submitted");
    //console.dir(job);

    htcondor.q(job, function(err, j) {
        console.log("condor_q info");
        console.dir(j);
    });

    job.onevent(function(event) {
        //console.dir(event);
        switch(event.MyType) {

        //normal status type events
        case "SubmitEvent":
        case "ExecuteEvent":
        case "JobImageSizeEvent":
            console.log(event.MyType + " on Proc:"+event.Proc);
            break;

        //critical events
        case "ShadowExceptionEvent":
            console.log(event.MyType + " on Proc:"+event.Proc);
            console.dir(event);
            break;

        //job ended
        case "JobTerminatedEvent":
            console.log(event.MyType + " on Proc:"+event.Proc);
            console.dir(event);

            //do something based on the ReturnValue
            console.log("returnvalue:"+event.ReturnValue);
            terminated++;
            if(terminated == 5) {
                console.log("all process finished");
                job.unwatch();
            }
            break;

        default:
            console.log(event.MyType);
            console.log("unknown event type.. stop watching");
            htcondor.remove(job);
            job.unwatch();
        }
    });
}).catch(function(err) {
    console.log("rejected:"+err);
}).done();
