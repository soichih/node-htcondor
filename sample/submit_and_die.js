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
    console.log("Submitted.. now removing job");
    htcondor.remove(job).then(function(ret) {
        console.dir(ret);
        console.dir("done now.. unwatching job");
        job.log.unwatch();
    }).catch(function(err) {
        console.log("remove failed");
        console.dir(err);
    });
}).catch(function(err) {
    console.log("submission rejected:"+err);
}).done();
