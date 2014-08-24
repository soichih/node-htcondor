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
    console.log("Submitted.. not removing job");
    htcondor.remove(job);
}).catch(function(err) {
    console.log("submission rejected:"+err);
}).done();
