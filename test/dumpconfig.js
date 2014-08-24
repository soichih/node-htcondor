
htcondor = require("../index.js");

htcondor.dumpconfig().then(function(configs) {
    console.dir(configs);
}).catch(function(err) {
    console.log("error occured");
    console.dir(err);
});

