
htcondor = require("../index.js");

//dump all
htcondor.q().then(function(jobs) {
    console.log("jobs :"+jobs.length);
    console.dir(jobs);
});

/*
//stream all
htcondor.q(function(err, job) {
    if(err) throw err;
    //this will be called as many times as there are jobs under user:donkri
    console.log(JSON.stringify(job, null, 4));
});
*/

/*
htcondor.q({id: "50938.178"}, 
function(err, job) {
    //this will be called as many times as there are jobs under user:donkri
    console.log(JSON.stringify(job, null, 4));
});
*/

/*
htcondor.q("50938.178", function(err, job) {
    //this will be called as many times as there are jobs under user:donkri
    console.log(JSON.stringify(job, null, 4));
});
*/

/*
htcondor.q({owner: "donkri"}, 
function(err, job) {
    //this will be called as many times as there are jobs under user:donkri
    console.log(JSON.stringify(job, null, 4));
});
*/

/*
htcondor.q({constraint: "JobStatus==5"}, function(err, job) {
    console.log(JSON.stringify(job, null, 4));
});
*/

/*
htcondor.q({constraint: "JobStatus==5", attributes: ["Iwd", "Owner", "JobStatus"]}, function(err, job) {
    if(err) {
        console.error(err);
    }
    console.log(JSON.stringify(job, null, 4));
});
*/

/*
htcondor.q({constraint: "JobStatus==5"}).then(function(jobs) {
    console.log(JSON.stringify(jobs, null, 4));
});
*/
