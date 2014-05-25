
htcondor = require("../index.js");
path = require("path");


homeDir = process.env.HOME;
htcondor.config.condorLocation = path.join(homeDir, "bosco");
htcondor.config.condorConfig = path.join(homeDir, "bosco/etc/condor_config");

htcondor.q().then(function(jobs) {
  console.log("Got jobs");
  console.log(jobs);



}, function(error) {

  console.log("Failed Jobs");
  console.log(error);


});
