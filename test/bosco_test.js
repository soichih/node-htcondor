
htcondor = require("../index.js");

//htcondor.config['condorLocation'] = "/Users/derekweitzel/bosco"
//htcondor.config['condorConfig'] = "/Users/derekweitzel/bosco/etc/condor_config"

htcondor.q().then(function(jobs) {
  console.log("Got jobs");
  console.log(jobs);



}, function(error) {

  console.log("Failed Jobs");
  console.log(error);


});
