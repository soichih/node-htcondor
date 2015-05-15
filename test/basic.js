var htcondor = require('../index');
var chai = require('chai');

var should = chai.should;
var expect = chai.expect;

describe('submit', function() {
    this.timeout(1000*60*3); //3 minutes enouch?
    var job;
    it('should fail with missing executable', function(done) {
        htcondor.submit({
            universe: "vanilla",
            executable: "/bin/missing",
            queue: 1
        }).then(function(_job) {
            done("should have failed");
        }).catch(function(err) {
            done();
        });
    });
    it('should submit to vanilla', function(done) {
        htcondor.submit({
            universe: "vanilla",
            executable: "/bin/hostname",
            queue: 1
        }).then(function(_job) {
            job = _job;
            done();
        }).catch(function(err) {
            done(err);
        });
    });
    it('should query job', function(done) {
        htcondor.q(job).then(function(jobs) {
            expect(jobs.length).to.equal(1);
            console.log(jobs);
            done();
        }).catch(function(err) {
            done(err);
        });
    });
    it('should remove the job submitted earlier', function(done) {
        htcondor.remove(job).then(function(ret) {
            console.log(ret);
            done();
        }).catch(function(err) {
            done(err);
        });
    });

    it('should submit with output to /tmp', function(done) {
        htcondor.submit({
            universe: "vanilla",
            executable: "/bin/hostname",
            output: "/tmp/htcondor.test.out",
            queue: 1
        }).then(function(_job) {
            job = _job;
            done();
        }).catch(function(err) {
            done(err);
        });
    });
    it('should query job with job.id as an argument', function(done) {
        htcondor.q(job.id).then(function(ret) {
            console.log(ret);
            done();
        }).catch(function(err) {
            done(err);
        });
    });
    it('should remove the job with job.id as an argument', function(done) {
        htcondor.remove(job.id).then(function(ret) {
            console.log(ret);
            done();
        }).catch(function(err) {
            done(err);
        });
    });
    it('should submit to vanilla and receive event', function(done) {
        htcondor.submit({
            universe: "vanilla",
            executable: "/bin/hostname",
            queue: 1
        }).then(function(_job) {
            _job.onevent(function(event) {
                if(event.MyType == "ExecuteEvent") {
                    console.log("received ExecuteEvent");
                    //console.dir(event);
                    done();
                }
            });
        }).catch(function(err) {
            done(err);
        });
    });
    it('should submit to vanilla and remove via shortcut', function(done) {
        htcondor.submit({
            universe: "vanilla",
            executable: "/bin/hostname",
            queue: 1
        }).then(function(_job) {
            _job.onevent(function(event) {
                if(event.MyType == "ExecuteEvent") {
                    console.log("received ExecuteEvent .. removing via shortcut");
                    _job.remove(done);
                }
            });
        }).catch(function(err) {
            done(err);
        });
    });
});
