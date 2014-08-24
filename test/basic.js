var htcondor = require('../index');

describe('submit', function() {
    var job;
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
    it('should remove the job submitted earlier', function(done) {
        htcondor.remove(job).then(function(ret) {
            console.log(ret);
            done();
        }).catch(function(err) {
            done(err);
        });
    });
});
