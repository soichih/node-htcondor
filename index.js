var fs = require('fs');
var spawn = require('child_process').spawn;

var Tail = require('tail').Tail;
var lib = require('./lib');
var adparser = require('./lib').adparser;

var temp = require('temp');
var Q = require('q');
var XML = require('xml-simple');

// Automatically track and cleanup files at exit
temp.track();

function parse_attrvalue(attr) {
    if(attr.s) {
        return attr.s;
    }
    if(attr.i) {
        return parseInt(attr.i);
    }
    if(attr.b) {
        if(attr.v == "t") return true;
        return false;
    }
    if(attr.r) {
        return parseFloat(attr.r);
    }
    if(attr.e) {
        //TODO
        return "expression:"+attr.e;
    }
    console.log("don't know how to parse");
    console.dir(attr);
}

exports.Joblog = function(path) {
    var callbacks = this.callbacks = [];

    //console.log("tailing joblog "+path);
    this.tail = new Tail(path, "</c>\n");
    this.tail.on("line", function(xml) {
        xml +="</c>";
        parse_jobxml(xml, function(event) {
            if(callbacks.length == 0) {
                //I will never catch SubmitEvent, since the callback isn't registered until 
                //after submission completes... But they get the job object which contains
                //pretty much the same info..
            }
            callbacks.forEach(function(callback) {
                callback(event);
            });
        });
    });

    function parse_jobxml(xml, callback) {
        XML.parse(xml, function(err, attrs) {
            if(err) {
                console.log("failed to parse job xml (skipping)");
                console.error(err);
                console.log(xml);
            } else {
                var event = {};
                attrs.a.forEach(function(attr) {
                    var name = attr['@'].n;
                    event[name] = parse_attrvalue(attr);
                }); 
                callback(event);
            }
        });
    };
};
exports.Joblog.prototype = {
    onevent: function(call) {
        //console.log("registering on event");
        this.callbacks.push(call);
    },
    unwatch: function() {
        this.tail.unwatch();
    }
};

function addslashes(str) {
    return (str + '').replace(/[\\"']/g, '\\$&').replace(/\u0000/g, '\\0');
}

exports.submit = function(submit_options) {
    var deferred = Q.defer();

    var submit = temp.createWriteStream('htcondor-submit.');
    temp.open('htcondor-log.', function(err, log) {
        submit_options['log'] = log.path;
        submit_options['log_xml'] = "True";

        //output submit file
        var queue = 1;
        for(key in submit_options) {
            var value = submit_options[key];
            switch(key) {
            case "transfer_input_files":
            case "transfer_output_files":
                value = [].concat(value); //force it to array if it's not
                submit.write(key+"="+value.join(",")+"\n");
                break;
            case "queue":
                //don't write out until the end
                queue = value;
                break;
            default:
                if(key[0] == "+") {
                    //+attribute needs to be quoted
                    submit.write(key+"=\""+addslashes(value)+"\"\n");
                } else {
                    submit.write(key+"="+value+"\n");
                }
            }
        }
        submit.write("queue "+queue+"\n");
        submit.end("\n", function() {
            //start watching joblog before submitting job
            var joblog = new exports.Joblog(log.path);

            if(submit_options.debug) {
                console.log("submit path:"+submit.path);
                fs.readFile(submit.path, 'utf8', function(err, data) {
                    console.log(data);
                });
            }

            //submit!
            condor_submit = spawn('condor_submit', ['-verbose', submit.path]);//, {cwd: __dirname});

            //load event
            var stdout = "";
            condor_submit.stdout.on('data', function (data) {
                stdout += data;
            });
            var stderr = "";
            condor_submit.stderr.on('data', function (data) {
                stderr += data;
            });
            //should I use exit instead of close?
            condor_submit.on('close', function (code, signal) {
                if(code !== 0) {
                    console.error("submit failed with code:"+code);
                    console.error(stdout);
                    console.error(stderr);
                    joblog.unwatch();
                    deferred.reject("condor_submit failed with code: "+code);
                } else {
                    //parse submit props
                    var lines = stdout.split("\n");
                    var empty = lines.shift();//condor_q returns empty line at the top..
                    var header = lines.shift(); //** Proc 49714580.0:
                    var header_tokens = header.split(" "); 
                    var jobid = header_tokens[2];
                    jobid = jobid.substring(0, jobid.length - 1); //remove last :
                    //jobid = jobid.split(".");
                    deferred.resolve({
                        //creating "job" object
                        id: jobid,
                        props: adparser.parse(lines), 
                        options: submit_options, 
                        log: joblog
                    });
                }
            });
        });
    });
    return deferred.promise;
};

//run simple condor command that takes job id as an argument
function condor_simple(cmd, opts) {
    var deferred = Q.defer();

    cmd = spawn(cmd, opts);//, {cwd: __dirname});

    //load event
    var stdout = "";
    cmd.stdout.on('data', function (data) {
        stdout += data;
    });
    var stderr = "";
    cmd.stderr.on('data', function (data) {
        stderr += data;
    });
    cmd.on('error', function (err) {
        console.dir(err);
        console.error(stderr);
        console.log(stdout);
        deferred.reject(err);
    });
    cmd.on('exit', function (code, signal) {
        if(code !== 0) {
            console.error(cmd+" failed with code:"+code);
            console.error(stderr);
            console.log(stdout);
            deferred.reject(code, signal);
        } else {
            deferred.resolve(stdout, stderr);
        }
    });
    return deferred.promise;
}

exports.remove = function(id, callback) {
    return condor_simple('condor_rm', [id]).nodeify(callback);
};
exports.release = function(id, callback) {
    return condor_simple('condor_release', [id]).nodeify(callback);
};
exports.hold = function(id, callback) {
    return condor_simple('condor_hold', [id]).nodeify(callback);
};
exports.q = function(id, callback) {
    var deferred = Q.defer();
    condor_simple('condor_q', [id, '-long', '-xml']).then(function(stdout, stderr) {
        //parse condor_q output
        XML.parse(stdout, function(err, attrs) {
            if(err) {
                console.error(err);
                deferred.reject(err);
            } else if(attrs) {
                var events = {};
                attrs.c.a.forEach(function(attr) {
                    var name = attr['@'].n;
                    events[name] = parse_attrvalue(attr);
                }); 
                deferred.resolve(events);
            }
        });
    });
    deferred.promise.nodeify(callback);
    return deferred.promise;
};

exports.eventlog = {
    tail: null,
    callbacks: [],
    on: function(callback) {
        this.callbacks.push(callback);
    },
    watch: function(path) {
        //console.log("eventlog lister listening on "+path);
        var $this = this;
        this.tail = new Tail(path, "...\n", {interval: 500});
        this.tail.on("line", function(ablock) {
            var lines = ablock.split("\n");
            var header = lines.shift();
            var eventid =  parseInt(header.substring(0,3));
            var props = {
                _jobid: header.substring(5,21),
                _timestamp: header.substring(23,37),
                _updatetime: new Date()
            };

            if(eventid == 28) { //Job ad information event
                //parse class ad key/value
                var props = adparser.parse(lines);
                $this.callbacks.forEach(function(callback) {
                    callback(props);
                });
            }
        });
    },
    unwatch: function() {
        this.tail.unwatch();
    }
}

