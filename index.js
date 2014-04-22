var fs = require('fs');
var spawn = require('child_process').spawn;
var extend = require('util')._extend;

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

exports.submit = function(submit_options, config) {
    var deferred = Q.defer();

    //set default config
    config = extend({
        tmpdir: '/tmp'
    }, config);

    var submit = temp.createWriteStream({dir: config.tmpdir, prefix:'htcondor-submit.'});
    temp.open({dir: config.tmpdir, prefix:'htcondor-log.'}, function(err, log) {
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

            //need to be quoted
            case "on_exit_hold_reason":
            case "periodic_hold_reason":
                submit.write(key+"=\""+addslashes(value)+"\"\n");
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

    var p = spawn(cmd, opts);//, {cwd: __dirname});

    //load event
    var stdout = "";
    p.stdout.on('data', function (data) {
        stdout += data;
    });
    var stderr = "";
    p.stderr.on('data', function (data) {
        stderr += data;
    });
    p.on('error', deferred.reject);
    p.on('close', function (code, signal) {
        if (signal) {
            deferred.reject(cmd+ " was killed by signal "+ signal);
        } else if (code !== 0) {
            deferred.reject(cmd+ " failed with exit code "+ code+ "\nSTDERR:"+ stderr + "\nSTDOUT:"+ stdout);
        } else {
            deferred.resolve(stdout, stderr);
        }
    });
    return deferred.promise;
}

exports.remove = function(opts, callback) {
    //console.log("calling condor_rm");
    //console.dir(opts);
    return condor_simple('condor_rm', opts).nodeify(callback);
    /*
    if(typeof id === 'array') {
        console.log("array given. passing everything");
        console.dir(id);
        return condor_simple('condor_rm', id).nodeify(callback);
    } else {
        //must be a single id
        return condor_simple('condor_rm', [id]).nodeify(callback);
    }
    */
};
exports.release = function(id, callback) {
    return condor_simple('condor_release', [id]).nodeify(callback);
};
exports.hold = function(id, callback) {
    return condor_simple('condor_hold', [id]).nodeify(callback);
};
exports.q = function(id, callback) {
    //console.log("condor_q -long -xml "+id);
    var deferred = Q.defer();
    
    var args=['-long', '-xml'];
    if(id)
        args.push(id);
    
    condor_simple('condor_q', args).then(function(stdout, stderr) {
        //parse condor_q output
        XML.parse(stdout, function(err, attrs) {
            if(err) {
                deferred.reject(err);
            } else if(attrs) {

                if (!attrs.c) {
                    if (id) //the requested job was not found... error
                        deferred.reject("Query for job "+id+" returned nothing");
                    else //no query was specified => there are no jobs
                        deferred.resolve([]);
                } else {
                
                    //if not array, wrap in array
                    var cs=Array.isArray(attrs.c)? attrs.c: [attrs.c];
                
                    var jobs=cs.map(function(c) {
                    
                        var events = {};
                        c.a.forEach(function(attr) {
                            var name = attr['@'].n;
                            events[name] = parse_attrvalue(attr);
                        });
                        return events;
                    });

                    //return one single job if only one was requested
                    if(id && jobs.length>0)
                        deferred.resolve(jobs[0]);
                    else //return an array of jobs otherwise
                        deferred.resolve(jobs);
                }
            }
        });
    }, deferred.reject);
    deferred.promise.nodeify(callback);
    return deferred.promise;
};

/* condor_history blocks!!!! WHY!
exports.history = function(id, callback) {
    //console.log("condor_q -long -xml "+id);
    var deferred = Q.defer();
    condor_simple('condor_history', [id, '-long', '-xml']).then(function(stdout, stderr) {
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
*/

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

