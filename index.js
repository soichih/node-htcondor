var fs = require('fs');
var spawn = require('child_process').spawn;
var extend = require('util')._extend;

var Tail = require('tail').Tail;
var adparser = require('./adparser').adparser;

var temp = require('temp');
var Q = require('q');
//var XML = require('xml-simple');
var xml2js = require('xml2js');

var path = require('path');

// Automatically track and cleanup files at exit
temp.track();

// Configuration for the module
exports.config = {
    condorLocation: null,
    condorConfig: null
}

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
                //see http://howtonode.org/understanding-process-next-tick
                process.nextTick(function() {
                    callback(event);
                });
            });
        });
    });

    var parser = new xml2js.Parser();
    function parse_jobxml(xml, callback) {
        //XML.parse(xml, function(err, attrs) {
        parser.parseString(xml, function(err, attrs) {
            if(err) {
                console.log("failed to parse job xml (skipping)");
                console.error(err);
                console.log(xml);
            } else {
                var event = {};
                attrs.c.a.forEach(function(attr) {
                    var name = attr.$.n;
                    event[name] = parse_attrvalue(attr);
                });
                callback(event);
            }
        });
    };
};
exports.Joblog.prototype = {
    onevent: function(call) {
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

function get_condor_env() {
    //add some extra env params
    var env = extend({}, process.env);
    if (exports.config.condorConfig) {
        env.CONDOR_CONFIG = exports.config.condorConfig;
    }
    if (exports.config.condorLocation) {
        env.PATH = 
                path.join(exports.config.condorLocation, 'bin') + ':' +
                path.join(exports.config.condorLocation, 'sbin') + ':' +
                env.PATH;
    }
    return env;
}

//run simple condor command that takes job id as an argument
function condor_simple(cmd, opts) {
    var deferred = Q.defer();
    var p = spawn(cmd, opts, {env: get_condor_env()});//, {cwd: __dirname});

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

exports.remove = function(config, callback) {
    //console.log("calling condor_rm");
    //console.dir(opts);
    var args = ['-totals'];

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //general opts
    if(config.name) { //name of scheduler
        args.push("-name");
        args.push(config.name);
    }
    if(config.pool) { //Use the given central manager to find daemons
        args.push("-pool");
        args.push(config.pool);
    }
    if(config.addr) { //Connect directly to the given "sinful string"
        args.push("-addr");
        args.push(config.addr);
    }
    if(config.reason) { //Use the given RemoveReason
        args.push("-reason");
        args.push(config.reason);
    }
    if(config.forces) { //Force the immediate local removal of jobs in the X state
        args.push("-forcex");
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //restriction-list
    if(config.id) { //job id (cluxster.proc)
        args.push(config.id);
    }
    if(config.owner) { //Remove all jobs owned by user
        args.push(config.owner);
    }
    if(config.constraint) { //Remove all jobs matching the boolean expression
        args.push("-constraint");
        args.push(config.constraint);
    }
    if(config.all) { //Remove all jobs (cannot be used with other constraints)
        args.push("-all");
    }

    return condor_simple('condor_rm', args).nodeify(callback);
};
exports.release = function(id, callback) {
    return condor_simple('condor_release', [id]).nodeify(callback);
};
exports.hold = function(id, callback) {
    return condor_simple('condor_hold', [id]).nodeify(callback);
};

//you can receive callbacks for each item, or use .then() to recieve list of all items
function condor_classads_stream(cmd, opts, item) {
    var deferred = Q.defer();
    var p = spawn(cmd, opts, {env: get_condor_env()});//, {cwd: __dirname});
    var buffer = "";
    var items = [];
    
    var parser = new xml2js.Parser();
    function getblock() {
        //look for start / end delimiter
        var s = buffer.indexOf("\n<c>\n");
        var e = buffer.indexOf("\n</c>\n");
        if(s != -1 && e != -1) {
            //var block = buffer.splice(spos, epos);
            xml = buffer.substring(s, e+5);
            buffer = buffer.substring(e+5);
            //XML.parse(xml, function(err, attrs) {
            parser.parseString(xml, function(err, attrs) {
                //console.dir(xml);
                if(err) {
                    console.log("failed to parse job xml (skipping)");
                    console.error(err);
                    console.log(xml);
                } else {
                    var event = {};
                    if(!attrs.c) {
                        event._no_attributes = true;
                    } else {
                        if(!attrs.c.a.forEach) {
                            attrs.c.a = [attrs.c.a];
                        }
                        attrs.c.a.forEach(function(attr) {
                            var name = attr.$.n;
                            event[name] = parse_attrvalue(attr);
                        });
                    }
                    if(item) {
                        item(null, event);
                    }
                    items.push(event);
                }
            });
            return true;
        } else {
            return false;
        }
    }
    p.stdout.on('data', function (data) {
        buffer += data.toString();
        while(getblock());
    });

    var stderr = "";
    p.stderr.on('data', function (data) {
        stderr += data;
    });
    p.on('error', function(err) {
        console.error(err);
        deferred.reject(err);
        item(err);
    });
    p.on('close', function (code, signal) {
        while(getblock());
        if (signal) {
            deferred.reject(cmd+ " was killed by signal "+ signal);
            if(item) {
                item({code: code, signal: signal, stdout: buffer, stderr: stderr});
            }
        } else if (code !== 0) {
            deferred.reject(cmd+ " failed with exit code "+ code+ "\nSTDERR:"+ stderr + "\nbuffer:"+ buffer);
            if(item) {
                item({code: code, signal: signal, stdout: buffer, stderr: stderr});
            }
        } else {
            deferred.resolve(items);
        }
    });
    return deferred.promise;
}

exports.q = function(config, item) {
    var args = ['-xml'];

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //general opts
    if(config.global) { //queue all schedulers in this pool
        args.push("-global");
    }
    if(config.submitter) { //get queue of specified submitter
        args.push("-submitter");
        args.push(config.submitter);
    }
    if(config.name) { //name of scheduler
        args.push("-name");
        args.push(config.name);
    }
    if(config.pool) { //use host as the central manager to query
        args.push("-pool");
        args.push(config.pool);
    }
    if(config.jobads) { //Read queue from a file of job ClassAds
        args.push("-jobads");
        args.push(config.jobads);
    }
    if(config.userlog) { //Read queue from a user log file
        args.push("-userlog");
        args.push(config.jobads);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //restriction-list
    if(config.id) { //job id (cluxster.proc)
        args.push(config.id);
    }
    if(config.owner) { //job id (cluxster.proc)
        args.push(config.owner);
    }
    if(config.constraint) { //Get information about jobs that match <expr>
        args.push("-constraint");
        args.push(config.constraint);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //output-opts
    if(config.attributes) {
        args.push("-attributes");
        args.push(config.attributes.join(","));
    }

    return condor_classads_stream('condor_q', args, item);
};

exports.drain = function(id, opts, callback) {
    opts=opts||[];
    opts.push(id);
    return condor_simple('condor_drain', opts).nodeify(callback);
};

exports.dumpconfig = function(callback) {
    var deferred = Q.defer();
    condor_simple('condor_config_val', ['-dump', '-expand']).then(function(out) {
        var outs = out.split("\n");
        var configs = {};
        outs.forEach(function(config) {
                if(config[0] == "#") return;
                if(config == "") return;
                var tokens = config.split(" = "); 
                //console.log(tokens[0] + " ... " + tokens[1]); 
                var key = tokens[0];
                var value = tokens[1];
                switch(value.toLowerCase()) {
                case "(null)":
                    value = null; break;
                case "false":
                    value = false; break;
                case "true":
                    value = true; break;
                }
                configs[key] = value;
        });
        deferred.resolve(configs);
    }).catch(function(err) {
        deferred.reject(err);
    });
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
};

//for various condor related codes
//https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers
exports.status_ids = {
    0: {label: "Unexpanded", code: "U"},
    1: {label: "Idle", code: "I"},
    2: {label: "Running", code: "R"},
    3: {label: "Removed", code: "X"},
    4: {label: "Completed", code: "C"},
    5: {label: "Held", code: "H"},
    6: {label: "Submission Error", code: "E"},
};
