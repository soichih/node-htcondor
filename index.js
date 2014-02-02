var Tail = require('tail').Tail;
var lib = require('./lib');
var adparser = require('./lib').adparser;
var spawn = require('child_process').spawn;
var fs = require('fs');
var temp = require('temp');
var Promise = require('promise');
var XML = require('xml-simple');

// Automatically track and cleanup files at exit
temp.track();

exports.Joblog = function(path) {
    var callbacks = this.callbacks = [];

    this.tail = new Tail(path, "</c>\n");
    this.tail.on("line", function(xml) {
        xml +="</c>";
        parse_jobxml(xml, function(event) {
            //console.log("number of listeners:"+callbacks.length);
            callbacks.forEach(function(callback) {
                callback(event);
            });
        });
    });
    this.tail.unwatch(); //let user start watching
    
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

    function parse_jobxml(xml, callback) {
        //console.log(xml);
        XML.parse(xml, function(err, attrs) {
            if(err) {
                console.log("failed to parse job xml (skipping)");
                console.log(err);
                throw err;
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
    watch: function(call) {
        this.tail.watch();
        this.callbacks.push(call);
    },
    unwatch: function() {
        this.tail.unwatch();
    }
};

exports.submit = function(submit_options, callback) {
    function submit_exception(code, message) {
        this.code = code;
        this.message = message;
        this.name = "submit_exception";
    }
    function addslashes(str) {
        return (str + '').replace(/[\\"']/g, '\\$&').replace(/\u0000/g, '\\0');
    }

    var promise = new Promise(function(resolve, reject) {
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
                var joblog = new exports.Joblog(log.path);

                //submit!
                console.log("submit path:"+submit.path);
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
                        console.log("submit failed with code:"+code);
                        console.log(stderr);
                        reject("condor_submit failed with code: "+code);
                    } else {
                        //parse submit props
                        var lines = stdout.split("\n");
                        var empty = lines.shift();//condor_q returns empty line at the top..
                        var header = lines.shift(); //** Proc 49714580.0:
                        var header_tokens = header.split(" "); 

                        var jobid = header_tokens[2];
                        jobid = jobid.substring(0, jobid.length - 1).split(".");
                        var id = {
                            cluster: parseInt(jobid[0]),
                            proc: parseInt(jobid[1])
                        };

                        var props = adparser.parse(lines);

                        //a bit of fake to be props from xml joblog.. 
                        //var jobid = jobid.substring(0, jobid.length - 1).split(".");

                        //joblog.props.MyType = "SubmitEvent";
                        resolve({id: id, props: props, options: submit_options, log: joblog});
                    }
                });
            });
        });
    });
    return promise.nodeify(callback);
};

//run simple condor command that takes job id as an argument
function condor_simple(cmd, job, callback) {
    var promise = new Promise(function(resolve, reject) {
        //console.log("removing job:"+job.id);
        //console.dir(job.id);

        cmd = spawn(cmd, [job.id.cluster+"."+job.id.proc]);//, {cwd: __dirname});

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
            console.log(stdout);
            console.log(stderr);
            reject(err);
        });
        cmd.on('exit', function (code, signal) {
            //console.log(stdout);
            //console.log(stderr);
            if(code !== 0) {
                console.log("condor_remove failed with code:"+code);
                console.log(stdout);
                console.log(stderr);
                reject(code, signal);
            } else {
                resolve();
            }
        });
    });
    return promise.nodeify(callback);
}

exports.remove = function(job, callback) {
    return condor_simple('condor_rm', job, callback);
};

exports.release = function(job, callback) {
    return condor_simple('condor_release', job, callback);
};

exports.hold = function(job, callback) {
    return condor_simple('condor_hold', job, callback);
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

