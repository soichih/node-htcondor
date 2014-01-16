var Tail = require('tail').Tail;
var lib = require('./lib');
var adparser = require('./lib').adparser;
var spawn = require('child_process').spawn;
var fs = require('fs');
var tmp = require('tmp');
var Promise = require('promise');
var XML = require('xml-simple');

exports.Joblog = function(path) {
    var callbacks = this.callbacks = [];

    //console.log("tailing");
    this.tail = new Tail(path, "</c>\n");
    //console.log("tailing");
    this.tail.on("line", function(xml) {
        xml +="</c>";
        parse_jobxml(xml, function(event) {
            //console.log("number of listeners:"+callbacks.length);
            callbacks.forEach(function(callback) {
                callback(event);
            });
        });
    });
    
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
    event: function(call) {
        //console.log("adding to callback");
        this.callbacks.push(call);
    },
    unwatch: function() {
        //console.log("unwatching");
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
        //create tmp file for submit file
        tmp.file({keep: true}, function(err, submit_path, submit_fd) {
            if (err) {
                reject(err);
            }

            //create tmp file for job log (xml)
            tmp.file({keep: true}, function(err, log_path) {
                submit_options['log'] = log_path;
                submit_options['log_xml'] = "True";

                //create submit file
                var out = fs.createWriteStream(submit_path);
                var queue = 1;
                for(key in submit_options) {
                    var value = submit_options[key];
                    switch(key) {
                    case "transfer_input_files":
                        //TODO handle case when value is not array
                        out.write(key+"="+value.join(",")+"\n");
                        break;
                    case "queue":
                        //don't write out until the end
                        queue = value;
                        break;
                    default:
                        if(key[0] == "+") {
                            //+attribute needs to be quoted
                            out.write(key+"=\""+addslashes(value)+"\"\n");
                        } else {
                            out.write(key+"="+value+"\n");
                        }
                    }
                }
                out.write("queue "+queue+"\n");
                out.end("\n", function() {
                    fs.close(submit_fd); //not sure if this is neede or not
                    
                    //console.log("submitted generated submit file:"+submit_path);

                    var joblog = new exports.Joblog(log_path);

                    //submit!
                    console.log("submit path:"+submit_path);
                    condor_submit = spawn('condor_submit', ['-verbose', submit_path]);//, {cwd: __dirname});

                    //why can't I wait until submit actually succedds? because condor
                    //publishes event at, or before submition succeeds. if we wait until
                    //we get all submit result, the event is already published 
                    //and client subscribing to submitevent will not hear from it.
                    //(work around maybe to store the submit event until someone subscribes for it later..)
                    resolve(joblog);

                    //load event
                    var stdout = "";
                    condor_submit.stdout.on('data', function (data) {
                        stdout += data;
                    });
                    var stderr = "";
                    condor_submit.stderr.on('data', function (data) {
                        stderr += data;
                    });
                    condor_submit.on('close', function (code) {
                        if(code !== 0) {
                            console.log("submit failed with code:"+code);
                            console.log(stderr);
                            throw stderr;
                            //reject("condor_submit failed with code: "+code);
                        } else {
                            /*
                            //parse submit props
                            var lines = stdout.split("\n");
                            var empty = lines.shift();//condor_q returns empty line at the top..
                            var header = lines.shift(); //** Proc 49714580.0:
                            var header_tokens = header.split(" "); 
                            var jobid = header_tokens[2];
                            jobid = jobid.substring(0, jobid.length - 1).split(".");
                            var props = adparser.parse(lines);

                            props._Cluster = parseInt(jobid[0]);
                            props._Proc = parseInt(jobid[1]);
                            
                            //now what do I do with this?
                            */
                        }
                    });
                });
        
            });
        });
    });
    return promise.nodeify(callback);
};

exports.eventlog = {
    tail: null,
    callbacks: [],
    on: function(callback) {
        this.callbacks.push(callback);
    },
    watch: function(path) {
        //console.log("eventlog lister listening on "+path);
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
                this.callbacks.forEach(function(callback) {
                    callback(props);
                });
            }
        });
    },
    unwatch: function() {
        this.tail.unwatch();
    }
}

