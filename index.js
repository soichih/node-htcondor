var spawn = require('child_process').spawn;
var fs = require('fs');
var xml2js = require('xml2js');

var jobs_watching = {};

exports.init = function(options) {
    console.log("initializing");
    eventlog_listener(options);    
};

function eventlog_listener(options) {
    console.log("start eventlog listener");

    //tail eventlog
    var stats = fs.statSync(options.eventlog_path);
    var buffer = new Buffer(64);
    var block = "";
    var delim = "...\n";
    fs.watch(options.eventlog_path, function(event) {
        if(event == "rename") {
            console.log("eventlog rename event.. not sure how to handle it");
        } else if(event == "change") {
            fs.exists(options.eventlog_path, function(exists) {
                var new_stats = fs.statSync(options.eventlog_path);
                fs.open(options.eventlog_path, "r", function(err, fd) {
                    while(stats.size < new_stats.size) {
                        //read some in sync..
                        var size = fs.readSync(fd, buffer, 0, buffer.length, stats.size);
                        stats.size += size;
                        block = block + buffer.toString("ascii", 0, size);

                        //search for delimiter
                        var pos = block.indexOf(delim);
                        if(pos != -1) {
                            ablock = block.substring(0, pos);
                            block = block.substring(pos+delim.length);
                            var lines = ablock.split("\n");
                            var header = lines[0];
                            //parse header
                            var props = {
                                _eventid: parseInt(header.substring(0,3)),
                                _jobid: header.substring(5,21),
                                _timestamp: header.substring(23,37),
                                _updatetime: new Date()
                            };
                            //do we care?
                            var jobid_tokens = props._jobid.split(".");
                            var clusterid = jobid_tokens[0];
                            var procid = jobid_tokens[1];
                            if(jobs_watching[clusterid] != undefined) {
                                if(props.eventid == 28) {//class ad update event
                                    //parse event body
                                    var body = lines.slice(1);
                                    body.forEach(function(line) {
                                        if(line == "") return;
                                        var kv = line.split(" = ");
                                        //remove double quote from value if it's quoted
                                        if(kv[1].indexOf("\"") == 0) {
                                            kv[1] = kv[1].substring(1, kv[1].length-1);
                                        }
                                        //convert to int if its int
                                        var i = parseInt(kv[1]);
                                        if(i == kv[1]) {
                                            kv[1] = i;
                                        }
                                        props[kv[0]] = kv[1];
                                    });
                                    //console.log(clusterid);
                                    //console.log(procid);
                                    //console.dir(props);
                                    jobs_watching[clusterid](props);
                                }
                            } else {
                                //console.log("ignoring "+clusterid);
                            }
                        }
                    }
                    fs.close(fd);
                });
            });
        }
    });
}

exports.query = function(query, callback) {
    //submit!
    condor_query = spawn('condor_q', ['-xml', query]);
    var stdout = "";
    var stderr = "";
    var skipped_header = false;
    var xml_parser = new xml2js.Parser();
    var jobs = {};
    condor_query.stdout.on('data', function (data) {
        stdout += data;
        if(skipped_header == false) {
            var pos = stdout.indexOf("\n<c>\n");
           if(pos != -1) {
                stdout = stdout.substring(pos+1);
                skipped_header = true;
            }
        } 

        while(true) {
            var pos = stdout.indexOf("\n</c>\n");
            if(pos != -1) {
                var xml = stdout.substring(0, pos+5);
                stdout = stdout.substring(pos+6);
                //consume one xml block
                //console.log("parsing one block", stdout.length);
                xml_parser.parseString(xml, function(err, result) {
                    //console.dir(result); 
                    var classads = {};
                    result.c.a.forEach(function(record) {
                        var name;
                        var value;
                        for(var k in record) {
                            var v = record[k]; 
                            switch(k) {
                            case "$": name = v.n; break;
                            case "i": value = parseInt(v[0]); break;
                            case "r": value = parseFloat(v[0]); break;
                            case "e": value = v[0]; break; //TODO ... expression;
                            case "s": value = v[0]; break;
                            case "b": value = (v[0].$.v == "t"); break; 
                            default:
                                console.log("unknown element type:");
                                console.dir(record);
                            }
                        }
                        classads[name] = value;
                    });
                    //console.log("parsed one classads");
                    if(jobs[classads.ClusterId] == undefined) {
                        jobs[classads.ClusterId] = {};
                    }
                    jobs[classads.ClusterId][classads.ProcId] = classads;
                });
            } else break;
        }
    });
    condor_query.stderr.on('data', function (data) {
        stderr += data;
    });
    condor_query.on('close', function (code) {
        if(code !== 0) {
            console.dir(stderr);
            return undefined;
        } else {
            //console.log(jobs.length);
            //console.dir(jobs);
            callback(jobs);
        }
    });
}

exports.watch = function(jobs, callback) {
    for(var clusterid in jobs) {
        jobs_watching[clusterid] = callback;
    };
}

//this is just for experiments
exports.submit = function(options, success, error) {
    //create submit file
    var submit_file = "";
    submit_file += "universe="+options.universe+"\n";
    submit_file += "executable="+options.executable+"\n";
    submit_file += "log=/tmp/log.xml\n";
    submit_file += "queue\n";

    console.log("submitting following submit file");
    console.log(submit_file);
    
    //submit!
    condor_submit = spawn('condor_submit', ['-verbose']);
    condor_submit.stdin.write(submit_file);
    condor_submit.stdin.end();
    var stdout = "";
    condor_submit.stdout.on('data', function (data) {
        stdout += data;
    });
    condor_submit.stderr.on('data', function (data) {
        console.log('grep stderr: ' + data);
    });
    condor_submit.on('close', function (code) {
        if(code !== 0) {
            error();
        } else {
            //parse stdout
            var stdout_lines = stdout.split("\n");
            var job = {id: undefined, classads: {}};
            stdout_lines.forEach(function(line) {
                if(line == "") return;
                if(line.indexOf("** Proc ") == 0) {
                    job.id = line.substring(9, line.length-1);
                } else {
                    parse_classad(line, job.classads);
                }
            });
            success(job);
        }
    });
};

//parse something like
//JOB_GLIDEIN_ClusterId = "$$(GLIDEIN_ClusterId:Unknown)"
function parse_classad(line, classads) {
    var pos = line.indexOf(" = ");
    var k = line.substring(0, pos);
    var v = line.substring(pos+3);
    if(v[0] == '"') {
        v = v.substring(1, v.length-1);
    } else {
        if(v == "true") {
            v = true;
        } else if(v == "false") {
            v = false;
        } else v = parseFloat(v);
    }
    classads[k] = v;
}


