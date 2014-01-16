var lib = require('./lib');
var adparser = require('./lib').adparser;
var spawn = require('child_process').spawn;
var fs = require('fs');
var tmp = require('tmp');
var Promise = require('promise');

function submit_exception(code, message) {
    this.code = code;
    this.message = message;
    this.name = "submit_exception";
}

function addslashes(str) {
    return (str + '').replace(/[\\"']/g, '\\$&').replace(/\u0000/g, '\\0');
}

exports.submit = function(submit_options, callback) {
    var promise = new Promise(function(resolve, reject) {

        //create tmp file for submit file
        tmp.file({keep: true}, function(err, submit_path) {
            if (err) {
                if(callback) {
                    callback(err);
                }
                reject(err);
            }

            //create tmp file for job log
            tmp.file({keep: true}, function(err, log_path) {
                submit_options['log'] = log_path;

                //create submit file
                var out = fs.createWriteStream(submit_path);
                for(key in submit_options) {
                    var value = submit_options[key];
                    switch(key) {
                    case "queue":
                        out.write("queue "+value+"\n");
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
                out.end("\n");
                console.log("submitted generated submit file:"+submit_path);

                //submit!
                condor_submit = spawn('condor_submit', ['-verbose', submit_path]);//, {cwd: __dirname});
                var stdout = "";
                condor_submit.stdout.on('data', function (data) {
                    stdout += data;
                });
                var stderr = "";
                condor_submit.stderr.on('data', function (data) {
                    stderr += data;
                });
                condor_submit.on('close', function (code) {
                    console.log(stderr);
                    if(code !== 0) {
                        console.log("condor_submit failed with code: "+code);
                    } else {
                        var lines = stdout.split("\n");
                        var empty = lines.shift();//condor_q returns empty line at the top..
                        var header = lines.shift(); //** Proc 49714580.0:
                        var header_tokens = header.split(" "); 
                        var jobid = header_tokens[2];
                        jobid = jobid.substring(0, jobid.length - 1).split(".");
                        var props = adparser.parse(lines);
                        props._cluster = parseInt(jobid[0]);
                        props._proc = parseInt(jobid[1]);
                        if(callback) {
                            callback(null, props);
                        }
                        resolve(props);
                    }
                });

            });
        });
    });
    return promise;
};

/*

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

*/
