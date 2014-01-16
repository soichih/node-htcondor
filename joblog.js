var Tail = require('tail').Tail;


/*
    on: function(callback) {
        callbacks.push(callback);
    },
    watch: function(path) {
        //console.log("eventlog lister listening on "+path);
        tail = new Tail(path, "...\n", {interval: 500});
        tail.on("line", function(ablock) {
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
                callbacks.forEach(function(callback) {
                    callback(props);
                });
            }
        });
    },
    unwatch: function() {
        tail.unwatch();
    }
}
*/
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

*/
