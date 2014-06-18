
exports.adparser = {
    //parse something like
    //JOB_GLIDEIN_ClusterId = "$$(GLIDEIN_ClusterId:Unknown)"
    parse_value: function(v) {
        if(v === "true") {
            return true;
        } else if(v === "false") {
            return false;
        } else if(v.indexOf("\"") == 0) {
            //remove double quote from value if it's quoted
            return v.substring(1, v.length-1);
        }

        //tryi converting to int if its int
        var i = parseInt(v);
        if(i == v) {
            return i;
        }

        //console.log("not sure how to parse:"+v);
        return v;
    },
    parse: function(lines) {
        //parse class ad key/value
        var props = {};
        var cont = null;
        var cont_key = null;
        lines.forEach(function(line) {
            if(line == "") return;
            if(cont) {
                if(line.indexOf('"') == -1) {
                    //continue on..
                    cont = cont+"\n"+line;
                } else {
                    //ended
                    props[cont_key] = exports.adparser.parse_value(cont);
                    console.log("parsed multline value:"+cont);
                    cont = null;
                }
            } else {
                var dpos = line.indexOf(" = ");
                if(dpos == -1) {
                    console.log("malformed value.. ignoring");
                    console.log(lines);
                } else {
                    var key = line.substring(0, dpos);
                    var value = line.substring(dpos+3);
                    if(value[0] == '"' && value.indexOf('"', 1) == -1) {
                        //found quoted string not delimited by " - probably continuing to the next line 
                        cont = value.substring(0, value.length);
                    } else {
                        props[key] = exports.adparser.parse_value(value);
                    }
                }
            }
        });
        return props;
    }
}
