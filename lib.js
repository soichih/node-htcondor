
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
        lines.forEach(function(line) {
            if(line == "") return;
            var dpos = line.indexOf(" = ");
            if(dpos == -1) {
                console.log("malformed value.. ignoring");
                console.log(line);
                //This occurs when a string contains newline like following sample
                //CurrentTime = time()
                //ReceivedBytes = 9647680.000000
                //Message = "Error from glidein_3592@hansen-a005.rcac.purdue.edu: dprintf hit fatal errors
                //"
            } else {
                var key = line.substring(0, dpos);
                var value = line.substring(dpos+3);
                props[key] = exports.adparser.parse_value(value);
            }
        });
        return props;
    }
}
