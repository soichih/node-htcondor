//var spawn = require('child_process').spawn;
var fs = require('fs');
var xml2js = require('xml2js');

//var jobs = {};

/*
exports.init = function(options) {
    console.log("initializing");
    //eventlog_listener(options);    
};
*/

exports.eventlog = require('./eventlog').eventlog;
