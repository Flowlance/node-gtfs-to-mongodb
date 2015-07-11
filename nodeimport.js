var async = require('async'),
    mongoose = require('mongoose'),
    path = require('path'),
    csv = require('csv'),
    fs = require('fs'),
    Db = require('mongodb').MongoClient;

function handleError(e) {
    console.error(e || 'Unknown Error');
    process.exit(1);
}

//load config.js
try {
    var config = require('./config.js');
    var mongo_url = 'mongodb://'+config.server.host+':'+config.server.port+'/'+config.server.database;
} catch (e) {
    handleError(new Error('Cannot find config.js'));
}

Db.connect(mongo_url, {w: 1}, function(err, db) {
	console.time("Importing time");
    async.eachSeries(config.GTFSFiles, function(GTFSFile, cb) {
        
	    if(GTFSFile) {
	        
	        var filepath = path.join(config.GTFSDir, GTFSFile.fileNameBase + '.txt');
	        
	        if (!fs.existsSync(filepath)) {
    	        console.log(filepath + ' doesn\'t exist, skipping...');
    	        return cb();
	        }
	        
	        console.log('Importing data from: ' + filepath);
	    
	        db.collection(GTFSFile.collection, function(e, collection) {
	            var input = fs.createReadStream(filepath);
	            var parser = csv.parse({columns: true});
	        
	            
	            parser.on('readable', function() {
	                while(line = parser.read()) {
	                    //remove null values
	                    for(var key in line){
	                        if(line[key] === null) {
	                            delete line[key];
	                        }
	                    }
	                
	                    //convert fields that should be int
	                    if(line.stop_sequence){
	                        line.stop_sequence = parseInt(line.stop_sequence, 10);
	                    }
	                    
	                    if(line.direction_id){
	                        line.direction_id = parseInt(line.direction_id, 10);
	                    }
	                    
	                    if(line.shape_pt_sequence){
	                        line.shape_pt_sequence = parseInt(line.shape_pt_sequence, 10);
	                    }
	                
	                    //make lat/lon array for stops
	                    if(line.stop_lat && line.stop_lon){
	                        line.loc = [parseFloat(line.stop_lon), parseFloat(line.stop_lat)];
	                    }
	                
	                    //make lat/long for shapes
	                    if(line.shape_pt_lat && line.shape_pt_lon){
	                        line.shape_pt_lon = parseFloat(line.shape_pt_lon);
	                        line.shape_pt_lat = parseFloat(line.shape_pt_lat);
	                        line.loc = [line.shape_pt_lon, line.shape_pt_lat];
	                    }
	                
	                    //insert into db
	                    collection.insert(line, function(e, inserted) {
	                        if(e) { handleError(e); }
	                    });
	                }
	            });
	            
	            parser.on('end', function(count){
	                cb();
	            });
	            
	            parser.on('error', handleError);
	            input.pipe(parser);
	        });
	    }
    
    }, function(err) {
	    // All done
	    if(err) handleError(err);
	    
	    console.log("Done importing files.");
	    console.timeEnd("Importing time");
	    process.exit();
    });
});