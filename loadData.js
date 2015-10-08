
var fs = require('fs');
var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;
var url = 'mongodb://localhost:27017/worldCities';

MongoClient.connect(url, { server: { ssl: false, sslValidate: false } }, function (error, db) {
    if (error) {
        console.error(error);
    }
    else {
    	db.collection('locations').createIndex({ location: '2dsphere' }, function (err) {
    		if (err) {
    			console.log(err);
    		}
    		else {
    	    	console.log('Loading file...');
    	    	var citiesContent = fs.readFileSync('./worldcities.csv').toString().split('\n');
    	    	var lines = [];
    	    	for (var i = 1; i < citiesContent.length; i++) {
    	    		if (citiesContent[i]) {
    	    			lines.push(citiesContent[i]);
    	    		}
    	    	}

    	    	var last = new Date();
    	    	var eta = 0;
    	    	var skipping = true;
    	    	var skipGroup = lines.length - 1;
    	    	var inserting = false;
    	    	var i = 0;
    	    	next();
    	    	function next() {
    	    		if (i < lines.length) {
    	    			if (i % 1000 === 0) {
    	    				var elapsed = new Date().getTime() - last.getTime();
    	    				last = new Date();
    	    				eta = (elapsed / 1000 * (lines.length - i) / 1000 ) / 60 + ' minutes remaining';
    	    			}
    	    			console.log('(' + (i + 1) + '/' + lines.length + '), ETA: ' + eta);
    	    			var data = lines[i].split(',');
    	    			var city = {
    	    					country: data[0].toUpperCase(),
    	    					city: data[1].toUpperCase(),
    	    					accentCity: data[2].toUpperCase(),
    	    					region: data[3].toUpperCase(),
    	    					population: parseInt(data[4]) || null,
    	    					location: [parseFloat(data[6]), parseFloat(data[5])]
    	    			};
    	    			if (!inserting) {
    	        			db.collection('locations').findOne(city, function (error, result) {
    	        				if (error) {
    	        					console.log(err);
    	        				}
    	        				else {
    	        					if (!result) {
    	        						if (skipping) {
    	        							
    	        							
    	        							
    	        							if (skipGroup >= 2) {
    	        								i = Math.max(0, i - skipGroup);
    	        								skipGroup = skipGroup >> 1;
    	        							}
    	        							else {
    	            							skipping = false;
    	            							i = Math.max(0, i - skipGroup);
    	        							}
    	        							next();
    	        							
    	        							
    	        							
    	        						}
    	        						else {
    	        							inserting = true;
    	        							insert(city);
    	        						}
    	        					}
    	        					else {
    	        						console.log('Already inserted');
    	        						if (!skipping) {
    	        	    					i++;	
    	        						}
    	        						else {
    	        							i = Math.min(lines.length, i + skipGroup);
    	        						}
    	    	    					next();
    	        					}
    	        				}
    	        			});
    	    			}
    	    			else {
    	    				insert(city);
    	    			}
    	    		}
    	    		else { 
    	    			db.close();
    	    		}
    	    	}
    	    	function insert (city) {
    	    		db.collection('locations').insert(city, function (error2) {
    	    			if (error2) {
    	    				console.log(error2);
    	    			}
    	    			else {
    	    				console.log('Added ' + city.country + ' ' + city.city);
    	    				i++;
    	    				next();
    	    			}
    	    		});
    	    	}
    		}
    	});
    }
});
