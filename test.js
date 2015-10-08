
var fs = require('fs');
var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;
var url = 'mongodb://localhost:27017/worldCities';

MongoClient.connect(url, { server: { ssl: false, sslValidate: false } }, function (error, db) {
    if (error) {
        console.error(error);
    }
    else {
    	var minDistance = 0;
    	var maxDistance = 10;
    	var distanceMultiplier = 1609.34; // Miles to meters conversion
    	var location = {
    			city: 'BOSTON',
    			region: 'MA',
    			country: 'US'
    	};
    	db.collection('locations').findOne(location, function (err, results) {
    		if (err) {
    			console.log(err);
    		}
    		else {
    			var query = {
    					$geoNear: {
    						near: { type: 'Points', coordinates: results.location },
    						distanceField: 'distance',
    						minDistance: minDistance * distanceMultiplier,
    						maxDistance: maxDistance * distanceMultiplier,
    						distanceMultiplier: 1 / distanceMultiplier,
    						limit: 10000000,
    						//num: 10,
    						spherical: true
    					}
    			};
    			db.collection('locations').aggregate([query]).toArray(function (err2, results) {
    				db.close();
    				if (err2) {
    					console.log(err2);
    				}
    				else {
    					for (var i = 0; i < results.length; i++) {
    						var location = results[i];
    						//console.log(location);
    						console.log(location.distance.toFixed(3) + ' miles - ' + location.city + ', ' + (location.country === 'US' ? location.region + ', ' : '') + location.country);
    					}
    				}
    			});
    		}
    	});
    }
});


