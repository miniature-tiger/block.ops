const mongodb = require('mongodb');


// Function checks whether a collection exists in a database
// ---------------------------------------------------------
// < Returns a promise >
function checkCollectionExists(db, collectionName) {
    let result = false;
    return new Promise(function(resolve, reject) {
        db.listCollections({}, {nameOnly: true}).toArray(function(error, collections) {
            let collectionPosition = collections.findIndex(fI => fI.name == collectionName);
            if (collectionPosition != -1) {
                if(collections[collectionPosition].type = 'collection') {
                    result = true;
                }
            }
            if (error) {
                console.log(error);
                reject(result);
            } else {
                resolve(result);
            }
        });
    });
}

module.exports.checkCollectionExists = checkCollectionExists;
