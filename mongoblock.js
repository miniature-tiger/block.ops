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



// Comment - processing of block operation
// ---------------------------------------------
function processComment(operation, mongoComment, db) {
    let appName = '', appVersion = '';
    try {
        // need to update for parley
        [appName, appVersion] = JSON.parse(operation.op[1].json_metadata).app.split('/');
    } catch(error) {
        // there are lots of errors - refine app derivation for null cases etc
    }
    let record = {author: operation.op[1].author, permlink: operation.op[1].permlink, blockNumber: operation.block, timestamp: operation.timestamp, transactionNumber: operation.trx_in_block, virtualTrxNumber: operation.virtual_op, transactionType: operation.op[0], application: appName, applicationVersion: appVersion};
    mongoComment(db, record);
}

module.exports.processComment = processComment;



// Comment - update / insert of mongo record
// -----------------------------------------------
function mongoComment(db, localRecord) {
    // Uses upsert - blocks may be processed in any order so author/permlink record may already exist if another operation is processed first
    db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink, application: {$exists : false}}, {$set: localRecord, $addToSet: {operations: 'comment'}}, {upsert: true})
        .catch(function(error) {
            if(error.code != 11000) {
                console.log(error);
            } else {
                // ignore 11000 errors as there are many duplicated operations in AppBase
            }
        });
}

module.exports.mongoComment = mongoComment;



// Function reports on comments
// ----------------------------
async function reportComments(MongoClient, url, dbName) {
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);
    const collection = db.collection('comments');

    collection.aggregate([
        { $match : {operations: 'comment'}},
        { $project : {_id: 0, application: 1, transactionType: 1}},
        { $group : {_id : {application : "$application"},
                    posts: { $sum: 1 },
                    }},
        { $sort : {posts:-1}}
        ]).toArray()
        .then(function(records) {
            for (let record of records) {
                record.application = record._id.application;
                delete record._id;
                console.log(record);
            }
            console.log('closing mongo db');
            client.close();
        });
}

module.exports.reportComments = reportComments;
