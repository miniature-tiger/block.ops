const mongodb = require('mongodb');


// Function checks whether a collection exists in a database
// ---------------------------------------------------------
// < Returns a promise > (since async function)
async function checkCollectionExists(db, collectionName) {
    let result = false;
    await db.listCollections({}, {nameOnly: true}).toArray()
        .then(function(collections) {
            let collectionPosition = collections.findIndex(fI => fI.name == collectionName);
            if (collectionPosition != -1) {
                if(collections[collectionPosition].type = 'collection') {
                    result = true;
                }
            }
        }).catch(function(error) {
            console.log('checkCollectionExists', error);
        });
    return result;
}

module.exports.checkCollectionExists = checkCollectionExists;



// Extracts blockNumber for a start and end blockNumbers for analysis period from blockDates index
// ---------------------------------------------
// < Returns a promise > (since async function)
async function dateToBlockNumber(localDate, localDatePlusOne, db) {
    await db.collection('blockDates').find({timestamp: {$gte: localDate, $lt: localDatePlusOne}}).project({ timestamp: 1, blockNumber: 1, _id: 0 }).toArray()
        .then(function(records) {
            result = records[0].blockNumber;
        }).catch(function(error) {
            console.log(error);
        });
    return result;
}

module.exports.dateToBlockNumber = dateToBlockNumber;



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
                console.log(error); // ignore 11000 errors as there are many duplicated operations in AppBase
            }
        });
}

module.exports.mongoComment = mongoComment;



// Author Reward - processing of block operation
// ---------------------------------------------
function processAuthorReward(operation, mongoAuthorReward, db) {
    let record = {author: operation.op[1].author, permlink: operation.op[1].permlink, author_payout: {sbd: Number(operation.op[1].sbd_payout.split(' ', 1)[0]), steem: Number(operation.op[1].steem_payout.split(' ', 1)[0]), vests: Number(operation.op[1].vesting_payout.split(' ', 1)[0])}};
    mongoAuthorReward(db, record);
}

module.exports.processAuthorReward = processAuthorReward;



// Author Reward - update / insert of mongo record
// -----------------------------------------------
function mongoAuthorReward(db, localRecord) {
    // Uses upsert - blocks may be processed in any order so author/permlink record may already exist if another operation is processed first
    db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink, author_payout: {$exists : false}}, {$set: localRecord, $addToSet: {operations: 'author_payout'}}, {upsert: true})
        .catch(function(error) {
            if(error.code != 11000) {
                console.log(error); // ignore 11000 errors as there are many duplicated operations in AppBase
            }
        });
}

module.exports.mongoAuthorReward = mongoAuthorReward;



// Benefactor Reward - processing of block operation
// -------------------------------------------------
function processBenefactorReward(operation, mongoBenefactorReward, db) {
    let record = {author: operation.op[1].author, permlink: operation.op[1].permlink, benefactor: operation.op[1].benefactor, benefactor_payout: {vests: Number(operation.op[1].reward.split(' ', 1)[0])}};
    mongoBenefactorReward(db, record);
}

module.exports.processBenefactorReward = processBenefactorReward;



// Benefactor Reward - update / insert of mongo record
// -----------------------------------------------
function mongoBenefactorReward(db, localRecord) {
    // Uses upsert - blocks may be processed in any order so author/permlink record may already exist if another operation is processed first
    db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink, "benefactors.user": {$ne: localRecord.benefactor}}, {$inc: {"benefactor_payout.vests": localRecord.benefactor_payout.vests}, $addToSet: {operations: 'benefactor_payout', benefactors: {user: localRecord.benefactor, vests: localRecord.benefactor_payout.vests}}}, {upsert: true})
        .catch(function(error) {
            if(error.code != 11000) {
                console.log(error); // ignore 11000 errors as there are many duplicated operations in AppBase
            }
        });
}

module.exports.mongoBenefactorReward = mongoBenefactorReward;



// Curator Reward - processing of block operation
// -------------------------------------------------
function processCuratorReward(operation, mongoCuratorReward, db) {
    let record = {author: operation.op[1].comment_author, permlink: operation.op[1].comment_permlink, curator: operation.op[1].curator, curator_payout: {vests: Number(operation.op[1].reward.split(' ', 1)[0])}};
    mongoCuratorReward(db, record);
}

module.exports.processCuratorReward = processCuratorReward;



// Curator Reward - update / insert of mongo record
// -----------------------------------------------
function mongoCuratorReward(db, localRecord) {
    // Uses upsert - blocks may be processed in any order so author/permlink record may already exist if another operation is processed first
    db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink, "curators.user": {$ne: localRecord.curator}}, {$inc: {"curator_payout.vests": localRecord.curator_payout.vests}, $addToSet: {operations: 'curator_payout', curators: {user: localRecord.curator, vests: localRecord.curator_payout.vests}}}, {upsert: true})
        .catch(function(error) {
            if(error.code != 11000) {
                console.log(error); // ignore 11000 errors as there are many duplicated operations in AppBase
            }
        });
}

module.exports.mongoCuratorReward = mongoCuratorReward;



// Function reports on comments
// ----------------------------
async function reportComments(MongoClient, url, dbName) {
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);
    const collection = db.collection('comments');

    collection.aggregate([
        { $match : {operations: 'comment'}},
        { $project : {_id: 0, application: 1, transactionType: 1, author_payout: 1, benefactor_payout: 1, curator_payout: 1}},
        { $group : {_id : {application : "$application"},
                    posts: { $sum: 1 },
                    author_payout_sbd: {$sum: "$author_payout.sbd"}, author_payout_steem: {$sum: "$author_payout.steem"}, author_payout_vests: {$sum: "$author_payout.vests"},
                    benefactor_payout_vests: {$sum: "$benefactor_payout.vests"},
                    curator_payout_vests: {$sum: "$curator_payout.vests"},
                    }},
        { $sort : {posts:-1}}
        ]).toArray()
        .then(function(records) {
            for (let record of records) {
                record.application = record._id.application;
                delete record._id;
                record.author_payout_sbd = Number(record.author_payout_sbd.toFixed(3));
                record.author_payout_steem = Number(record.author_payout_steem.toFixed(3));
                record.author_payout_vests = Number(record.author_payout_vests.toFixed(6));
                record.benefactor_payout_vests = Number(record.benefactor_payout_vests.toFixed(6));
                record.curator_payout_vests = Number(record.curator_payout_vests.toFixed(6));
                console.log(record);
            }
            console.log('closing mongo db');
            console.log('------------------------------------------------------------------------');
            console.log('------------------------------------------------------------------------');
            client.close();
        })
}

module.exports.reportComments = reportComments;


// Function reports on blocks processed
// ------------------------------------
async function reportBlocksProcessed(db, openBlock, closeBlock, retort) {

    const collection = db.collection('blocksProcessed');

    if (retort == 'report') {

        collection.aggregate([
            { $match : {blockNumber: { $gte: openBlock, $lt: closeBlock }}},
            { $project : {_id: 0, blockNumber: 1, status: 1}},
            { $group : {_id : {status : "$status"},
                        count: { $sum: 1 },
                        }},
            ]).toArray()
            .then(function(records) {
                for (let record of records) {
                    record.status = record._id.status;
                    delete record._id;
                    console.log(record);
                }
            console.log('closing mongo db');
            console.log('------------------------------------------------------------------------');
            console.log('------------------------------------------------------------------------');
            client.close();
        }).catch(function(error) {
            console.log(error);
        });
    }

    if (retort == 'return') {
        let result = [];

        collection
            .find({ blockNumber: { $gte: openBlock, $lt: closeBlock }, status: 'OK'})
            .project({ blockNumber: 1, _id: 0 })
            .sort({blockNumber:1})
            .toArray()
            .then(function(records) {
                for (let record of records) {
                    result.push(record.blockNumber)
                }
                client.close();
            }).catch(function(error) {
                console.log(error);
            });
    }
}

module.exports.reportBlocksProcessed = reportBlocksProcessed;
