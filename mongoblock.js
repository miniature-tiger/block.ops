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
    let record = {author: operation.op[1].author, permlink: operation.op[1].permlink, blockNumber: operation.block, timestamp: operation.timestamp, transactionNumber: operation.trx_in_block, transactionType: 'commentUnverified', application: appName, applicationVersion: appVersion};
    mongoComment(db, record, operation, 0);
}

module.exports.processComment = processComment;



// Comment - update / insert of mongo record
// -----------------------------------------------
function mongoComment(db, localRecord, operation, reattempt) {
    let maxReattempts = 1;
    recordOperation = {transactionNumber: localRecord.transactionNumber, transactionType: 'comment', status: 'OK'};
    // Uses upsert - blocks may be processed in any order so author/permlink record may already exist if another operation is processed first
    db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink, application: {$exists : false}}, {$set: localRecord, $addToSet: {operations: 'comment'}}, {upsert: true})
        .then(function(response) {
            mongoOperationProcessed(db, localRecord.blockNumber, recordOperation, 1, 0);
        })
        .catch(function(error) {
            if(error.code == 11000) {
                db.collection('comments').find(
                    {author: localRecord.author, permlink: localRecord.permlink, operations: 'comment'}
                ).toArray()
                .then(function(commentArray) {
                    if(commentArray.length > 0) {
                        if (commentArray[0].blockNumber < localRecord.blockNumber) {
                            // Comment in document is from older block - so this operation is a comment edit
                            mongoOperationProcessed(db, localRecord.blockNumber, recordOperation, 1, 0);
                            // add this operation to comment edits section of document?
                        } else {
                            // have to edit document with this comment as the new comment
                            db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink}, {$set: localRecord, $addToSet: {operations: 'comment'}}, {upsert: false})
                            mongoOperationProcessed(db, localRecord.blockNumber, recordOperation, 1, 0);
                            // and add old comment to comment edits?
                        }
                    } else {
                        if (reattempt < maxReattempts) {
                            console.log('E11000 error with <', localRecord.author, localRecord.blockNumber, '> comment. Re-attempting...');
                            mongoComment(db, localRecord, operation, 1);
                        } else {
                            console.log('E11000 error with <', localRecord.author, localRecord.blockNumber, '> comment. Maximum reattempts surpassed.');
                        }
                    }
                });
            } else {
                console.log('Non-standard error with <', localRecord.author, localRecord.blockNumber, '> comment.');
                console.log(error);
            }
        });
}

module.exports.mongoComment = mongoComment;



// Vote - processing of block operation
// ---------------------------------------------
function processVote(operation, mongoVote, db) {
    let record = {author: operation.op[1].author, permlink: operation.op[1].permlink, voter: operation.op[1].voter, percent: Number((operation.op[1].weight/100).toFixed(0)), vote_timestamp: operation.timestamp, vote_blockNumber: operation.block, transactionNumber: operation.trx_in_block, transactionType: operation.op[0]};
    mongoVote(db, record, 0);
}

module.exports.processVote = processVote;



// Vote - update / insert of mongo record
// -----------------------------------------------
function mongoVote(db, localRecord, reattempt) {
    let maxReattempts = 1;
    db.collection('comments').find({ author: localRecord.author, permlink: localRecord.permlink, "curators.voter": localRecord.voter}).toArray()
        .then(function(result) {
            // Adds all vote details to curation set if author / permlink / voter combination not found
            if(result.length === 0) {
                db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink}, {$addToSet: {curators: {voter: localRecord.voter, percent: localRecord.percent, vote_timestamp: localRecord.vote_timestamp, vote_blockNumber: localRecord.vote_blockNumber}, operations: 'vote'}}, {upsert: true})
                    .then(function(response) {
                        recordOperation = {transactionNumber: localRecord.transactionNumber, transactionType: localRecord.transactionType, status: 'OK'};
                        mongoOperationProcessed(db, localRecord.vote_blockNumber, recordOperation, 1, 0);
                    })
                    .catch(function(error) {
                        if(error.code == 11000) {
                            if (reattempt < maxReattempts) {
                                console.log('E11000 error with <', localRecord.voter, localRecord.vote_blockNumber, '> ActiveVote. Re-attempting...');
                                mongoVote(db, localRecord, 1);
                            } else {
                                console.log('E11000 error with <', localRecord.voter, localRecord.vote_blockNumber, '> ActiveVote. Maximum reattempts surpassed.');
                            }
                        } else {
                            console.log('Non-standard error with <', localRecord.voter, localRecord.vote_blockNumber, '> ActiveVote.');
                            console.log(error);
                        }
                    });
            // Updates existing vote details for voter in curation array if author / permlink / voter combination is found
            } else {
                db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink, curators: { $elemMatch: { voter: localRecord.voter}}},
                              {$set: {"curators.$.voter": localRecord.voter, "curators.$.percent": localRecord.percent, "curators.$.vote_timestamp": localRecord.vote_timestamp,
                                      "curators.$.vote_blockNumber": localRecord.vote_blockNumber},
                                $addToSet: {operations: 'vote'}}, {upsert: false})
                    .then(function(response) {
                        recordOperation = {transactionNumber: localRecord.transactionNumber, transactionType: localRecord.transactionType, status: 'OK'};
                        mongoOperationProcessed(db, localRecord.vote_blockNumber, recordOperation, 1, 0);
                    })
                    .catch(function(error) {
                          console.log('Error: vote', localRecord.voter);
                    });
            }
        })
}

module.exports.mongoVote = mongoVote;



// ActiveVotes - processing of active votes (extracted at end of voting period)
// ----------------------------------------------------------------------------
function processActiveVote(localVote, localAuthor, localPermlink, localBlockNumber, localVirtualOp, mongoActiveVote, db) {
    let formatVote = {voter: localVote.voter, curation_weight: localVote.weight, rshares: Number(localVote.rshares),
                              percent: Number((localVote.percent/100).toFixed(2)), reputation: Number(localVote.reputation), vote_timestamp: localVote.time}
    let record = {author: localAuthor, permlink: localPermlink, activeVote: formatVote};
    mongoActiveVote(db, localBlockNumber, localVirtualOp, record, 0);
}

module.exports.processActiveVote = processActiveVote;



// ActiveVotes - update / insert of mongo record
// ---------------------------------------------
function mongoActiveVote(db, activeBlockNumber, activeVirtualOp, localRecord, reattempt) {
    let maxReattempts = 1;
    db.collection('comments').find({ author: localRecord.author, permlink: localRecord.permlink, "curators.voter": localRecord.activeVote.voter}).toArray()
        .then(function(result) {
            // Adds all vote details to curation set if author / permlink / voter combination not found
            if(result.length === 0) {
                db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink}, {$addToSet: {curators: localRecord.activeVote, operations: 'active_votes'}}, {upsert: true})
                    .then(function(response) {
                        db.collection('blocksProcessed').updateOne({ blockNumber: activeBlockNumber, operations: { $elemMatch: { virtualOp: activeVirtualOp}}}, {$inc: {"operations.$.activeVotesProcessed": 1}}, {upsert: false}, (error, results) => {
                            if(error) {
                                console.log(error);
                            }
                        });
                    })
                    .catch(function(error) {
                        if(error.code == 11000) {
                            if (reattempt < maxReattempts) {
                                console.log('E11000 error with <', localRecord.activeVote.voter, activeBlockNumber, '> ActiveVote. Re-attempting...');
                                mongoActiveVote(db, localRecord, 1);
                            } else {
                                console.log('E11000 error with <', localRecord.activeVote.voter, activeBlockNumber, '> ActiveVote. Maximum reattempts surpassed.');
                            }
                        } else {
                            console.log('Non-standard error with <', localRecord.activeVote.voter, activeBlockNumber, '> ActiveVote.');
                            console.log(error);
                        }
                    });
            // Updates existing vote details for voter in curation array if author / permlink / voter combination is found
            } else {
                db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink, curators: { $elemMatch: { voter: localRecord.activeVote.voter}}},
                              {$set: {"curators.$.voter": localRecord.activeVote.voter, "curators.$.curation_weight": localRecord.activeVote.curation_weight, "curators.$.rshares": localRecord.activeVote.rshares,
                                "curators.$.percent": localRecord.activeVote.percent, "curators.$.reputation": localRecord.activeVote.reputation, "curators.$.vote_timestamp": localRecord.activeVote.vote_timestamp},
                                $addToSet: {operations: 'active_votes'}}, {upsert: false})
                    .then(function(response) {
                        db.collection('blocksProcessed').updateOne({ blockNumber: activeBlockNumber, operations: { $elemMatch: { virtualOp: activeVirtualOp}}}, {$inc: {"operations.$.activeVotesProcessed": 1}}, {upsert: false}, (error, results) => {
                            if(error) {
                                console.log(error);
                            }
                        });
                    })
                    .catch(function(error) {
                          console.log('Error: active_vote', localRecord.activeVote.voter);
                    });
            }
        })
}

module.exports.mongoActiveVote = mongoActiveVote;



// Author Reward - processing of block operation
// ---------------------------------------------
function processAuthorReward(operation, mongoAuthorReward, db) {
    let record = {author: operation.op[1].author, permlink: operation.op[1].permlink, author_payout: {sbd: Number(operation.op[1].sbd_payout.split(' ', 1)[0]), steem: Number(operation.op[1].steem_payout.split(' ', 1)[0]), vests: Number(operation.op[1].vesting_payout.split(' ', 1)[0])}, payout_blockNumber: operation.block, payout_timestamp: operation.timestamp };
    mongoAuthorReward(db, record, operation.virtual_op, 0);
}

module.exports.processAuthorReward = processAuthorReward;



// Author Reward - update / insert of mongo record
// -----------------------------------------------
function mongoAuthorReward(db, localRecord, virtualOp, reattempt) {
    // Uses upsert - blocks may be processed in any order so author/permlink record may already exist if another operation is processed first
    let maxReattempts = 1;
    db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink}, {$set: localRecord, $addToSet: {operations: 'author_payout'}}, {upsert: true})
        .then(function(response) {
            recordOperation = {virtualOp: virtualOp, transactionType: 'author_reward', status: 'OK'};
            mongoOperationProcessed(db, localRecord.payout_blockNumber, recordOperation, 1, 0);
        })
        .catch(function(error) {
            if(error.code == 11000) {
                if (reattempt < maxReattempts) {
                    console.log('E11000 error with <', localRecord.author, localRecord.payout_blockNumber, '> author_payout. Re-attempting...');
                    mongoAuthorReward(db, localRecord, virtualOp, 1);
                } else {
                    console.log('E11000 error with <', localRecord.author, localRecord.payout_blockNumber, '> author_payout. Maximum reattempts surpassed.');
                }
            } else {
                console.log('Non-standard error with <', localRecord.author, localRecord.payout_blockNumber, '> author_payout.');
                console.log(error);
            }
        });
}

module.exports.mongoAuthorReward = mongoAuthorReward;



// Benefactor Reward - processing of block operation
// -------------------------------------------------
function processBenefactorReward(operation, mongoBenefactorReward, db) {
    let record = {author: operation.op[1].author, permlink: operation.op[1].permlink, benefactor: operation.op[1].benefactor, benefactor_timestamp: operation.timestamp,
                          benefactor_payout: {sbd: Number(operation.op[1].sbd_payout.split(' ', 1)[0]), steem: Number(operation.op[1].steem_payout.split(' ', 1)[0]), vests: Number(operation.op[1].vesting_payout.split(' ', 1)[0])}, virtualOp: operation.virtual_op, payout_blockNumber: operation.block};
    mongoBenefactorReward(db, record, 0);
}

module.exports.processBenefactorReward = processBenefactorReward;



// Benefactor Reward - update / insert of mongo record
// -----------------------------------------------
function mongoBenefactorReward(db, localRecord, reattempt) {
    // Uses upsert - blocks may be processed in any order so author/permlink record may already exist if another operation is processed first
    let maxReattempts = 1;
    recordOperation = {virtualOp: localRecord.virtualOp, transactionType: 'benefactor_reward', status: 'OK'};
    db.collection('comments').find({ author: localRecord.author, permlink: localRecord.permlink, "benefactors.user": localRecord.benefactor}).toArray()
        .then(function(result) {
            // Adds all benefactor details to benefactor set if author / permlink / benefactor combination not found
            if(result.length === 0) {
                db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink, "benefactors.user": {$ne: localRecord.benefactor}},
                                                    {$inc: {"benefactor_payout.sbd": localRecord.benefactor_payout.sbd, "benefactor_payout.steem": localRecord.benefactor_payout.steem, "benefactor_payout.vests": localRecord.benefactor_payout.vests},
                                                        $addToSet: {operations: 'benefactor_payout', benefactors: {user: localRecord.benefactor, sbd: localRecord.benefactor_payout.sbd, steem: localRecord.benefactor_payout.steem, vests: localRecord.benefactor_payout.vests, timestamp: localRecord.benefactor_timestamp}}},
                                                    {upsert: true})
                    .then(function(response) {
                        mongoOperationProcessed(db, localRecord.payout_blockNumber, recordOperation, 1, 0);
                    })
                    .catch(function(error) {
                        if(error.code == 11000) {
                            if (reattempt < maxReattempts) {
                                console.log('E11000 error with <', localRecord.benefactor, localRecord.payout_blockNumber, '> benefactor_payout. Re-attempting...');
                                mongoBenefactorReward(db, localRecord, 1);
                            } else {
                                console.log('E11000 error with <', localRecord.benefactor, localRecord.payout_blockNumber, '> benefactor_payout. Maximum reattempts surpassed.');
                                console.log('benefactor reward error', localRecord.payout_blockNumber, localRecord.virtualOp)
                            }
                        } else {
                            console.log('Non-standard error with <', localRecord.benefactor, localRecord.payout_blockNumber, '> benefactor_payout.');
                            console.log('benefactor reward error', localRecord.payout_blockNumber, localRecord.virtualOp)
                            console.log(error);
                        }
                    });
            // No need to update 'comments' collection. Either a repeat operation in a block or a rerun of a block with operation already inserted in 'comments'
            } else {
                mongoOperationProcessed(db, localRecord.payout_blockNumber, recordOperation, 1, 0);
            }
    })
}

module.exports.mongoBenefactorReward = mongoBenefactorReward;



// Curator Reward - processing of block operation
// -------------------------------------------------
function processCuratorReward(operation, mongoCuratorReward, db) {
    let record = {author: operation.op[1].comment_author, permlink: operation.op[1].comment_permlink, voter: operation.op[1].curator, reward_timestamp: operation.timestamp, curator_payout: {vests: Number(operation.op[1].reward.split(' ', 1)[0])}, virtualOp: operation.virtual_op, payout_blockNumber: operation.block};
    mongoCuratorReward(db, record, 0);
}

module.exports.processCuratorReward = processCuratorReward;



// Curator Reward - update / insert of mongo record
// -----------------------------------------------
function mongoCuratorReward(db, localRecord, reattempt) {
    let maxReattempts = 1;
    db.collection('comments').find({ author: localRecord.author, permlink: localRecord.permlink, "curators.voter": localRecord.voter}).toArray()
        .then(function(result) {
            // Adds all vote details to curation set if author / permlink / voter combination not found
            if(result.length === 0) {
                db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink}, {$inc: {"curator_payout.vests": localRecord.curator_payout.vests},
                                                        $addToSet: {curators: {voter: localRecord.voter, vests: localRecord.curator_payout.vests, reward_timestamp: localRecord.reward_timestamp}, operations: 'curator_payout'}}, {upsert: true})
                    .then(function(response) {
                        recordOperation = {virtualOp: localRecord.virtualOp, transactionType: 'curator_reward', status: 'OK'};
                        mongoOperationProcessed(db, localRecord.payout_blockNumber, recordOperation, 1, 0);
                    })
                    .catch(function(error) {
                        if(error.code == 11000) {
                            if (reattempt < maxReattempts) {
                                console.log('E11000 error with <', localRecord.voter, localRecord.payout_blockNumber, '> curator_payout. Re-attempting...');
                                mongoVote(db, localRecord, 1);
                            } else {
                                console.log('E11000 error with <', localRecord.voter, localRecord.payout_blockNumber, '> curator_payout. Maximum reattempts surpassed.');
                            }
                        } else {
                            console.log('Non-standard error with <', localRecord.voter, localRecord.payout_blockNumber, '> curator_payout.');
                            console.log(error);
                        }
                    });
            // Updates existing vote details for voter in curation array if author / permlink / voter combination is found
            } else {
                db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink, curators: { $elemMatch: { voter: localRecord.voter}}},
                              {$set: {"curators.$.voter": localRecord.voter, "curators.$.vests": localRecord.curator_payout.vests, "curators.$.reward_timestamp": localRecord.reward_timestamp},
                                $addToSet: {operations: 'curator_payout'}}, {upsert: false})
                    .then(function(response) {
                        recordOperation = {virtualOp: localRecord.virtualOp, transactionType: 'curator_reward', status: 'OK'};
                        mongoOperationProcessed(db, localRecord.payout_blockNumber, recordOperation, 1, 0);
                    })
                    .catch(function(error) {
                          console.log('Error: curator_payout', localRecord.voter);
                    });
            }
        })
}


module.exports.mongoCuratorReward = mongoCuratorReward;



// Initialisation of blocksProcessed documents for each block and handling of reprocessed blocks
// ---------------------------------------------------------------------------------------------

function mongoBlockProcessed(db, blockRecord, reattempt) {
    let maxReattempts = 1;
    // Add record of block to blocksProcessed collection in database
    db.collection('blocksProcessed').updateOne({ blockNumber: blockRecord.blockNumber, status: {$ne : 'OK'}}, {$set: blockRecord}, {upsert: true})
        .catch(function(error) {
            if(error.code == 11000) {
                if (reattempt < maxReattempts) {
                    console.log('E11000 error with <', blockRecord.blockNumber, '> mongoBlockProcessed. Re-attempting...');
                    mongoBlockProcessed(db, blockRecord, 1);
                } else {
                    console.log('E11000 error with <', blockRecord.blockNumber, '> mongoBlockProcessed. Maximum reattempts surpassed.');
                }
            } else {
                console.log('Non-standard error with <', blockRecord.blockNumber, '> mongoBlockProcessed.');
                console.log(error);
            }
    });
}

module.exports.mongoBlockProcessed = mongoBlockProcessed;



// Update blocksProcessed with details of a single operation processed
// -------------------------------------------------------------------
function mongoOperationProcessed(db, localBlockNumber, operationRecord, operationsIncluded, reattempt) {
    let maxReattempts = 1;
    db.collection('blocksProcessed').findOneAndUpdate({ blockNumber: localBlockNumber}, {$addToSet: {operations: operationRecord}, $inc: {operationsProcessed: operationsIncluded}}, {upsert: true, returnOriginal: false, maxTimeMS: 1000})
        .then(function(response) {
            if (response.value == null) {
                console.log('------------------------');
                console.log('null response from mongoOperationProcessed')
                console.dir(response, {depth: null});
                db.collection('blocksProcessed').find({ blockNumber: localBlockNumber}).toArray()
                    .then(function(temp) {
                        console.log('------------------------');
                        console.dir(temp, {depth: null});
                        console.log('------------------------');
                    })
            }
            if ((response.value.operationsCount == response.value.operationsProcessed) && (response.value.status == 'Processing')) {
                db.collection('blocksProcessed').updateOne({ blockNumber: localBlockNumber}, {$set: {status: 'OK'}})
            }
        })
        .catch(function(error) {
            if(error.code == 11000) {
                if (reattempt < maxReattempts) {
                    console.log('E11000 error with <', localBlockNumber, operationRecord, '> mongoOperationProcessed. Re-attempting...');
                    mongoOperationProcessed(db, localBlockNumber, operationRecord, operationsIncluded, 1);
                } else {
                    console.log('E11000 error with <', localBlockNumber, operationRecord, '> mongoOperationProcessed. Maximum reattempts surpassed.');
                }
            } else {
                console.log('Non-standard error with <', localBlockNumber, operationRecord, '> mongoOperationProcessed.');
                console.log(error);
            }
        });
}

module.exports.mongoOperationProcessed = mongoOperationProcessed;



// Function to self-validate comments based on time between comment blockNumber and author payout
// ----------------------------------------------------------------------------------------------
function validateComments(db, localOperation) {
    let msecondsInSevenDays = 604800000;
    // Find document in comments based on author / permlink
    db.collection('comments').find(
        {author: localOperation.op[1].author, permlink: localOperation.op[1].permlink, operations: 'comment'}
    ).toArray()
    .then(function(commentArray) {
        if(commentArray.length > 0) {
            // Check if difference between document timestamp and author payout timestamp is 7 days
            if (new Date(localOperation.timestamp) - new Date(commentArray[0].timestamp) == msecondsInSevenDays) {
                // Change comment transactionType to 'commentOriginal'
                db.collection('comments').updateOne({ author: localOperation.op[1].author, permlink: localOperation.op[1].permlink}, {$set: {transactionType: 'commentOriginal'}}, {upsert: false})
                    .catch(function(error) {
                        console.log(error);
                    });
            } else {
                // Change comment transactionType to 'commentEdit'
                db.collection('comments').updateOne({ author: localOperation.op[1].author, permlink: localOperation.op[1].permlink}, {$set: {transactionType: 'commentOriginal'}}, {upsert: false})
                    .catch(function(error) {
                        console.log(error);
                    });
            }
        } else {
            // No comment found - add note for future comments added so that immediately verify?
        }
    });
}

module.exports.validateComments = validateComments;



// Function reports on comments
// ----------------------------
async function reportCommentsMongoOld(db, openBlock, closeBlock) {

    db.collection('comments').aggregate([
        { $match :  {$and :[{operations: 'comment'},
                    {blockNumber: { $gte: openBlock, $lt: closeBlock }}]} },
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

module.exports.reportCommentsMongoOld = reportCommentsMongoOld;



// Market share reports on Steem applications
// ------------------------------------------
// Note that payments are in relation to comments made in the date range - not payments made within the date range
async function reportCommentsMongo(db, openBlock, closeBlock) {
    return db.collection('comments').aggregate([
            { $match :  {$and :[{operations: 'comment'},
                        {blockNumber: { $gte: openBlock, $lt: closeBlock }}]} },
            { $project : {_id: 0, application: 1, author: 1, transactionType: 1, author_payout: 1, benefactor_payout: 1, curator_payout: 1}},
            { $group : {_id : {application : "$application", author: "$author"},
                        posts: { $sum: 1 },
                        author_payout_sbd: {$sum: "$author_payout.sbd"},
                        author_payout_steem: {$sum: "$author_payout.steem"},
                        author_payout_vests: {$sum: "$author_payout.vests"},
                        benefactor_payout_vests: {$sum: "$benefactor_payout.vests"},
                        curator_payout_vests: {$sum: "$curator_payout.vests"},
                        }},
            { $group : {_id : {application : "$_id.application"},
                        authors: {$sum: 1},
                        posts: {$sum: "$posts"},
                        author_payout_sbd: {$sum: "$author_payout_sbd"},
                        author_payout_steem: {$sum: "$author_payout_steem"},
                        author_payout_vests: {$sum: "$author_payout_vests"},
                        benefactor_payout_vests: {$sum: "$benefactor_payout_vests"},
                        curator_payout_vests: {$sum: "$curator_payout_vests"}
                        }},
            { $sort : {authors:-1}}
        ]).toArray().catch(function(error) {
            console.log(error);
        });
}

module.exports.reportCommentsMongo = reportCommentsMongo;




// Function reports on blocks processed
// ------------------------------------
async function reportBlocksProcessed(db, openBlock, closeBlock, retort) {

    if (retort == 'report') {

        db.collection('blocksProcessed').aggregate([
            { $match : {blockNumber: { $gte: openBlock, $lt: closeBlock }}},
            { $project : {_id: 0, blockNumber: 1, status: 1, operationsCount: 1, operationsProcessed: 1}},
            { $group : {_id : {status : "$status"},
                        count: { $sum: 1 },
                        }},
            ]).toArray()
            .then(function(records) {
                for (let record of records) {
                    //record.status = record._id.status;
                    //delete record._id;
                    console.dir(record, {depth: null});
                }
        }).catch(function(error) {
            console.log(error);
        });

        db.collection('blocksProcessed').aggregate([
            { $match : {blockNumber: { $gte: openBlock, $lt: closeBlock }, status: {$ne: 'OK'}}},
            //{ $project : {_id: 0, blockNumber: 1, status: 1, operationsCount: 1, operationsProcessed: 1}},
            ]).toArray()
            .then(function(records) {
                for (let record of records) {
                    //record.status = record._id.status;
                    //delete record._id;
                    console.dir(record, {depth: null});
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

    let okArray = [], result = [];

        await db.collection('blocksProcessed')
            .find({ blockNumber: { $gte: openBlock, $lt: closeBlock }, status: 'OK'})
            .project({ blockNumber: 1, _id: 0 })
            .sort({blockNumber:1})
            .toArray()
            .then(function(records) {
                for (let record of records) {
                    okArray.push(record.blockNumber)
                }
            }).catch(function(error) {
                console.log(error);
            });

        let j = 0;
        for (var i = openBlock; i < closeBlock; i+=1) {
            if (j == okArray.length || i != okArray[j]) {
                result.push(i);
            } else {
                j+=1;
            }
        }
        console.log('okArray');
        if (okArray.length == 0) {
            console.log('0 ok blocks');
        } else {
            console.log(okArray[0], okArray[okArray.length-1], okArray.length);
        }
        console.log('result');
        if (result.length == 0) {
            console.log('0 blocks to process');
        } else {
            console.log(result[0], result[result.length-1], result.length);
        }
        return result;
    }
}

module.exports.reportBlocksProcessed = reportBlocksProcessed;



// Function resets operations counter in blocksProcessed for blocks being rerun
// ----------------------------------------------------------------------------
function resetBlocksProcessed(db, firstInArray, lastInArray) {
    db.collection('blocksProcessed')
        .updateMany({ blockNumber: { $gte: firstInArray, $lte: lastInArray }, status: {$ne: 'OK'}},
                {$set: {operationsProcessed: 0}, $pull: { operations: {transactionType: "notHandled"}}}, {upsert: false})
        .catch(function(error) {
            console.log(error);
        });
}

module.exports.resetBlocksProcessed = resetBlocksProcessed;



// Function lists comments for an application to give an idea of comment structure
// -------------------------------------------------------------------------------
function findCommentsMongo(localApp, db, openBlock, closeBlock) {
    db.collection('comments').find(

        {$and : [
            { blockNumber: { $gte: openBlock, $lt: closeBlock }},
            {operations: 'comment'},
            //{operations: 'vote'},
            //{operations: 'author_payout'},
        ]}

    ).toArray()
        .then(function(details) {
            for (let comment of details) {
                console.dir(comment, { depth: null });
            }
            client.close();
        })
        .catch(function(error) {
            console.log(error);
        });
}

module.exports.findCommentsMongo = findCommentsMongo;



// Investigation Mongo
// -------------------------------------------------------------------------------
function investigationMongo(db, openBlock, closeBlock) {
    db.collection('comments').find(
        {$and : [
            { blockNumber: { $gte: openBlock, $lt: closeBlock }},
            { curators: {$exists : true}},
        ]}).sort({author:1}).toArray()
        .then(function(details) {
            let i = 0, counter = 0, max = 0;
            console.log(details.length + ' records')
            for (let comment of details) {
                for (let indiv of comment.curators) {
                    let differential = (new Date(indiv.timestamp) - new Date(comment.timestamp)) - (7*24*60*60*1000);
                    if (differential != 0) {
                        console.log(differential);
                        console.log(comment);
                    } else {
                        counter +=1;
                    }
                    if (comment.curators.length > max && comment.curators.length > 10) {
                        console.log(comment);
                        max = comment.curators.length
                    }
                }
            }
            console.log('counter', counter);
            client.close();
        })
        .catch(function(error) {
            console.log(error);
        });
}

module.exports.investigationMongo = investigationMongo;



// Analysis of curator rewards: vests to rshares ratio for a single voter or all voters
// ------------------------------------------------------------------------------------
function findCuratorMongo(voter, db, openBlock, closeBlock) {
    let steemPerVests = 0.00049495; // Temporary while indexes are built for all the various currencies and measures!
    let rsharesToVoteValue = 867700000000; // Temporary while indexes are built for all the various currencies and measures!
    console.log(openBlock, closeBlock, voter);
    let minRshares = 100000000000; // 100bn

    // If no account name is chosen then all ratios are examined for all users and the top ones logged
    if (voter == undefined) {
        db.collection('comments').aggregate([
                { $match :  {$and :[
                                { payout_blockNumber: { $gte: openBlock, $lt: closeBlock }},
                                { curators: { $exists : true}}
                            ]} },
                { $project : { author: 1, permlink: 1, blockNumber: 1, curators: {$filter: {input: "$curators", as: "curator", cond: { $and: [{$gt: [ "$$curator.vests", 0 ] }, {$gt: [ "$$curator.rshares", minRshares ]}] }}}}},
                { $unwind : "$curators" },
                { $project : { _id: 0, curators: {voter: 1, vests: 1, rshares: 1, ratio: { $divide: [ "$curators.vests", "$curators.rshares" ]}, SP_reward: { $multiply: [ "$curators.vests", steemPerVests ]}, STU_vote_value: { $divide: [ "$curators.rshares", rsharesToVoteValue ]}}, author: 1, permlink: 1, blockNumber: 1}},
                { $sort : { "curators.ratio": -1}}
            ]).toArray()
            .then(function(curatorArray) {
                processResult(curatorArray);
            })
            .catch(function(error) {
                console.log(error);
            });
    // If an account name is chosen then all the curation ratios for this account are found and sorted
    } else {
        db.collection('comments').aggregate([
                { $match :  {$and :[
                                { payout_blockNumber: { $gte: openBlock, $lt: closeBlock }},
                                { curators: { $exists : true}},
                                { "curators.voter": voter},
                            ]} },
                { $project : { author: 1, permlink: 1, blockNumber: 1, curators: {$filter: {input: "$curators", as: "curator", cond: { $and: [{$gt: [ "$$curator.vests", 0 ] }, {$gt: [ "$$curator.rshares", minRshares ]}] }  }}}},
                { $unwind : "$curators" },
                { $match: {"curators.voter": voter}},
                { $project : { _id: 0, curators: {voter: 1, vests: 1, rshares: 1, ratio: { $divide: [ "$curators.vests", "$curators.rshares" ]}, SP_reward: { $multiply: [ "$curators.vests", steemPerVests ]}, STU_vote_value: { $divide: [ "$curators.rshares", rsharesToVoteValue ]}}, author: 1, permlink: 1, blockNumber: 1}},
                { $sort : { "curators.ratio": -1}}
            ]).toArray()
            .then(function(curatorArray) {
                processResult(curatorArray);
            })
            .catch(function(error) {
                console.log(error);
            });
    }

    function processResult(curatorArray) {
        let i = 0;
        for (let vote of curatorArray) {
            if (i < 10) {
                vote.curators.ratio = Number((vote.curators.ratio*1000000000).toFixed(3));
                vote.curators.SP_reward = Number((vote.curators.SP_reward).toFixed(3));
                vote.curators.STU_vote_value = Number((vote.curators.STU_vote_value).toFixed(3));
                console.dir(vote, { depth: null });
            }
            i+=1;
        }
        console.log('Total count analysed', i);
        client.close();
    }
}

module.exports.findCuratorMongo = findCuratorMongo;
