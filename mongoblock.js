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
    let result = 0;
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
function processComment(localOperation, localOperationNumber, mongoComment, db) {
    let appName = '';
    let appVersion = '';
    let msecondsInSevenDays = 604800000;
    let commentTimestamp = new Date(localOperation.timestamp + '.000Z');
    let json = {};

    // Parsing application / version data
    if (localOperation.op[1].hasOwnProperty('json_metadata')) {
        try {
            json = JSON.parse(localOperation.op[1].json_metadata);
        } catch(error) {
            [appName, appVersion] = ['other', 'badJson']
        }

        if (json == null || appName == 'other')
            [appName, appVersion] = ['other', 'badJson']
        else {
            if (json.hasOwnProperty('app')) {
                if (json.app == null) {
                    [appName, appVersion] = ['other', 'nullApp']
                } else if (json.app.hasOwnProperty('name')) { // parley
                    appName = json.app.name;
                } else if (json.app instanceof Array) { // cryptoowls
                    console.log(json.app, typeof json.app, json.app instanceof Array)
                    appName = json.app[0];
                } else {
                    [appName, appVersion] = json.app.split('/');
                }
            } else {
                [appName, appVersion] = ['other', 'noApp']
            }
        }
    } else {
        [appName, appVersion] = ['other', 'noJson']
    }


    // Basic depth measure (post or comment)
    if (localOperation.op[1].parent_author == '') {
        postComment = 0;
    } else {
        postComment = 1;
    }

    // Setting record if no author_payout (i.e. blocks running forwards)
    let commentRecord = {author: localOperation.op[1].author, permlink: localOperation.op[1].permlink, blockNumber: localOperation.block, timestamp: commentTimestamp,
                    transactionNumber: localOperation.trx_in_block, operationNumber: localOperationNumber, transactionType: 'commentUnverified', application: appName, applicationVersion: appVersion, postComment: postComment};

    // Self-validation of original comments using 7 day payout period
    db.collection('comments').find({ author: localOperation.op[1].author, permlink: localOperation.op[1].permlink, operations: 'author_reward'}).toArray()
        .then(function(commentArray) {
            if(commentArray.length > 0) {
                if (commentArray[0].payout_timestamp - commentTimestamp == msecondsInSevenDays) {
                    commentRecord.transactionType = 'commentOriginal';
                } else {
                    commentRecord.transactionType = 'commentEdit';
                }
            }
            mongoComment(db, commentRecord, 0);
        });
}

module.exports.processComment = processComment;



// Comment - update / insert of mongo record
// -----------------------------------------------
function mongoComment(db, localRecord, reattempt) {
    let maxReattempts = 1;
    let logComment = {transactionNumber: localRecord.transactionNumber, operationNumber: localRecord.operationNumber, transactionType: 'comment', count: 1, status: 'OK'};
    // Uses upsert - blocks may be processed in any order so author/permlink record may already exist if another operation is processed first
    db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink, application: {$exists : false}}, {$set: localRecord, $addToSet: {operations: 'comment'}}, {upsert: true})
        .then(function(response) {
            mongoOperationProcessed(db, localRecord.blockNumber, logComment, 1, 0);
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
                            mongoOperationProcessed(db, localRecord.blockNumber, logComment, 1, 0);
                            // add this operation to comment edits section of document?
                        } else {
                            // have to edit document with this comment as the new comment
                            db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink}, {$set: localRecord, $addToSet: {operations: 'comment'}}, {upsert: false})
                            mongoOperationProcessed(db, localRecord.blockNumber, logComment, 1, 0);
                            // and add old comment to comment edits?
                        }
                    } else {
                        if (reattempt < maxReattempts) {
                            console.log('E11000 error with <', localRecord.author, localRecord.blockNumber, '> comment. Re-attempting...');
                            mongoComment(db, localRecord, 1);
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
function processVote(localOperation, localOperationNumber, mongoVote, db) {
    let voteRecord = {author: localOperation.op[1].author, permlink: localOperation.op[1].permlink, voter: localOperation.op[1].voter, percent: Number((localOperation.op[1].weight/100).toFixed(0)), vote_timestamp: new Date(localOperation.timestamp + '.000Z'), vote_blockNumber: localOperation.block, transactionNumber: localOperation.trx_in_block, operationNumber: localOperationNumber, transactionType: localOperation.op[0]};
    mongoVote(db, voteRecord, 0);
}

module.exports.processVote = processVote;



// Vote - update / insert of mongo record
// -----------------------------------------------
function mongoVote(db, localRecord, reattempt) {
    let maxReattempts = 1;
    let logVote = {transactionNumber: localRecord.transactionNumber, operationNumber: localRecord.operationNumber, transactionType: 'vote', count: 1, status: 'OK'};
    db.collection('comments').find({ author: localRecord.author, permlink: localRecord.permlink, "curators.voter": localRecord.voter}).toArray()
        .then(function(result) {
            // Adds all vote details to curation set if author / permlink / voter combination not found
            if(result.length === 0) {
                db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink}, {$addToSet: {curators: {voter: localRecord.voter, percent: localRecord.percent, vote_timestamp: localRecord.vote_timestamp, vote_blockNumber: localRecord.vote_blockNumber}, operations: 'vote'}}, {upsert: true})
                    .then(function(response) {
                        mongoOperationProcessed(db, localRecord.vote_blockNumber, logVote, 1, 0);
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
                        mongoOperationProcessed(db, localRecord.vote_blockNumber, logVote, 1, 0);
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
async function processActiveVote(localVote, localAuthor, localPermlink, localBlockNumber, localVirtualOp, mongoActiveVote, db) {
    let formatVote = {voter: localVote.voter, curation_weight: localVote.weight, rshares: Number(localVote.rshares),
                              percent: Number((localVote.percent/100).toFixed(2)), reputation: Number(localVote.reputation), vote_timestamp: new Date(localVote.time + '.000Z')}
    let activeRecord = {author: localAuthor, permlink: localPermlink, activeVote: formatVote};

    return activeRecord;
}

module.exports.processActiveVote = processActiveVote;



// ActiveVotes - update / insert of mongo record
// ---------------------------------------------
async function mongoActiveVote(db, activeBlockNumber, activeVirtualOp, localRecord, reattempt) {
    let maxReattempts = 1;
    let activeUpdateStatus = '';
    await db.collection('comments').find({ author: localRecord.author, permlink: localRecord.permlink, "curators.voter": localRecord.activeVote.voter}).toArray()
        .then(async function(result) {
            // Adds all vote details to curation set if author / permlink / voter combination not found
            if(result.length === 0) {
                await db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink}, {$inc: {"rshares": localRecord.activeVote.rshares}, $addToSet: {curators: localRecord.activeVote, operations: 'active_votes'}}, {upsert: true})
                    .then(function(response) {
                        activeUpdateStatus = response.result;
                    })
                    .catch(function(error) {
                        if(error.code == 11000) {
                            if (reattempt < maxReattempts) {
                                console.log('E11000 error with <', localRecord.activeVote.voter, activeBlockNumber, '> ActiveVote. Re-attempting...');
                                mongoActiveVote(db, activeBlockNumber, activeVirtualOp, localRecord, 1);
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
                // No previous insert in Mongo for this active vote; entry was from vote or curation reward operations
                let arrayPosition = result[0].curators.findIndex(fI => fI.voter == localRecord.activeVote.voter);
                if(!(result[0].curators[arrayPosition].hasOwnProperty('rshares'))) {
                    await db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink, curators: { $elemMatch: { voter: localRecord.activeVote.voter}}},
                                  {$inc: {"rshares": localRecord.activeVote.rshares}, $set: {"curators.$.voter": localRecord.activeVote.voter, "curators.$.curation_weight": localRecord.activeVote.curation_weight, "curators.$.rshares": localRecord.activeVote.rshares,
                                    "curators.$.percent": localRecord.activeVote.percent, "curators.$.reputation": localRecord.activeVote.reputation, "curators.$.vote_timestamp": localRecord.activeVote.vote_timestamp},
                                    $addToSet: {operations: 'active_votes'}}, {upsert: false})
                        .then(function(response) {
                            activeUpdateStatus = response.result;
                        })
                        .catch(function(error) {
                              console.log('Error: active_vote', localRecord.activeVote.voter);
                        });
                } else {
                  // Previous insert in Mongo for this active vote; double operation or rerun - need to skip to avoid double counting in $inc
                  activeUpdateStatus = 'skipped';
                }
            }
        })
    return activeUpdateStatus;
}

module.exports.mongoActiveVote = mongoActiveVote;



// Author Reward - processing of block operation
// ---------------------------------------------
function processAuthorReward(localOperation, mongoAuthorReward, db) {
    let authorRecord = {author: localOperation.op[1].author, permlink: localOperation.op[1].permlink,
                    author_payout: {sbd: Number(localOperation.op[1].sbd_payout.split(' ', 1)[0]), steem: Number(localOperation.op[1].steem_payout.split(' ', 1)[0]), vests: Number(localOperation.op[1].vesting_payout.split(' ', 1)[0])},
                    payout_blockNumber: localOperation.block, payout_timestamp: new Date(localOperation.timestamp + '.000Z') };
    mongoAuthorReward(db, authorRecord, localOperation.virtual_op, 0);
}

module.exports.processAuthorReward = processAuthorReward;



// Author Reward - update / insert of mongo record
// -----------------------------------------------
function mongoAuthorReward(db, localRecord, virtualOp, reattempt) {
    // Uses upsert - blocks may be processed in any order so author/permlink record may already exist if another operation is processed first
    let maxReattempts = 1;
    let logAuthor = {virtualOp: virtualOp, transactionType: 'author_reward', count: 1, status: 'OK'};
    db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink}, {$set: localRecord, $addToSet: {operations: 'author_reward'}}, {upsert: true})
        .then(function(response) {
            mongoOperationProcessed(db, localRecord.payout_blockNumber, logAuthor, 1, 0);
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
function processBenefactorReward(localOperation, mongoBenefactorReward, db) {
    let benefactorRecord = {author: localOperation.op[1].author, permlink: localOperation.op[1].permlink, benefactor: localOperation.op[1].benefactor, benefactor_timestamp: new Date(localOperation.timestamp + '.000Z'),
                          benefactor_payout: {sbd: Number(localOperation.op[1].sbd_payout.split(' ', 1)[0]), steem: Number(localOperation.op[1].steem_payout.split(' ', 1)[0]), vests: Number(localOperation.op[1].vesting_payout.split(' ', 1)[0])},
                          virtualOp: localOperation.virtual_op, payout_blockNumber: localOperation.block};
    mongoBenefactorReward(db, benefactorRecord, 0);
}

module.exports.processBenefactorReward = processBenefactorReward;



// Benefactor Reward - update / insert of mongo record
// -----------------------------------------------
function mongoBenefactorReward(db, localRecord, reattempt) {
    // Uses upsert - blocks may be processed in any order so author/permlink record may already exist if another operation is processed first
    let maxReattempts = 1;
    let logBenefactor = {virtualOp: localRecord.virtualOp, transactionType: 'benefactor_reward', count: 1, status: 'OK'};
    db.collection('comments').find({ author: localRecord.author, permlink: localRecord.permlink, "benefactors.user": localRecord.benefactor}).toArray()
        .then(function(result) {
            // Adds all benefactor details to benefactor set if author / permlink / benefactor combination not found
            if(result.length === 0) {
                db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink, "benefactors.user": {$ne: localRecord.benefactor}},
                                                    {$inc: {"benefactor_payout.sbd": localRecord.benefactor_payout.sbd, "benefactor_payout.steem": localRecord.benefactor_payout.steem, "benefactor_payout.vests": localRecord.benefactor_payout.vests},
                                                        $addToSet: {operations: 'benefactor_payout', benefactors: {user: localRecord.benefactor, sbd: localRecord.benefactor_payout.sbd, steem: localRecord.benefactor_payout.steem, vests: localRecord.benefactor_payout.vests, timestamp: localRecord.benefactor_timestamp}}},
                                                    {upsert: true})
                    .then(function(response) {
                        mongoOperationProcessed(db, localRecord.payout_blockNumber, logBenefactor, 1, 0);
                    })
                    .catch(function(error) {
                        if(error.code == 11000) {
                            if (reattempt < maxReattempts) {
                                console.log('E11000 error with <', localRecord.benefactor, localRecord.payout_blockNumber, '> benefactor_payout. Re-attempting...');
                                mongoBenefactorReward(db, localRecord, 1);
                            } else {
                                console.log('E11000 error with <', localRecord.benefactor, localRecord.payout_blockNumber, '> benefactor_payout. Maximum reattempts surpassed.');
                            }
                        } else {
                            console.log('Non-standard error with <', localRecord.benefactor, localRecord.payout_blockNumber, '> benefactor_payout.');
                            console.log(error);
                        }
                    });
            // No need to update 'comments' collection. Either a repeat operation in a block or a rerun of a block with operation already inserted in 'comments'
            } else {
                mongoOperationProcessed(db, localRecord.payout_blockNumber, logBenefactor, 1, 0);
            }
    })
}

module.exports.mongoBenefactorReward = mongoBenefactorReward;



// Curator Reward - processing of block operation
// -------------------------------------------------
function processCuratorReward(localOperation, mongoCuratorReward, db) {
    let curatorRecord = {author: localOperation.op[1].comment_author, permlink: localOperation.op[1].comment_permlink, voter: localOperation.op[1].curator, reward_timestamp: new Date(localOperation.timestamp + '.000Z'),
                  curator_payout: {vests: Number(localOperation.op[1].reward.split(' ', 1)[0])}, virtualOp: localOperation.virtual_op, payout_blockNumber: localOperation.block};
    mongoCuratorReward(db, curatorRecord, 0);
}

module.exports.processCuratorReward = processCuratorReward;



// Curator Reward - update / insert of mongo record
// -----------------------------------------------
function mongoCuratorReward(db, localRecord, reattempt) {
    let maxReattempts = 1;
    let logCurator = {virtualOp: localRecord.virtualOp, transactionType: 'curator_reward', count: 1, status: 'OK'};
    db.collection('comments').find({ author: localRecord.author, permlink: localRecord.permlink, "curators.voter": localRecord.voter}).toArray()
        .then(function(result) {
            // Adds all vote details to curation set if author / permlink / voter combination not found
            if(result.length === 0) {
                db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink}, {$inc: {"curator_payout.vests": localRecord.curator_payout.vests},
                                                        $addToSet: {curators: {voter: localRecord.voter, vests: localRecord.curator_payout.vests, reward_timestamp: localRecord.reward_timestamp}, operations: 'curator_payout'}}, {upsert: true})
                    .then(function(response) {
                        mongoOperationProcessed(db, localRecord.payout_blockNumber, logCurator, 1, 0);
                    })
                    .catch(function(error) {
                        if(error.code == 11000) {
                            if (reattempt < maxReattempts) {
                                console.log('E11000 error with <', localRecord.voter, localRecord.payout_blockNumber, '> curator_payout. Re-attempting...');
                                mongoCuratorReward(db, localRecord, 1);
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
                // No previous insert in Mongo for this curation reward; entry was from vote or active vote operations
                let arrayPosition = result[0].curators.findIndex(fI => fI.voter == localRecord.voter);
                if(!(result[0].curators[arrayPosition].hasOwnProperty('vests'))) {
                    db.collection('comments').updateOne({ author: localRecord.author, permlink: localRecord.permlink, curators: { $elemMatch: { voter: localRecord.voter}}},
                                  {$inc: {"curator_payout.vests": localRecord.curator_payout.vests}, $set: {"curators.$.voter": localRecord.voter, "curators.$.vests": localRecord.curator_payout.vests, "curators.$.reward_timestamp": localRecord.reward_timestamp},
                                    $addToSet: {operations: 'curator_payout'}}, {upsert: false})
                        .then(function(response) {
                            mongoOperationProcessed(db, localRecord.payout_blockNumber, logCurator, 1, 0);
                        })
                        .catch(function(error) {
                              console.log('Error: curator_payout', localRecord.voter);
                        });
                // Previous insert in Mongo for this curation reward; double operation or rerun - need to skip to avoid double counting in $inc
                } else {
                    mongoOperationProcessed(db, localRecord.payout_blockNumber, logCurator, 1, 0);
                }
            }
        })
}


module.exports.mongoCuratorReward = mongoCuratorReward;



// Initialisation of blocksProcessed documents for each block and handling of reprocessed blocks
// ---------------------------------------------------------------------------------------------
function mongoBlockProcessed(db, localBlockRecord, reattempt) {
    let maxReattempts = 1;
    // Add record of block to blocksProcessed collection in database
    db.collection('blocksProcessed').findOneAndUpdate({ blockNumber: localBlockRecord.blockNumber, status: {$ne : 'OK'}}, {$set: localBlockRecord}, {upsert: true, returnOriginal: false, maxTimeMS: 1000})
        .then(function(response) {
            if (response.value.hasOwnProperty('operationsProcessed') && response.value.hasOwnProperty('activeVoteSetProcessed')) {
                if ((response.value.operationsCount == response.value.operationsProcessed) && (response.value.activeVoteSetCount == response.value.activeVoteSetProcessed) && (response.value.status == 'Processing')) {
                    db.collection('blocksProcessed').updateOne({ blockNumber: localBlockRecord.blockNumber}, {$set: {status: 'OK'}})
                    //console.log(response.value.operationsCount, response.value.operationsProcessed, response.value.activeVoteSetCount, response.value.activeVoteSetProcessed, response.value.status, 'set to ok - mongoBlockProcessed', localBlockRecord.blockNumber)
                }
            }
        })
        .catch(function(error) {
            if(error.code == 11000) {
                if (reattempt < maxReattempts) {
                    console.log('E11000 error with <', localBlockRecord.blockNumber, '> mongoBlockProcessed. Re-attempting...');
                    mongoBlockProcessed(db, localBlockRecord, 1);
                } else {
                    console.log('E11000 error with <', localBlockRecord.blockNumber, '> mongoBlockProcessed. Maximum reattempts surpassed.');
                }
            } else {
                console.log('Non-standard error with <', localBlockRecord.blockNumber, '> mongoBlockProcessed.');
                console.log(error);
            }
    });
}

module.exports.mongoBlockProcessed = mongoBlockProcessed;



// Update blocksProcessed with details of a single operation processed
// -------------------------------------------------------------------
function mongoOperationProcessed(db, localBlockNumber, operationLog, operationsIncluded, reattempt) {
    let maxReattempts = 1;
    db.collection('blocksProcessed').findOneAndUpdate({ blockNumber: localBlockNumber}, {$addToSet: {operations: operationLog}, $inc: {operationsProcessed: operationsIncluded}}, {upsert: true, returnOriginal: false, maxTimeMS: 1000})
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
            if ((response.value.operationsCount == response.value.operationsProcessed) && (response.value.activeVoteSetCount == response.value.activeVoteSetProcessed) && (response.value.status == 'Processing')) {
                db.collection('blocksProcessed').updateOne({ blockNumber: localBlockNumber}, {$set: {status: 'OK'}})
                //console.log(response.value.operationsCount, response.value.operationsProcessed, response.value.activeVoteSetCount, response.value.activeVoteSetProcessed, response.value.status, 'set to ok - mongoOperationProcessed', localBlockNumber)
            }
        })
        .catch(function(error) {
            if(error.code == 11000) {
                if (reattempt < maxReattempts) {
                    console.log('E11000 error with <', localBlockNumber, operationLog, '> mongoOperationProcessed. Re-attempting...');
                    mongoOperationProcessed(db, localBlockNumber, operationLog, operationsIncluded, 1);
                } else {
                    console.log('E11000 error with <', localBlockNumber, operationLog, '> mongoOperationProcessed. Maximum reattempts surpassed.');
                }
            } else {
                console.log('Non-standard error with <', localBlockNumber, operationLog, '> mongoOperationProcessed.');
                console.log(error);
            }
        });
}

module.exports.mongoOperationProcessed = mongoOperationProcessed;



// Update blocksProcessed with details for a set of active_votes (which is not a block operation so requires separate treatmenet)
// ----------------------------------------------------------------------------------------------------------------------------
async function mongoActiveProcessed(db, localActiveBlockNumber, localActiveLog, activeVoteSetIncluded, startEnd, reattempt) {
    let maxReattempts = 1;

    // Function carries out updates at the start and the end of each set of active_votes
    if (startEnd == 'start') {
        // Check if already created activevote op holder for blocknumber / associated virtual op number / type active vote
        await db.collection('blocksProcessed').find({blockNumber: localActiveBlockNumber, "operations.associatedOp": localActiveLog.associatedOp}).toArray()
            .then(function(result) {
                if (result.length === 0) {
                    // If not found: add to set
                    db.collection('blocksProcessed').updateOne({ blockNumber: localActiveBlockNumber}, {$addToSet: {operations: localActiveLog}}, {upsert: true})
                        .catch(function(error) {
                            if(error.code == 11000) {
                                if (reattempt < maxReattempts) {
                                    console.log('E11000 error with <', localActiveBlockNumber, '> mongoActiveProcessed. Re-attempting...');
                                    mongoActiveProcessed(db, localActiveBlockNumber, localActiveLog, activeVoteSetIncluded, startEnd, 1) ;
                                } else {
                                    console.log('E11000 error with <', localActiveBlockNumber, '> mongoActiveProcessed. Maximum reattempts surpassed.');
                                }
                            } else {
                                console.log('Non-standard error with <', localActiveBlockNumber, '> mongoActiveProcessed.');
                                console.log(localActiveBlockNumber, localActiveLog.associatedOp, localActiveLog.transactionType)
                                console.log(error);
                            }
                        })

                } else {
                    // Already set up active_votes in operations - so must be a re-run or repeat operation - reset
                    db.collection('blocksProcessed').findOneAndUpdate(  { blockNumber: localActiveBlockNumber, operations: { $elemMatch: { associatedOp: localActiveLog.associatedOp}}},
                                                                        { $set: { "operations.$.associatedOp": localActiveLog.associatedOp, "operations.$.transactionType": localActiveLog.transactionType,
                                                                                  "operations.$.count": localActiveLog.count, "operations.$.activeVotesCount": localActiveLog.activeVotesCount}},
                                                                        { upsert: false, returnOriginal: false, maxTimeMS: 1000})

                }
            });
    // Processing 'end' of active_vote set update
    } else {
        db.collection('blocksProcessed').findOneAndUpdate(  { blockNumber: localActiveBlockNumber, operations: { $elemMatch: { associatedOp: localActiveLog.associatedOp}}},
                                                            { $set: {"operations.$.status": localActiveLog.status, "operations.$.activeVotesProcessed": localActiveLog.activeVotesProcessed},
                                                              $inc: {activeVoteSetProcessed: activeVoteSetIncluded}},
                                                            { upsert: false, returnOriginal: false, maxTimeMS: 1000})
            .then(function(response) {
                if (response.value == null) {
                    if (reattempt < maxReattempts) {
                        console.log('null response from mongoActiveProcessed <', localActiveBlockNumber, '>. Re-attempting...')
                        mongoActiveProcessed(db, localActiveBlockNumber, localActiveLog, activeVoteSetIncluded, startEnd, 1) ;
                    } else {
                        console.log('null response from mongoActiveProcessed <', localActiveBlockNumber, '>. Maximum reattempts surpassed. Error logged.');
                        let errorRecord = {blockNumber: localActiveBlockNumber, status: 'error'};
                        mongoblock.mongoErrorLog(db, errorRecord, 0);
                    }
                } else if ((response.value.operationsCount == response.value.operationsProcessed) && (response.value.activeVoteSetCount == response.value.activeVoteSetProcessed) && (response.value.status == 'Processing')) {
                    db.collection('blocksProcessed').updateOne({ blockNumber: localActiveBlockNumber}, {$set: {status: 'OK'}})
                    //console.log(response.value.operationsCount, response.value.operationsProcessed, response.value.activeVoteSetCount, response.value.activeVoteSetProcessed, response.value.status, 'set to ok - mongoActiveProcessed', localActiveBlockNumber)
                }
            })
            .catch(function(error) {
                console.log(error)
                console.log('Active votes log - end update - error.', localActiveBlockNumber, localActiveLog.associatedOp)
            });
    }
}

module.exports.mongoActiveProcessed = mongoActiveProcessed;



// Update blocksProcessed for an error
// -----------------------------------
  function mongoErrorLog(db, localErrorRecord, reattempt) {
    let maxReattempts = 1;
    db.collection('blocksProcessed').findOneAndUpdate({ blockNumber: localErrorRecord.blockNumber}, {$set: {status: localErrorRecord.status}}, {upsert: true, returnOriginal: false, maxTimeMS: 1000} )
        .then(function(response) {
            console.log('Error update:', response.value.blockNumber, response.value.status, response.value.operationsCount, response.value.operationsError)
        })
        .catch(function(error) {
            if(error.code == 11000) {
                if (reattempt < maxReattempts) {
                    console.log('E11000 error with <', localErrorRecord.blockNumber, '> mongoErrorLog. Re-attempting...');
                    mongoErrorLog(db, localErrorRecord, 1);
                } else {
                    console.log('E11000 error with <', localErrorRecord.blockNumber, '> mongoErrorLog. Maximum reattempts surpassed.');
                }
            } else {
                console.log('Non-standard error with <', localErrorRecord.blockNumber, '> mongoErrorLog.');
                console.log(error);
            }
        });
}

module.exports.mongoErrorLog = mongoErrorLog;



// Function to self-validate comments based on time between comment blockNumber and author payout
// ----------------------------------------------------------------------------------------------
function validateComments(db, localOperation) {
    let msecondsInSevenDays = 604800000;
    let payoutDate = new Date(localOperation.timestamp + '.000Z');
    // Find document in comments based on author / permlink
    db.collection('comments').find(
        {author: localOperation.op[1].author, permlink: localOperation.op[1].permlink, operations: 'comment'}
    ).toArray()
    .then(function(commentArray) {
        if(commentArray.length > 0) {
            // Check if difference between document timestamp and author payout timestamp is 7 days
            if (payoutDate - commentArray[0].timestamp == msecondsInSevenDays) {
                // Change comment transactionType to 'commentOriginal'
                db.collection('comments').updateOne({ author: localOperation.op[1].author, permlink: localOperation.op[1].permlink}, {$set: {transactionType: 'commentOriginal'}}, {upsert: false})
                    .catch(function(error) {
                        console.log(error);
                    });
            } else {
                // Change comment transactionType to 'commentEdit'
                db.collection('comments').updateOne({ author: localOperation.op[1].author, permlink: localOperation.op[1].permlink}, {$set: {transactionType: 'commentEdit'}}, {upsert: false})
                    .catch(function(error) {
                        console.log(error);
                    });
            }
        } else {
            // No 'expectedCreation' date required - comment operations are validated against any pre-existing author_payout operations in function processComment
        }
    });
}

module.exports.validateComments = validateComments;



// Function to find comments for currency exchange calculations on each hour
// -------------------------------------------------------------------------
async function mongoFillPrices(db, openBlock, closeBlock) {
    console.log(openBlock, closeBlock)
    let commentsForPrices = [];

    await db.collection('comments').aggregate([
        { $match :  {$and:[
                      { operations: 'author_reward'},
                      { "author_payout.vests": { $gte: 250}},
                      { payout_blockNumber: { $gte: openBlock, $lt: closeBlock }},
                    ]}},
        { $project : {_id: 0, payout_timestamp: 1, dateObject: {$dateToParts: {date: "$payout_timestamp"}}, "curator_payout.vests": 1, "author_payout.steem": 1, "author_payout.sbd": 1, "author_payout.vests": 1, rshares: 1, author: 1, permlink: 1, payout_blockNumber: 1 }},
        { $match :  {$and:[
                      {"dateObject.minute": { $gte: 20, $lte: 40}},
                      {"curator_payout.vests": { $gt: 0}}
                    ]}},
        { $sort: {"dateObject.year": 1, "dateObject.month": 1, "dateObject.day": 1, "dateObject.hour": 1, "author_payout.steem": -1, "curator_payout.vests": -1}},
        { $group : {_id: {year: "$dateObject.year", month: "$dateObject.month", day: "$dateObject.day", hour: "$dateObject.hour"},
                          dateHour: {$first: "$payout_timestamp"},
                          author: {$first: "$author"},
                          permlink: {$first: "$permlink"},
                          payout_blockNumber: {$first: "$payout_blockNumber"},
                          author_payout_steem: {$first: "$author_payout.steem"},
                          author_payout_sbd: {$first: "$author_payout.sbd"},
                          author_payout_vests: {$first: "$author_payout.vests"},
                          curator_payout_vests: {$first: "$curator_payout.vests"},
                          rshares: {$first: "$rshares"},
                  }},
        { $sort: {payout_blockNumber: 1}}
        ])
        .toArray()
        .then(function(prices) {
            for (let price of prices) {
                price._id = price.dateHour.toISOString().slice(0, 13);
            }
            commentsForPrices = prices;
        })
        return commentsForPrices;
}

module.exports.mongoFillPrices = mongoFillPrices;



// Function to add price information to Mongo prices collection
// -------------------------------------------------------------------------
function mongoPrice(db, localRecord, reattempt) {
    let maxReattempts = 1;
    return db.collection('prices').insertOne(localRecord)
        .catch(function(error) {
            if(error.code == 11000) {
                if (reattempt < maxReattempts) {
                    console.log('E11000 error with <', localRecord.payout_blockNumber, '> mongoPrice. Re-attempting...');
                    mongoPrice(db, localRecord, 1)
                } else {
                    console.log('E11000 error with <', localRecord.payout_blockNumber, '> mongoPrice. Maximum reattempts surpassed.');
                }
            } else {
                console.log('Non-standard error with <', localRecord.payout_blockNumber, '> mongoPrice.');
                console.log(error);
            }
    });
}

module.exports.mongoPrice = mongoPrice;



// Function to aggregate price information over date range
// -------------------------------------------------------
async function obtainPricesMongo(db, openBlock, closeBlock) {

    return await db.collection('prices').aggregate([
            { $match :    {payout_blockNumber: { $gte: openBlock, $lt: closeBlock }}},
            { $project :  {_id: 1, vestsPerSTU: 1, rsharesPerSTU: 1, steemPerSTU: 1}},
            { $sort: {_id: 1 }},
        ])
        .toArray();
}

module.exports.obtainPricesMongo = obtainPricesMongo;



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



// Function shows detail of a single block document from blocksProcessed
// ---------------------------------------------------------------------
async function showBlockMongo(db, openBlock) {
      // Provide additional detail on error / processing blocks for debugging
      await db.collection('blocksProcessed').aggregate([
              { $match : {blockNumber: { $gte: openBlock, $lte: openBlock}}}
          ]).toArray()
          .then(function(records) {
              for (let record of records) {
                  delete record._id;
                  console.dir(record, {depth: null});
              }
          })
          .catch(function(error) {
              console.log(error);
          });
      console.log('------------------------------------------------------------------------');

      await db.collection('blocksProcessed').aggregate([
              { $match : {blockNumber: { $gte: openBlock, $lte: openBlock}}},
              { $project : {_id: 0, operations: 1 }},
              { $unwind : "$operations"},
              { $sort : {"operations.virtualOp": 1, "operations.transactionNumber": 1, "operations.operationNumber": 1, }},
          ]).toArray()
          .then(function(records) {
              for (let record of records) {
                  delete record._id;
                  console.dir(record, {depth: null});
              }
          })
          .catch(function(error) {
              console.log(error);
          });
      console.log('------------------------------------------------------------------------');
      console.log('closing mongo db');
      client.close();
}

module.exports.showBlockMongo = showBlockMongo;



// Function reports on blocks processed or returns blocks to process for fill operations
// -------------------------------------------------------------------------------------
async function reportBlocksProcessed(db, openBlock, closeBlock, retort, detail) {

    if (retort == 'report') {
        let blocksPerDay = [];

        // Extract number of existing blocks from blockDates (determined in setup)
        await db.collection('blockDates')
            .aggregate([
                { $match : {blockNumber: {$gte: openBlock, $lte: closeBlock}}},
                { $project : {_id: 0, date: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp" }}, blockNumber: 1}},
            ])
            .toArray()
            .then(function(blockDates) {
                for (var i = 0; i < blockDates.length-1; i+=1) {
                    blocksPerDay.push({date: blockDates[i].date, blocks_exist: (blockDates[i+1].blockNumber - blockDates[i].blockNumber)})
                }
            })
        // Aggregate status of blocks processed in chosen date/blocks range and consolidate with above data on existing blocks
        await db.collection('blocksProcessed').aggregate([
                { $match : {blockNumber: { $gte: openBlock, $lt: closeBlock }}},
                { $project : {_id: 0, date: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp" }}, blockNumber: 1, status: 1}},
                { $group : {_id : {date: "$date", status: "$status"}, count: { $sum: 1 }, }},
                { $sort: {"_id.date": 1, "_id.status": 1}},
            ])
            .toArray()
            .then(function(records) {

                let formattedRecords = [];
                    for (let row of records) {
                        collectionPosition = blocksPerDay.findIndex(fI => fI.date == row._id.date);
                        if (collectionPosition == -1) {
                            blocksPerDay.push({date: row._id.date, status: row._id.status, count: row.count })
                        } else {
                            blocksPerDay[collectionPosition][row._id.status] = row.count
                        }
                    }
                console.log('------------------------------------------------------------------------');
                console.log(blocksPerDay)
                console.log('------------------------------------------------------------------------');
        }).catch(function(error) {
            console.log(error);
        });

        if (detail == 'detail') {
            // Provide additional detail on error / processing blocks for debugging
            await db.collection('blocksProcessed').aggregate([
                    { $match : {blockNumber: { $gte: openBlock, $lt: closeBlock }, status: {$ne: 'OK'}}} //status: {$ne: 'OK'}}},
                    //{ $project : {_id: 0, blockNumber: 1, status: 1, operationsCount: 1, operationsProcessed: 1}},
                ]).toArray()
                .then(function(records) {
                    for (let record of records) {
                        delete record._id;
                        console.dir(record, {depth: null});
                    }
                }).catch(function(error) {
                    console.log(error);
                });
        }
        console.log('closing mongo db');
        console.log('------------------------------------------------------------------------');
        client.close();
    }

    // Returns blocks to process for fill operations
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
            if (okArray.length != 1000) {
                console.log(okArray)
            }
        }
        console.log('result');
        if (result.length == 0) {
            console.log('0 blocks to process');
        } else {
            console.log(result[0], result[result.length-1], result.length);
            if (result.length != 1000) {
                console.log(result)
            }
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
                {$set: {operationsProcessed: 0, activeVoteSetProcessed: 0}, $pull: { operations: {transactionType: "notHandled", transactionType: "active_vote"}}}, {upsert: false})
        .catch(function(error) {
            console.log(error);
        });
}

module.exports.resetBlocksProcessed = resetBlocksProcessed;



// Function lists comments for an application to give an idea of comment structure
// -------------------------------------------------------------------------------
function findCommentsMongo(localApp, db, openBlock, closeBlock) {
    console.log(openBlock, closeBlock)
    db.collection('comments').find(

        {$and : [
            { payout_blockNumber: { $gte: openBlock, $lt: closeBlock }},
            //{operations: 'comment'},
            //{operations: 'vote'},
            //{operations: 'author_reward'},
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
            { operations: 'curator_payout'},
            { operations: 'comment'},
        ]}).sort({author:1}).toArray()
        .then(function(details) {
            let i = 0, counter = 0, max = 0;
            console.log(details.length + ' records')
            for (let comment of details) {
                for (let indiv of comment.curators) {
                    let differential = (indiv.timestamp - comment.timestamp) - (7*24*60*60*1000);
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
    let minRshares = 50000000000; // 50bn

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



// Function reports on blocks processed or returns blocks to process for fill operations
// -------------------------------------------------------------------------------------
async function validateCommentsMongo(db, openBlock, closeBlock) {

        // Breakdown between original, edited, and unverified comments
        await db.collection('comments').aggregate([
                { $match : {blockNumber: { $gte: openBlock, $lt: closeBlock }}},
                { $project : {_id: 0, date: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp" }}, transactionType: 1, author_payout: 1}}, //, operationsCount: 1, operationsProcessed: 1
                { $group :
                    {_id : {date: "$date", transactionType: "$transactionType"},
                    count: { $sum: 1 },
                    author_payout_vests: {$sum: "$author_payout.vests"},
                }},
                { $sort: {"_id.date": 1, "_id.transactionType": 1}},
            ])
            .toArray()
            .then(function(records) {
                console.log('------------------------------------------------------------------------');
                console.log('Breakdown between original, edited, and unverified comments');
                console.log(records)
                console.log('------------------------------------------------------------------------');
        }).catch(function(error) {
            console.log(error);
        });


        // Breakdown of comment payouts by creation date
        await db.collection('comments').aggregate([
                { $match :
                    { $and :[
                        { payout_blockNumber: { $gte: openBlock, $lt: closeBlock }},
                        { operations: 'author_reward'},
                        { operations: 'active_votes'},
                    ]}
                },
                { $project : {_id: 0, date: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp" }}, operations: 1}}, //, operationsCount: 1, operationsProcessed: 1
                { $group :
                    {_id : {date: "$date"},
                    count: { $sum: 1 },

                }},
                { $sort: {"_id.date": 1}},
            ])
            .toArray()
            .then(function(records) {
                console.log('------------------------------------------------------------------------');
                console.log('Breakdown of comment payouts by creation date');
                console.dir(records, {depth: null})
                console.log('------------------------------------------------------------------------');
        }).catch(function(error) {
            console.log(error);
        });


        // Check all operations in block processed
        await db.collection('blocksProcessed').aggregate([
                { $match :
                    { $and :[
                        { blockNumber: { $gte: openBlock, $lt: closeBlock }},
                    ]}},
                { $project : {_id: 0, blockNumber: 1, "operations.count": 1, operationsProcessed: 1, operationsCount: 1, }}, //, operationsCount: 1, operationsProcessed: 1
                { $unwind : "$operations"},
                { $group :
                    {_id : {blockNumber: "$blockNumber", operationsCount: "$operationsCount"},
                    individual_count: { $sum: "$operations.count"}}},
                { $project : {"_id.blockNumber": 1, individual_count: 1, "_id.operationsCount": 1, check_zero: { $subtract: [ "$_id.operationsCount", "$individual_count" ]}}},
                ])
                .toArray()
                .then(function(checks) {
                    let countCheckCounter = 0;
                    console.log('------------------------------------------------------------------------');
                    console.log('Check all operations in block processed');

                    for (let check of checks) {
                        if (check.check_zero != 0) {
                            console.dir(check, {depth: null})
                            countCheckCounter += 1;
                        }
                    }
                    console.log(checks.length + ' records checked. ' + countCheckCounter + ' errors.')

                    console.log('------------------------------------------------------------------------');
            }).catch(function(error) {
                console.log(error);
            });


            // Check of active vote processing
            await db.collection('blocksProcessed').aggregate([
                    { $match :
                        { $and :[
                            { blockNumber: { $gte: openBlock, $lt: closeBlock }},
                            { "operations.transactionType": 'author_reward'}
                        ]}},
                    { $project : {_id: 0, blockNumber: 1, operations: {virtualOp: 1, associatedOp: 1, transactionType: 1, activeVotesCount: 1, activeVotesProcessed: 1 }}}, //"$curators.vests", "$curators.rshares"
                    { $unwind : "$operations"},
                    { $project : {_id: 0, blockNumber: 1, operations: {virtualNumber: {$ifNull: ["$operations.associatedOp", "$operations.virtualOp"]}, transactionType: 1, activeVotesCount: 1, activeVotesProcessed: 1 }}},
                    { $match :
                        { $or :[
                            {"operations.transactionType": 'author_reward'},
                            {"operations.transactionType": 'active_vote'},
                        ]}},
                    { $group: {_id : { blockNumber: "$blockNumber", virtualOp: "$operations.virtualNumber"},
                                        activeVotesCount: { $sum: "$operations.activeVotesCount"},
                                        activeVotesProcessed: { $sum: "$operations.activeVotesProcessed"},
                                        count: { $sum: 1}
                                      }},
                    ])
                    .toArray()
                    .then(function(checks) {
                        let countCheckCounter = 0;
                        console.log('------------------------------------------------------------------------');
                        console.log('Check of active vote processing');
                        //console.dir(checks, {depth: null})
                        for (let check of checks) {
                            if (check.activeVotesCount != check.activeVotesProcessed || check.count != 2) {
                                console.dir(check, {depth: null})
                                countCheckCounter += 1;
                            }
                        }
                        console.log(checks.length + ' records checked. ' + countCheckCounter + ' errors.')
                        console.log('------------------------------------------------------------------------');
                }).catch(function(error) {
                    console.log(error);
                });

        client.close();
}

module.exports.validateCommentsMongo = validateCommentsMongo;



// Vote timing analysis: Covers all posts (not comments), all users / self-votes / a parameterised user group
// ----------------------------------------------------------------------------------------------------------
async function voteTimingMongo(db, openBlock, closeBlock, userGroup) {
    let voteBreakdown = [];
    let boundaryArray = [];
    let secondBuckets = 60; // 60 = 1 minute buckets, 60 * 60 = 1 hour buckets
    let analysisDurationSeconds = 60 * 60; // 60 * 60 = first hour breakdown, 60 * 60 * 24 * 7 = full week breakdown
    let numberOfBuckets = analysisDurationSeconds / secondBuckets;

    for (var i = 0; i < numberOfBuckets + 1; i+=1) {
        boundaryArray.push(i * secondBuckets * 1000)
    }

    await db.collection('comments').aggregate([
            { $match :
                { $and :[
                    { blockNumber: { $gte: openBlock, $lt: closeBlock }},
                    { transactionType: { $ne: 'commentEdit' }},
                    { postComment: 0 },
                    { operations : 'active_votes' },
                ]}},
            { $unwind : "$curators" },
            { $project : {  _id: 0, author: 1, "curators.voter": 1,
                            self_vote: {$cond: { if: { $eq: [ "$author", "$curators.voter"] }, then: "self", else: "other" }},
                            userGroup: {$cond: { if: { $in: [ "$curators.voter", userGroup] }, then: true, else: false }},
                            timestamp: 1,
                            curators: {vote_timestamp: 1, dateHour: {$substr: ["$curators.vote_timestamp", 0, 13]},
                            //curators: {vote_timestamp: 1, dateHour: {$substr: ["$payout_timestamp", 0, 13]},
                            dateObject: {$dateToParts: {date: "$curators.vote_timestamp"}}, rshares: 1, vests: 1, percent: 1,
                            vote_mseconds: { $subtract: [ "$curators.vote_timestamp", "$timestamp" ]}}}},
            { $lookup : {   from: "prices",
                            localField: "curators.dateHour",
                            foreignField: "_id",
                            as: "curator_vote_prices"   }},
            { $project : {_id: 0, timestamp: 1, self_vote: 1, userGroup: 1, "curator_vote_prices": { "$arrayElemAt": [ "$curator_vote_prices", 0 ]} ,
                              curators: {vote_timestamp: 1, rshares: 1, vests: 1, percent: 1, vote_mseconds: 1 }}}, //
            { $project : {_id: 0, timestamp: 1, self_vote: 1, userGroup: 1, "curator_vote_prices.rsharesPerSTU": 1,
                              vote_value: { $divide: [ "$curators.rshares", "$curator_vote_prices.rsharesPerSTU" ]},
                              curator_payout_value: { $divide: [ "$curators.vests", "$curator_vote_prices.vestsPerSTU" ]},
                              curators: {vote_timestamp: 1, rshares: 1, vests: 1, percent: 1, vote_mseconds: 1 }}},
            { $facet: {
                "all": [
                    { $bucket: {
                          groupBy: "$curators.vote_mseconds",
                          boundaries: boundaryArray,
                          default: "other",
                          output: {
                             "rshares": { $sum: "$curators.rshares"},
                             "upvote_rshares": { $sum : { $cond: [{ $gte: ['$curators.rshares', 0]}, "$curators.rshares", 0]}},
                             "downvote_rshares": { $sum : { $cond: [{ $lt: ['$curators.rshares', 0]}, "$curators.rshares", 0]}},
                             "vote_value": { $sum: "$vote_value"},
                             "upvote_vote_value": { $sum : { $cond: [{ $gte: ['$vote_value', 0]}, "$vote_value", 0]}},
                             "downvote_vote_value": { $sum : { $cond: [{ $lt: ['$vote_value', 0]}, "$vote_value", 0]}},
                             "curator_vests": { $sum: "$curators.vests"},
                             "curator_payout_value": { $sum: "$curator_payout_value"},
                             "count": { $sum: 1 }

                          }
                       }
                    }
                ],
                "userGroup": [
                    { $match : { userGroup: true }},
                    { $bucket: {
                          groupBy: "$curators.vote_mseconds",
                          boundaries: boundaryArray,
                          default: "other",
                          output: {
                             "rshares": { $sum: "$curators.rshares"},
                             "upvote_rshares": { $sum : { $cond: [{ $gte: ['$curators.rshares', 0]}, "$curators.rshares", 0]}},
                             "downvote_rshares": { $sum : { $cond: [{ $lt: ['$curators.rshares', 0]}, "$curators.rshares", 0]}},
                             "vote_value": { $sum: "$vote_value"},
                             "upvote_vote_value": { $sum : { $cond: [{ $gte: ['$vote_value', 0]}, "$vote_value", 0]}},
                             "downvote_vote_value": { $sum : { $cond: [{ $lt: ['$vote_value', 0]}, "$vote_value", 0]}},
                             "curator_vests": { $sum: "$curators.vests"},
                             "curator_payout_value": { $sum: "$curator_payout_value"},
                             "count": { $sum: 1 }
                          }
                       }
                    }
                ],
                "self_votes": [
                    { $match : { self_vote: 'self'}},
                    { $bucket: {
                          groupBy: "$curators.vote_mseconds",
                          boundaries: boundaryArray,
                          default: "other",
                          output: {
                             "rshares": { $sum: "$curators.rshares"},
                             "upvote_rshares": { $sum : { $cond: [{ $gte: ['$curators.rshares', 0]}, "$curators.rshares", 0]}},
                             "downvote_rshares": { $sum : { $cond: [{ $lt: ['$curators.rshares', 0]}, "$curators.rshares", 0]}},
                             "vote_value": { $sum: "$vote_value"},
                             "upvote_vote_value": { $sum : { $cond: [{ $gte: ['$vote_value', 0]}, "$vote_value", 0]}},
                             "downvote_vote_value": { $sum : { $cond: [{ $lt: ['$vote_value', 0]}, "$vote_value", 0]}},
                             "curator_vests": { $sum: "$curators.vests"},
                             "curator_payout_value": { $sum: "$curator_payout_value"},
                             "count": { $sum: 1 }
                          }
                       }
                    }
                ],
            }}
        ])
        .toArray()
        .then(function(voteAnalyses) {
            Object.keys(voteAnalyses[0]).forEach(function(voteAnalysis) {
                let i = 0;
                for (let voteBucket of voteAnalyses[0][voteAnalysis]) {
                    if (voteBucket._id != "other") {
                        voteBucket.bucket = (voteBucket._id / (secondBuckets * 1000))
                    } else {
                        voteBucket.bucket = "other";
                    }
                    delete voteBucket._id
                    voteBucket.vote_value = Number(voteBucket.vote_value.toFixed(3));
                    voteBucket.upvote_vote_value = Number(voteBucket.upvote_vote_value.toFixed(3));
                    voteBucket.downvote_vote_value = Number(voteBucket.downvote_vote_value.toFixed(3));
                    voteBucket.curator_vests = Number(voteBucket.curator_vests.toFixed(3));
                    voteBucket.curator_payout_value = Number(voteBucket.curator_payout_value.toFixed(3));
                    voteBucket.curation_ratio = Number((voteBucket.curator_payout_value / voteBucket.upvote_vote_value).toFixed(3));
                }
            })
            voteBreakdown = voteAnalyses[0];
            console.log('------------------------------------------------------------------------');
        }).catch(function(error) {
            console.log(error);
        });
    return voteBreakdown;
}

module.exports.voteTimingMongo = voteTimingMongo;
