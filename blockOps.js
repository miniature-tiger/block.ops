// Time reporting for measuring speed of block parsing
// ---------------------------------------------------
console.log('------------------------------------------------------------------------');
console.log('------------------------------------------------------------------------');
console.log('------------------------------------------------------------------------');
console.log('                               Start                   ');
const launchTime = Date.now();
console.log('                             time = ' + (Date.now() - launchTime));
console.log('------------------------------------------------------------------------');


// Imports
// -------
const mongodb = require('mongodb');
const fsPromises = require('fs').promises;
const path = require('path');
const steemrequest = require('./steemrequest/steemrequest.js')
const mongoblock = require('./mongoblock.js')
const helperblock = require('./helperblock.js')
const steemdata = require('./steemdata.js')
const postprocessing = require('./postprocessing.js')


// MongoDB
// -------
const MongoClient = mongodb.MongoClient;
const url = 'mongodb://localhost:27017';
const dbName = 'blockOps3';


// Command Line inputs and parameters
// ----------------------------------
let commandLine = process.argv.slice(2)[0];
let parameter1 = process.argv.slice(2)[1];
let parameter2 = process.argv.slice(2)[2];
let parameter3 = process.argv.slice(2)[3];
console.log('command: ', commandLine, '| | parameter1: ',parameter1, '| | parameter2: ', parameter2);
console.log('------------------------------------------------------------------------');


// Command handling
// ----------------
if (commandLine == 'setup') {
    updateBlockDates();
} else if (commandLine == 'remove') {
    removeCollection(parameter1);
} else if (commandLine == 'checkBlockDates') {
    checkAllBlockDates();
} else if (commandLine == 'filloperations') {
    fillOperations();
} else if (commandLine == 'patchoperations') {
    patchVirtualOperations();
} else if (commandLine == 'fillprices') {
    fillPrices();
} else if (commandLine == 'displayprices') {
    displayPrices();
} else if (commandLine == 'reportcomments') {
    // Market share reports on Steem applications
    reportComments();
} else if (commandLine == 'reportblocks') {
    reportBlocks();
} else if (commandLine == 'findcomments') {
    findComments();
} else if (commandLine == 'investigation') {
    investigation();
} else if (commandLine == 'findcurator') {
    findCurator();
} else if (commandLine == 'postcuration') {
    postCuration();
} else if (commandLine == 'validate') {
    validateComments();
} else if (commandLine == 'showblock') {
    showBlock();
} else if (commandLine == 'votetiming') {
    voteTiming();
} else if (commandLine == 'utopianvotes') {
    utopianVotes();
} else if (commandLine == 'transfersummary') {
    transferSummary();
} else if (commandLine == 'delegationsummary') {
    delegationSummary();
} else {
    // end
}


// Function to insert blockNumber and timestamp of first block of each day
// ------------------------------------------------------------------------
async function updateBlockDates() {
    // Opening MongoDB
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);
    const collection = db.collection('blockDates');

    let latestB = await steemrequest.getLatestBlockNumber();
    console.log('Latest block:', latestB.blockNumber, latestB.timestamp);

    let loopBlock = 1, loopDate = new Date(), loopStartSet = false, record = {}, secondsPerBlock = 3;

    // Checks whether blockDates have previously been loaded
    // If true uses most recent blockDate as starting point for loading further blockDates
    let checkC = await mongoblock.checkCollectionExists(db, 'blockDates');
    if (checkC == true) {
        console.log('Blockdates previously loaded.');
        await collection.find({}).sort({timestamp:-1}).limit(1).toArray()
            .then(function(maxDate) {
                if(maxDate.length == 1) {
                    console.log('Latest blockdate loaded:', maxDate[0].blockNumber, maxDate[0].timestamp);
                    loopBlock = Math.min(maxDate[0].blockNumber + (24*60*20), latestB.blockNumber);
                    loopDate = helperblock.forwardOneDay(maxDate[0].timestamp);
                    loopStartSet = true;
                    console.log('Attempted block:', loopBlock, loopDate);
                }
            }).catch(function(error) {
                console.log(error)
            });
    }
    // If no previous blockDates starts loading from block 1
    if (loopStartSet == false) {
        let startBBody = await steemrequest.getBlockHeaderAppBase(1);
        let startB = steemrequest.processBlockHeader(startBBody, 1);
        loopBlock = startB.blockNumber;
        loopDate = new Date(Date.UTC(startB.timestamp.getUTCFullYear(), startB.timestamp.getUTCMonth(), startB.timestamp.getUTCDate()));
        console.log('startB', startB);

        record = {blockNumber: loopBlock, timestamp: startB.timestamp};
        console.log(record);
        collection.insertOne(record, (error, results) => {
            if (error) console.log(error);
        });
        console.log('block 1 - logged on db:')
        // go forward one day
        loopDate = helperblock.forwardOneDay(loopDate);
        loopBlock = loopBlock - helperblock.blocksToMidnight(startB.timestamp, loopDate, 3);

        collection.createIndex({timestamp: 1}, {unique:true})
            .catch(function(error) {
                console.log(error);
            });
    }

    // Loop to load blockDates
    let counter = 0, attemptArray = [];
    while (loopDate <= latestB.timestamp) {
        console.log('----------------');
        // Checks where block is first block of UTC day
        let firstB = await steemrequest.checkFirstBlock(loopBlock, loopDate);
        console.log('checking ' + loopBlock + ' ... timestamp ', firstB.timestamp, 'vs..', loopDate);
        console.log(firstB);
        // If block is first block insert record to database
        if (firstB.check == true) {
            record = {blockNumber: loopBlock, timestamp: firstB.timestamp};
            collection.insertOne(record, (error, results) => {
                if (error) console.log(error);
            });
            console.log('yes, first block of day - logged on db:')
            console.log(record.blockNumber, record.timestamp);
            // Update loopDate to next day - make initial estimate for next blockNumber
            loopBlock = loopBlock + (24*60*20);
            loopDate = helperblock.forwardOneDay(loopDate);
            counter = 0, attemptArray = [];
            secondsPerBlock = 3;
        // If block is not first block revise estimate of first blockNumber based on 3 second blocks
        } else {
            console.log('...recalculating');
            counter += 1;
            attemptArray.push({blockNumber: loopBlock, timestamp: firstB.timestamp});
            // Workaround for when estimation process gets stuck
            if (counter % 5 == 0) {
                console.log(attemptArray);
                secondsPerBlock = Math.round((attemptArray[attemptArray.length-1].timestamp - attemptArray[attemptArray.length-2].timestamp) / (attemptArray[attemptArray.length-1].blockNumber - attemptArray[attemptArray.length-2].blockNumber)/1000);
                //console.log( (attemptArray[attemptArray.length-1].timestamp - attemptArray[attemptArray.length-2].timestamp) , (attemptArray[attemptArray.length-1].blockNumber - attemptArray[attemptArray.length-2].blockNumber) , secondsPerBlock );
                console.log('adjusting seconds per block to ' + secondsPerBlock + ' seconds due to large number of missing blocks. Resets once first block found.');
                loopBlock = Math.min(loopBlock - helperblock.blocksToMidnight(firstB.timestamp, loopDate, secondsPerBlock), latestB.blockNumber);
            // blocksToMidnight revises estimate based on 3 second blocks
            } else {
                loopBlock = Math.min(loopBlock - helperblock.blocksToMidnight(firstB.timestamp, loopDate, secondsPerBlock), latestB.blockNumber);
            }
        }
        console.log('----------------');
    }
    // Closing MongoDB
    console.log('closing');
    client.close();
}


// Function to remove a collection (handle with care!)
// ---------------------------------------------------
async function removeCollection() {
    // Opening MongoDB
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);
    // Deletes all records in collection - leaves index intact
    db.collection(parameter1).deleteMany({})
        .then(function() {
            // Closing MongoDB
            console.log('closing');
            client.close();
        })
        .catch(function(error) {
            console.log(error);
        });
}


// Function to test succesful construction of blockDates list
// ----------------------------------------------------------
async function checkAllBlockDates() {
    // Opening MongoDB
    client = await MongoClient.connect(url, { useNewUrlParser: true })
    const db = client.db(dbName);
    const collection = db.collection('blockDates');
    console.log('Connected to server.');

    // Gets first and last dates to check
    let latestB = await steemrequest.getLatestBlockNumber();
    console.log('latest block:', latestB);
    let startBBody = await steemrequest.getBlockHeaderAppBase(1);
    let startB = steemrequest.processBlockHeader(startBBody, 1);
    console.log('start block:', startB)
    checkDate = new Date(Date.UTC(startB.timestamp.getUTCFullYear(), startB.timestamp.getUTCMonth(), startB.timestamp.getUTCDate()));

    // Loops makes various checks - no blocks on date / more than one block / first block not at midnight (possible)
    let previousNumber = 1;
    let expectedNumber = 24*60*60/3;
    while (checkDate <= latestB.timestamp) {
        openDate = checkDate;
        closeDate = helperblock.forwardOneDay(checkDate);
        await collection.find({timestamp: {$gte: openDate, $lt: closeDate}}).project({ timestamp: 1, blockNumber: 1, _id: 0 }).toArray()
            .then(function(records) {
                if (records.length == 0) {
                    console.log(openDate, 'no blocks');
                } else if (records.length > 1) {
                    console.log(openDate, 'many blocks:', records.length);
                } else if (records[0].timestamp.getUTCHours() != 0 || records[0].timestamp.getUTCMinutes() != 0 || records[0].timestamp.getUTCSeconds() != 0) {
                    blocksPerDay = records[0].blockNumber - previousNumber;
                    previousNumber = records[0].blockNumber;
                    console.log('blocks present:', blocksPerDay, 'blocks missing:', blocksPerDay - expectedNumber);
                    console.log(records[0].timestamp, records[0].blockNumber, 'non standard time');
                } else {
                    blocksPerDay = records[0].blockNumber - previousNumber;
                    previousNumber = records[0].blockNumber;
                    console.log('blocks present:', blocksPerDay, 'blocks missing:', blocksPerDay - expectedNumber);
                    console.log(records[0].timestamp, records[0].blockNumber, 'OK:', records.length, ' block', );
                }
                checkDate = helperblock.forwardOneDay(checkDate);
            }).catch(function(error) {
                console.log(error)
            });
    }
    // Closing MongoDB
    console.log('closing');
    client.close();
}


// Function to process a loop of blocks (including virtual operations)
// -------------------------------------------------------------------
async function fillOperations() {
    // Opening MongoDB
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let checkCo = await mongoblock.checkCollectionExists(db, 'comments');
    if (checkCo == false) {
        db.collection('comments').createIndex({author: 1, permlink: 1}, {unique:true});
    }
    db.collection('transfers').createIndex({blockNumber: 1, from: 1, to: 1}, {unique:true});
    let checkTr = await mongoblock.checkCollectionExists(db, 'transfers');
    if (checkTr == false) {
        db.collection('transfers').createIndex({blockNumber: 1, from: 1, to: 1}, {unique:true});
    }

    let checkDg = await mongoblock.checkCollectionExists(db, 'delegation');
    if (checkDg == false) {
        db.collection('delegation').createIndex({blockNumber: 1, delegator: 1, delegatee: 1}, {unique:true});
    }
    let checkBp = await mongoblock.checkCollectionExists(db, 'blocksProcessed');
    if (checkBp == false) {
        db.collection('blocksProcessed').createIndex({blockNumber: 1}, {unique:true});
    }



    let blocksStarted = 0;
    let blocksCompleted = 0;
    let errorCount = 0;
    let blocksToProcess = 0;
    let blocksPerRound = 8; // x blocks called for processing at a time
    let unknownOperations = [];
    let unknownVirtuals = [];
    let blockProcessNumber = 1000;
    let priorArrayCount = 0;
    let blocksOK = 0;
    let blockProcessArray = [];
    let blockProcessArrayFlag = false;
    let debug = false;

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    console.log(openBlock, closeBlock, parameterIssue);

    blocksToProcess = closeBlock - openBlock;
    console.log(openBlock, blocksToProcess);

    fiveBlock('original');

    // Function to extract data of x blocks from the blockchain (originally five blocks)
    // ---------------------------------------------------------------------------------
    async function fiveBlock(localMarker) {
        if(debug == true) {console.log('localMarker', localMarker)};
        let launchBlocks = Math.min(blocksPerRound, blocksToProcess - blocksStarted - blocksOK);
        console.log(launchBlocks, blocksToProcess - blocksStarted - blocksOK);
        for (var i = 0; i < launchBlocks; i+=1) {
            //console.log('blocksStarted - priorArrayCount == blockProcessArray.length', blocksStarted, - priorArrayCount, blockProcessArray.length)
            if(blocksStarted - priorArrayCount == blockProcessArray.length) {
                blockProcessArrayFlag = true;
                do {
                    priorArrayCount += blockProcessArray.length;
                    if(debug == true) {console.log('localMarker', localMarker, 'ARRAY CALLED', openBlock + priorArrayCount + blocksOK, Math.min(openBlock + priorArrayCount + blocksOK + blockProcessNumber, closeBlock)-1)};
                    blockProcessArray = await mongoblock.reportBlocksProcessed(db, openBlock + priorArrayCount + blocksOK, Math.min(openBlock + priorArrayCount + blocksOK + blockProcessNumber, closeBlock), 'return');
                    blocksOK += ( Math.min(openBlock + priorArrayCount + blocksOK + blockProcessNumber, closeBlock) - (openBlock + priorArrayCount + blocksOK) - blockProcessArray.length);
                    if(debug == true) {console.log('blocksOK updated from last array call:', localMarker, blocksOK)};
                }
                while (blockProcessArray.length == 0 && (blocksStarted + blocksOK < blocksToProcess));
                blockProcessArrayFlag = false;

                // Reset operations counter in blocksProcessed for blocks being rerun
                mongoblock.resetBlocksProcessed(db, blockProcessArray[0], blockProcessArray[blockProcessArray.length-1]);
            }

            if (blocksStarted + blocksOK < blocksToProcess) {
                if(debug == true) {console.log('localMarker, blockNo = blockProcessArray[blocksStarted - priorArrayCount]:', localMarker, blocksStarted, priorArrayCount, blocksStarted - priorArrayCount)};
                blockNo = blockProcessArray[blocksStarted - priorArrayCount];
                // Gets data for one block, processes it in callback "processOps"
                if(debug == true) {console.log('getOpsAppBase', blockNo)};
                steemrequest.getOpsAppBase(blockNo, processOps);
                blocksStarted += 1;
            } else {
                console.log('break');
                completeOperationsLoop()
                break;
            }
        }
    }

    // Function to process block of operations
    // -----------------------------------------
    function processOps(error, response, body, localBlockNo) {
        if (!error) {
            try {
                let result = JSON.parse(body).result;
                if( result == undefined) {
                    console.log(localBlockNo)
                    console.dir(JSON.parse(body), {depth: null})
                }
                //console.dir(JSON.parse(body), {depth: null})
                let numberOfOps = result.length;
                let opsNotHandled = 0;
                let timestamp = new Date(result[result.length-1].timestamp + '.000Z');
                let transactionNumber = -1;
                let operationNumber = 0;
                let virtualOpNumber = 0;
                let skippedOperations = 0;
                let authorRewardCount = 0;

                // Setting check number for number of active_votes sets to be processed in a block
                for (let operation of result) {
                    if (operation.virtual_op != virtualOpNumber) {
                        virtualOpNumber = operation.virtual_op;
                        if (operation.op[0] == 'author_reward') {
                            authorRewardCount += 1;
                        }
                    }
                }
                virtualOpNumber = 0;

                // Add block document to blocksProcessed collection in Mongo
                let blockRecord = {blockNumber: localBlockNo, timestamp: timestamp, status: 'Processing', operationsCount: numberOfOps, activeVoteSetCount: authorRewardCount, activeVoteSetProcessed: 0};
                mongoblock.mongoBlockProcessed(db, blockRecord, 0);

                for (let operation of result) {
                    let skipFlag = false;

                    // Handles grouped votes - block transactions with many votes in different operation numbers (e.g. 4-0, 4-1, 4-2, 4-3)
                    if (operation.trx_in_block == transactionNumber) {
                        operationNumber += 1;
                    } else {
                        transactionNumber = operation.trx_in_block
                        operationNumber = 0
                    }

                    // Allows for skipping of repeat virtual operations in the same block which have the same virtual_op number as each other
                    if (operation.virtual_op > 0) {
                        if (operation.virtual_op == virtualOpNumber) {
                            // repeat operation - skip and add one to skipped operations
                            skipFlag = true;
                            skippedOperations += 1;
                        } else {
                            virtualOpNumber = operation.virtual_op;
                        }
                    }

                    // Main loop for controlling processing of operations
                    if (skipFlag == false) {
                        // Transaction operations
                        if (operation.op[0] == 'comment') {
                            mongoblock.processComment(operation, operationNumber, mongoblock.mongoComment, db);
                        } else if (operation.op[0] == 'vote') {
                            mongoblock.processVote(operation, operationNumber, mongoblock.mongoVote, db);
                        } else if (operation.op[0] == 'transfer') {
                            mongoblock.processTransfer(operation, operationNumber, mongoblock.mongoTransfer, db);
                        } else if (operation.op[0] == 'delegate_vesting_shares') {
                            mongoblock.processDelegation(operation, operationNumber, mongoblock.mongoDelegation, db);
                        // Virtual operations
                        } else if (operation.op[0] == 'author_reward') {
                            mongoblock.validateComments(db, operation);
                            activeVotes(operation, db);
                            mongoblock.processAuthorReward(operation, mongoblock.mongoAuthorReward, db);
                        } else if (operation.op[0] == 'comment_benefactor_reward') {
                            mongoblock.processBenefactorReward(operation, mongoblock.mongoBenefactorReward, db);
                        } else if (operation.op[0] == 'curation_reward') {
                            mongoblock.processCuratorReward(operation, mongoblock.mongoCuratorReward, db);
                        } else {
                        // Operations not handled:
                            opsNotHandled += 1;
                            if (!unknownVirtuals.includes(operation.op[0])) {
                                unknownVirtuals.push(operation.op[0]);
                            }
                        }
                    }
                }

                // Supplement block document with all count of operations that are skipped or not handled (for check-total purposes)
                let recordOperation = {transactionType: 'notHandled', ops_not_handled: opsNotHandled, skipped_ops: skippedOperations, count: opsNotHandled + skippedOperations, status: 'OK'};
                mongoblock.mongoOperationProcessed(db, localBlockNo, recordOperation, opsNotHandled + skippedOperations, 0);
                blocksCompleted += 1;

                if(debug == true) {console.log('blocksStarted - blocksCompleted', blocksStarted, - blocksCompleted)};
                if (blocksCompleted + blocksOK == blocksToProcess) {
                    completeOperationsLoop();

                } else if ((blocksStarted - blocksCompleted < 4) && (blockProcessArrayFlag == false)) {
                    fiveBlock('marker' + blocksStarted);
                }
            } catch (error) {
                console.log(error)
                console.log('Error in processing virtual ops:', localBlockNo, 'Error logged.');
                let errorRecord = {blockNumber: localBlockNo, status: 'error'};
                mongoblock.mongoErrorLog(db, errorRecord, 0);
                blocksCompleted += 1;
                errorCount += 1;
            }
        } else {
            console.log('Error in processing virtual ops:', localBlockNo);
            if (error.errno = 'ENOTFOUND') {
                console.log('ENOTFOUND: Most likely error is connection lost. Error logged.'); // to do: deal with checking which blocks loaded, reconnecting, and restarting loop.
                console.log(error);
            } else {
                console.log(error);
            }

            let errorRecord = {blockNumber: localBlockNo, status: 'error'};
            mongoblock.mongoErrorLog(db, errorRecord, 0);
            blocksCompleted += 1;
            errorCount += 1;
        }
    }

    function completeOperationsLoop() {
        let runTime = Date.now() - launchTime;
        console.log('unknownOperations: ', unknownOperations);
        console.log('End time: ' + (Date.now() - launchTime));
        console.log('----------------');
        console.log('REPORT');
        console.log('Blocks covered: ' + openBlock + ' to ' + closeBlock);
        console.log('Blocks processed: ' + blocksCompleted);
        console.log('Blocks previously processed: ' + blocksOK);
        console.log('Error Count: ' + errorCount);
        console.log('Average speed: ' + (runTime/blocksCompleted/1000).toFixed(4) + 's.');
        console.log(unknownVirtuals);
        //console.log('db closing');
        //client.close();
    }

// Closing fillOperations
}


// Separated outside of main space
async function activeVotes(localOperation, db) {
    await steemrequest.getActiveVotes(localOperation.op[1].author, localOperation.op[1].permlink)
        .then(async function(votesReturned) {
            let votesList = JSON.parse(votesReturned);

            // Setting up check controls for each active votes run
            let activeProcessedCount = 0
            let activeVotesRunIsComplete = 0;
            let logStatus = 'Processing';
            let logActive = { associatedOp: localOperation.virtual_op, transactionType: 'active_vote', count: 0, status: logStatus, activeVotesCount: votesList.result.length }
            mongoblock.mongoActiveProcessed(db, localOperation.block, logActive, 0, 'start', 0);

            // Loop to process list of active votes - now only updated in Mongo once the full run is complete
            for (let vote of votesList.result) {
                let activeData = await mongoblock.processActiveVote(vote, localOperation.op[1].author, localOperation.op[1].permlink, localOperation.block, localOperation.virtual_op, mongoblock.mongoActiveVote, db);
                let dataInsertCheck = await mongoblock.mongoActiveVote(db, localOperation.block, localOperation.virtual_op, activeData, 0);
                if (dataInsertCheck == 'skipped' || dataInsertCheck.ok == 1) {
                    activeProcessedCount += 1;
                } else {
                    // Issue with active Vote processing - block will be marked as error due to short count
                    console.log('Issue with active Vote processing: block', localOperation.block)
                }
            }
            // active_vote set completes successfully
            if (activeProcessedCount == votesList.result.length) {
                logStatus = 'OK';
                logActive = { associatedOp: localOperation.virtual_op, transactionType: 'active_vote', status: 'OK', activeVotesProcessed: activeProcessedCount }
                activeVotesRunIsComplete = 1;
            } else {
                // Failure in active_vote processing - logged as error
                console.log('Failure in active upvote processing - reported as error')
                logStatus = 'Error';
                logActive = { associatedOp: localOperation.virtual_op, transactionType: 'active_vote', status: 'Error', activeVotesProcessed: activeProcessedCount }
                let errorRecord = {blockNumber: localOperation.block, status: 'error'};
                mongoblock.mongoErrorLog(db, errorRecord, 0);
            }
            // Block document in Mongo updated with status of active vote run set
            mongoblock.mongoActiveProcessed(db, localOperation.block, logActive, activeVotesRunIsComplete, 'end', 0);
        })
        .catch(function(error) {
            console.log('Error in ', localOperation.block, localOperation.op[1].author, localOperation.op[1].permlink, 'active votes. Error logged.');
            let errorRecord = {blockNumber: localOperation.block, status: 'error'};
            mongoblock.mongoErrorLog(db, errorRecord, 0);
            console.log(error)
        })
}


// Patch virtual operations
// --------------------------------------------------------
// After a halt of the blockchain there can be a very large number of virtual operations associated with one block
// This patch can be applied where these virtual operations cannot be obtained due to steemit.api server timeout
async function patchVirtualOperations() {

    let opsNotHandled = 0;
    let preProcessed = 0;
    let preActiveProcessed = 0;
    let count = 0;
    let lastCall = Date.now();
    let lastMongo = Date.now();

    // Connect to Mongo
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    // Function to load json file of operations into memory
    function jsonFile(folderName, fileName) {
        console.log(path.join(__dirname, folderName, fileName))
        return fsPromises.readFile(path.join(__dirname, folderName, fileName), 'utf8')
            .catch(function(error) {
                console.log(error);
            });
    }

    //Reset operations counter in blocksProcessed for blocks being rerun
    db.collection('blocksProcessed')
        .updateMany({ blockNumber: 26038153, status: {$ne: 'OK'}},
                {$set: {operationsProcessed: 0, activeVoteSetProcessed: 0}, $pull: { operations: {transactionType: "notHandled"}}}, {upsert: false})
        .catch(function(error) {
            console.log(error);
        });

    // Call function to read json file into memory, parse, and annotate with default statuses
    let lostArray = await jsonFile('lostBlocks', '26038153.json');
    console.log('Array loaded.', ((Date.now() - launchTime)/1000/60).toFixed(2));
    lostArray = JSON.parse(lostArray);
    for (let entry of lostArray) {
        entry.opStatus = 'unprocessed';
        if (entry.op[0] == 'author_reward') {
            entry.associatedStatus = 'unprocessed';
        }
    }
    console.log('Array parsed and annotated.', ((Date.now() - launchTime)/1000/60).toFixed(2));

    // Start of processing loop - add blockrecord (operations processed log) to blocksProcessed
    let timestamp = new Date('2018-09-17T19:56:51.000Z');
    let blockRecord = {blockNumber: 26038153, timestamp: timestamp, status: 'Processing', operationsCount: lostArray.length, activeVoteSetCount: 6759, activeVoteSetProcessed: 0};
    mongoblock.mongoBlockProcessed(db, blockRecord, 0);

    // Update statuses in lostArray based on any previous runs
    await db.collection('blocksProcessed')
        .aggregate([
            { $match : {blockNumber: 26038153}},
            { $project : {_id: 0, operations: 1 }},
            { $unwind : "$operations"},
        ])
        .toArray()
        .then(async function(records) {
            console.log('Aggregation finished.', ((Date.now() - launchTime)/1000/60).toFixed(2));
            for (let record of records) {
                if (record.operations.hasOwnProperty('virtualOp')) {
                    let arrayPosition = lostArray.findIndex(fI => fI.virtual_op == record.operations.virtualOp);
                    lostArray[arrayPosition].opStatus = record.operations.status;
                } else if (record.operations.hasOwnProperty('associatedOp')) {
                    let arrayPosition = lostArray.findIndex(fI => fI.virtual_op == record.operations.associatedOp);
                    lostArray[arrayPosition].associatedStatus = record.operations.status;
                } else {
                    console.log('neither virtualop nor associatedop');
                }
            }
            patchRunner();
    });

    // Main processor - takes each lostArray operation and processes using existing functions
    // Timeouts used to regulate calls to API (otherwise loop can make many calls very quickly)
    async function patchRunner() {
        if (count == lostArray.length) {
            finishUp();
        } else {

            if (count % 100 == 0) {
                console.log(count + ' records of ' + lostArray.length + ' processed.', ((Date.now() - launchTime)/1000/60).toFixed(2));
            }
            let operation = lostArray[count];

            if (operation.op[0] == 'author_reward') {
                if (operation.opStatus != 'OK') {
                    console.log('getting comment', operation.op[1].author, operation.op[1].permlink);
                    lastCall = Date.now();
                    await steemrequest.getComment(operation.op[1].author, operation.op[1].permlink)
                            .then(async function(body) {
                                result = JSON.parse(body).result;
                                let forwardDate = new Date(result.created + '.000Z');
                                forwardDate.setUTCDate(forwardDate.getUTCDate()+7);
                                operation.timestamp = forwardDate.toISOString().slice(0, 19);
                                mongoblock.validateComments(db, operation);
                                mongoblock.processAuthorReward(operation, mongoblock.mongoAuthorReward, db);
                                lastMongo = Date.now();
                          });
                } else {
                    preProcessed += 1;
                }
                if (operation.associatedStatus != 'OK') {
                    activeVotes(operation, db);
                    lastCall = Date.now();
                    lastMongo = Date.now();
                } else {
                    preActiveProcessed += 1;
                }
                count += 1;
                setTimeout(patchRunner, Math.max(100 - (Date.now() - lastCall), 50 - (Date.now() - lastMongo), 0));

            } else if (operation.op[0] == 'curation_reward') {
                if (operation.opStatus != 'OK') {
                    mongoblock.processCuratorReward(operation, mongoblock.mongoCuratorReward, db);
                    lastMongo = Date.now();
                } else {
                    preProcessed += 1;
                }
                count += 1;
                setTimeout(patchRunner, Math.max(200 - (Date.now() - lastMongo), 0));

            } else if (operation.op[0] == 'comment_benefactor_reward') {
                if (operation.opStatus != 'OK') {
                    mongoblock.processBenefactorReward(operation, mongoblock.mongoBenefactorReward, db);
                    lastMongo = Date.now();
                } else {
                    preProcessed += 1;
                }
                count += 1;
                setTimeout(patchRunner, Math.max(100 - (Date.now() - lastMongo), 0));
            } else {
                opsNotHandled += 1;
                patchRunner();
            }
        }
    }

    // Adding numbers of blocks preprocessed or not handled and checking complete
    function finishUp() {
        db.collection('blocksProcessed').findOneAndUpdate(  { blockNumber: 26038153},
                                                            { $inc: {activeVoteSetProcessed: preActiveProcessed}},
                                                            { upsert: false, returnOriginal: false, maxTimeMS: 2000})
            .then(function(response) {
                if ((response.value.operationsCount == response.value.operationsProcessed) && (response.value.activeVoteSetCount == response.value.activeVoteSetProcessed) && (response.value.status == 'Processing')) {
                    db.collection('blocksProcessed').updateOne({ blockNumber: 26038153}, {$set: {status: 'OK'}})
                }
            });

        let recordOperation = {transactionType: 'notHandled', ops_not_handled: opsNotHandled, skipped_ops: preProcessed, count: opsNotHandled + preProcessed, status: 'OK'};
        mongoblock.mongoOperationProcessed(db, 26038153, recordOperation, opsNotHandled + preProcessed, 0);
        console.log('----- Process Completed -----', ((Date.now() - launchTime)/1000/60).toFixed(2));
    }
}



// Function to convert parameters for a date range into blockNumbers
// -----------------------------------------------------------------
// Accepts two dates as parameters  or a date and a number
// Parameters are global and defined on command line
async function blockRangeDefinition(db) {

    let openBlock = 0, closeBlock = 0, parameterIssue = false;

    if (!(isNaN(parameter1))) {
        openBlock = Number(parameter1);
    } else if (typeof parameter1 == 'string') {
        openDate = new Date(parameter1 + 'T00:00:00.000Z');
        openDateEnd = helperblock.forwardOneDay(openDate);
        openBlock = await mongoblock.dateToBlockNumber(openDate, openDateEnd, db)
            .catch(function(error) {
                console.log(error);
                parameterIssue = true;
            });
    } else {
        parameterIssue = true;
    }

    if (!(isNaN(parameter2))) {
        closeBlock = openBlock + Number(parameter2);
    } else if (typeof parameter2 == 'string') {
        closeDate = new Date(parameter2 + 'T00:00:00.000Z');
        closeDateEnd = helperblock.forwardOneDay(closeDate);
        closeBlock = await mongoblock.dateToBlockNumber(closeDate, closeDateEnd, db)
            .catch(function(error) {
                console.log(error);
                parameterIssue = true;
            });
    } else {
        parameterIssue = true;
    }

    return [openBlock, closeBlock, parameterIssue];
}


// Function to fill prices collection
// ----------------------------------
async function fillPrices() {
    let pricesArray = [];
    let HF20 = 26256743;

    // Connect to Mongo
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    // Check prices index exists
    let checkCp = await mongoblock.checkCollectionExists(db, 'prices');
    if (checkCp == false) {
        //db.collection('prices').createIndex({payout_blockNumber: 1}, {unique:true});
    }

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);

    let firstHour = await mongoblock.showBlockMongo(db, openBlock, false);
    if (firstHour[0].hasOwnProperty('timestamp')) {
        firstHourID = firstHour[0].timestamp.toISOString().slice(0, 13);
    } else {
        console.log('have blocks for date range been processed?')
        parameterIssue = true;
    }

    let lastHour = await mongoblock.showBlockMongo(db, closeBlock-1, false);
    if (lastHour[0].hasOwnProperty('timestamp')) {
        lastHourID = lastHour[0].timestamp.toISOString().slice(0, 13);
    } else {
        console.log('have blocks for date range been processed?')
        parameterIssue = true;
    }

    if (parameterIssue == false) {
        // Select a comment from each hour (around the 20-40 minute mark!)
        pricesArray = await mongoblock.mongoFillPrices(db, openBlock, closeBlock);
        console.log(pricesArray)

        // Check first and last hour present
        let priceIssue = false;

        if (pricesArray[0]._id != firstHourID) {
            console.log('price for first hour of date range is missing');
            priceIssue = true;
        }
        if (pricesArray[pricesArray.length-1]._id != lastHourID) {
            console.log('price for last hour of date range is missing');
            priceIssue = true;
        }

        if (priceIssue == false) {
            // Skip prices that have previously been inserted to prices collection
            let previousPrices = await db.collection('prices')
                .find({payout_blockNumber: { $gte: openBlock, $lt: closeBlock }})
                .project({_id: 1})
                .toArray();
            let countSkips = 0;

            // Loop through pricesArray starts here
            for (let comment of pricesArray) {

                let priorFlag = false;
                for (let priorDate of previousPrices) {
                    if (comment._id == priorDate._id) {
                        priorFlag = true;
                    }
                }

                if (priorFlag == true) {
                    // Skips record
                    countSkips += 1;
                } else {
                    // Pulls record and calculates currency ratios using payout data from operations (vests, sbd, steem) and payout data from post (STU)
                    await steemrequest.getComment(comment.author, comment.permlink)
                        .then(async function(body) {
                            result = JSON.parse(body).result;
                            comment.basis = 'derived';
                            //comment._id = comment.dateHour.toISOString().slice(0, 13);
                            comment.curator_payout_vests = Number(comment.curator_payout_vests.toFixed(6));
                            comment.curator_payout_value = Number(result.curator_payout_value.split(' ', 1)[0]);
                            comment.author_payout_value = Number(result.total_payout_value.split(' ', 1)[0]);
                            comment.beneficiaries_payout_value = 0;
                            comment.total_payout_value = Number((comment.curator_payout_value + comment.author_payout_value).toFixed(3));
                            beneficiariesSum = 0;
                            if (result.beneficiaries.length > 0) {
                                for (var i = 0; i < result.beneficiaries.length; i+=1) {
                                    beneficiariesSum += result.beneficiaries[i].weight;
                                }
                                comment.total_payout_value = Number(((comment.author_payout_value / (1-(beneficiariesSum/10000))) + comment.curator_payout_value).toFixed(3));
                                comment.beneficiaries_payout_value = Number((comment.total_payout_value - comment.author_payout_value - comment.curator_payout_value).toFixed(3));
                            }
                            comment.vestsPerSTU = Number((comment.curator_payout_vests / comment.curator_payout_value).toFixed(3));
                            comment.STUPerVests = Number((comment.curator_payout_value / comment.curator_payout_vests).toPrecision(8));
                            comment.steemPerSTU = 0;
                            comment.STUPerSteem = 0;
                            if (comment.author_payout_steem > 0) {
                                comment.steemPerSTU = Number((comment.author_payout_steem / (comment.author_payout_value - comment.author_payout_sbd - (comment.author_payout_vests/comment.vestsPerSTU))).toFixed(3));
                                comment.STUPerSteem = Number(((comment.author_payout_value - comment.author_payout_sbd - (comment.author_payout_vests/comment.vestsPerSTU)) / (comment.author_payout_steem)).toPrecision(8));
                            }

                            if (comment.payout_blockNumber < HF20) {
                                console.log('HF19')
                                comment.rsharesPerSTU = Number((comment.rshares / comment.total_payout_value).toFixed(3));
                                comment.STUPerRshares = Number((comment.total_payout_value / comment.rshares).toPrecision(8));
                            } else {
                                console.log('HF20')
                                comment.rsharesPerSTU = Number(((comment.rshares * 0.75) / (comment.author_payout_value + comment.beneficiaries_payout_value)).toFixed(3));
                                comment.STUPerRshares = Number(((comment.author_payout_value + comment.beneficiaries_payout_value)/ (comment.rshares * 0.75)).toPrecision(8));
                            }
                            await mongoblock.mongoPrice(db, comment, 0);

                            console.log(comment)
                    })
                }
            }
        console.log(countSkips + ' record skips as already exits in prices collection.')

        let updatedPrices = await db.collection('prices')
            .find({payout_blockNumber: { $gte: openBlock, $lt: closeBlock }})
            .project({_id: 1, vestsPerSTU: 1, steemPerSTU: 1, rsharesPerSTU:1, STUPerVests: 1, STUPerRshares: 1, STUPerSteem: 1})
            .toArray();

        for (var j = 0; j < updatedPrices.length-1; j+=1) {
            let nextDate = new Date(updatedPrices[j+1]._id + ':00:00.000Z');
            let currentDate = new Date(updatedPrices[j]._id + ':00:00.000Z');
            if ( nextDate - currentDate > (1000 * 60 * 60)) {
                // interpolate
                console.log('interpolating...')
                let gapsNumber = Number(((nextDate - currentDate) / (1000 * 60 * 60)).toFixed(0));
                for (var k = 1; k < gapsNumber; k+=1) {
                    let interpolatedDate = new Date(currentDate.getTime() + ((1000 * 60 * 60) * k));
                    let interpolate =
                        {   _id: interpolatedDate.toISOString().slice(0, 13),
                            basis: 'interpolated',
                            vestsPerSTU: (updatedPrices[j].vestsPerSTU + (k * (updatedPrices[j+1].vestsPerSTU - updatedPrices[j].vestsPerSTU) / gapsNumber)),
                            steemPerSTU: (updatedPrices[j].steemPerSTU + (k * (updatedPrices[j+1].steemPerSTU - updatedPrices[j].steemPerSTU) / gapsNumber)),
                            rsharesPerSTU: (updatedPrices[j].rsharesPerSTU + (k * (updatedPrices[j+1].rsharesPerSTU - updatedPrices[j].rsharesPerSTU) / gapsNumber)),
                            STUPerVests: (updatedPrices[j].STUPerVests + (k * (updatedPrices[j+1].STUPerVests - updatedPrices[j].STUPerVests) / gapsNumber)),
                            STUPerSteem: (updatedPrices[j].STUPerSteem + (k * (updatedPrices[j+1].STUPerSteem - updatedPrices[j].STUPerSteem) / gapsNumber)),
                            STUPerRshares: (updatedPrices[j].STUPerRshares + (k * (updatedPrices[j+1].STUPerRshares - updatedPrices[j].STUPerRshares) / gapsNumber)),
                        }
                        console.log(interpolate)
                    // update to prices
                    await mongoblock.mongoPrice(db, interpolate, 0);
                }
            }
        }
        console.log('closing mongo db');
        client.close();
        }
    } else {
        console.log('Parameter issue');
    }
}


// Function to display and export prices over specific dates
// ----------------------------------------------------------
// allows date graphs to be produced for reasonableness checks
async function displayPrices() {
    // Connect to Mongo
    let displayPrices = [];
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    if (parameterIssue == false) {
        displayPrices = await mongoblock.obtainPricesMongo(db, openBlock, closeBlock);
        for (let price of displayPrices) {
            console.log(price)
        }
        const fieldNames = ['_id', 'vestsPerSTU', 'steemPerSTU', 'rsharesPerSTU', 'STUPerVests', 'STUPerSteem', 'STUPerRshares'];
        postprocessing.dataExport(displayPrices.slice(0), 'prices_export', fieldNames);

        console.log('closing mongo db');
        client.close();
    } else {
        console.log('Parameter issue');
    }
}



// Function to provide parameters for reportBlocksProcessed
// --------------------------------------------------------
async function reportBlocks() {
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    if (parameterIssue == false) {
        await mongoblock.reportBlocksProcessed(db, openBlock, closeBlock, 'report', parameter3);
    } else {
        console.log('Parameter issue');
    }
}



// Function to provide parameters for findCommentsMongo
// ----------------------------------------------------
async function findComments() {
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    if (parameterIssue == false) {
        await mongoblock.findCommentsMongo(parameter3, db, openBlock, closeBlock);
    } else {
        console.log('Parameter issue');
    }
}


// Function to provide parameters for reportCommentsMongo
// ----------------------------------------------------
async function reportComments() {
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);

    if (parameterIssue == false) {
        let marketShareCreatedSummary = await mongoblock.reportCommentsMongo(db, openBlock, closeBlock, parameter3, 'created', 'all', 'default');
        let marketSharePayoutSummary = await mongoblock.reportCommentsMongo(db, openBlock, closeBlock, parameter3, 'payout', 'all', 'default');
        let exportData = postprocessing.marketShareProcessing(marketShareCreatedSummary, marketSharePayoutSummary);
        let fieldNames = ['application', 'authors', 'authorsRank', 'posts', 'postsRank', 'author_payout_sbd', 'author_payout_steem', 'author_payout_vests', 'benefactor_payout_sbd', 'benefactor_payout_steem', 'benefactor_payout_vests', 'curator_payout_vests', 'author_payout_sbd_STU', 'author_payout_steem_STU', 'author_payout_vests_STU', 'benefactor_payout_sbd_STU', 'benefactor_payout_steem_STU', 'benefactor_payout_vests_STU', 'curator_payout_vests_STU'];
        postprocessing.dataExport(exportData.slice(0), 'marketShareTest', fieldNames);

        let productionStatsPerDayData = await mongoblock.reportCommentsMongo(db, openBlock, closeBlock, parameter3, 'created', 'all', 'allByDate');
        let exportData2 = postprocessing.productionStatsByDayProcessing(productionStatsPerDayData);
        //console.log( exportData2 )
        fieldNames = ['date', 'authors', 'posts', 'author_payout_sbd', 'author_payout_steem', 'author_payout_vests', 'benefactor_payout_sbd', 'benefactor_payout_steem', 'benefactor_payout_vests', 'curator_payout_vests', 'author_payout_sbd_STU', 'author_payout_steem_STU', 'author_payout_vests_STU', 'benefactor_payout_sbd_STU', 'benefactor_payout_steem_STU', 'benefactor_payout_vests_STU', 'curator_payout_vests_STU'];
        postprocessing.dataExport(exportData2.slice(0), 'productionStatsByDay', fieldNames);

        let postsStatsPerDayData = await mongoblock.reportCommentsMongo(db, openBlock, closeBlock, parameter3, 'created', 'posts', 'allByDate');
        let exportData3 = postprocessing.productionStatsByDayProcessing(postsStatsPerDayData);
        //console.log( exportData3 )
        fieldNames = ['date', 'authors', 'posts', 'author_payout_sbd', 'author_payout_steem', 'author_payout_vests', 'benefactor_payout_sbd', 'benefactor_payout_steem', 'benefactor_payout_vests', 'curator_payout_vests', 'author_payout_sbd_STU', 'author_payout_steem_STU', 'author_payout_vests_STU', 'benefactor_payout_sbd_STU', 'benefactor_payout_steem_STU', 'benefactor_payout_vests_STU', 'curator_payout_vests_STU'];
        postprocessing.dataExport(exportData3.slice(0), 'postsStatsByDay', fieldNames);

        console.log('');
        console.log('closing mongo db');
        console.log('------------------------------------------------------------------------');
        let runTime = Date.now() - launchTime;
        console.log('End time: ' + runTime);
        console.log('------------------------------------------------------------------------');
        client.close();
    } else {
        console.log('Parameter issue');
    }
}



// Investigation
// -------------
async function investigation() {
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    if (parameterIssue == false) {
        await mongoblock.investigationMongo(db, openBlock, closeBlock);
    } else {
        console.log('Parameter issue');
    }
}



// Analysis of curation reward vs vote sizes
// -----------------------------------------
async function findCurator() {
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    if (parameterIssue == false) {
        await mongoblock.findCuratorMongo(parameter3, db, openBlock, closeBlock);
    } else {
        console.log('Parameter issue');
    }
}



// Validation routines for comments
// --------------------------------
async function validateComments() {
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    if (parameterIssue == false) {
        await mongoblock.validateCommentsMongo(db, openBlock, closeBlock);
    } else {
        console.log('Parameter issue');
    }
}



// Set-up for routine to show a single block
// -----------------------------------------
async function showBlock() {
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    await mongoblock.showBlockMongo(db, Number(parameter1), true);
}



// Vote-timing histogram and curation rewards v votes ratio analysis - includes export
// -----------------------------------------------------------------------------------
async function voteTiming() {
    let voteTimingArray = [];
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    if (parameterIssue == false) {

        let voteTimingArray = await mongoblock.voteTimingMongo(db, openBlock, closeBlock, steemdata.bidbotArray);
        const fieldNames = ['bucket', 'rshares', 'upvote_rshares', 'downvote_rshares', 'vote_value', 'upvote_vote_value', 'downvote_vote_value', 'curator_vests', 'curator_payout_value', 'curation_ratio', 'count'];

        Object.keys(voteTimingArray).forEach(function(voteAnalysis) {
            console.log(voteAnalysis);
            console.dir(voteTimingArray[voteAnalysis], {depth: null})
            postprocessing.dataExport(voteTimingArray[voteAnalysis].slice(0), voteAnalysis, fieldNames);
        })

    } else {
        console.log('Parameter issue');
    }
    console.log('closing mongo db');
    client.close();
}



// Post curation for a single block
// --------------------------------
async function postCuration() {
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    if (parameterIssue == false) {
        console.log('Getting curation posts.');
        let curationPosts = await mongoblock.postCurationMongo(db, openBlock, closeBlock);

        console.dir(curationPosts, {depth: null})
        for (let post of curationPosts) {
            console.log('Getting comment.');
            await steemrequest.getComment(post.author, post.permlink)
                .then(async function(body) {
                    let result = JSON.parse(body).result;
                    post.curator_payout_value = Number(result.curator_payout_value.split(' ', 1)[0]);
                    post.author_payout_value = Number(result.total_payout_value.split(' ', 1)[0]);
                    post.beneficiaries_payout_value = 0;
                    post.total_payout_value = Number((post.curator_payout_value + post.author_payout_value).toFixed(3));
                    beneficiariesSum = 0;
                    if (result.beneficiaries.length > 0) {
                        for (var i = 0; i < result.beneficiaries.length; i+=1) {
                            beneficiariesSum += result.beneficiaries[i].weight;
                        }
                        post.total_payout_value = Number(((post.author_payout_value / (1-(beneficiariesSum/10000))) + post.curator_payout_value).toFixed(3));
                        post.beneficiaries_payout_value = Number((post.total_payout_value - post.author_payout_value - post.curator_payout_value).toFixed(3));
                    }
                    post.author_payout_perc = Number((post.author_payout_value / post.total_payout_value).toFixed(3));
                    post.lost_weight_perc = Number((post.lost_weight / post.weight).toFixed(3));
                    post.rshares_rewardpool_perc = Number((post.rshares_rewardpool / post.rshares).toFixed(4));
                    post.author_payout_rewardpool_perc = Number((post.author_payout_value / (post.total_payout_value / (1 - post.rshares_rewardpool_perc))).toFixed(3));
                    post.curator_payout_scaled = post.curator_payout_value * post.curation_vests_full / post.vests_sum;
                    post.author_payout_vestsfull_perc = Number((post.author_payout_value / (post.total_payout_value - post.curator_payout_value + post.curator_payout_scaled)).toFixed(3));
                    console.dir(post, {depth: null})
                });
        }
        console.log('closing mongo db');
        client.close();
    } else {
        console.log('Parameter issue');
    }
}



// Utopian votes analysis
// --------------------------------
async function utopianVotes() {
    let utopianVoteSplitByDay = [];

    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    if (parameterIssue == false) {
        utopianVoteSplitByDay = await mongoblock.utopianVotesMongo(db, openBlock, closeBlock);
        const fieldNames = ['voteDay', 'steemstem', 'steemmakers', 'mspwaves', 'comments', 'other',
                                    'development', 'analysis', 'translations', 'tutorials', 'video-tutorials',
                                    'bug-hunting', 'ideas', 'graphics', 'blog', 'documentation', 'copywriting', 'antiabuse'];
        postprocessing.dataExport(utopianVoteSplitByDay.slice(0), 'utopianVoteSplitByDay', fieldNames);

    let utopianAuthors = await mongoblock.utopianAuthorsMongo(db, openBlock, closeBlock);
    const fieldNames2 = ['_id.author', 'percent', 'count'];
    postprocessing.dataExport(utopianAuthors.slice(0), 'utopianAuthors', fieldNames2);

    } else {
        console.log('Parameter issue');
    }

console.log('closing mongo db');
client.close();


}



// Summaries of transfers
// --------------------------------
async function transferSummary() {
    let transferArray = [];

    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    if (parameterIssue == false) {
        transferArray = await mongoblock.transferSummaryMongo(db, openBlock, closeBlock, parameter3);
        console.dir(transferArray, {depth: null})
    } else {
        console.log('Parameter issue');
    }

console.log('closing mongo db');
client.close();
}



// Summaries of delegations
// --------------------------------
async function delegationSummary() {
    let delegationArray = [];

    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    if (parameterIssue == false) {
        delegationArray = await mongoblock.delegationSummaryMongo(db, openBlock, closeBlock, parameter3);
        console.dir(delegationArray, {depth: null})
    } else {
        console.log('Parameter issue');
    }

console.log('closing mongo db');
client.close();
}
