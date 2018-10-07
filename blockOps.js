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
const steemrequest = require('./steemrequest/steemrequest.js')
const mongoblock = require('./mongoblock.js')
const helperblock = require('./helperblock.js')
const postprocessing = require('./postprocessing.js')


// MongoDB
// -------
const MongoClient = mongodb.MongoClient;
const url = 'mongodb://localhost:27017';
const dbName = 'blockOpsTesting';


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
} else if (commandLine == 'fillprices') {
    fillPrices();
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
} else if (commandLine == 'validate') {
    validateComments();
} else if (commandLine == 'showblock') {
    showBlock();
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
    let checkCb = await mongoblock.checkCollectionExists(db, 'blocksProcessed');
    if (checkCb == false) {
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

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    console.log(openBlock, closeBlock, parameterIssue);

    blocksToProcess = closeBlock - openBlock;
    console.log(openBlock, blocksToProcess);

    fiveBlock();

    // Function to extract data of x blocks from the blockchain (originally five blocks)
    // ---------------------------------------------------------------------------------
    async function fiveBlock() {

        let launchBlocks = Math.min(blocksPerRound, blocksToProcess - blocksStarted - blocksOK);
        console.log(launchBlocks, blocksToProcess - blocksStarted - blocksOK);
        for (var i = 0; i < launchBlocks; i+=1) {
            if(blocksStarted - priorArrayCount == blockProcessArray.length) {
                do {
                    priorArrayCount += blockProcessArray.length;
                    blockProcessArray = await mongoblock.reportBlocksProcessed(db, openBlock + priorArrayCount + blocksOK, Math.min(openBlock + priorArrayCount + blocksOK + blockProcessNumber, closeBlock), 'return');
                    console.log('array called', openBlock + priorArrayCount + blocksOK, Math.min(openBlock + priorArrayCount + blocksOK + blockProcessNumber, closeBlock)-1);
                    blocksOK += ( Math.min(openBlock + priorArrayCount + blocksOK + blockProcessNumber, closeBlock) - (openBlock + priorArrayCount + blocksOK) - blockProcessArray.length);
                    console.log('blocksOK', blocksOK);
                }
                while (blockProcessArray.length == 0 && (blocksStarted + blocksOK < blocksToProcess));

                // Reset operations counter in blocksProcessed for blocks being rerun
                mongoblock.resetBlocksProcessed(db, blockProcessArray[0], blockProcessArray[blockProcessArray.length-1]);
            }

            if (blocksStarted + blocksOK < blocksToProcess) {
                blockNo = blockProcessArray[blocksStarted - priorArrayCount];
                // Gets data for one block, processes it in callback "processOps"
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
                    if (operation.op[0] == 'author_reward') {
                        authorRewardCount += 1;
                    }
                }

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
                        if (operation.op[0] == 'comment') {
                            mongoblock.processComment(operation, operationNumber, mongoblock.mongoComment, db);
                        } else if (operation.op[0] == 'vote') {
                            mongoblock.processVote(operation, operationNumber, mongoblock.mongoVote, db);
                        } else if (operation.op[0] == 'author_reward') {
                            mongoblock.validateComments(db, operation);
                            activeVotes(operation);
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

                if (blocksCompleted + blocksOK == blocksToProcess) {
                    completeOperationsLoop();

                } else if (blocksStarted - blocksCompleted < 4) {
                    fiveBlock();
                }
            } catch (error) {
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
            } else {
                console.log(error);
            }

            let errorRecord = {blockNumber: localBlockNo, status: 'error'};
            mongoblock.mongoErrorLog(db, errorRecord, 0);
            blocksCompleted += 1;
            errorCount += 1;
        }
    }

    async function activeVotes(localOperation) {
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
        //console.log('db closing');
        //client.close();
    }

// Closing fillOperations
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
    // Connect to Mongo
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    // Check prices index exists
    let checkCp = await mongoblock.checkCollectionExists(db, 'prices');
    if (checkCp == false) {
        db.collection('prices').createIndex({payout_blockNumber: 1}, {unique:true});
    }

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    if (parameterIssue == false) {
        // Select a comment from each hour (around the 30 minute mark)
        pricesArray = await mongoblock.mongoFillPrices(db, openBlock, closeBlock);

        // Skip prices that have previously been inserted to prices collection
        let previousPrices = await db.collection('prices')
            .find({payout_blockNumber: { $gte: openBlock, $lt: closeBlock }})
            .project({_id: 0, date: 1})
            .toArray();

        let countSkips = 0;
        for (let comment of pricesArray) {
            priorFlag = false;
            for (let priorDate of previousPrices) {
                if (comment._id.year == priorDate.date.year && comment._id.month == priorDate.date.month && comment._id.day == priorDate.date.day && comment._id.hour == priorDate.date.hour) {
                    priorFlag = true;
                }
            }

            if (priorFlag == true) {
                // Skips record
                countSkips += 1;
            } else {
                // Pulls record and calculates currency rations using payout data from operations (vests, sbd, steem) and payout data from post (STU)
                await steemrequest.getComment(comment.author, comment.permlink)
                    .then(async function(body) {
                        result = JSON.parse(body).result;
                        comment.date = comment._id;
                        delete comment._id;
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
                        comment.rsharesPerSTU = Number((comment.rshares / comment.total_payout_value).toFixed(3));
                        comment.steemPerSTU = 0;
                        if (comment.author_payout_steem > 0) {
                            comment.steemPerSTU = Number((comment.author_payout_steem / (comment.author_payout_value - comment.author_payout_sbd - (comment.author_payout_vests/comment.vestsPerSTU))).toFixed(3));
                        }
                        await mongoblock.mongoPrice(db, comment, 0);
                })
            }
        }
        console.log(countSkips + ' record skips as already exits in prices collection.')
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
        let marketShareSummary = await mongoblock.reportCommentsMongo(db, openBlock, closeBlock);
        let exportData = postprocessing.marketShareProcessing(marketShareSummary);
        const fieldNames = ['application', 'authors', 'authorsRank', 'posts', 'postsRank', 'author_payout_sbd', 'author_payout_steem', 'author_payout_vests', 'benefactor_payout_vests', 'curator_payout_vests'];
        postprocessing.dataExport(exportData.slice(0), 'marketShareTest', fieldNames);
        console.log('');
        console.log('closing mongo db');
        console.log('------------------------------------------------------------------------');
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



// Set-up for validation routines for comments
// -------------------------------------------
async function showBlock() {
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    await mongoblock.showBlockMongo(db, Number(parameter1));
}
