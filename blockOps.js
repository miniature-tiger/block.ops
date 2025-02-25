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
} else if (commandLine == 'partialremove') {
    partialRemove();
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
} else if (commandLine == 'bidbotprofit') {
    bidbotProfitability();
} else if (commandLine == 'transfersummary') {
    transferSummary();
} else if (commandLine == 'delegationsummary') {
    delegationSummary();
} else if (commandLine == 'creationsummary') {
    accountCreationSummary();
} else if (commandLine == 'followsummary') {
    followSummary();
} else if (commandLine == 'powersummary') {
    powerSummary();
} else if (commandLine == 'earningsdistribution') {
    earningsDistribution();
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


// Function to remove part of a collection (i.e. between dates)
// ---------------------------------------------------
async function partialRemove() {
    // Opening MongoDB
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
    if (parameterIssue == false) {
        // Deletes records in range
        db.collection(parameter3).deleteMany({ payout_blockNumber: { $gte: openBlock, $lt: closeBlock }})
            .then(function() {
                // Closing MongoDB
                console.log('closing mongo db');
                client.close();
            })
            .catch(function(error) {
                console.log(error);
            });

      } else {
          console.log('Parameter issue');
      }
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

    // Creating indexes if process is being run for the first time
    let checkCo = await mongoblock.checkCollectionExists(db, 'comments');
    if (checkCo == false) {
        db.collection('comments').createIndex({author: 1, permlink: 1}, {unique:true});
        db.collection('comments').createIndex({blockNumber: 1}, {unique:false});
        db.collection('comments').createIndex({payout_blockNumber: 1}, {unique:false});
    }

    let checkTr = await mongoblock.checkCollectionExists(db, 'transfers');
    if (checkTr == false) {
        db.collection('transfers').createIndex({blockNumber: 1, from: 1, to: 1, transactionNumber: 1, operationNumber: 1}, {unique:true});
    }

    let checkDg = await mongoblock.checkCollectionExists(db, 'delegation');
    if (checkDg == false) {
        db.collection('delegation').createIndex({blockNumber: 1, delegator: 1, delegatee: 1}, {unique:true});
    }

    let checkCa = await mongoblock.checkCollectionExists(db, 'createAccounts');
    if (checkCa == false) {
        db.collection('createAccounts').createIndex({blockNumber: 1, creator: 1, account: 1, transactionNumber: 1, operationNumber: 1}, {unique:true});
    }

    let checkFo = await mongoblock.checkCollectionExists(db, 'follows');
    if (checkFo == false) {
        db.collection('follows').createIndex({blockNumber: 1, transactionNumber: 1, operationNumber: 1, following: 1 }, {unique:true});
    }

    db.collection('vesting').createIndex({blockNumber: 1, referenceNumber: 1, type: 1, from: 1, to: 1 }, {unique:true});
    let checkVe = await mongoblock.checkCollectionExists(db, 'vesting');
    if (checkVe == false) {
        db.collection('vesting').createIndex({blockNumber: 1, referenceNumber: 1, type: 1, from: 1, to: 1 }, {unique:true});
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
    let apiCalls = 0;
    let apiCallsPerMinute = 0;
    let callDelay = 1500;
    let difficultBlocks = [23791925, 28091061];

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
    console.log(openBlock, closeBlock, parameterIssue);

    blocksToProcess = closeBlock - openBlock;
    console.log(openBlock, blocksToProcess);

    fiveBlock('original');

    // Function to extract data of x blocks from the blockchain (originally five blocks)
    // ---------------------------------------------------------------------------------
    async function fiveBlock(localMarker) {
        if(debug === true) {console.log('localMarker', localMarker)};
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
                if(debug == true) {console.log('calling getOpsAppBase', blockNo)};
                apiCalls += 1;
                //console.log("steemrequest", blockNo, " || seconds:", ((Date.now() - launchTime)/1000).toFixed(1))
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
        //console.log("processOps", localBlockNo, " || seconds:", ((Date.now() - launchTime)/1000).toFixed(1))
        if(debug === true) {console.log('localBlockNo start processOps', localBlockNo)};
        if (!error) {
            try {
                // Workaround for exceptional blocks with jsonparse issues
                if (difficultBlocks.includes(localBlockNo)) {
                    body = fixBlock(body, localBlockNo);
                }
                //console.log(body);
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
                        } else if (operation.op[0] == 'account_create') {
                            mongoblock.processAccountCreation(operation, operationNumber, mongoblock.mongoAccountCreation, db);
                        } else if (operation.op[0] == 'account_create_with_delegation') {
                            mongoblock.processAccountCreation(operation, operationNumber, mongoblock.mongoAccountCreation, db);
                        } else if (operation.op[0] == 'claim_account') {
                            mongoblock.processAccountCreation(operation, operationNumber, mongoblock.mongoAccountCreation, db);
                        } else if (operation.op[0] == 'create_claimed_account') {
                            mongoblock.processAccountCreation(operation, operationNumber, mongoblock.mongoAccountCreation, db);
                        } else if (operation.op[0] == 'custom_json') {
                            if (operation.op[1].id == 'follow') {
                                  mongoblock.processFollows(operation, operationNumber, mongoblock.mongoFollows, db);
                            } else {
                                opsNotHandled += 1;
                            }
                        } else if (operation.op[0] == 'withdraw_vesting') { // Power down
                            mongoblock.processVesting(operation, operationNumber, mongoblock.mongoVesting, db);
                        } else if (operation.op[0] == 'transfer_to_vesting') { // Power up
                            mongoblock.processVesting(operation, operationNumber, mongoblock.mongoVesting, db);
                        // Virtual operations
                        } else if (operation.op[0] == 'author_reward') {
                            mongoblock.validateComments(db, operation);
                            apiCalls+=1;
                            //console.log("activeVotes", localBlockNo)
                            activeVotes(operation, db);
                            mongoblock.processAuthorReward(operation, mongoblock.mongoAuthorReward, db);
                        } else if (operation.op[0] == 'comment_benefactor_reward') {
                            mongoblock.processBenefactorReward(operation, mongoblock.mongoBenefactorReward, db);
                        } else if (operation.op[0] == 'curation_reward') {
                            mongoblock.processCuratorReward(operation, mongoblock.mongoCuratorReward, db);
                        } else if (operation.op[0] == 'fill_vesting_withdraw') { // Virtual Op - power down payment
                            mongoblock.processVesting(operation, operationNumber, mongoblock.mongoVesting, db);
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
                //console.log("blockCompleted:", localBlockNo, " || seconds:", ((Date.now() - launchTime)/1000).toFixed(1))
                blocksCompleted += 1;

                if(debug === true) {console.log('blocksStarted - blocksCompleted, localBlockNo', blocksStarted, - blocksCompleted, localBlockNo)};
                if (blocksCompleted + blocksOK === blocksToProcess) {
                    completeOperationsLoop();
                //} else if ((blocksStarted - blocksCompleted === 0) && (blockProcessArrayFlag === false)) {
                } else if ((blocksStarted - blocksCompleted === Math.round(blocksPerRound/2,0)) && (blockProcessArrayFlag == false)) {
                    //console.log('fiveBlock', 'blocksStarted: ',blocksStarted , 'blocksCompleted: ', blocksCompleted);
                    //console.log("pre-call", blocksStarted, blocksCompleted, " || seconds:", ((Date.now() - launchTime)/1000).toFixed(1))
                    apiCallsPerMinute = Math.round(apiCalls / ((Date.now() - launchTime) / 60000), 1)
                    if (apiCallsPerMinute > 350) {
                        callDelay = Math.min(callDelay + 30, 1700)
                    } else if (apiCallsPerMinute < 300) {
                        callDelay = Math.max(callDelay - 30, 1300)
                    } else {
                        callDelay = Math.round(1500 + (callDelay - 1500) * 0.9)
                    }
                    setTimeout(function(passedBlocksStarted, passedApiCalls, passedApiCallsPerMinute) {
                        //console.log("call", ((Date.now() - launchTime)/1000).toFixed(1))
                        console.log('|| API calls:', passedApiCalls, ' || Seconds:', Math.round((Date.now() - launchTime)/1000, 0), ' || API calls per minute:', passedApiCallsPerMinute, '|| call delay:', callDelay);
                        fiveBlock('marker' + passedBlocksStarted);
                    }, callDelay, blocksStarted, apiCalls, apiCallsPerMinute)
                } else {
                    //console.log("no call", blocksStarted, blocksCompleted)
                    //console.log('else clause', 'blocksStarted: ',blocksStarted , 'blocksCompleted: ', blocksCompleted);
                }
            } catch (error) {
                console.log(error);
                console.log('Error in processing virtual ops:', localBlockNo, 'Error logged.');
                let errorRecord = {blockNumber: localBlockNo, status: 'error'};
                mongoblock.mongoErrorLog(db, errorRecord, 0);
                blocksCompleted += 1;
                errorCount += 1;
                if(debug === true) {console.log('errorCount', errorCount)};
                if (blocksCompleted + blocksOK == blocksToProcess) {
                    completeOperationsLoop();
                //} else if ((blocksStarted - blocksCompleted === 0) && (blockProcessArrayFlag === false)) {
                } else if ((blocksStarted - blocksCompleted === Math.round(blocksPerRound/2,0)) && (blockProcessArrayFlag == false)) {
                    setTimeout(function(passedBlocksStarted, passedApiCalls) {
                        //console.log('|| API calls:', passedApiCalls, ' || Seconds:', Math.round((Date.now() - launchTime)/1000, 0), ' || API calls per minute:', Math.round(apiCalls / ((Date.now() - launchTime) / 60000), 1))
                        //console.log("error call", ((Date.now() - launchTime)/1000).toFixed(1))
                        fiveBlock('marker' + passedBlocksStarted);
                    }, 10000, blocksStarted, apiCalls)
                } else {
                    //console.log("no call", blocksStarted, blocksCompleted)
                }
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
            if(debug === true) {console.log('errorCount', errorCount)};

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

    // Workaround function for exceptional blocks with jsonparse issues
    // ----------------------------------------------------------------
    function fixBlock(localBody, blockToFix) {

        console.log('Fixing block.');
        let result = '';
        let openFirst = 0, closeFirst = 0;
        if (blockToFix == 23791925) {
            openFirst = localBody.indexOf('{"trx_id"', 0);
            closeFirst = localBody.indexOf('{"trx_id"', openFirst + 10);
            result = localBody.slice(0, openFirst) + localBody.slice(closeFirst, localBody.length);
        } else if (blockToFix == 28091061) {
            result = localBody;
        }

        return result;


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

    let blockNumberPatch = Number(parameter1);
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
        console.log(path.join(__dirname, 'lostBlocks', folderName, fileName));
        return fsPromises.readFile(path.join(__dirname, 'lostBlocks', folderName, fileName), 'utf8')
            .catch(function(error) {
                console.log(error);
            });
    }

    //Reset operations counter in blocksProcessed for blocks being rerun
    db.collection('blocksProcessed')
        .updateMany({ blockNumber: blockNumberPatch, status: {$ne: 'OK'}},
                {$set: {operationsProcessed: 0, activeVoteSetProcessed: 0}, $pull: { operations: {transactionType: "notHandled"}}}, {upsert: false})
        .catch(function(error) {
            console.log(error);
        });

    // Call function to read json file into memory, parse, and annotate with default statuses
    let lostArray = await jsonFile(parameter1, parameter1 + '.json');
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
    //let timestamp = new Date('2018-09-17T19:56:51.000Z');
    let timestamp = new Date(lostArray[0].timestamp + '.000Z');
    let blockRecord = {blockNumber: blockNumberPatch, timestamp: timestamp, status: 'Processing', operationsCount: lostArray.length, activeVoteSetCount: 6759, activeVoteSetProcessed: 0};
    mongoblock.mongoBlockProcessed(db, blockRecord, 0);

    // Update statuses in lostArray based on any previous runs
    await db.collection('blocksProcessed')
        .aggregate([
            { $match : {blockNumber: blockNumberPatch}},
            { $project : {_id: 0, operations: 1 }},
            { $unwind : "$operations"},
        ])
        .toArray()
        .then(async function(records) {
            console.log('Aggregation finished.', ((Date.now() - launchTime)/1000/60).toFixed(2));
            for (let record of records) {
                if (record.operations.hasOwnProperty('virtualOp')) {
                    let arrayPosition = lostArray.findIndex(fI => fI.virtual_op == record.operations.virtualOp);
                    if (arrayPosition == -1) {
                        console.dir(record, {depth: null})

                    }
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
                setTimeout(patchRunner, Math.max(50 - (Date.now() - lastMongo), 0));

            } else if (operation.op[0] == 'comment_benefactor_reward') {
                if (operation.opStatus != 'OK') {
                    mongoblock.processBenefactorReward(operation, mongoblock.mongoBenefactorReward, db);
                    lastMongo = Date.now();
                } else {
                    preProcessed += 1;
                }
                count += 1;
                setTimeout(patchRunner, Math.max(50 - (Date.now() - lastMongo), 0));

            } else if (operation.op[0] == 'fill_vesting_withdraw') {
                if (operation.opStatus != 'OK') {
                    mongoblock.processVesting(operation, 0, mongoblock.mongoVesting, db);
                    lastMongo = Date.now();
                } else {
                    preProcessed += 1;
                }
                count += 1;
                setTimeout(patchRunner, Math.max(50 - (Date.now() - lastMongo), 0));

            } else {
                opsNotHandled += 1;
                patchRunner();
            }
        }
    }

    // Adding numbers of blocks preprocessed or not handled and checking complete
    function finishUp() {
        db.collection('blocksProcessed').findOneAndUpdate(  { blockNumber: blockNumberPatch},
                                                            { $inc: {activeVoteSetProcessed: preActiveProcessed}},
                                                            { upsert: false, returnOriginal: false, maxTimeMS: 2000})
            .then(function(response) {
                if ((response.value.operationsCount == response.value.operationsProcessed) && (response.value.activeVoteSetCount == response.value.activeVoteSetProcessed) && (response.value.status == 'Processing')) {
                    db.collection('blocksProcessed').updateOne({ blockNumber: blockNumberPatch}, {$set: {status: 'OK'}})
                }
            });

        let recordOperation = {transactionType: 'notHandled', ops_not_handled: opsNotHandled, skipped_ops: preProcessed, count: opsNotHandled + preProcessed, status: 'OK'};
        mongoblock.mongoOperationProcessed(db, blockNumberPatch, recordOperation, opsNotHandled + preProcessed, 0);
        console.log('----- Process Completed -----', ((Date.now() - launchTime)/1000/60).toFixed(2));
    }
}



// Function to convert parameters for a date range into blockNumbers
// -----------------------------------------------------------------
// Accepts two dates as parameters  or a date and a number
// Parameters are global and defined on command line
async function blockRangeDefinition(paramOne, paramTwo, db) {

    let openBlock = 0, closeBlock = 0, parameterIssue = false;

    if (!(isNaN(paramOne))) {
        openBlock = Number(paramOne);
    } else if (typeof paramOne == 'string') {
        openDate = new Date(paramOne + 'T00:00:00.000Z');
        console.log(openDate)
        openDateEnd = helperblock.forwardOneDay(openDate);
        console.log(openDateEnd)
        openBlock = await mongoblock.dateToBlockNumber(openDate, openDateEnd, db)
            .catch(function(error) {
                console.log(error);
                parameterIssue = true;
            });
    } else {
        parameterIssue = true;
    }

    if (!(isNaN(paramTwo))) {
        closeBlock = openBlock + Number(paramTwo);
    } else if (typeof paramTwo == 'string') {
        closeDate = new Date(paramTwo + 'T00:00:00.000Z');
        console.log(closeDate)
        closeDateEnd = helperblock.forwardOneDay(closeDate);
        console.log(closeDateEnd)
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
    let HF20 = steemdata.hardfork.hf20;
    let pricesArray = [];

    // Connect to Mongo
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    // Check prices index exists
    let checkCp = await mongoblock.checkCollectionExists(db, 'prices');
    if (checkCp == false) {
        //db.collection('prices').createIndex({payout_blockNumber: 1}, {unique:true});
    }

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);

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
            let previousPrices = await mongoblock.obtainPricesMongo(db, openBlock, closeBlock, {sourcePayout: {$exists: true}});
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
                            comment = await postprocessing.payoutPrices(comment, body, HF20);
                            await mongoblock.mongoPrice(db, comment, 0);
                    });
                }
            }
            console.log(countSkips + ' record skips as already exits in prices collection.');

            // Interpolation
            let updatedPrices = await mongoblock.obtainPricesMongo(db, openBlock, closeBlock, {sourcePayout: {$exists: true}});

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
                                sourcePayout: 'interpolated',
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

            // Select a power-down payment from each hour (around the 20-40 minute mark)
            let powerArray = await mongoblock.mongoPowerPrices(db, openBlock, closeBlock);

            // Skip prices that have previously been inserted to prices collection
            let previousPowerPrices = await mongoblock.obtainPricesMongo(db, openBlock, closeBlock, {sourcePower: {$exists: true}});

            // Loop through powerArray starts here
            let countSkipsTwo = 0;
            for (let powerdown of powerArray) {
                let priceRecord = {};

                let priorFlag = false;
                for (let priorDate of previousPowerPrices) {
                    if (powerdown._id == priorDate._id) {
                        priorFlag = true;
                    }
                }

                if (priorFlag == true) {
                    // Skips record
                    countSkipsTwo += 1;
                } else {
                    priceRecord._id = powerdown._id;
                    priceRecord.sourcePower = 'derived';
                    priceRecord.depositedAmount = powerdown.depositedAmount;
                    priceRecord.withdrawnAmount = powerdown.withdrawnAmount;
                    priceRecord.vestsPerSteem = Number((powerdown.withdrawnAmount / powerdown.depositedAmount).toFixed(6));
                    await mongoblock.mongoPrice(db, priceRecord, 0);
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

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
    if (parameterIssue == false) {
        displayPrices = await mongoblock.obtainPricesMongo(db, openBlock, closeBlock, {});
        for (let price of displayPrices) {
            console.log(price)
        }
        const fieldNames = ['_id', 'vestsPerSTU', 'steemPerSTU', 'rsharesPerSTU', 'STUPerVests', 'STUPerSteem', 'STUPerRshares', 'vestsPerSteem'];
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

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
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

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
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

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);

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

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
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

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
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

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
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

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
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

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
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
    let utopianTimingArray = [];

    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
    if (parameterIssue == false) {

        let utopianPosts = await mongoblock.reportUtopianCommentsMongo(db, openBlock, closeBlock, 'created', 'posts', 'default');
        console.dir(utopianPosts, {depth: null})

        let fieldNamesPosts = ['_id.dateDay', '_id.utopianType', '_id.utopianTask', 'authors', 'posts', 'author_payout_sbd', 'author_payout_steem', 'author_payout_vests', 'benefactor_payout_sbd', 'benefactor_payout_steem', 'benefactor_payout_vests', 'curator_payout_vests', 'author_payout_sbd_STU', 'author_payout_steem_STU', 'author_payout_vests_STU', 'benefactor_payout_sbd_STU', 'benefactor_payout_steem_STU', 'benefactor_payout_vests_STU', 'curator_payout_vests_STU'];
        postprocessing.dataExport(utopianPosts.slice(0), 'utopianPosts', fieldNamesPosts);

        [utopianVoteSplitByDay, utopianTimingArray] = await mongoblock.utopianVotesMongo(db, openBlock, closeBlock);
        const fieldNames = ['index', 'steemstem', 'utopianTask', 'mspwaves', 'moderatorComment', 'comments', 'other',
                                    'development', 'analysis', 'translations', 'tutorials', 'video-tutorials',
                                    'bug-hunting', 'ideas', 'graphics', 'blog', 'documentation', 'copywriting', 'visibility', 'antiabuse', 'iamutopian'];
        postprocessing.dataExport(utopianVoteSplitByDay.slice(0), 'utopianVoteSplitByDay', fieldNames);

        const fieldNames2 = ['vote_time', 'vote_days', 'category'];
        postprocessing.dataExport(utopianTimingArray.slice(0), 'utopianTiming', fieldNames2);

    } else {
        console.log('Parameter issue');
    }

    console.log('closing mongo db');
    client.close();
}



// Bidbot profitability analysis
// --------------------------------
async function bidbotProfitability() {

    let botsIncArray = [ "appreciator", "boomerang", "booster", "buildawhale", "postpromoter", "rocky1", "smartsteem", "upme" ]
    let bidbotTransferArray = [];
    let bidbotOutput = [];
    let counter = 0;

    // Connects to MongoDB
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    // Defines and validates parameters
    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
    if (parameterIssue == false) {

        // Obtains all transfers to and from listed bidbots in date range
        bidbotTransferArray = await mongoblock.bidbotTransfersMongo(db, openBlock, closeBlock, botsIncArray);

        // Parses transfer memo and obtains bidbot vote values from comments collection based on author/permlink
        for (let transfer of bidbotTransferArray) {
            if (transfer.memo.includes("/@")) {
                let url = transfer.memo.substr(transfer.memo.indexOf("/@")+2, transfer.memo.length - (transfer.memo.indexOf("/@")+2));
                let [urlAccount, urlPermlink] = url.split("/");
                let urlPostVote = await mongoblock.bidbotVoteValuesMongo(db, urlAccount, urlPermlink, transfer.to);
                if (urlPostVote.length > 0) {
                    let voteValueVoting = Number((urlPostVote[0].voteDate_value * 0.75).toFixed(3));
                    let voteValuePayout = Number((urlPostVote[0].votePayout_value * 0.75).toFixed(3));
                    bidbotOutput.push({bidbot: transfer.to, author: urlPostVote[0].author, transfer: transfer.amount, voteValueVoting: voteValueVoting, voteValuePayout: voteValuePayout, voteTimestamp: urlPostVote[0].curators.vote_timestamp, voteDateHour: urlPostVote[0].voteDateHour, payoutDateHour: urlPostVote[0].payoutDateHour })
                    counter += 1;
                }
            }
        }
        console.log(counter);

        // Output to csv file
        const fieldNames = ['bidbot', 'author', 'transfer', 'voteValueVoting', 'voteValuePayout', 'voteTimestamp', 'voteDateHour', 'payoutDateHour']
        postprocessing.dataExport(bidbotOutput.slice(0), 'bidbotAnalysis', fieldNames);

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

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
    if (parameterIssue == false) {
        transferArray = await mongoblock.transferSummaryMongo(db, openBlock, closeBlock, parameter3);
        console.dir(transferArray, {depth: null})
        // Output to csv file
        const fieldNames = ['from', 'to', 'amount', 'currency', 'timestamp', 'party']
        postprocessing.dataExport(transferArray[0].party.slice(0), 'transfersByParty', fieldNames);
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

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
    if (parameterIssue == false) {
        delegationArray = await mongoblock.delegationSummaryMongo(db, openBlock, closeBlock, parameter3);
        console.dir(delegationArray, {depth: null})
    } else {
        console.log('Parameter issue');
    }

    console.log('closing mongo db');
    client.close();
}



// Summaries of account creation
// --------------------------------
async function accountCreationSummary() {
    let accountCreationArray = [];

    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
    if (parameterIssue == false) {
        accountCreationArray = await mongoblock.accountCreationSummaryMongo(db, openBlock, closeBlock);
        let fieldNames = ['_id.dateHour', '_id.type', '_id.creator', '_id.HF20', 'feeAmount', 'count']
        // Output to csv file
        Object.keys(accountCreationArray[0]).forEach(function(summary) {
              console.log(summary);
              console.dir(accountCreationArray[0][summary], {depth: null})
              postprocessing.dataExport(accountCreationArray[0][summary].slice(0), summary, fieldNames);
        })
    } else {
        console.log('Parameter issue');
    }

    console.log('closing mongo db');
    client.close();
}



// Summaries of account creation
// --------------------------------
async function followSummary() {
    let followsArray = [];

    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
    if (parameterIssue == false) {
        followsArray = await mongoblock.followSummaryMongo(db, openBlock, closeBlock, parameter3);
        console.dir(followsArray, {depth: null})
    } else {
        console.log('Parameter issue');
    }

    console.log('closing mongo db');
    client.close();
}



// Summaries of powering up and down
// -----------------------------------
async function powerSummary() {
    let powerArray = [];

    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
    if (parameterIssue == false) {
        powerArray = await mongoblock.powerSummaryMongo(db, openBlock, closeBlock);
        console.dir(powerArray[0], {depth: null})
        console.dir(powerArray[1], {depth: null})
        const fieldNames = ['_id.date', 'powerUp', 'powerDown', 'downReleaseVests', 'downReleaseSteem'];
        postprocessing.dataExport(powerArray[0].slice(0), 'powerUpDownDate', fieldNames);
    } else {
        console.log('Parameter issue');
    }

    console.log('closing mongo db');
    client.close();
}



// Earnings distribution summary
// -----------------------------------
async function earningsDistribution() {

    const fieldNamesEarnings = ['user', 'retained', 'postCount', 'author_payout_STU', 'userGroupVoteCount', 'userGroup_payout_STU', 'voteCount', 'curator_payout_STU', 'benefactorCount', 'benefactor_payout_STU', 'total_payout_STU'];
    const fieldNamesDistribution = ['earnings', 'userCount', 'userCountRetained', 'userCountPerc', 'postCount', 'author_payout_STU', 'userGroupVoteCount', 'userGroup_payout_STU', 'voteCount', 'curator_payout_STU', 'benefactorCount', 'benefactor_payout_STU', 'total_payout_STU'];

    client = await MongoClient.connect(url, { useNewUrlParser: true, connectTimeoutMS: 600000, socketTimeoutMS: 600000 });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(parameter1, parameter2, db);
    if (parameterIssue == false) {
        let [openBlockTwo, closeBlockTwo, parameterIssueTwo] = await blockRangeDefinition(helperblock.forwardOneMonth(new Date(parameter1 + 'T00:00:00.000Z')).toISOString().slice(0, 10), helperblock.forwardOneMonth(new Date(parameter2 + 'T00:00:00.000Z')).toISOString().slice(0, 10), db);
        let authorEarnings = await mongoblock.authorEarningsMongo(db, openBlock, closeBlock, 'all', parameter3);
        authorEarnings = postprocessing.tidyID(authorEarnings);
        let authorEarningsTwo = await mongoblock.authorEarningsMongo(db, openBlockTwo, closeBlockTwo, 'all', parameter3);
        authorEarningsTwo = postprocessing.tidyID(authorEarningsTwo);
        authorEarnings = postprocessing.checkPresent(authorEarnings, authorEarningsTwo);
        postprocessing.dataExport(authorEarnings.slice(0), 'authorEarnings', fieldNamesEarnings);
        let authorDistribution = await postprocessing.earningsDistribution(authorEarnings, 1, 10000, 'author_payout_STU');
        postprocessing.dataExport(authorDistribution.slice(0), 'authorDistribution', fieldNamesDistribution);
        let authorDistribution50 = await postprocessing.earningsDistribution(authorEarnings, 50, 1500, 'author_payout_STU');
        postprocessing.dataExport(authorDistribution50.slice(0), 'authorDistribution50', fieldNamesDistribution);
        //console.dir(authorDistribution, {depth: null})
        console.log('Timecheck: authorDistribution ' + (Date.now() - launchTime)/1000/60);

        let bidbotEarnings = await mongoblock.voteGroupEarningsMongo(db, openBlock, closeBlock, 'all', steemdata.bidbotArray, parameter3);
        bidbotEarnings = postprocessing.tidyID(bidbotEarnings);
        let bidbotDistribution = await postprocessing.earningsDistribution(bidbotEarnings, 1, 10000, 'userGroup_payout_STU');

        postprocessing.dataExport(bidbotDistribution.slice(0), 'bidbotDistribution', fieldNamesDistribution);
        //console.dir(bidbotDistribution, {depth: null});
        let combinedEarnings = postprocessing.combineByUser(authorEarnings, 'author_payout_STU', bidbotEarnings, 'userGroup_payout_STU', -1);
        postprocessing.dataExport(combinedEarnings.slice(0), 'authorbidbot', fieldNamesEarnings);
        let authorbidbotDistribution = await postprocessing.earningsDistribution(combinedEarnings, 1, 10000, 'total_payout_STU');
        postprocessing.dataExport(authorbidbotDistribution.slice(0), 'authorbidbotDistribution', fieldNamesDistribution);
        let authorbidbotDistributionLow = await postprocessing.earningsDistribution(combinedEarnings, 0.1, 11, 'total_payout_STU');
        postprocessing.dataExport(authorbidbotDistributionLow.slice(0), 'authorbidbotDistributionLow', fieldNamesDistribution);
        let authorbidbotDistribution50 = await postprocessing.earningsDistribution(combinedEarnings, 50, 1500, 'total_payout_STU');
        postprocessing.dataExport(authorbidbotDistribution50.slice(0), 'authorbidbotDistribution50', fieldNamesDistribution);
        console.log('Timecheck: bidbotDistribution ' + (Date.now() - launchTime)/1000/60);

        let curatorEarnings = await mongoblock.curatorEarningsMongo(db, openBlock, closeBlock, 'all', parameter3);
        curatorEarnings = postprocessing.tidyID(curatorEarnings);
        let curatorDistribution = await postprocessing.earningsDistribution(curatorEarnings, 1, 10000, 'curator_payout_STU');
        postprocessing.dataExport(curatorDistribution.slice(0), 'curatorDistribution', fieldNamesDistribution);
        //console.dir(curatorEarnings, {depth: null})
        combinedEarnings = postprocessing.combineByUser(combinedEarnings.slice(0), 'total_payout_STU', curatorEarnings, 'curator_payout_STU', 1);
        console.log('Timecheck: curatorDistribution ' + (Date.now() - launchTime)/1000/60);

        let benefactorEarnings = await mongoblock.benefactorEarningsMongo(db, openBlock, closeBlock, 'all', parameter3);
        benefactorEarnings = postprocessing.tidyID(benefactorEarnings);
        let benefactorDistribution = await postprocessing.earningsDistribution(benefactorEarnings, 1, 10000, 'benefactor_payout_STU');
        postprocessing.dataExport(benefactorDistribution.slice(0), 'benefactorDistribution', fieldNamesDistribution);
        //console.dir(benefactorEarnings, {depth: null})
        combinedEarnings = postprocessing.combineByUser(combinedEarnings.slice(0), 'total_payout_STU', benefactorEarnings, 'benefactor_payout_STU', 1);
        postprocessing.dataExport(combinedEarnings.slice(0), 'combinedEarnings', fieldNamesEarnings);
        console.log('Timecheck: benefactorDistribution ' + (Date.now() - launchTime)/1000/60);

        let combinedDistribution = await postprocessing.earningsDistribution(combinedEarnings, 1, 10000, 'total_payout_STU');
        postprocessing.dataExport(combinedDistribution.slice(0), 'combinedDistribution', fieldNamesDistribution);
        let combinedDistribution50 = await postprocessing.earningsDistribution(combinedEarnings, 50, 1500, 'total_payout_STU');
        postprocessing.dataExport(combinedDistribution50.slice(0), 'combinedDistribution50', fieldNamesDistribution);
        console.log('Timecheck: combinedDistribution ' + (Date.now() - launchTime)/1000/60);

        console.log('End time: ' + (Date.now() - launchTime)/1000/60);
        console.log('----------------');
    } else {
        console.log('Parameter issue');
    }

    console.log('closing mongo db');
    client.close();
}
