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


// MongoDB
// -------
const MongoClient = mongodb.MongoClient;
const url = 'mongodb://localhost:27017';
const dbName = 'blockOps';


// Command Line inputs and parameters
// ----------------------------------
let commandLine = process.argv.slice(2)[0];
let parameter1 = process.argv.slice(2)[1];
let parameter2 = process.argv.slice(2)[2];
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
} else if (commandLine == 'reportcomments') {
    mongoblock.reportComments(MongoClient, url, dbName);
} else if (commandLine == 'reportblocks') {
    reportBlocks();
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

    let loopBlock = 1, loopDate = new Date(), loopStartSet = false, record = {};

    // Checks whether blockDates have previously been loaded
    // If true uses most recent blockDate as starting point for loading further blockDates
    let checkC = await mongoblock.checkCollectionExists(db, 'blockDates');
    if (checkC == true) {
        console.log('Blockdates previously loaded.');
        await collection.find({}).sort({timestamp:-1}).limit(1).toArray()
            .then(function(maxDate) {
                if(maxDate.length == 1) {
                    console.log('Latest blockdate loaded:', maxDate[0].blockNumber, maxDate[0].timestamp);
                    loopBlock = maxDate[0].blockNumber + (24*60*20);
                    loopDate = helperblock.forwardOneDay(maxDate[0].timestamp);
                    loopStartSet = true;
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
        loopBlock = loopBlock - helperblock.blocksToMidnight(startB.timestamp, loopDate);

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
        // If block is not first block revise estimate of first blockNumber based on 3 second blocks
        } else {
            console.log('...recalculating');
            counter += 1;
            attemptArray.push(loopBlock);
            // Workaround for when estimation process gets stuck
            if (counter % 5 == 0) {
                loopBlock = Math.round((attemptArray[attemptArray.length-1] + attemptArray[attemptArray.length-2])/2);
                console.log('moving to average of last two block numbers');
            // blocksToMidnight revises estimate based on 3 second blocks
            } else {
                loopBlock = loopBlock - helperblock.blocksToMidnight(firstB.timestamp, loopDate);
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
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);
    const collection = db.collection('blockDates');

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


    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    console.log(openBlock, closeBlock, parameterIssue);

    blocksToProcess = closeBlock - openBlock;
    console.log(openBlock, blocksToProcess);

    fiveBlock(openBlock);

    // Function to extract data of x blocks from the blockchain (originally five blocks)
    // ---------------------------------------------------------------------------------
    function fiveBlock(localOpenBlock) {
        let launchBlocks = Math.min(blocksPerRound, blocksToProcess - blocksStarted)
        console.log(launchBlocks, blocksToProcess - blocksStarted);
        for (var i = 0; i < launchBlocks; i+=1) {
            blockNo = localOpenBlock + i;
            console.log('----------------');
            console.log('started ' + blockNo);
            // Gets data for one block, processes it in callback "processOps"
            steemrequest.getOpsAppBase(blockNo, processOps);
            blocksStarted += 1;
            console.log('----------------');
        }
    }

    // Function to process block of operations
    // -----------------------------------------
    function processOps(error, response, body, localBlockNo) {
        if (!error && response.statusCode == 200) {
            try {
                let result = JSON.parse(body).result;
                for (let operation of result) {
                    // Extracts blockNumber just once for all operations
                    if (operation.trx_in_block == 0) {
                        blockProcessed = operation.block;
                        timestamp = operation.timestamp;
                    }
                    if (operation.op[0] == 'comment') {
                        mongoblock.processComment(operation, mongoblock.mongoComment, db);
                    } else {
                        // Operations not handled:
                        if (!unknownOperations.includes(operation.op[0])) {
                            unknownOperations.push(operation.op[0]);
                        }
                    }
                }
                // Add record of block processed to database
                let blockRecord = {blockNumber: blockProcessed, timestamp: timestamp, status: 'OK'};
                db.collection('blocksProcessed').insertOne(blockRecord, (error, results) => {
                    if(error) { if(error.code != 11000) {console.log(error);}}
                });

                blocksCompleted += 1;
                console.log('----------------');
                console.log('finished ' + blockProcessed + ' (' + blocksCompleted + ')' );
                console.log('----------------');

                if (blocksCompleted == blocksToProcess) {
                    let runTime = Date.now() - launchTime;
                    console.log('unknownOperations: ', unknownOperations);
                    console.log('End time: ' + (Date.now() - launchTime));
                    console.log('----------------');
                    console.log('REPORT');
                    console.log('Start date: ', openDate);
                    console.log('Blocks covered: ', openBlock + ' to ' + closeBlock);
                    console.log('Blocks processed: ' + blocksCompleted);
                    console.log('Error Count: ' + errorCount);
                    console.log('Average speed: ' + (runTime/blocksCompleted/1000).toFixed(4) + 's.');
                    console.log('db closing');
                    //mclient.close();


                } else if (blocksStarted - blocksCompleted < 5) {
                    fiveBlock(openBlock + blocksStarted);
                }
            } catch (error) {
                console.log(error);
            }
        } else {
            console.log('Most likely error is connection lost'); // to do: deal with checking which blocks loaded, reconnecting, and restarting loop.
            console.log(localBlockNo);
            let blockRecord = {blockNumber: localBlockNo, status: 'error'};
            db.collection('blocksProcessed').insertOne(blockRecord, (error, results) => {
                if(error) { if(error.code != 11000) {console.log(error);}}
            });
            blocksCompleted += 1;
            errorCount += 1;
        }
    }
// Closing fillOperations
}


// Function to convert parameters for a date range into blockNumbers
// -----------------------------------------------------------------
// Accepts two dates as parameters  or a date and a number
// Parameters are global and defined on command line
async function blockRangeDefinition(db) {

    let openBlock = 0, closeBlock = 0, parameterIssue = false;

    if (typeof parameter1 == 'string') {
        openDate = new Date(parameter1);
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
        closeDate = new Date(parameter2);
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


// Function to provide parameters for reportBlocksProcessed
// --------------------------------------------------------
async function reportBlocks() {
    client = await MongoClient.connect(url, { useNewUrlParser: true });
    console.log('Connected to server.');
    const db = client.db(dbName);

    let [openBlock, closeBlock, parameterIssue] = await blockRangeDefinition(db);
    if (parameterIssue == false) {
        mongoblock.reportBlocksProcessed(db, openBlock, closeBlock, 'return');
    } else {
        console.log('Parameter issue');
    }
}
