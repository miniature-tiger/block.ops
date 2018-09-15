// Time reporting for measuring speed of block parsing
// ---------------------------------------------------
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
