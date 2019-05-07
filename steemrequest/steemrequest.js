const request = require('request');
const requestPromise = require('request-promise-native');

let url20 = 'https://api.steemit.com'



// Function returns global properties including latest block number (uses condenser_api.get_dynamic_global_properties)
// -------------------------------------------------------------------------------------------------------------------
// < Returns a promise >
function getDynamicGlobalPropertiesAppBase() {
    let latestBlock = {};

    let dataString = '{"jsonrpc":"2.0", "method":"condenser_api.get_dynamic_global_properties", "params":[], "id":1}';
    let options = {
        url: url20,
        method: 'POST',
        body: dataString,
    }

    return requestPromise(options)
        .catch(function(error) {
            console.log(error);
        });
}

module.exports.getDynamicGlobalPropertiesAppBase = getDynamicGlobalPropertiesAppBase;




// Function extracts blockNumber and timestamp from global properties (obtained using getDynamicGlobalPropertiesAppBase)
// ---------------------------------------------------------------------------------------------------------------------
function processLatestBlockNumber(body) {
    let result = JSON.parse(body).result;
    let blockDate = new Date(result.time + '.000Z')
    let record = {blockNumber: result.head_block_number, timestamp: blockDate}
    return record;
}

module.exports.processLatestBlockNumber = processLatestBlockNumber;



// Function obtains latest block number using two functions above
// --------------------------------------------------------------
module.exports.getLatestBlockNumber = async function () {
    let body = await getDynamicGlobalPropertiesAppBase();
    let latestBlock = processLatestBlockNumber(body);
    return latestBlock;
}



// Function returns a single block header from AppBase (uses condenser_api.get_block_header)
// -----------------------------------------------------------------------------------------
// < Returns a promise >
function getBlockHeaderAppBase(localBlockNo) {
    let headerRecord = {};

    let dataString = '{"jsonrpc":"2.0", "method":"condenser_api.get_block_header", "params": [' + localBlockNo + '], "id":1}';
    let options = {
        url: url20,
        method: 'POST',
        body: dataString
    }

    return requestPromise(options)
        .catch(function(error) {
            console.log(error);
        });
}

module.exports.getBlockHeaderAppBase = getBlockHeaderAppBase;



// Function extracts timestamp from block header and returns a blockNumber / UTC timestamp record
// ------------------------------------------------------------------------------------------
function processBlockHeader(body, blockNumber) {
    let result = JSON.parse(body).result;
    let blockDate = new Date(result.timestamp + '.000Z')
    let record = {blockNumber: blockNumber, timestamp: blockDate};
    return record;
}

module.exports.processBlockHeader = processBlockHeader;



// Function checks whether a steem block is the first block of the UTC day
// -----------------------------------------------------------------------
// The check compares the chosen block header information to the immediately prior block header information, checking:
// (1) Whether the chosen block is actually on the correct UTC date;
// (2) Whether the immediately prior block is on the previous UTC date.
async function checkFirstBlock(blockNo, date) {
    let result = false;
    let bodyFirst = await getBlockHeaderAppBase(blockNo);
    let blockRecordFirst = processBlockHeader(bodyFirst, blockNo);

    // workaround for first block - known to be the first block of the day
    if (blockNo == 1) {
        return {check: true, timestamp: blockRecordFirst.timestamp};
    }

    let bodyPrior = await getBlockHeaderAppBase(blockNo-1);
    let blockRecordPrior = processBlockHeader(bodyPrior, blockNo-1);

    if (checkSameDay(blockRecordFirst.timestamp, date) == true && checkSameDay(forwardOneDay(blockRecordPrior.timestamp), date) == true) {
        result = {check: true, timestamp: blockRecordFirst.timestamp};
    } else {
        result = {check: false, timestamp: blockRecordFirst.timestamp};
    }

    return result;
}

module.exports.checkFirstBlock = checkFirstBlock;



// Function returns a single block from AppBase with all operations including virtual operations (uses condenser_api.get_ops_in_block)
// -----------------------------------------------------------------------------------------------------------------------------------
function getBlockAppBase(localBlockNo, processBlock) {
    dataString = '{"jsonrpc":"2.0", "method":"condenser_api.get_block", "params": [' + localBlockNo + '], "id":1}';
    let options = {
        url: url20,
        method: 'POST',
        body: dataString
    }
    request(options, function(error, response, body) {
        processBlock(error, response, body, localBlockNo);
    });
}

module.exports.getBlockAppBase = getBlockAppBase;



// Function returns a single block from AppBase with only virtual operations (uses condenser_api.get_ops_in_block with true for virtual only)
// ------------------------------------------------------------------------------------------------------------------------------------------
function getOpsAppBase(localBlockNo, processOps) {
    dataString = '{"jsonrpc":"2.0", "method":"condenser_api.get_ops_in_block", "params": [' + localBlockNo + ', false], "id":1}';
    let options = {
        url: url20,
        method: 'POST',
        body: dataString,
        timeout: 10000
    }
    //console.log('getOpsAppBase localBlockNo', localBlockNo);
    request(options, function(error, response, body) {
        processOps(error, response, body, localBlockNo);
    });
}

module.exports.getOpsAppBase = getOpsAppBase;



// Function returns active votes from a single author / premlink defined comment
// -----------------------------------------------------------------------------
// < Returns a promise >
function getActiveVotes(localAuthor, localPermlink) {
    dataString = '{"jsonrpc":"2.0", "method":"condenser_api.get_active_votes", "params":["' + localAuthor + '", "' + localPermlink + '"], "id":1}';
    let options = {
        url: url20,
        method: 'POST',
        body: dataString
    }

    return requestPromise(options)
        .catch(function(error) {
            console.log('Error in steemrequest.getActiveVotes:', 'Error name:',  error.name, 'Error message:', error.message, 'API params:', error.options.body);
        });
}

module.exports.getActiveVotes = getActiveVotes;



// Function returns comment from a single author / premlink combination
// --------------------------------------------------------------------
// < Returns a promise >
function getComment(localAuthor, localPermlink) {
    dataString = '{"jsonrpc":"2.0", "method":"condenser_api.get_content", "params":["' + localAuthor + '", "' + localPermlink + '"], "id":1}';
    let options = {
        url: url20,
        method: 'POST',
        body: dataString
    }

    return requestPromise(options)
        .catch(function(error) {
            console.log(error);
        });
}

module.exports.getComment = getComment;



// Function checks whether two UTC dates are on the same day
// ---------------------------------------------------------
// < Returns a boolean >
function checkSameDay(firstDate, secondDate) {
    let result = true;
    if (firstDate.getUTCFullYear() != secondDate.getUTCFullYear()) {
        result = false;
    }
    if (firstDate.getUTCMonth() != secondDate.getUTCMonth()) {
        result = false;
    }
    if (firstDate.getUTCDate() != secondDate.getUTCDate()) {
        result = false;
    }
    return result;
}



// Function returns a UTC date one day forward in time
// ---------------------------------------------------
function forwardOneDay(localDate) {
    let result = new Date(localDate.getTime());
    result.setUTCDate(localDate.getUTCDate()+1);
    return result;
}
