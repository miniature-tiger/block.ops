
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

module.exports.checkSameDay = checkSameDay;



// Function returns a UTC date one day forward in time
// ---------------------------------------------------
function forwardOneDay(localDate) {
    let result = new Date(localDate.getTime());
    result.setUTCDate(localDate.getUTCDate()+1);
    return result;
}

module.exports.forwardOneDay = forwardOneDay;



// Function calculates the number of 3 second blocks between the firstDate and Midnight of the secondDate
// ------------------------------------------------------------------------------------------------------
function blocksToMidnight(firstDate, secondDate, secondsPerBlock) {
    let secondDateMidnight = new Date(Date.UTC(secondDate.getUTCFullYear(), secondDate.getUTCMonth(), secondDate.getUTCDate()));
    let result = Math.round((firstDate - secondDateMidnight)/1000/secondsPerBlock);
    return result;
}

module.exports.blocksToMidnight = blocksToMidnight;



// Function translates date into new date based on date-and-hour only (i.e. no minutes or seconds)
// ------------------------------------------------------------------------------------------------------
function UTCDateHourOnly(localDate) {
    let hourOnlyDate = new Date(Date.UTC(localDate.getUTCFullYear(), localDate.getUTCMonth(), localDate.getUTCDate(), localDate.getUTCHours()));
    return hourOnlyDate;
}

module.exports.UTCDateHourOnly = UTCDateHourOnly;
