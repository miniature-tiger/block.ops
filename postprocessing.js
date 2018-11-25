const Json2csvParser = require('json2csv').Parser;
const fs = require('fs');



// Postprocessing of marketshare report based on data extracted from MongoDB
// -------------------------------------------------------------------------
function marketShareProcessing(createdSummary, payoutSummary) {
    let combinedSummary = [];
    let blankPayoutRecord = {
                  author_payout_sbd: 0, author_payout_steem: 0, author_payout_vests: 0, benefactor_payout_vests: 0, curator_payout_vests: 0,
                  author_payout_sbd_STU: 0, author_payout_steem_STU: 0, author_payout_vests_STU: 0, benefactor_payout_vests_STU: 0, curator_payout_vests_STU: 0,
                    };
    let otherData = Object.assign({application: 'other', authors: 0, posts: 0}, blankPayoutRecord);

    for (var i = 0; i < createdSummary.length; i+=1) {
        let payoutMatch = -1;
        let combinedRecord = {};
        for (var j = 0; j < payoutSummary.length; j+=1) {
            if ( createdSummary[i]._id.application == payoutSummary[j]._id.application ) {
                payoutMatch = j;
                payoutSummary[j].match = true;
            }
        }
        if (payoutMatch != -1) {
            combinedRecord =  { application: createdSummary[i]._id.application,
                                authors: createdSummary[i].authors,
                                posts: createdSummary[i].posts,
                                author_payout_sbd: Number(payoutSummary[payoutMatch].author_payout_sbd.toFixed(3)),
                                author_payout_steem: Number(payoutSummary[payoutMatch].author_payout_steem.toFixed(3)),
                                author_payout_vests: Number(payoutSummary[payoutMatch].author_payout_vests.toFixed(3)),
                                benefactor_payout_sbd: Number(payoutSummary[payoutMatch].benefactor_payout_sbd.toFixed(3)),
                                benefactor_payout_steem: Number(payoutSummary[payoutMatch].benefactor_payout_steem.toFixed(3)),
                                benefactor_payout_vests: Number(payoutSummary[payoutMatch].benefactor_payout_vests.toFixed(3)),
                                curator_payout_vests: Number(payoutSummary[payoutMatch].curator_payout_vests.toFixed(3)),
                                author_payout_sbd_STU: Number(payoutSummary[payoutMatch].author_payout_sbd_STU.toFixed(3)),
                                author_payout_steem_STU: Number(payoutSummary[payoutMatch].author_payout_steem_STU.toFixed(3)),
                                author_payout_vests_STU: Number(payoutSummary[payoutMatch].author_payout_vests_STU.toFixed(3)),
                                benefactor_payout_sbd_STU: Number(payoutSummary[payoutMatch].benefactor_payout_sbd_STU.toFixed(3)),
                                benefactor_payout_steem_STU: Number(payoutSummary[payoutMatch].benefactor_payout_steem_STU.toFixed(3)),
                                benefactor_payout_vests_STU: Number(payoutSummary[payoutMatch].benefactor_payout_vests_STU.toFixed(3)),
                                curator_payout_vests_STU: Number(payoutSummary[payoutMatch].curator_payout_vests_STU.toFixed(3)),
                              }
        } else {
            combinedRecord = Object.assign({ application: createdSummary[i]._id.application,
                                authors: createdSummary[i].authors,
                                posts: createdSummary[i].posts,
                              }, blankPayoutRecord);
        }
        combinedSummary.push(combinedRecord);
    }

    for (let app of payoutSummary) {
        if (!(app.hasOwnProperty('match'))) {
            let combinedRecord = {  application: app._id.application,
                                    authors: 0,
                                    posts: 0,
                                    author_payout_sbd: Number(app.author_payout_sbd.toFixed(3)),
                                    author_payout_steem: Number(app.author_payout_steem.toFixed(3)),
                                    author_payout_vests: Number(app.author_payout_vests.toFixed(3)),
                                    benefactor_payout_sbd: Number(app.benefactor_payout_sbd.toFixed(3)),
                                    benefactor_payout_steem: Number(app.benefactor_payout_steem.toFixed(3)),
                                    benefactor_payout_vests: Number(app.benefactor_payout_vests.toFixed(3)),
                                    curator_payout_vests: Number(app.curator_payout_vests.toFixed(3)),
                                    author_payout_sbd_STU: Number(app.author_payout_sbd_STU.toFixed(3)),
                                    author_payout_steem_STU: Number(app.author_payout_steem_STU.toFixed(3)),
                                    author_payout_vests_STU: Number(app.author_payout_vests_STU.toFixed(3)),
                                    benefactor_payout_sbd_STU: Number(app.benefactor_payout_sbd_STU.toFixed(3)),
                                    benefactor_payout_steem_STU: Number(app.benefactor_payout_steem_STU.toFixed(3)),
                                    benefactor_payout_vests_STU: Number(app.benefactor_payout_vests_STU.toFixed(3)),
                                    curator_payout_vests_STU: Number(app.curator_payout_vests_STU.toFixed(3)),
                                  }
            combinedSummary.push(combinedRecord);
        }
    }

    for (var k = combinedSummary.length - 1; k > -1 ; k-=1) {
        if (combinedSummary[k].application == '' || combinedSummary[k].application == ' ' || combinedSummary[k].application == null || combinedSummary[k].application == "null" || combinedSummary[k].application == "other") {
            otherData.authors += combinedSummary[k].authors;
            otherData.posts += combinedSummary[k].posts;
            otherData.author_payout_sbd += combinedSummary[k].author_payout_sbd;
            otherData.author_payout_steem += combinedSummary[k].author_payout_steem;
            otherData.author_payout_vests += combinedSummary[k].author_payout_vests;
            otherData.benefactor_payout_vests += combinedSummary[k].benefactor_payout_vests;
            otherData.curator_payout_vests += combinedSummary[k].curator_payout_vests;
            otherData.author_payout_sbd_STU += combinedSummary[k].author_payout_sbd_STU;
            otherData.author_payout_steem_STU += combinedSummary[k].author_payout_steem_STU;
            otherData.author_payout_vests_STU += combinedSummary[k].author_payout_vests_STU;
            otherData.benefactor_payout_vests_STU += combinedSummary[k].benefactor_payout_vests_STU;
            otherData.curator_payout_vests_STU += combinedSummary[k].curator_payout_vests_STU;
            combinedSummary.splice(k,1);
        }
    }

    ranking('authors', 'Desc');
    ranking('posts', 'Desc' );

    combinedSummary.push(otherData);
    for (var l = 0; l < combinedSummary.length; l+=1) {
        console.log(combinedSummary[l])
    }
    return combinedSummary;

    function ranking(rankItem, rankOrder) {
        let rankingArray = [];
        let sortedArray = [];
        for (var m = 0; m < combinedSummary.length; m+=1) {
            rankingArray.push(combinedSummary[m][rankItem]);
        }
        if (rankOrder == 'Desc') {
            sortedArray = rankingArray.slice().sort(function(a,b){return b-a});
        } else {
            sortedArray = rankingArray.slice().sort(function(a,b){return a-b});
        }
        let finalRanks = rankingArray.slice().map(function(c){return sortedArray.indexOf(c)+1});
        let outputItem = rankItem + 'Rank'
        for (var n = 0; n < combinedSummary.length; n+=1) {
            combinedSummary[n][outputItem] = finalRanks[n];
        }
        return finalRanks;
    }
}

module.exports.marketShareProcessing = marketShareProcessing;



// Postprocessing of marketshare report based on data extracted from MongoDB
// -------------------------------------------------------------------------
function productionStatsByDayProcessing(summary) {

    for (let day of summary) {
          day.date = day._id.dateDay;
          delete day._id;
          day.author_payout_sbd = Number(day.author_payout_sbd.toFixed(3));
          day.author_payout_steem = Number(day.author_payout_steem.toFixed(3));
          day.author_payout_vests = Number(day.author_payout_vests.toFixed(3));
          day.benefactor_payout_sbd = Number(day.author_payout_sbd.toFixed(3));
          day.benefactor_payout_steem = Number(day.author_payout_steem.toFixed(3));
          day.benefactor_payout_vests = Number(day.benefactor_payout_vests.toFixed(3));
          day.curator_payout_vests = Number(day.curator_payout_vests.toFixed(3));
          day.author_payout_sbd_STU = Number(day.author_payout_sbd_STU.toFixed(3));
          day.author_payout_steem_STU = Number(day.author_payout_steem_STU.toFixed(3));
          day.author_payout_vests_STU = Number(day.author_payout_vests_STU.toFixed(3));
          day.benefactor_payout_sbd_STU = Number(day.benefactor_payout_sbd_STU.toFixed(3));
          day.benefactor_payout_steem_STU = Number(day.benefactor_payout_steem_STU.toFixed(3));
          day.benefactor_payout_vests_STU = Number(day.benefactor_payout_vests_STU.toFixed(3));
          day.curator_payout_vests_STU = Number(day.curator_payout_vests_STU.toFixed(3));
    }

    return summary;

}

module.exports.productionStatsByDayProcessing = productionStatsByDayProcessing;



// Export of data into csv file
// ----------------------------
function dataExport(localData, localFileName, fields) {
    console.log('--------- Exporting Data : ' + localFileName + '---------')
    let json2csvParser = new Json2csvParser({fields});
    let csvExport = json2csvParser.parse(localData);
    fs.writeFile(localFileName + '.csv', csvExport, function (error) {
        if (error) throw error;
    });
    return;
}

module.exports.dataExport = dataExport;



// Remove separate _id object from Mongo output array
// --------------------------------------------------
function tidyID(arrayOutput) {
    let result = [];
    for (let i = 0; i < arrayOutput.length; i+=1) {
        let entry = arrayOutput[i]._id;
        Object.keys(arrayOutput[i]).forEach(function(field) {
            if (field != '_id') {
                entry[field] = arrayOutput[i][field];
            }
        });
        result.push(entry)
    }
    return result;
}

module.exports.tidyID = tidyID;


// Combine earnings list inputs into single list
// ---------------------------------------------
function combineByUser(authorInput, curatorInput, benefactorInput) {
    for (let authorRecord of authorInput) {
        authorRecord["voteCount"] = 0;
        authorRecord["curator_payout_STU"] = 0;
        authorRecord["benefactorCount"] = 0;
        authorRecord["benefactor_payout_STU"] = 0;
        authorRecord["total_payout_STU"] = authorRecord.author_payout_STU;
    }

    for (let curatorRecord of curatorInput) {
        let authorRecord = authorInput.find(author => author.user == curatorRecord.user)
        if (authorRecord == undefined) {
            curatorRecord["postCount"] = 0;
            curatorRecord["author_payout_STU"] = 0;
            curatorRecord["benefactorCount"] = 0;
            curatorRecord["benefactor_payout_STU"] = 0;
            curatorRecord["total_payout_STU"] = curatorRecord.curator_payout_STU;
            authorInput.push(curatorRecord);
        } else {
            authorRecord["voteCount"] = curatorRecord.voteCount;
            authorRecord["curator_payout_STU"] = curatorRecord.curator_payout_STU;
            authorRecord["total_payout_STU"] += curatorRecord.curator_payout_STU;
        }
    }

    for (let benefactorRecord of benefactorInput) {
        let authorRecord = authorInput.find(author => author.user == benefactorRecord.user)
        if (authorRecord == undefined) {
            benefactorRecord["postCount"] = 0;
            benefactorRecord["author_payout_STU"] = 0;
            benefactorRecord["voteCount"] = 0;
            benefactorRecord["curator_payout_STU"] = 0;
            benefactorRecord["total_payout_STU"] = benefactorRecord.benefactor_payout_STU;
            authorInput.push(benefactorRecord);
        } else {
            authorRecord["benefactorCount"] = benefactorRecord.benefactorCount;
            authorRecord["benefactor_payout_STU"] = benefactorRecord.benefactor_payout_STU;
            authorRecord["total_payout_STU"] += benefactorRecord.benefactor_payout_STU;
        }
    }
    return authorInput;
}

module.exports.combineByUser = combineByUser;



// Convert earnings list into earnings distribution
// ------------------------------------------------
function earningsDistribution(earningsList, bucketSize, maxBucketSize, aggregateKey) {
    //let boundaryArray = [0, 0.0001];
    let boundaryArray = [];
    let entry = {earnings: 0, userCount: 0};
    let numberOfBuckets = maxBucketSize / bucketSize;
    let catchMax = 10000000;

    Object.keys(earningsList[0]).forEach(function(field) {
        if (field != 'user') {
            entry[field] = 0;
        }
    });

    for (let i = 0; i < numberOfBuckets + 1; i+=1) {
        entry['earnings'] = i * bucketSize
        boundaryArray.push(Object.assign({}, entry));
    }
    entry['earnings'] = catchMax;
    boundaryArray.push(Object.assign({}, entry));

     for (let j = 0; j < earningsList.length; j+=1) {
        for (let k = 0; k < boundaryArray.length-1; k+=1) {
            if (earningsList[j][aggregateKey] >= boundaryArray[k].earnings && earningsList[j][aggregateKey] < boundaryArray[k+1].earnings) {
                boundaryArray[k]['userCount'] += 1;
                Object.keys(earningsList[j]).forEach(function(field) {
                    if (field != 'user') {
                        boundaryArray[k][field] += earningsList[j][field];
                    }
                });
            }
        }
    }
    boundaryArray.splice(boundaryArray.length-1, 1);

    return boundaryArray;
}

module.exports.earningsDistribution = earningsDistribution;
