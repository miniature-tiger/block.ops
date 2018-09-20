const Json2csvParser = require('json2csv').Parser;
const fs = require('fs');

// Postprocessing of marketshare report based on data extracted from MongoDB
// -------------------------------------------------------------------------
function marketShareProcessing(summary) {
    otherData = {authors: 0, posts: 0, author_payout_sbd: 0, author_payout_steem: 0, author_payout_vests: 0, benefactor_payout_vests: 0, curator_payout_vests: 0, application: 'other'};
    for (var i = summary.length - 1; i > -1 ; i-=1) {
        summary[i].application = summary[i]._id.application;
        delete summary[i]._id;
        summary[i].author_payout_sbd = Number(summary[i].author_payout_sbd.toFixed(3));
        summary[i].author_payout_steem = Number(summary[i].author_payout_steem.toFixed(3));
        summary[i].author_payout_vests = Number(summary[i].author_payout_vests.toFixed(6));
        summary[i].benefactor_payout_vests = Number(summary[i].benefactor_payout_vests.toFixed(6));
        summary[i].curator_payout_vests = Number(summary[i].curator_payout_vests.toFixed(6));

        if (summary[i].application == '' || summary[i].application == ' ' || summary[i].application == null || summary[i].application == "null") {
            otherData.authors += summary[i].authors;
            otherData.posts += summary[i].posts;
            otherData.author_payout_sbd += summary[i].author_payout_sbd;
            otherData.author_payout_steem += summary[i].author_payout_steem;
            otherData.author_payout_vests += summary[i].author_payout_vests;
            otherData.benefactor_payout_vests += summary[i].benefactor_payout_vests;
            otherData.curator_payout_vests += summary[i].curator_payout_vests;
            summary.splice(i,1);
        }
    }

    ranking('authors', 'Desc');
    ranking('posts', 'Desc' );

    summary.push(otherData);
    for (var i = 0; i < summary.length; i+=1) {
        console.log(summary[i])
    }
    return summary;

    function ranking(rankItem, rankOrder) {
            let rankingArray = [];
            let sortedArray = [];
            for (var j = 0; j < summary.length; j+=1) {
                rankingArray.push(summary[j][rankItem]);
            }
            if (rankOrder == 'Desc') {
                sortedArray = rankingArray.slice().sort(function(a,b){return b-a});
            } else {
                sortedArray = rankingArray.slice().sort(function(a,b){return a-b});
            }
            let finalRanks = rankingArray.slice().map(function(c){return sortedArray.indexOf(c)+1});
            let outputItem = rankItem + 'Rank'
            for (var k = 0; k < summary.length; k+=1) {
                summary[k][outputItem] = finalRanks[k];
            }
            return finalRanks;
        }
}

module.exports.marketShareProcessing = marketShareProcessing;



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
