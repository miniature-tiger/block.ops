# Block.Ops
A steem blockchain block operations analysis tool.

## To install locally

### Required packages
Block.Ops requires Node.js, npm, and MongoDB. If you do not already have these packages installed you can read more about their installation here:
* Node.js and npm: https://nodejs.org/en/
* MongoDB: https://www.mongodb.com/download-center#production

Production testing used Node.js v10.9.0 (this 'Latest Features' version is required) and MongoDB version v4.0.0. Run on macOS High Sierra.

### Block.Ops files
* Download all files to your local computer using the "Clone of download" ---> "Download ZIP" buttons on the block.ops repository of github.
* Move the downloaded files to the directory of your choice.

### npm module installations
Block.Ops requires several npm modules. Set up package.json in your chosen directory and install the following npm modules: 
* request and request-promise-native (for API calls to the steem blockchain): 
https://www.npmjs.com/package/request-promise-native
versions used: request@2.88.0 and request-promise-native@1.0.5
* mongodb (the official MongoDB driver for Node.js):
https://www.npmjs.com/package/mongodb
version used: mongodb@3.1.4

### Set-up Block.Ops blocknumber index
Block.Ops is run with node.js via the command line. 
* Launch MongoDB.
* Run setup using => $ node blockOps setup
* Test succesful setup of blockDates list => $ node blockOps checkBlockDates

### Block.Ops functions
Once the blocknumber index has been created the following commands can be used for Block.Ops functionality:
$ node blockOps ...

* filloperations <date> <date or number of blocks> 
  e.g. $ node blockOps filloperations "2018-09-03" 10
  Runs through loop of blocks (starting from first block of first date parameter) to analyse operations and add to MongoDB
  
* reportblocks <date> <date or number of blocks> 
  e.g. $ node blockOps reportblocks "2018-09-02" 1200
  Reports on status of blocks processed for date range 
  
* reportcomments  
  Reports on post numbers per application (currently for all blocks processed)
  
* remove <nameOfCollectionToRemove>
  Removes all records from a collection (handle with care!)

----------------------------------------------

## Road map:

### Short term
* Add date range to reportcomments
* Include chosen operations in filloperations to allow marketshare analysis to be run 
  (needs author numbers and author, curator and benefactor payout numbers)
* Consider other operations to be analysed from the list below:
  
  Definitely need:
  'author_reward',
  'curation_reward',
  'comment_benefactor_reward',
  
  Likely to include:
  'vote',
  'custom_json',
  
  For consideration:
  
  'transfer',
  'claim_reward_balance',
  'transfer_to_vesting',
  'transfer_to_savings',
  'fill_transfer_from_savings',
  
  'producer_reward',
  'account_witness_vote',
  'account_witness_proxy',

  'return_vesting_delegation',
  'delegate_vesting_shares',
  
  'fill_vesting_withdraw',
  'withdraw_vesting',
  
  'feed_publish',
  'comment_options',
  'account_update',
  
  'limit_order_cancel',
  'limit_order_create',
  'fill_order',
  
  'account_create',
  'account_create_with_delegation',
  
  'delete_comment'
  

### To be completed
