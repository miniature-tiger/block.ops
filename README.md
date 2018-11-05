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

##### Processing

Once the blocknumber index has been created the following commands can be used for Block.Ops functionality:
$ node blockOps ...

* filloperations < date or block number > < date or number of blocks > 

  e.g. $ node blockOps filloperations "2018-09-03" 10   --->   processes the first 10 blocks of 3rd September
  e.g. $ node blockOps filloperations 20000000 10   --->   processes 10 blocks starting with block number 20million
  
Runs through loop of blocks (starting from first block of first date parameter) to analyse operations and add the data to MongoDB.

* patchoperations 

  A specific fix for picking up virtual operation of block 26038153 from the long blockchain stoppage on Sep 17 2018.

* remove < nameOfCollectionToRemove >
  
Removes all records from a collection **(handle with care!)**.

##### Checking blocks loaded

Once blocks have been processed the following commands can be used to check the status of those blocks:
$ node blockOps ...

* reportblocks < date or block number > < date or number of blocks > 
  
  e.g. $ node blockOps reportblocks "2018-09-02" "2018-09-04"
  
Reports on status of blocks processed for date range. Blocks can have three different statuses: 'OK', error', or 'processing' (the latter means that the block failed to finish adding all the operations and complete validation).

Blocks with 'error' / 'processing' status can be picked up and rerun simply by rerunning the range of blocks previously processed ('OK' status blocks are not rerun). Typically around 1% of blocks will fail to process correctly simply due to API call failures. 1 or 2 reruns to pick up these blocks can be processed very quickly.

* validate < date or block number > < date or number of blocks > 

Runs a number of consistency tests on the loaded blocks.
  
* showblock < block number > 

Shows the blocksProcessed status report for a single block.
  

##### Analyses

* reportcomments < date or block number > < date or number of blocks >  
  
Reports on post numbers per application (currently for all blocks processed)
  
* findcurator < date or block number > < date or number of blocks > < user >
  
Reports on ratio of vests to rshares (i.e. vote payout to vote size) and finds highest ratios (i.e. best curation reward).
Adding a user name returns only those votes from the individual user.

----------------------------------------------

## Road map: (still fluid!)

1) Complete processing of desired operations - First stage complete - All operations and virtual operation related to comments, votes, rewards, transfers, delegation, follows, reblogs, account creation, power ups/downs included. 
2) Add a user-friendly front-end
3) Add charts
4) Realtime / forward processing


#### Future consideration

* Consider other operations to be analysed from the list below: 
  'claim_reward_balance',
  
  'transfer_to_savings',
  'transfer_from_savings',
  'fill_transfer_from_savings', // virtual operation that shows completion of transfers to / from savings after 3 days 
  'cancel_transfer_from_savings'
  
  'set_withdraw_vesting_route',
  'return_vesting_delegation',
  
  'producer_reward',
  'account_witness_vote',
  'account_witness_proxy',
  'witness_set_properties',
  'witness_update',
  
  'feed_publish',
  'comment_options',
  'account_update',
  
  'limit_order_cancel',
  'limit_order_create',
  'limit_order_create2',
  'fill_order',
  
  'convert',
  'fill_convert_request',
  
  'delete_comment'
  
  'request_account_recovery',
  'recover_account'
  

### To be completed
