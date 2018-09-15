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
* Run setup using => $node blockOps setup

### Block.Ops functions
Once the blocknumber index has been created the following commands can be used for Block.Ops functionality:
* ... 




----------------------------------------------

## Road map:

### Historic Analysis
* 
