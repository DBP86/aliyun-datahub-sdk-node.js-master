var Rest = require('./rest').Rest
var path = require('./rest').path
var aliyunAccount = require('./auth')
var _ = require('lodash')

var DataHub= function (accessId, accessKey, endPoint, args) {
  args || (args = {})
  securityToken = args.securityToken || ''
  this.account = new aliyunAccount(accessId, accessKey, securityToken)
  this.endPoint = endPoint
  this.rest = new Rest(this.account, this.endPoint, args)
}

DataHub.prototype.createProject = function (projectName, params, callback) {
  var urn = path.project(projectName)
  var requestParams = _.pick(params, ['Comment'])

  this.rest.post(urn, requestParams, callback)
}

DataHub.prototype.listProject = function (callback) {
  var urn = path.projects()

  this.rest.get(urn, callback)
}

DataHub.prototype.getProject = function (projectName, callback) {
  var urn = path.project(projectName)

  this.rest.get(urn, callback)
}

DataHub.prototype.updateProject = function (projectName, params, callback) {
  var urn = path.project(projectName)
  var requestParams = _.pick(params, ['Comment'])

  this.rest.put(urn, requestParams, callback)
}

DataHub.prototype.deleteProject = function (projectName, callback) {
  var urn = path.project(projectName)

  this.rest.delete(urn, callback)
}

DataHub.prototype.createTopic = function (projectName, topicName, params, callback) {
  var urn = path.topic(projectName, topicName)
  var requestParams = _.pick(params, ['Action', 'ShardCount', 'Lifecycle', 'RecordType', 'RecordSchema', 'Comment'])

  this.rest.post(urn, requestParams, callback)
}

DataHub.prototype.getTopic = function (projectName, topicName, callback) {
  var urn = path.topic(projectName, topicName)

  this.rest.get(urn, callback)
}

DataHub.prototype.listTopic = function (projectName, callback) {
  var urn = path.topics(projectName)

  this.rest.get(urn,callback)
}

DataHub.prototype.updateTopic = function (projectName, topicName, params, callback) {
  var urn = path.topic(projectName, topicName)
  var requestParams = _.pick(params, ['Comment'])

  this.rest.put(urn, requestParams, callback)
}

DataHub.prototype.deleteTopic = function (projectName, topicName, callback) {
  var urn = path.topic(projectName, topicName)

  this.rest.delete(urn, callback)
}

DataHub.prototype.listShard = function (projectName, topicName, callback) {
  var urn = path.shards(projectName, topicName)

  this.rest.get(urn, callback)
}

DataHub.prototype.splitShard = function (projectName, topicName, params, callback) {
  var urn = path.shards(projectName, topicName)
  var requestParams = _.pick(params, ['Action', 'ShardId', 'SplitKey'])

  this.rest.post(urn, requestParams, callback)
}

DataHub.prototype.mergeShard = function (projectName, topicName, params, callback) {
  var urn = path.shards(projectName, topicName)
  var requestParams = _.pick(params, ['Action', 'ShardId', 'AdjacentShardId'])

  this.rest.post(urn, requestParams, callback)
}

DataHub.prototype.getCursor = function (projectName, topicName, shardId, params, callback) {
  var urn = path.shard(projectName, topicName, shardId)
  var params = _.pick(params, ['Action', 'Type', 'SystemTime', 'Sequence'])
  var requestParams = {
    Action: params.Action || 'cursor',
    Type: params.Type || 'LATEST'
  }

  if (params.Type === 'SYSTEM_TIME') requestParams.SystemTime = params.SystemTime || new Date().getTime()
  if (params.Type === 'SEQUENCE') requestParams.Sequence = params.Sequence
  
  this.rest.post(urn, requestParams, callback)
}

DataHub.prototype.putTupleRecord = function (projectName, topicName, params, callback) {
  var urn = path.shards(projectName, topicName)
  var recordsParams = _.pick(params, ['Action', 'ShardId', 'Attributes', 'Data'])
  var requestParams = {
    Action: params.Action || 'pub',
    Records: [{
      ShardId: recordsParams.ShardId || '0',
      Attributes: recordsParams.Attributes || {a: 'b'},
      Data: recordsParams.Data
    }]
  }

  this.rest.post(urn, requestParams, callback)
}

DataHub.prototype.putBlobRecord = function (projectName, topicName, params, callback) {
  var urn = path.shards(projectName, topicName)
  var recordParams = _.pick(params, ['Action', 'ShardId', 'Attributes', 'Data'])
  var requestParams = {
    Action: params.Action || 'pub',
    Records: [{
      ShardId: recordParams.ShardId || '0',
      Attributes: recordParams.Attributes || {a: 'b'},
      Data: Buffer.from(recordParams.Data).toString('base64')
    }]
  }

  this.rest.post(urn, requestParams, callback)
}

DataHub.prototype.getRecords = function (projectName, topicName, shardId, params, callback) {
  var urn = path.shard(projectName, topicName, shardId)
  var requestParams = _.pick(params, ['Cursor', 'Limit'])
  requestParams.Action = 'sub'
  this.rest.post(urn, requestParams, callback)
}

DataHub.prototype.createField = function (projectName, topicName, params, callback) {
  var urn = path.topic(projectName, topicName)
  var requestParams = _.pick(params, ['Action', 'FieldName', 'FieldType'])

  this.rest.post(urn, requestParams, callback)
}
module.exports = DataHub