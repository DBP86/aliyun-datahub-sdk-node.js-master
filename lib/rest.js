var clientVersion = require('./version').clientVersion
var request = require('request')

var HTTPMethod = {
  GET: 'GET',
  POST: 'POST',
  PUT: 'PUT',
  HEAD: 'HEAD',
  DELETE: 'DELETE'
}

var contentType = {
  HTTPJson: 'application/json',
  HTTPProtobuf: 'application/x-protobuf'
}

var commonHeaders = {
  acceptEncoding: 'Accept-Encoding',
  Authorization: 'Authorization',
  cacheControl: 'Cache-Control',
  chunked: 'chunked',
  clientVersion: 'x-datahub-client-version',
  contentDisposition: 'Content-Disposition',
  contentEncoding: 'Content-Encoding',
  contentLength: 'Content-Length',
  contentMD5: 'Content-MD5',
  contentType: 'contentType',
  date: 'Date',
  ETag: 'ETag',
  expires: 'Expires',
  host: 'Host',
  lastModified: 'Last-Modified',
  location: 'Location',
  range: 'Range',
  rawSize: 'x-datahub-content-raw-size',
  requestAction: 'x-datahub-request-action',
  requestId: 'x-datahub-request-id',
  securityToken: 'x-datahub-security-token',
  transferEncoding: 'Transfer-Encoding',
  userAgent: 'User-Agent'
}

var path = {
  projects: function () {
    return '/projects'
  },
  project: function (projectName) {
    return '/projects/' + projectName
  },
  topics: function (projectName) {
    return '/projects/' + projectName + '/topics'
  },
  topic: function (projectName, topicName) {
    return '/projects/' + projectName + '/topics/' + topicName
  },
  shards: function (projectName, topicName) {
    return '/projects/' + projectName + '/topics/' + topicName + '/shards'
  },
  shard: function (projectName, topicName, shardId) {
    return '/projects/' + projectName + '/topics/' + topicName + '/shards/' + shardId
  },
  connectors: function (projectName, topicName) {
    return '/projects/' + projectName + '/topics/' + topicName + '/connectors'
  },
  connector: function (projectName, topicName, ConnectorType) {
    return '/projects/' + projectName + '/topics/' + topicName + '/connectors/' + ConnectorType
  },
  done_time: function (projectName, topicName, ConnectorType) {
    return '/projects/' + projectName + '/topics/' + topicName + '/connectors/' + ConnectorType + '?donetime'
  },
  subscriptions: function (projectName, topicName) {
    return '/projects/' + projectName + '/topics/' + topicName + '/subscriptions'
  },
  subscription: function (projectName, topicName, subscriptions) {
    return '/projects/' + projectName + '/topics/' + topicName + '/subscriptions/' + subscriptions
  },
  offsets: function (projectName, topicName, subscriptions) {
    return '/projects/' + projectName + '/topics/' + topicName + '/subscriptions/' + subscriptions + '/offsets'
  }
}

var Rest = function (account, endPoint, args) {
  this.account = account
  this.endPoint = endPoint
}

Rest.prototype.getHeaders = function () {
  var headers = {
    "x-datahub-client-version": clientVersion,
    "Date": new Date().toUTCString(),
    "Content-Type": contentType.HTTPJson
  }

  if (this.account.securityToken) headers["x-datahub-security-token"] = this.account.securityToken

  return headers
}

Rest.prototype.request = function (method, urn, params, callback) {
  var headers = this.getHeaders()
  var Authorization = this.account.getSign(method, urn, headers, params)
  headers.Authorization = Authorization
  option = {
    url: this.endPoint + urn,
    method: method,
    headers: headers,
    rejectUnauthorized: false
  }

  if (params) option.json = params

  request(option, function (err, res, body) {
    if (err) return callback(err)
    if (body && body.ErrorCode) return callback(body)
    callback(null, body)
  })
}

Rest.prototype.post = function (urn, params, callback) {
  this.request(HTTPMethod.POST, urn, params, callback)
}

Rest.prototype.delete = function (urn, callback) {
  this.request(HTTPMethod.DELETE, urn, null, callback)
}

Rest.prototype.put = function (urn, params, callback) {
  this.request(HTTPMethod.PUT, urn, params, callback)
}

Rest.prototype.get = function (urn, callback) {
  this.request(HTTPMethod.GET, urn, null, callback)
}

module.exports = {
  HTTPMethod: HTTPMethod,
  contentType: contentType,
  commonHeaders: commonHeaders,
  path: path,
  Rest: Rest
}