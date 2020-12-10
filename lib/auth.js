var commonHeaders = require('./rest').commonHeaders
var crypto = require('crypto')

var aliyunAccount = function (accessId, accessKey, securityToken) {
  this.accessId = accessId
  this.accessKey = accessKey
  this.securityToken = securityToken
}

var getCanonicalizedDataHubHeaders = function (requestHeaders) {
  var canonicalizedDataHubHeaders = ''
  canonicalizedDataHubHeaders += 'x-datahub-client-version:' + requestHeaders["x-datahub-client-version"]

  for (var commonHeader in commonHeaders) {
    if (commonHeader === 'clientVersion') continue
    if (/x-datahub/.test(commonHeaders[commonHeader]) && requestHeaders[commonHeaders[commonHeader]]) {
      canonicalizedDataHubHeaders += '\n' + commonHeaders[commonHeader] + ':' + requestHeaders[commonHeaders[commonHeader]]
    }
  }

  return canonicalizedDataHubHeaders
}

var getCanonicalizedResource = function (urn, requestParams) {
  var canonicalizedResource = ''
  canonicalizedResource += urn
  return canonicalizedResource
}

aliyunAccount.prototype.getSign = function (method, urn, requestHeaders, requestParams) {
  var canonicalizedDataHubHeaders = getCanonicalizedDataHubHeaders(requestHeaders)
  var canonicalizedResource = getCanonicalizedResource(urn, requestParams)
  var sign = method + '\n' + requestHeaders["Content-Type"] + '\n' + requestHeaders["Date"] + '\n' + canonicalizedDataHubHeaders + '\n' + canonicalizedResource
  var signature = crypto.createHmac('sha1', this.accessKey).update(sign).digest().toString('base64')
  var Authorization = 'DATAHUB ' + this.accessId + ':' + signature
  return Authorization
}

module.exports = aliyunAccount