var util = require('util');
var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN;
var async = require("async");
var path = require("path");
var request = require("request");

util.inherits(CouchDown, AbstractLevelDOWN)

function CouchDown (location) {
  this.location = location;
  AbstractLevelDOWN.call(this, location)
}

CouchDown.prototype._open = function (options, callback) {
  var self = this;
  callback(null, self);
}

CouchDown.prototype._put = function (key, value, options, callback) {
  var self = this;
  var putpath = this.location + "/" + encodeURIComponent(key);
  var stringval = JSON.stringify({value: value});
  function again() {
    request.get(putpath, function(error, response, body) {
           
      try {
        var decoded = JSON.parse(body);
        decoded.value = value;
        var sv = JSON.stringify(decoded);
        request({method: 'PUT', url: putpath, body: sv}, function(error, response, body) {
          if (error) {
            callback(error);
          } else if (response.statusCode == 409) {
            again();
          } else if (response.statusCode >= 400) {
            callback("code " + response.statusCode + " failed to write " + key + " to couchdb at " + self.location);
          } else {
            callback();
          }
        });
      } catch (e) {
        callback(e);
      }
    });
  };
  request({method: 'PUT', url: putpath, body: stringval}, function(error, response, body) {
    if (error) {
      callback(error);
    } else if (response.statusCode == 409) {
        again();
    } else if (response.statusCode >= 400) {
      callback("code " + response.statusCode + " failed to write " + key + " to couchdb at " + self.location);
    } else {
      callback();
    }
  });
}

CouchDown.prototype._get = function (key, options, callback) {
  var self = this;
  var getpath = this.location + "/" + encodeURIComponent(key);
  request.get(getpath, function(error, response, body) {
    if (error) {
      callback(error, null);
    } else if (response.statusCode != 200) {
      callback("failed to read " + key + " from couchdb at " + self.location);
    } else {
      var decbody;
      try {
        decbody = JSON.parse(body);
        callback(null, decbody.value);
      } catch (e) {
        callback(e, null);
      }
    }
  });
}

CouchDown.prototype._del = function (key, options, callback) {
  var delpath = this.location + "/" + encodeURIComponent(key);
  request.delete(delpath, function(error, response, body) {
    if (error) {
      callback(error);
    } else {
      callback(null);
    }
  });
}

CouchDown.prototype._batch = function(array, options, callback) {
  var self = this;
  async.each(array, function(item, finished) {
    if (item.type == "put") {
      self.put(item.key, item.value, options, finished);
    } else if (item.type == "del") {
      self.del(item.key, options, finished);
    } else {
      finished(new Error("Unknown batch type", item.type));
    }
  }, function(err) {
    if (err) return callback(err);
    callback();
  });
}

module.exports = function(location) {
  return new CouchDown(location);
};
