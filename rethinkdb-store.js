'use strict';

var _ = require('lodash');
var r = require('rethinkdb');

var storename = 'rethinkdb-store';

module.exports = function(opts) {
  var seneca = this;
  var desc;
  var conn;

  function error(args, err, cb) {
    if(err) {
      seneca.log.error('entity', err, {store:storename});
      return true;
    }
    else return false;
  }

  function configure(spec, cb) {
    if( !_.isUndefined(spec.connect) && !spec.connect) {
      return cb();
    }

    var conf = spec || {};

    var dbopts = seneca.util.deepextend({
      host: 'localhost',
      port: 28015,
      db: 'test'
    }, conf);

    r.connect(dbopts, function(err, connection) {
      if(err)
        return seneca.die('connect', err, conf);

      conn = connection;
      seneca.log.debug('init', 'connect');
      cb();
    });
  }

  var store = {
    name: storename,

    close: function(args, cb) {
      conn.close(function(err) {
        if(err)
          return seneca.die('close', err);
        else
          cb();
      });
    },

    save: function(args, cb) {
      // TODO: Implement
      console.log('Save');
      cb('Not implemented');
    },

    load: function(args, cb) {
      // TODO: Implement
      console.log('Load');
      cb('Not implemented');
    },

    list: function(args, cb) {
      // TODO: Implement
      console.log('List');
      cb('Not implemented');
    },

    remove: function(args, cb) {
      // TODO: Implement
      console.log('Remove');
      cb('Not implemented');
    },

    native: function(args, cb) {
      // TODO: Implement
      console.log('Native');
      cb('Not implemented');
    }
  };

  var meta = seneca.store.init(seneca, opts, store);
  desc = meta.desc;

  seneca.add({init: store.name, tag: meta.tag}, function(args, done) {
    configure(opts, function(err) {
      if(err) {
        return seneca.die('store', err, {
          store: store.name,
          desc: desc
        });
      }
      return done();
    });
  });

  return {name:store.name, tag:meta.tag};
};
