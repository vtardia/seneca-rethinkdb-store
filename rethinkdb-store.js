/*jslint node: true */
"use strict";

var _ = require('lodash');
var r = require('rethinkdb');

var storename = 'rethinkdb-store';

module.exports = function(opts) {
  var seneca = this;
  var desc;
  var conn;
  var db;

  function configure(spec, cb) {
    if( !_.isUndefined(spec.connect) && !spec.connect) {
      return cb();
    }

    var conf = spec || {};

    var dbopts = seneca.util.deepextend({
      host: '127.0.0.1',
      port: 28015,
      db: 'test'
    }, conf);
    db = dbopts.db;

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
        if (err) return cb(err);
        return cb();
      });
    },

    save: function(args, cb) {
      var ent = args.ent;

      var update = true;

      var canon = ent.canon$({object: true});
      var zone = canon.zone;
      var base = canon.base;
      var name = canon.name;

      var tableName = tablename(args.ent);

      if (!ent.id) {
        update = false;
        if (ent.id$) {
          ent.id = ent.id$;
        }
      }

      if (update) {
        return do_save(ent.id);
      } else {
        return do_save();
      }

      function do_save(id) {
        var rdent = ent.data$(true, 'string');

        rdent.entity$ = ent.entity$;

        if(!id) {
          return do_insert(rdent);
        } else {
          rdent.id = id;
          return do_update(rdent);
        }

      }

      function do_update(rdent) {

        r.db(db).table(tableName).get(rdent.id).update(rdent, {returnChanges: true}).run(conn, function(err, result) {
          if(err)
            return cb(err);
          else if(result.changed > 0)
            return cb(null, ent.make$(result.changes[0].new_val));
          else
            return cb(null, ent.make$(rdent));
        });
      }

      function do_insert(rdent) {
        r.db(db).table(tableName).insert(rdent, {returnChanges: true}).run(conn, function(err, result) {
          if(err) return cb(err);
          return cb(null, ent.make$(result.changes[0].new_val));
        });
      }
    },

    load: function(args, cb) {
      var qent = args.qent;
      var q = args.q;

      var canon = qent.canon$({object: true});
      var name = canon.name;

      r.db(db).table(tablename(qent)).get(q.id).run(conn, function(err, result) {
        if(err) {
          return cb(err);
        }
        return cb(null, result ? qent.make$(result) : null);
      });
    },

    list: function(args, cb) {
      var qent = args.qent;
      var q = args.q;

      var canon = qent.canon$({object: true});
      var name = canon.name;

      r.db(db).table(tablename(qent)).filter(q).run(conn, function(err, cursor) {
        if(err) return cb(err);

        var list = [];
        cursor.each(function(err, item) {
          if(err) return cb(err);
          list.push(qent.make$(item));
        });

        return cb(null, list);
      });
    },

    remove: function(args, cb) {
      var qent = args.qent;
      var q = args.q;

      var canon = qent.canon$({object: true});
      var name = canon.name;

      var all  = q.all$; // default false
      var load  = _.isUndefined(q.load$) ? true : q.load$; // default true

      if(all) {
        r.db(db).table(tablename(qent)).delete({returnChanges: load}).run(conn, function(err, result) {
          if(err) return cb(err);

          cb(null, _.map(load ? result.changes : [], function(e) {
            return qent.make$(e.old_val);
          }));
        });
      }
      else {
        var q_clean = _.omit(q, function(k) {
          return _.endsWith(k, '$');
        }); // Remove cruft

        r.db(db).table(tablename(qent)).filter(q_clean).delete({returnChanges: load}).run(conn, function (err, result) {
          if(err) return cb(err);

          cb(null, _.map(load ? result.changes : [], function(e) {
            return qent.make$(e.old_val);
          }));
        });
      }
    },

    native: function(args, cb) {
      var ent = args.ent;
      var canon = ent.canon$({object: true});
      var name = canon.name;

      args.exec(null, {
        r: r,
        db: db,
        table: tablename(ent),
        ent: ent
      });
    }
  };

  var tablename = function (entity) {
    var canon = entity.canon$({object:true});
    return (canon.base?canon.base+'_':'')+canon.name;
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

  return {
    name: store.name,
    tag: meta.tag
  };
};
