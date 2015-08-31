'use strict';

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
      conn.close(function() {
        cb();
      });
    },

    save: function(args, cb) {
      var ent = args.ent;

      var create = (null == ent.id);

      var canon = ent.canon$({object: true});
      var zone = canon.zone;
      var base = canon.base;
      var name = canon.name;

      if(create) {
        if(ent.id$) {
          var id = ent.id$;
          delete ent.id$;
          do_save(id);
        }
        else {
          this.act(
            {role:'basic', cmd:'generate_id',
            name:name, base:base, zone:zone},
            function(err, id) {
              if(err) return cb(err);
              do_save(id);
            }
          );
        }
      }
      else {
        do_save(ent.id);
      }

      function do_save(id) {
        var rdent = ent.data$(true, 'string');

        if(id) {
          rdent.id = id;
        }

        rdent.entity$ = ent.entity$;

        r.db(db).table(name).get(rdent.id).run(conn, function(err, result) {
          if(err) return cb(err);

          if(!result)
            do_insert(rdent);
          else
            do_update(result, rdent);
        });
      }

      function do_update(prev, rdent) {
        var obj = seneca.util.deepextend(prev, rdent);

        r.db(db).table(name).get(rdent.id).update(obj, {returnChanges: true}).run(conn, function(err, result) {
          if(err)
            cb(err);
          else if(result.changed > 0)
            cb(null, ent.make$(result.changes[0].new_val));
          else
            cb(null, ent.make$(rdent));
        });
      }

      function do_insert(rdent) {
        r.db(db).table(name).insert(rdent, {returnChanges: true}).run(conn, function(err, result) {
          if(err) return cb(err);
          cb(null, ent.make$(result.changes[0].new_val));
        });
      }
    },

    load: function(args, cb) {
      var qent = args.qent;
      var q = args.q;

      var canon = qent.canon$({object: true});
      var name = canon.name;

      r.db(db).table(name).get(q.id).run(conn, function(err, result) {
        if(err) return cb(err);
        cb(null, result ? qent.make$(result) : null);
      });
    },

    list: function(args, cb) {
      var qent = args.qent;
      var q = args.q;

      var canon = qent.canon$({object: true});
      var name = canon.name;

      r.db(db).table(name).filter(q).run(conn, function(err, cursor) {
        if(err) return cb(err);

        var list = [];
        cursor.each(function(err, item) {
          if(err) return cb(err);
          list.push(qent.make$(item));
        });

        cb(null, list);
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
        r.db(db).table(name).delete({returnChanges: load}).run(conn, function(err, result) {
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

        r.db(db).table(name).filter(q_clean).delete({returnChanges: load}).run(conn, function (err, result) {
          if(err) return cb(err);

          cb(null, _.map(load ? result.changes : [], function(e) {
            return qent.make$(e.old_val);
          }));
        });
      }
    },

    native: function(args, cb) {
      // TODO: Implement
      console.log('Native Driver not implemented');
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

  return {
    name: store.name,
    tag: meta.tag
  };
};
