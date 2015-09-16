/*jslint node: true */
"use strict";

var seneca = require('seneca');
var lab = exports.lab = require('lab').script();
var async = require('async');
var shared = require('seneca-store-test');

var describe = lab.describe;
var it = lab.it;

var si = seneca();

si.__testcount = 0;

var testcount = 0;

si.use(require('..'), {
  name: 'test',
  host: '127.0.0.1',
  db: 'test',
  port: 28015
});

describe('rethinkdb', function() {
  it('basic', function(done) {
    testcount++;
    shared.basictest(si, done);
  });

  it('native', function(done) {
    async.series({
      passthrough: function(cb) {
        var foo = si.make('foo');
        foo.native$({
          exec: function(err, args) {
            /**
             * args: {
             *  r: <rethinkdb-driver>
             *  db: <database-name>
             *  table: <table-name>
             *  ent: <entity-creator>
             * }
             *
             * This should allow you full freedom to
             * manipulate the result set.
             */
            cb();
          }
        });
      }
    }, function (err, out) {
      si.__testcount++;
      done();
    });
  });

  it('close', function(done) {
    shared.closetest(si, testcount, done);
  });
});
