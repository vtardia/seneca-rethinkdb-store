'use strict';

var assert = require('assert');
var seneca = require('seneca');
var async = require('async');

var shared = require('seneca-store-test');

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

  it('close', function(done) {
    shared.closetest(si, testcount, done);
  });
});
