/* jshint node: true, mocha: true */

'use strict';

if (process.browser) {
  return;
}

var index = require('../lib'),
    services = require('../lib/services'),
    types = require('../lib/types'),
    assert = require('assert'),
    buffer = require('buffer'),
    path = require('path'),
    tmp = require('tmp'),
    fs = require('fs');

const snappy = require('snappy');
const { doesNotMatch } = require('assert');
const codecs = {
  snappy: function (buf, cb) {
    console.log('original value ', buf);
    console.log('original value length', buf.length);
    // Avro appends checksums to compressed blocks, which we skip here.
    const buffer = snappy.uncompressSync(buf.slice(0, buf.length - 4));
    console.log('uncompress value ', buffer);
    console.log('uncompress value length', buffer.length);
    return buffer;
  }
};

var Buffer = buffer.Buffer;

var DPATH = path.join(__dirname, 'dat');

suite('index', function () {

  suite('parse', function () {

    var parse = index.parse;

    test('type object', function () {
      var obj = {
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      };
      assert(parse(obj) instanceof types.builtins.RecordType);
    });

    test('protocol object', function () {
      var obj = {protocol: 'Foo'};
      assert(parse(obj) instanceof services.Service);
    });

    test('type instance', function () {
      var type = parse({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });
      assert.strictEqual(parse(type), type);
    });

    test('stringified type schema', function () {
      assert(parse('"int"') instanceof types.builtins.IntType);
    });

    test('type name', function () {
      assert(parse('double') instanceof types.builtins.DoubleType);
    });

    test('type schema file', function () {
      var t1 = parse({type: 'fixed', name: 'id.Id', size: 64});
      var t2 = parse(path.join(__dirname, 'dat', 'Id.avsc'));
      assert.deepEqual(JSON.stringify(t1), JSON.stringify(t2));
    });

  });

  test('createFileDecoder', function (cb) {
    var n = 0;
    var fileDec = index.createFileDecoder(path.join(DPATH, 'analyse_tombstone.avro'), {codecs})
      .on('metadata', function (type) {
        console.log('metadata');
        console.log(type.name);
      })
      .on('data', function (obj) {
        console.log('data');
        n++;
        console.log(obj);
      })
      .on('error', err => {
        console.log('err');
        console.log(err);
        assert(false);
      })
      .on('end', function () {
        console.log('end');
        //assert.equal(n, 10);
        cb();
      }).on('close', function() {
        console.log('close');
      }).on('unpipe', function() {
        console.log('unpipe');
      }).on('finish', function() {
        console.log('finish');
      });
     
    console.log(fileDec.eventNames());
    fileDec.emit('data', {a:"a"});

  });

  test('createFileEncoder', function (cb) {
    var type = types.Type.forSchema({
      type: 'record',
      name: 'Person',
      fields: [
        {name: 'name', type: 'string'},
        {name: 'age', type: 'int'}
      ]
    });
    var path = tmp.fileSync().name;
    var encoder = index.createFileEncoder(path, type);
    encoder.write({name: 'Ann', age: 32});
    encoder.end({name: 'Bob', age: 33});
    var n = 0;
    encoder.on('finish', function () {
      setTimeout(function () { // Hack to wait until the file is flushed.
        index.createFileDecoder(path)
          .on('data', function (obj) {
            n++;
            assert(type.isValid(obj));
          })
          .on('end', function () {
            assert.equal(n, 2);
            cb();
          });
      }, 50);
    });
  });

  test('extractFileHeader', function () {
    var header;
    var fpath = path.join(DPATH, 'person-10.avro');
    header = index.extractFileHeader(fpath);
    assert(header !== null);
    assert.equal(typeof header.meta['avro.schema'], 'object');
    header = index.extractFileHeader(fpath, {decode: false});
    assert(Buffer.isBuffer(header.meta['avro.schema']));
    header = index.extractFileHeader(fpath, {size: 2});
    assert.equal(typeof header.meta['avro.schema'], 'object');
    header = index.extractFileHeader(path.join(DPATH, 'person-10.avro.raw'));
    assert(header === null);
    header = index.extractFileHeader(
      path.join(DPATH, 'person-10.no-codec.avro')
    );
    assert(header !== null);
  });

});
