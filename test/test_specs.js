'use strict';

if (process.browser) {
  return;
}

let specs = require('../lib/specs'),
    assert = require('assert'),
    path = require('path');


let DPATH = path.join(__dirname, 'dat');


suite('specs', () => {

  suite('assembleProtocol', () => {

    let assembleProtocol = specs.assembleProtocol;

    test('missing file', (done) => {
      assembleProtocol('./dat/foo', (err) => {
        assert(err);
        done();
      });
    });

    test('single file', (done) => {
      let fpath = path.join(DPATH, 'Hello.avdl');
      assembleProtocol(fpath, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          namespace: 'org.apache.avro.test',
          protocol: 'Simple',
          doc: 'An example protocol in Avro IDL.\n\nInspired by the Avro specification IDL page:\nhttps://avro.apache.org/docs/current/idl.html#example',
          types: [
            {
              aliases: ['org.foo.KindOf'],
              doc: 'An enum.',
              type: 'enum',
              name: 'Kind',
              symbols: ['FOO', 'BAR', 'BAZ']
            },
            {
              doc: 'An enum with a default value.',
              type: 'enum',
              name: 'Letters',
              symbols: ['A', 'B', 'C'],
              default: 'A'
            },
            {type: 'fixed', doc: 'A fixed.', name: 'MD5', size: 16},
            {
              type: 'record',
              name: 'TestRecord',
              doc: 'A record.',
              fields: [
                {
                  type: {type: 'string', foo: 'first and last'},
                  order: 'ignore',
                  name: 'name'
                },
                {type: 'Kind', order: 'descending', name: 'kind'},
                {type: 'MD5', name: 'hash'},
                {
                  doc: 'A field.',
                  type: ['MD5', 'null'],
                  aliases: ['hash'],
                  name: 'nullableHash'
                },
                {
                  type: {
                    type: 'array',
                    items: {type: 'long', logicalType: 'date'}
                  },
                  name: 'arrayOfDates'
                },
                {
                  type: {type: 'map', values: 'boolean'},
                  name: 'someMap',
                  'default': {'true': true}
                },
                {
                  doc: '',
                  type: 'string',
                  name: 'fieldWithEmptyDoc',
                },
              ]
            },
            {
              type: 'error',
              name: 'TestError',
              doc: 'An error.',
              fields: [{type: 'string', name: 'message'}]
            },
            {type: 'error', name: 'EmptyError', fields: []}
          ],
          messages: {
            hello: {
              doc: 'Greeting.',
              response: 'string',
              request: [{ type: 'string', name: 'greeting', 'default': 'hi'}]
            },
            echo: {
              response: 'TestRecord',
              request: [{type: 'TestRecord', name: 'record'}]
            },
            add: {
              doc: 'Adding.',
              response: 'int',
              request: [
                {type: 'int', name: 'arg1'},
                {type: 'int', name: 'arg2'}
              ]
            },
            echoBytes: {
              doc: 'Echoing.',
              response: 'bytes',
              request: [{type: 'bytes', name: 'data'}]
            },
            error: {response: 'null', request: [], errors: ['TestError']},
            errors: {
              response: 'string',
              request: [],
              errors: ['TestError', 'EmptyError']
            },
            ping: {response: 'null', request: [], 'one-way': true},
            pong: {response: 'null', request: [], 'one-way': true}
          }
        });
        done();
      });
    });

    test('custom file', (done) => {
      let fpath = path.join(DPATH, 'Custom.avdl');
      assembleProtocol(fpath, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          doc: 'A protocol using advanced features.',
          namespace: 'org.apache.avro.test',
          messages: {
            ok: {
              response: {type: 'enum', symbols: ['SUCCESS', 'FAILURE']},
              request: []
            },
            hash: {
              response: 'int',
              request: [
                {
                  name: 'fixed',
                  type: {type: 'fixed', size: 2},
                  'default': 'aa'
                },
                {type: 'long', name: 'length'}
              ]
            },
            import: {
              response: 'null',
              request: [],
              'one-way': true
            },
          },
          types: [
            {
              type: 'record',
              name: 'Person',
              fields: [
                {
                  type: {
                    type: 'enum',
                    name: 'Name',
                    symbols: ['ANN', 'BOB']
                  },
                  name: 'name',
                  'default': 'ANN'
                }
              ]
            }
          ]
        });
        done();
      });
    });

    test('custom import hook', (done) => {
      let opts = {
        importHook: createImportHook({'foo.avdl': 'protocol Foo {}'})
      };
      assembleProtocol('foo.avdl', opts, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {protocol: 'Foo'});
        done();
      });
    });

    test('empty file', (done) => {
      let opts = {
        importHook: createImportHook({'foo.avdl': ''})
      };
      assembleProtocol('foo.avdl', opts, (err) => {
        assert(/eof/.test(err.message));
        done();
      });
    });

    test('duplicate message', (done) => {
      let hook = createImportHook({
        '1.avdl': 'protocol First { double one(); int one(); }'
      });
      assembleProtocol('1.avdl', {importHook: hook}, (err) => {
        assert(/duplicate message/.test(err.message));
        done();
      });
    });

    test('import idl', (done) => {
      let opts = {
        importHook: createImportHook({
          '1.avdl': 'import idl "2.avdl"; protocol First {}',
          '2.avdl': 'protocol Second { fixed One(1); int one(); }'
        })
      };
      assembleProtocol('1.avdl', opts, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'First',
          messages: {one: {request: [], response: 'int'}},
          types: [{name: 'One', type: 'fixed', size: 1}]
        });
        done();
      });
    });

    test('import idl from namespaced protocol name', (done) => {
      let opts = {
        importHook: createImportHook({
          '1.avdl': 'import idl "2.avdl"; protocol first.First {}',
          '2.avdl': 'protocol Second { fixed One(1); }'
        })
      };
      assembleProtocol('1.avdl', opts, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'first.First',
          types: [{name: 'One', type: 'fixed', size: 1, namespace: ''}]
        });
        done();
      });
    });

    test('import idl inside protocol', (done) => {
      let opts = {
        importHook: createImportHook({
          '1.avdl': 'protocol First {int two(); import idl "2.avdl";}',
          '2.avdl': 'protocol Second { fixed Foo(1); }'
        })
      };
      assembleProtocol('1.avdl', opts, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'First',
          messages: {two: {request: [], response: 'int'}},
          types: [{name: 'Foo', type: 'fixed', size: 1}]
        });
        done();
      });
    });

    test('import idl strip redundant namespaces', (done) => {
      let opts = {
        importHook: createImportHook({
          '1.avdl': 'protocol test.First { import idl "2.avdl"; fixed One(1); }',
          '2.avdl': 'protocol other.Second { import idl "3.avdl"; fixed Two(2); }',
          '3.avdl': 'protocol test.Third { fixed Three(3); }',
        })
      };
      assembleProtocol('1.avdl', opts, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'test.First',
          types: [
            {name: 'Three', type: 'fixed', size: 3},
            {name: 'Two', type: 'fixed', size: 2, namespace: 'other'},
            {name: 'One', type: 'fixed', size: 1}
          ]
        });
        done();
      });
    });

    test('import idl from nested paths', (done) => {
      let opts = {
        importHook: createImportHook({
          'a/1.avdl': 'import idl "2.avdl"; protocol A1 { fixed One(1); }',
          'a/2.avdl': 'import idl "../b/3.avdl"; protocol A2 { fixed Two(2); }',
          'b/3.avdl': 'protocol B3 { fixed Three(3); }'
        })
      };
      assembleProtocol('a/1.avdl', opts, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A1',
          types: [
            {name: 'Three', type: 'fixed', size: 3},
            {name: 'Two', type: 'fixed', size: 2},
            {name: 'One', type: 'fixed', size: 1}
          ]
        });
        done();
      });
    });

    test('duplicate message from import', (done) => {
      let hook = createImportHook({
        '1.avdl': 'import idl "2.avdl";\nprotocol First { double one(); }',
        '2.avdl': 'protocol Second { int one(); }'
      });
      assembleProtocol('1.avdl', {importHook: hook}, (err) => {
        assert(/duplicate message/.test(err.message));
        done();
      });
    });

    test('repeated import', (done) => {
      let opts = {
        importHook: createImportHook({
          '1.avdl': 'import idl "2.avdl";import idl "3.avdl";protocol A {}',
          '2.avdl': 'import idl "3.avdl";protocol B { enum Number { ONE } }',
          '3.avdl': 'protocol C { enum Letter { A } }'
        })
      };
      assembleProtocol('1.avdl', opts, (err, schema) => {
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [
            {name: 'Letter', type: 'enum', symbols: ['A']},
            {name: 'Number', type: 'enum', symbols: ['ONE']}
          ]
        });
        done();
      });
    });

    test('import protocol', (done) => {
      let opts = {
        importHook: createImportHook({
          '1': 'import protocol "2";import protocol "3.avpr"; protocol A {}',
          '2': JSON.stringify({
            protocol: 'B',
            types: [{name: 'Letter', type: 'enum', symbols: ['A']}],
            messages: {ping: {request: [], response: 'boolean'}}
          }),
          '3.avpr': '{"protocol": "C"}'
        })
      };
      assembleProtocol('1', opts, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          messages: {ping: {request: [], response: 'boolean'}},
          types: [
            {name: 'Letter', type: 'enum', symbols: ['A']}
          ]
        });
        done();
      });
    });

    test('import protocol with namespace', (done) => {
      let hook = createImportHook({
        'A': 'import protocol "B";import protocol "C";protocol A {}',
        'B': JSON.stringify({
          protocol: 'bb.B',
          namespace: 'b', // Takes precedence.
          types: [{name: 'Letter', type: 'enum', symbols: ['A']}]
        }),
        'C': JSON.stringify({
          protocol: 'C',
          namespace: 'c',
          types: [{name: 'Letter', type: 'enum', symbols: ['A']}]
        })
      });
      assembleProtocol('A', {importHook: hook}, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [
            {namespace: 'b', name: 'Letter', type: 'enum', symbols: ['A']},
            {namespace: 'c', name: 'Letter', type: 'enum', symbols: ['A']}
          ]
        });
        done();
      });
    });

    test('import protocol with namespaced name', (done) => {
      let hook = createImportHook({
        'A': 'import protocol "B";protocol A {}',
        'B': JSON.stringify({
          protocol: 'b.B',
          types: [{name: 'Letter', type: 'enum', symbols: ['A']}]
        })
      });
      assembleProtocol('A', {importHook: hook}, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [
            {namespace: 'b', name: 'Letter', type: 'enum', symbols: ['A']}
          ]
        });
        done();
      });
    });

    test('import protocol with duplicate message', (done) => {
      let hook = createImportHook({
        'A': 'import protocol "B";import protocol "C";protocol A {}',
        'B': JSON.stringify({
          protocol: 'B',
          messages: {ping: {request: [], response: 'boolean'}}
        }),
        'C': JSON.stringify({
          protocol: 'C',
          messages: {ping: {request: [], response: 'boolean'}}
        })
      });
      assembleProtocol('A', {importHook: hook}, (err) => {
        assert(/duplicate message/.test(err.message));
        done();
      });
    });

    test('import schema', (done) => {
      let hook = createImportHook({
        '1': 'import schema "2"; protocol A {}',
        '2': JSON.stringify({name: 'Number', type: 'enum', symbols: ['1']})
      });
      assembleProtocol('1', {importHook: hook}, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [
            {name: 'Number', type: 'enum', symbols: ['1']}
          ]
        });
        done();
      });
    });

    test('import hook error', (done) => {
      let hook = function (fpath, kind, cb) {
        if (path.basename(fpath) === 'A.avdl') {
          cb(null, 'import schema "hi"; protocol A {}');
        } else {
          cb(new Error('foo'));
        }
      };
      assembleProtocol('A.avdl', {importHook: hook}, (err) => {
        assert(/foo/.test(err.message));
        done();
      });
    });

    test('import hook idl error', (done) => {
      let hook = function (fpath, kind, cb) {
        if (path.basename(fpath) === 'A.avdl') {
          cb(null, 'import idl "hi"; protocol A {}');
        } else {
          cb(new Error('bar'));
        }
      };
      assembleProtocol('A.avdl', {importHook: hook}, (err) => {
        assert(/bar/.test(err.message));
        done();
      });
    });

    test('import invalid kind', (done) => {
      let hook = createImportHook({'A.avdl': 'import foo "2";protocol A {}'});
      assembleProtocol('A.avdl', {importHook: hook}, (err) => {
        assert(/invalid import/.test(err.message));
        done();
      });
    });

    test('import invalid JSON', (done) => {
      let hook = createImportHook({
        '1': 'import schema "2"; protocol A {}',
        '2': '{'
      });
      assembleProtocol('1', {importHook: hook}, (err) => {
        assert(err);
        assert.equal(err.path, '2');
        done();
      });
    });

    test('annotated union', (done) => {
      let hook = createImportHook({
        '1': 'protocol A { /** 1 */ @bar(true) union { null, int } foo(); }'
      });
      assembleProtocol('1', {importHook: hook}, (err) => {
        assert(/union annotations/.test(err.message));
        done();
      });
    });

    test('commented import', (done) => {
      let hook = createImportHook({
        '1': '/* import idl "2"; */ // import idl "3"\nprotocol A {}',
        '2': 'foo', // Invalid IDL.
        '3': 'bar'  // Same.
      });
      assembleProtocol('1', {importHook: hook}, (err) => {
        assert.strictEqual(err, null);
        done();
      });
    });

    test('qualified name', (done) => {
      let hook = createImportHook({
        '1': 'protocol A { fixed one.One(1); }',
      });
      assembleProtocol('1', {importHook: hook}, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [{name: 'one.One', type: 'fixed', size: 1}]
        });
        done();
      });
    });

    test('inline fixed', (done) => {
      let hook = createImportHook({
        '1': 'protocol A { record Two { fixed One(1) one; } }',
      });
      assembleProtocol('1', {importHook: hook}, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [{
            name: 'Two',
            type: 'record',
            fields: [
              {name: 'one', type: {name: 'One', type: 'fixed', size: 1}}
            ]
          }]
        });
        done();
      });
    });

    test('one way void', (done) => {
      let hook = createImportHook({
        '1': 'protocol A { void ping(); @foo(true) void pong(); }',
      });
      let opts = {importHook: hook, oneWayVoid: true};
      assembleProtocol('1', opts, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          messages: {
            ping: {response: 'null', request: [], 'one-way': true},
            pong: {
              response: {foo: true, type: 'null'},
              request: [],
              'one-way': true
            }
          }
        });
        done();
      });
    });

    test('javadoc precedence', (done) => {
      let hook = createImportHook({
        '1': 'protocol A {/**1*/ @doc(2) fixed One(1);}',
      });
      let opts = {importHook: hook, reassignJavadoc: true};
      assembleProtocol('1', opts, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [
            {name: 'One', type: 'fixed', size: 1, doc: 2}
          ]
        });
        done();
      });
    });

    test('reset namespace', (done) => {
      let hook = createImportHook({
        '1': 'protocol A { import idl "2"; }',
        '2': '@namespace("b") protocol B { @namespace("") fixed One(1); }'
      });
      assembleProtocol('1', {importHook: hook}, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [{name: 'One', type: 'fixed', size: 1}]
        });
        done();
      });
    });

    test('reset nested namespace', (done) => {
      let hook = createImportHook({
        '1': 'protocol A { import idl "2"; }',
        '2': 'import idl "3"; @namespace("b") protocol B {}',
        '3': 'protocol C { fixed Two(1); }'
      });
      assembleProtocol('1', {importHook: hook}, (err, schema) => {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [{name: 'Two', type: 'fixed', size: 1}]
        });
        done();
      });
    });

    // Import hook from strings.
    function createImportHook(imports) {
      return function (fpath, kind, cb) {
        let key = path.normalize(fpath);
        let str = imports[key];
        delete imports[key];
        process.nextTick(() => { cb(null, str); });
      };
    }

  });


  suite('readSchema', () => {

    let readSchema = specs.readSchema;

    test('anonymous record', () => {
      assert.deepEqual(
        readSchema('/** A foo. */ record { int foo; }'),
        {
          doc: 'A foo.',
          type: 'record',
          fields: [{type: 'int', name: 'foo'}]
        }
      );
    });

    test('fixed', () => {
      assert.deepEqual(
        readSchema('@logicalType("address") @live(true) fixed Address(6)'),
        {
          type: 'fixed',
          size: 6,
          live: true,
          name: 'Address',
          logicalType: 'address'
        }
      );
    });

    test('no implicit collection tags', () => {
      assert.throws(
        () => {
          readSchema(
            'record { array int bars; }',
            {delimitedCollections: true}
          );
        },
        /</
      );
    });

    test('mismatched implicit collection tags', () => {
      assert.throws(
        () => { readSchema('array < int'); },
        />/
      );
    });

    test('implicit collection tags', () => {
      assert.deepEqual(
        readSchema('record { array int bars; }'),
        {
          type: 'record',
          fields: [{type: {type: 'array', items: 'int'}, name: 'bars'}]
        }
      );
    });

    test('mismatched implicit collection tags', () => {
      assert.throws(() => {
        readSchema('record { array < int bars; }');
      }, />/);
    });

    test('default type ref', () => {
      assert.deepEqual(
        readSchema('@precision(4) @scale(2) decimal'),
        {type: 'bytes', logicalType: 'decimal', precision: 4, scale: 2}
      );
    });

    test('custom type ref', () => {
      let typeRefs = {foo: {logicalType: 'foo', type: 'long'}};
      assert.deepEqual(
        readSchema('record { foo bar; }', {typeRefs}),
        {
          type: 'record',
          fields: [
            {
              name: 'bar',
              type: {type: 'long', logicalType: 'foo'}
            }
          ]
        }
      );
    });

    test('type ref overwrite attributes', () => {
      let typeRefs = {ip: {logicalType: 'ip', type: 'fixed', size: 4}};
      assert.deepEqual(
        readSchema('record { @size(16) ip ipV6; }', {typeRefs}),
        {
          type: 'record',
          fields: [
            {
              name: 'ipV6',
              type: {type: 'fixed', size: 16, logicalType: 'ip'}
            }
          ]
        }
      );
    });

  });

  suite('readProtocol', () => {

    let readProtocol = specs.readProtocol;

    test('anonymous protocol with javadoced type', () => {
      assert.deepEqual(
        readProtocol('protocol { /** Foo. */ int; }'),
        {types: [{doc: 'Foo.', type: 'int'}]}
      );
    });

    test('invalid message suffix', () => {
      assert.throws(() => {
        readProtocol('protocol { void foo() repeated; }');
      }, /suffix/);
    });

    test('imports', () => {
      assert.throws(() => {
        readProtocol('protocol { import idl "Foo.avdl"; }');
      }, /unresolvable/);
    });
  });

  suite('read', () => {

    let read = specs.read;

    test('inline protocol', () => {
      assert.deepEqual(
        read('protocol { /** Foo. */ int; }'),
        {types: [{doc: 'Foo.', type: 'int'}]}
      );
    });

    test('protocol path', () => {
      assert.deepEqual(
        read(path.join(DPATH, 'Ping.avdl')),
        {
          protocol: 'Ping',
          messages: {ping: {request: [], response: 'id.Id'}},
          types: [{type: 'fixed', name: 'Id', size: 64, namespace: 'id'}]
        }
      );
    });

    test('path to type schema', () => {
      assert.deepEqual(
        read(path.join(DPATH, 'Id.avsc')),
        {type: 'fixed', name: 'Id', size: 64, namespace: 'id'}
      );
    });

    test('path to type IDL', () => {
      assert.deepEqual(
        read(path.join(DPATH, 'Id.avdl')),
        {type: 'fixed', name: 'Id', size: 64, namespace: 'id'}
      );
    });

    test('invalid string', () => {
      let str = 'protocol { void foo() repeated; }';
      assert.equal(read(str), str);
    });

  });

  suite('Tokenizer', () => {

    let Tokenizer = specs.Tokenizer;

    test('next', () => {
      assert.deepEqual(
        getTokens('hello; "you"'),
        [
          {id: 'name', pos: 0, val: 'hello'},
          {id: 'operator', pos: 5, val: ';'},
          {id: 'string', pos: 6, val: '"you"'}
        ]
      );
    });

    test('next silent', () => {
      let t = new Tokenizer('fee 1');
      assert.equal(t.next().val, 'fee');
      assert.strictEqual(t.next({val: '2', silent: true}), undefined);
      assert.equal(t.next().val, '1');
    });

    test('invalid comment', () => {
      assert.throws(() => { getToken('/** rew'); });
    });

    test('invalid string', () => {
      assert.throws(() => { getToken('"rewr\\"re'); }, /unterminated/);
    });

    test('valid JSON', () => {
      [
        {str: '324,', val: 324},
        {str: '3,', val: 3},
        {str: '-54,', val: -54},
        {str: '-5.4)', val: -5.4},
        {str: '"324",', val: '324'},
        {str: '"hello \\"you\\""r', val: 'hello "you"'},
        {str: '{}o', val: {}},
        {str: '{"a": 1},', val: {a: 1}},
        {str: '[]', val: []},
        {str: 'true+1', val: true},
        {str: 'null.1', val: null},
        {str: 'false::', val: false},
        {str: '["[", {"}": null}, true]', val: ['[', {'}': null}, true]},
      ].forEach((el) => {
        assert.deepEqual(getToken(el.str, 'json').val, el.val);
      });
    });

    test('invalid JSON', () => {
      assert.throws(() => { getToken('{"rew": "3}"', 'json'); });
      assert.throws(() => { getToken('{"rew": "3}"]', 'json'); });
    });

    test('name', () => {
      [
        {str: 'hi', val: 'hi'},
        {str: '`i3i`', val: 'i3i'}
      ].forEach((el) => {
        assert.deepEqual(getToken(el.str).val, el.val);
      });
    });

    test('non-matching', () => {
      assert.throws(() => { getToken('\n1', 'name'); });
      assert.throws(() => { getToken('{', undefined, '}'); });
    });

    function getToken(str, id, val) {
      let tokenizer = new Tokenizer(str);
      return tokenizer.next({id, val});
    }

    function getTokens(str) {
      let tokenizer = new Tokenizer(str);
      let tokens = [];
      let token;
      while ((token = tokenizer.next()).id !== '(eof)') {
        tokens.push(token);
      }
      return tokens;
    }

  });

});
