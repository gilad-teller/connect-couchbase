/**
 * Created by christophermina on 5/9/14.
 */


/**
 * Module dependencies.
 */

var assert = require('assert')
    , session = require('express-session')
    , CouchbaseStore = require('./')(session);

var store = new CouchbaseStore({host:"127.0.0.1:8091", bucket:"default", username: 'admin', password: 'password'});
const mockSession = { cookie: { maxAge: 2000 }, name: 'cm' };

store.on('connect', function(){
    // #set()
    store.set('123', mockSession, function(err, ok){
        assert.ok(!err, '#set() got an error');
        assert.ok(ok, '#set() is not ok');

        // #get()
        store.get('123', function(err, data){
            console.log("RETRIEVED: " + data.name);
            assert.ok(!err, '#get() got an error');
            assert.deepEqual(mockSession, data);

            // #all()
            store.all(function(err, sessions) {
                if (err) {
                    console.log("AN ERROR OCCURRED GETTING ALL SESSION: " + err);
                }

                if (sessions.length != 0) {
                    assert.deepEqual(mockSession, sessions[0]);
                }

                store.touch('123', mockSession, function(err) {
                    assert.ok(!err, '#touch() got an error');

                    // #set null
                    store.set('123', mockSession, function(err){
                        if (err) {
                            console.log("AN ERROR OCCURRED SETTING SESSION: " + err);
                        }

                        store.destroy('123', function(err){
                            if (err) {
                                console.log("AN ERROR OCCURRED DESTROYING SESSION: " + err);
                            }

                            store.get('123', function(err, data){
                                assert.ok(data === undefined, '#get() did not receive an error');
                                store.client.disconnect();
                            });
                        });
                    });
                });
            });
        });
    });
});

store.on('disconnect', function (err) {
    console.log('tests complete');
    process.exit(0);
});
