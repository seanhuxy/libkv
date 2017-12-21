package postgres

import (
	"fmt"
	"testing"
	"time"

	"github.com/seanhuxy/libkv"
	"github.com/seanhuxy/libkv/store"
	"github.com/seanhuxy/libkv/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	client   = "192.168.99.102:5432"
	user     = "testuser"
	password = "testpasswd"
	bucket   = "testdb"
)

func makePostgresClient(t *testing.T) store.Store {
	kv, err := New(
		[]string{client},
		&store.Config{
			ConnectionTimeout: 1 * time.Second,
			Bucket:            bucket,
			Username:          user,
			Password:          password,
		},
	)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestRegister(t *testing.T) {
	Register()

	kv, err := libkv.NewStore(store.POSTGRES, []string{client}, &store.Config{
		ConnectionTimeout: 3 * time.Second,
		Bucket:            bucket,
		Username:          user,
		Password:          password,
	})
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Postgres); !ok {
		t.Fatal("Error registering and initializing postgres")
	}
}

func TestPostgresStore(t *testing.T) {

	kv := makePostgresClient(t)

	defer testutils.RunCleanup(t, kv)

	testPutMultipleTimes(t, kv)
	testAtomicPutMultipleTimes(t, kv)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	// testutils.RunTestWatch(t, kv)
	// testutils.RunTestLock(t, kv)
	// testutils.RunTestLockTTL(t, kv, lockKV)
	// testutils.RunTestLockWait(t, kv, lockKV)
	// testutils.RunTestTTL(t, kv, ttlKV)
}

func testPutMultipleTimes(t *testing.T, kv store.Store) {

	key := "testPutMulti"
	value1 := []byte("bar")
	value2 := []byte("foo")

	failMsg := fmt.Sprintf("Fail key %s", key)

	err := kv.Put(key, value1, nil)
	assert.NoError(t, err, failMsg)

	pair, err := kv.Get(key)
	assert.NoError(t, err, failMsg)
	if assert.NotNil(t, pair, failMsg) {
		assert.NotNil(t, pair.Value, failMsg)
	}
	assert.Equal(t, value1, pair.Value)

	err = kv.Put(key, value2, nil)
	assert.NoError(t, err, failMsg)
	pair, err = kv.Get(key)
	assert.NoError(t, err, failMsg)
	if assert.NotNil(t, pair, failMsg) {
		assert.NotNil(t, pair.Value, failMsg)
	}
	assert.Equal(t, value2, pair.Value)

	// Delete the key
	err = kv.Delete(key)
	assert.NoError(t, err, failMsg)

	// Get should fail
	pair, err = kv.Get(key)
	assert.Error(t, err, failMsg)
	assert.Nil(t, pair, failMsg)

	// Exists should return false
	ok, err := kv.Exists(key)
	assert.NoError(t, err, failMsg)
	assert.False(t, ok, failMsg)
}

func testAtomicPutMultipleTimes(t *testing.T, kv store.Store) {

	key := "testAtomicPutMulti"
	value1 := []byte("bar")
	value2 := []byte("foo")
	value3 := []byte("zoo")

	failMsg := fmt.Sprintf("Fail key %s", key)

	ok, pair, err := kv.AtomicPut(key, value1, nil, nil)
	assert.True(t, ok, failMsg)
	assert.NoError(t, err, failMsg)
	if assert.NotNil(t, pair, failMsg) {
		assert.NotNil(t, pair.Value, failMsg)
	}
	assert.Equal(t, value1, pair.Value)

	ok, pair, err = kv.AtomicPut(key, value2, pair, nil)
	assert.True(t, ok, failMsg)
	assert.NoError(t, err, failMsg)
	if assert.NotNil(t, pair, failMsg) {
		assert.NotNil(t, pair.Value, failMsg)
	}
	assert.Equal(t, value2, pair.Value)

	// AtomicPut fail if the key exists but no pervious specifiedj
	ok, _, err = kv.AtomicPut(key, value3, nil, nil)
	assert.False(t, ok, failMsg)
	assert.NotNil(t, err, failMsg)

	// Delete the key
	err = kv.Delete(key)
	assert.NoError(t, err, failMsg)

	// Get should fail
	pair, err = kv.Get(key)
	assert.Error(t, err, failMsg)
	assert.Nil(t, pair, failMsg)

	// Exists should return false
	ok, err = kv.Exists(key)
	assert.NoError(t, err, failMsg)
	assert.False(t, ok, failMsg)
}
