package postgres

import (
	"testing"
	"time"

	"github.com/seanhuxy/libkv"
	"github.com/seanhuxy/libkv/store"
	"github.com/seanhuxy/libkv/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	client = "192.168.99.102:5432"
)

func makePostgresClient(t *testing.T) store.Store {
	kv, err := New(
		[]string{client},
		&store.Config{
			ConnectionTimeout: 3 * time.Second,
			Username:          "postgres",
			Password:          "postgres",
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
		Username:          "postgres",
		Password:          "postgres",
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

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	// testutils.RunTestWatch(t, kv)
	// testutils.RunTestLock(t, kv)
	// testutils.RunTestLockTTL(t, kv, lockKV)
	// testutils.RunTestLockWait(t, kv, lockKV)
	// testutils.RunTestTTL(t, kv, ttlKV)
}

// test put two times
