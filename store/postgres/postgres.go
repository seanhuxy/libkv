package postgres

import (
	"fmt"
	"log"
	"strings"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"

	"github.com/seanhuxy/libkv"
	"github.com/seanhuxy/libkv/store"
)

// KV represents the db table stored in postgres
type KV struct {
	Key       string `gorm:"primary_key"`
	Value     []byte
	LastIndex uint64
}

type Postgres struct {
	db *gorm.DB
}

func Register() {
	libkv.AddStore(store.POSTGRES, New)
}

func New(addrs []string, options *store.Config) (store.Store, error) {

	args := []string{}
	if len(addrs) != 1 {
		return nil, fmt.Errorf("Invalid DB Address")
	}

	host, port, err := getHostnameAndPort(addrs[0])
	if err != nil {
		return nil, err
	}

	args = append(args, fmt.Sprintf("host=%s", host))
	args = append(args, fmt.Sprintf("port=%s", port))

	// TODO: add TLS support
	args = append(args, fmt.Sprintf("sslmode=disable"))

	if options != nil {
		if options.Username != "" {
			args = append(args, fmt.Sprintf("user=%s", options.Username))
			args = append(args, fmt.Sprintf("password=%s", options.Password))
		}

		if options.Bucket != "" {
			args = append(args, fmt.Sprintf("dbname=%s", options.Bucket))
		}

		// if options.TLS != nil {
		// 	pgOpts.TLSConfig = options.TLS
		// 	pgOpts.Addr = store.CreateEndpoints(addrs, "https")[0]
		// } else {
		// 	pgOpts.Addr = store.CreateEndpoints(addrs, "http")[0]
		// }

		// if options.ConnectionTimeout != 0 {
		// 	pgOpts.ReadTimeout = options.ConnectionTimeout
		// 	pgOpts.WriteTimeout = options.ConnectionTimeout
		// }
	}

	// db, err := gorm.Open("postgres", "user=postgres password=postgres host=192.168.99.102 port=5432 sslmode=disable")
	db, err := gorm.Open("postgres", strings.Join(args, " "))
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// use singular as table name
	db.SingularTable(true)

	if err := db.DropTableIfExists(&KV{}).Error; err != nil {
		log.Println(err)
		return nil, err
	}

	if err := db.CreateTable(&KV{}).Error; err != nil {
		log.Println(err)
		return nil, err
	}

	return &Postgres{
		db: db,
	}, nil
}

func getHostnameAndPort(addr string) (string, string, error) {

	res := strings.Split(addr, ":")
	if len(res) != 2 {
		return "", "", fmt.Errorf("Invalid Address")
	}
	return res[0], res[1], nil
}

func toKVPair(kv *KV) *store.KVPair {

	return &store.KVPair{
		Key:       kv.Key,
		Value:     kv.Value,
		LastIndex: kv.LastIndex,
	}
}

func (p *Postgres) normalize(key string) string {
	key = store.Normalize(key)
	return strings.TrimPrefix(key, "/")
}

// Put a value at the specified key
func (p *Postgres) Put(key string, value []byte, options *store.WriteOptions) (err error) {

	pair := &KV{
		Key:       p.normalize(key),
		Value:     value,
		LastIndex: 0,
	}
	log.Printf("put key=%v val=%v\n", pair.Key, pair.Value)
	tx := p.db.Begin()
	defer func() {
		if err != nil {
			log.Printf("put %s err: %s", pair.Key, err)
			tx.Rollback()
		} else {
			log.Printf("put %s done", pair.Key)
			tx.Commit()
		}
	}()

	prev, errGet := p.get(tx, pair.Key)
	if errGet != nil {
		if errGet != store.ErrKeyNotFound {
			err = errGet
			return
		}
		// not exists
		err = tx.Create(&pair).Error
		return
	}

	pair.LastIndex = prev.LastIndex + 1
	err = tx.Save(&pair).Error
	return
}

func (p *Postgres) get(tx *gorm.DB, key string) (*KV, error) {

	pair := &KV{
		Key: p.normalize(key),
	}
	err := tx.First(pair, "key = ?", pair.Key).Error
	if err == gorm.ErrRecordNotFound {
		// log.Printf("get %s not found", pair.Key)
		return nil, store.ErrKeyNotFound
	}
	// log.Printf("get %s=%s from db\n", pair.Key, pair.Value)
	log.Printf("get %s=%s; lastIndex=%v\n", pair.Key, pair.Value, pair.LastIndex)
	return pair, nil
}

// Get a value given its key
func (p *Postgres) Get(key string) (*store.KVPair, error) {

	pair, err := p.get(p.db, key)
	if err != nil {
		return nil, err
	}
	return toKVPair(pair), nil
}

// Delete the value at the specified key
func (p *Postgres) Delete(key string) error {

	pair := KV{
		Key: p.normalize(key),
	}

	log.Printf("delete %s from db\n", pair.Key)
	//// DELETE FROM kvpair WHERE key = 'string_primary_key';
	if err := p.db.Delete(&pair, "key = ?", pair.Key).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return store.ErrKeyNotFound
		}
		return err
	}
	return nil
}

func (p *Postgres) exists(tx *gorm.DB, key string) (bool, error) {
	_, err := p.get(tx, key)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Exists verifies if a Key exists in the store
func (p *Postgres) Exists(key string) (bool, error) {
	return p.exists(p.db, key)
}

// Watch for changes on a key
func (p *Postgres) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

// WatchTree watches for changes on child nodes under
// a given directory
func (p *Postgres) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

// NewLock creates a lock for a given key.
// The returned Locker is not held and must be acquired
// with `.Lock`. The Value is optional.
func (p *Postgres) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, store.ErrCallNotSupported
}

// List the content of a given prefix
func (p *Postgres) List(directory string) ([]*store.KVPair, error) {

	condition := fmt.Sprintf("%s%%", p.normalize(directory))
	pairs := []*KV{}

	// SELECT * FROM kvpair where key LIKE "prefix%"
	if err := p.db.Find(&pairs, "key LIKE ?", condition).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}
	if len(pairs) == 0 {
		return nil, store.ErrKeyNotFound
	}

	result := []*store.KVPair{}
	for _, pair := range pairs {
		result = append(result, toKVPair(pair))
	}
	return result, nil
}

// DeleteTree deletes a range of keys under a given directory
func (p *Postgres) DeleteTree(directory string) error {

	pair := []*KV{}
	condition := fmt.Sprintf("%s%%", p.normalize(directory))

	// DELETE FROM kvpair WHERE key = 'string_primary_key';
	if err := p.db.Delete(&pair, "key LIKE ?", condition).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return store.ErrKeyNotFound
		}
		return err
	}
	return nil
}

// AtomicPut atomic CAS operation on a single value.
// Pass previous = nil to create a new key.
func (p *Postgres) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (ok bool, result *store.KVPair, err error) {

	ok = false
	result = nil
	err = nil

	pair := &KV{
		Key:   p.normalize(key),
		Value: value,
	}

	tx := p.db.Begin()
	defer func() {
		if err != nil {
			log.Printf("atomic put key=%s failed: %s", pair.Key, err)
			tx.Rollback()
		} else {
			log.Printf("atomic put key=%s done", pair.Key)
			tx.Commit()
		}
	}()

	log.Printf("atomic put key=%s value=%s\n", pair.Key, pair.Value)

	if previous == nil {
		// if the pervious doesn't exist, create one
		if err = tx.Create(&pair).Error; err != nil {
			return
		}
	} else {
		// get the current value
		curr, errGet := p.get(tx, pair.Key)
		if errGet != nil {
			err = errGet
			return
		}

		log.Printf("CAS compare: key=%s prev=%v  curr=%v ", pair.Key, previous.LastIndex, curr.LastIndex)
		// if lastIndeices do not matched, return error
		if curr.LastIndex != previous.LastIndex {
			err = store.ErrKeyModified
			return
		}

		// if matched, do the update
		pair.LastIndex = curr.LastIndex + 1
		if err = tx.Save(pair).Error; err != nil {
			return
		}
	}
	result = toKVPair(pair)
	ok = true
	return
}

// AtomicDelete atomic delete of a single value
func (p *Postgres) AtomicDelete(key string, previous *store.KVPair) (ok bool, err error) {

	normalizedKey := p.normalize(key)
	ok = false
	err = nil

	log.Printf("atomic delete key=%s \n", normalizedKey)

	tx := p.db.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	if previous == nil {
		if err = tx.Delete(&KV{}, "key = ?", normalizedKey).Error; err != nil {
			return
		}
	} else {
		// get the current value
		curr, errGet := p.get(tx, normalizedKey)
		if errGet != nil {
			err = errGet
			return
		}
		// if lastIndeices do not matched, return error
		if curr.LastIndex != previous.LastIndex {
			err = store.ErrKeyModified
			return
		}
		// if matched, do the deletion
		if err = tx.Delete(&KV{}, "key = ?", normalizedKey).Error; err != nil {
			return
		}
	}
	ok = true
	return
}

// Close the store connection
func (p *Postgres) Close() {

	err := p.db.Close()
	if err != nil {
		log.Println(err)
	}
	return
}
