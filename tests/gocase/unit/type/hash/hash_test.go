/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package hash

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks/tests/gocase/util"
)

func getKeys(hash map[string]string) []string {
	r := make([]string, 0)
	for key := range hash {
		r = append(r, key)
	}
	return r
}

func getVals(hash map[string]string) []string {
	r := make([]string, 0)
	for _, val := range hash {
		r = append(r, val)
	}
	return r
}

func TestHash(t *testing.T) {
	configOptions := []util.ConfigOptions{
		{
			Name:       "txn-context-enabled",
			Options:    []string{"yes", "no"},
			ConfigType: util.YesNo,
		},
		{
			Name:       "resp3-enabled",
			Options:    []string{"yes", "no"},
			ConfigType: util.YesNo,
		},
		{
			Name:       "hash-field-expiration",
			Options:    []string{"yes", "no"},
			ConfigType: util.YesNo,
		},
		{
			Name:       "rocksdb.read_options.async_io",
			Options:    []string{"yes", "no"},
			ConfigType: util.YesNo,
		},
	}

	configsMatrix, err := util.GenerateConfigsMatrix(configOptions)
	require.NoError(t, err)

	for _, configs := range configsMatrix {
		testHash(t, configs)
	}
}

var testHash = func(t *testing.T, configs map[string]string) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	smallhash := make(map[string]string)
	bighash := make(map[string]string)

	t.Run("HSET/HLEN - Small hash creation", func(t *testing.T) {
		for i := 0; i < 8; i++ {
			key := "__avoid_collisions__" + util.RandString(0, 8, util.Alpha)
			val := "__avoid_collisions__" + util.RandString(0, 8, util.Alpha)
			if _, ok := smallhash[key]; ok {
				i--
			}
			rdb.HSet(ctx, "smallhash", key, val)
			smallhash[key] = val
		}
		require.Equal(t, int64(8), rdb.HLen(ctx, "smallhash").Val())
	})

	t.Run("HSET/HLEN - Big hash creation", func(t *testing.T) {
		for i := 0; i < 1024; i++ {
			key := "__avoid_collisions__" + util.RandString(0, 8, util.Alpha)
			val := "__avoid_collisions__" + util.RandString(0, 8, util.Alpha)
			if _, ok := bighash[key]; ok {
				i--
			}
			rdb.HSet(ctx, "bighash", key, val)
			bighash[key] = val
		}
		require.Equal(t, int64(1024), rdb.HLen(ctx, "bighash").Val())
	})

	t.Run("HSET wrong number of args", func(t *testing.T) {
		pattern := ".*wrong number.*"
		util.ErrorRegexp(t, rdb.HSet(ctx, "hmsetmulti", "key1", "val1", "key2").Err(), pattern)
	})

	t.Run("HSET supports multiple fields", func(t *testing.T) {
		require.Equal(t, int64(2), rdb.HSet(ctx, "hmsetmulti", "key1", "val1", "key2", "val2").Val())
		require.Equal(t, int64(0), rdb.HSet(ctx, "hmsetmulti", "key1", "val1", "key2", "val2").Val())
		require.Equal(t, int64(1), rdb.HSet(ctx, "hmsetmulti", "key1", "val1", "key3", "val3").Val())
	})

	t.Run("HSETEXPIRE wrong number of args", func(t *testing.T) {
		pattern := ".*wrong number.*"
		ttlStr := "3600"
		testKey := "hsetKey"
		r := rdb.Do(ctx, "hsetexpire", testKey, ttlStr)
		util.ErrorRegexp(t, r.Err(), pattern)
	})

	t.Run("HSETEXPIRE incomplete pairs", func(t *testing.T) {
		pattern := ".*field-value pairs must be complete.*"
		ttlStr := "3600"
		testKey := "hsetKey"
		r := rdb.Do(ctx, "hsetexpire", testKey, ttlStr, "key1", "val1", "key2")
		util.ErrorRegexp(t, r.Err(), pattern)
	})

	t.Run("HSET/HSETEXPIRE/HSETEXPIRE/persist update expire time", func(t *testing.T) {
		ttlStr := "3600"
		testKey := "hsetKeyUpdateTime"
		// create an hash without expiration
		r := rdb.Do(ctx, "hset", testKey, "key1", "val1", "key2", "val2")
		require.NoError(t, r.Err())
		noExp := rdb.ExpireTime(ctx, testKey)
		// make sure there is not exp set on the key
		assert.Equal(t, -1*time.Nanosecond, noExp.Val())
		// validate we inserted the key/vals
		values := rdb.HGetAll(ctx, testKey)
		assert.Equal(t, 2, len(values.Val()))

		// update the hash and add expiration
		r = rdb.Do(ctx, "hsetexpire", testKey, ttlStr, "key3", "val3")
		require.NoError(t, r.Err())
		assert.Equal(t, "OK", r.Val())
		firstExp := rdb.ExpireTime(ctx, testKey)
		firstExpireTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(firstExp.Val()).Unix()
		// validate there is exp set on the key
		assert.NotEqual(t, -1, firstExpireTime)
		assert.Greater(t, firstExpireTime, time.Now().Unix())
		// validate we updated the key/vals
		values = rdb.HGetAll(ctx, testKey)
		assert.Equal(t, 3, len(values.Val()))

		// update the has and expiration
		time.Sleep(1 * time.Second)
		r = rdb.Do(ctx, "hsetexpire", testKey, ttlStr, "key4", "val4")
		require.NoError(t, r.Err())
		assert.Equal(t, "OK", r.Val())
		// validate there is exp set on the key and it is new
		secondExp := rdb.ExpireTime(ctx, testKey)
		secondExpireTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(secondExp.Val()).Unix()
		assert.NotEqual(t, -1, secondExpireTime)
		assert.Greater(t, secondExpireTime, time.Now().Unix())
		assert.Greater(t, secondExpireTime, firstExpireTime)
		// validate we updated the key/vals
		values = rdb.HGetAll(ctx, testKey)
		assert.Equal(t, 4, len(values.Val()))

		//remove expiration on the key and verify
		r = rdb.Do(ctx, "persist", testKey)
		require.NoError(t, r.Err())
		persist := rdb.ExpireTime(ctx, testKey)
		assert.Equal(t, -1*time.Nanosecond, persist.Val())
		// validate we still have the correct number of key/vals
		values = rdb.HGetAll(ctx, testKey)
		assert.Equal(t, 4, len(values.Val()))
	})

	t.Run("HSETEXPIRE/HLEN/EXPIRETIME - Small hash creation", func(t *testing.T) {
		ttlStr := "3600"
		testKey := "hsetexsmallhash"
		hsetExSmallHash := make(map[string]string)
		for i := 0; i < 8; i++ {
			key := "__avoid_collisions__" + util.RandString(0, 8, util.Alpha)
			val := "__avoid_collisions__" + util.RandString(0, 8, util.Alpha)
			if _, ok := hsetExSmallHash[key]; ok {
				i--
			}
			rdb.Do(ctx, "hsetexpire", testKey, ttlStr, key, val)
			hsetExSmallHash[key] = val
		}
		require.Equal(t, int64(8), rdb.HLen(ctx, testKey).Val())
		val := rdb.ExpireTime(ctx, testKey).Val()
		expireTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(val).Unix()
		require.Greater(t, expireTime, time.Now().Unix())
	})

	t.Run("HSETEXPIRE/HLEN/EXPIRETIME - Big hash creation", func(t *testing.T) {
		ttlStr := "3600"
		testKey := "hsetexbighash"
		hsetExBigHash := make(map[string]string)
		for i := 0; i < 1024; i++ {
			key := "__avoid_collisions__" + util.RandString(0, 8, util.Alpha)
			val := "__avoid_collisions__" + util.RandString(0, 8, util.Alpha)
			if _, ok := hsetExBigHash[key]; ok {
				i--
			}
			rdb.Do(ctx, "hsetexpire", testKey, ttlStr, key, val)
			hsetExBigHash[key] = val
		}
		require.Equal(t, int64(1024), rdb.HLen(ctx, testKey).Val())
		val := rdb.ExpireTime(ctx, testKey).Val()
		expireTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(val).Unix()
		require.Greater(t, expireTime, time.Now().Unix())
	})

	t.Run("HSETEXPIRE/HLEN/EXPIRETIME - Multi field-value pairs creation", func(t *testing.T) {
		ttlStr := "3600"
		testKey := "hsetexbighashPair"
		hsetExBigHash := make(map[string]string)
		cmd := []string{"hsetexpire", testKey, ttlStr}
		for i := 0; i < 10; i++ {
			key := "__avoid_collisions__" + util.RandString(0, 8, util.Alpha)
			val := "__avoid_collisions__" + util.RandString(0, 8, util.Alpha)
			if _, ok := hsetExBigHash[key]; ok {
				i--
			}
			cmd = append(cmd, key, val)
			hsetExBigHash[key] = val
		}
		args := make([]interface{}, len(cmd))
		for i, v := range cmd {
			args[i] = v
		}
		rdb.Do(ctx, args...)
		require.Equal(t, int64(10), rdb.HLen(ctx, testKey).Val())
		val := rdb.ExpireTime(ctx, testKey).Val()
		expireTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(val).Unix()
		require.Greater(t, expireTime, time.Now().Unix())
	})

	t.Run("HGET against the small hash", func(t *testing.T) {
		var err error
		for key, val := range smallhash {
			res := rdb.HGet(ctx, "smallhash", key)
			if val != res.Val() {
				err = res.Err()
				break
			}
		}
		require.NoError(t, err)
	})

	t.Run("HGET against the big hash", func(t *testing.T) {
		var err error
		for key, val := range bighash {
			res := rdb.HGet(ctx, "bighash", key)
			if val != res.Val() {
				err = res.Err()
				break
			}
		}
		require.NoError(t, err)
	})

	t.Run("HGET against non existing key", func(t *testing.T) {
		var rv []string
		rv = append(rv, rdb.HGet(ctx, "samllhash", "__123123123__").Val())
		rv = append(rv, rdb.HGet(ctx, "bighash", "__123123123__").Val())
		require.Equal(t, []string{"", ""}, rv)
	})

	t.Run("HSET in update and insert mode", func(t *testing.T) {
		var rv []string
		k := getKeys(smallhash)[0]
		rv = append(rv, fmt.Sprintf("%d", rdb.HSet(ctx, "smallhash", k, "newval1").Val()))
		smallhash[k] = "newval1"
		rv = append(rv, rdb.HGet(ctx, "smallhash", k).Val())
		rv = append(rv, fmt.Sprintf("%d", rdb.HSet(ctx, "smallhash", "__foobar123__", "newval").Val()))
		k = getKeys(bighash)[0]
		rv = append(rv, fmt.Sprintf("%d", rdb.HSet(ctx, "bighash", k, "newval2").Val()))
		bighash[k] = "newval2"
		rv = append(rv, rdb.HGet(ctx, "bighash", k).Val())
		rv = append(rv, fmt.Sprintf("%d", rdb.HSet(ctx, "bighash", "__foobar123__", "newval").Val()))
		rv = append(rv, fmt.Sprintf("%d", rdb.HDel(ctx, "smallhash", "__foobar123__").Val()))
		rv = append(rv, fmt.Sprintf("%d", rdb.HDel(ctx, "bighash", "__foobar123__").Val()))
		require.Equal(t, []string{"0", "newval1", "1", "0", "newval2", "1", "1", "1"}, rv)
	})

	t.Run("HSETNX target key missing - small hash", func(t *testing.T) {
		rdb.HSetNX(ctx, "smallhash", "__123123123__", "foo")
		require.Equal(t, "foo", rdb.HGet(ctx, "smallhash", "__123123123__").Val())
	})

	t.Run("HSETNX target key exists - small hash", func(t *testing.T) {
		rdb.HSetNX(ctx, "smallhash", "__123123123__", "bar")
		res := rdb.HGet(ctx, "smallhash", "__123123123__").Val()
		rdb.HDel(ctx, "smallhash", "__123123123__")
		require.Equal(t, "foo", res)
	})

	t.Run("HSETNX target key missing - big hash", func(t *testing.T) {
		rdb.HSetNX(ctx, "bighash", "__123123123__", "foo")
		require.Equal(t, "foo", rdb.HGet(ctx, "bighash", "__123123123__").Val())
	})

	t.Run("HSETNX target key exists - big hash", func(t *testing.T) {
		rdb.HSetNX(ctx, "bighash", "__123123123__", "bar")
		res := rdb.HGet(ctx, "bighash", "__123123123__").Val()
		rdb.HDel(ctx, "bighash", "__123123123__")
		require.Equal(t, "foo", res)
	})

	t.Run("HSETNX multiple values, wrong number of arguments", func(t *testing.T) {
		hashSetName := "test-hash-set"
		r := rdb.Do(ctx, "HSETNX", hashSetName, "field1", "value1", "field2")
		require.ErrorContains(t, r.Err(), "wrong number of arguments")
		rdb.Del(ctx, hashSetName)
	})

	t.Run("HSETNX multiple values, target keys missing", func(t *testing.T) {
		hashSetName := "test-hash-set"
		r := rdb.Do(ctx, "HSETNX", hashSetName, "field1", "value1", "field2", "value2")
		val, err := r.Int64()
		require.NoError(t, err)
		require.Equal(t, int64(2), val)

		rdb.Del(ctx, hashSetName)
	})

	t.Run("HSETNX multiple values, target keys exist", func(t *testing.T) {
		hashSetName := "test-hash-set"
		rdb.HSet(ctx, hashSetName, "field1", "value1")
		rdb.HSet(ctx, hashSetName, "field2", "value2")

		r := rdb.Do(ctx, "HSETNX", hashSetName, "field1", "value1-changed", "field2", "value2-changed")
		val, err := r.Int64()
		require.NoError(t, err)
		require.Equal(t, int64(0), val)

		value1 := rdb.HGet(ctx, hashSetName, "field1").Val()
		require.Equal(t, "value1", value1)

		value2 := rdb.HGet(ctx, hashSetName, "field2").Val()
		require.Equal(t, "value2", value2)

		rdb.Del(ctx, hashSetName)
	})

	t.Run("HSETNX multiple values, some of the target keys exist", func(t *testing.T) {
		hashSetName := "test-hash-set"
		rdb.HSet(ctx, hashSetName, "field1", "value1")
		rdb.HSet(ctx, hashSetName, "field2", "value2")

		r := rdb.Do(ctx, "HSETNX", hashSetName, "field1", "value1-changed", "field2", "value2-changed",
			"field3", "value3", "field4", "value4")
		val, err := r.Int64()
		require.NoError(t, err)
		require.Equal(t, int64(2), val)

		value1 := rdb.HGet(ctx, hashSetName, "field1").Val()
		require.Equal(t, "value1", value1)

		value2 := rdb.HGet(ctx, hashSetName, "field2").Val()
		require.Equal(t, "value2", value2)

		value3 := rdb.HGet(ctx, hashSetName, "field3").Val()
		require.Equal(t, "value3", value3)

		value4 := rdb.HGet(ctx, hashSetName, "field4").Val()
		require.Equal(t, "value4", value4)

		rdb.Del(ctx, hashSetName)
	})

	t.Run("HMSET wrong number of args", func(t *testing.T) {
		pattern := ".*wrong number.*"
		util.ErrorRegexp(t, rdb.HMSet(ctx, "smallhash", "key1", "val1", "key2").Err(), pattern)
	})

	t.Run("HMSET - small hash", func(t *testing.T) {
		var args []string
		for key := range smallhash {
			newval := util.RandString(0, 8, util.Alpha)
			smallhash[key] = newval
			args = append(args, key, newval)
		}
		require.Equal(t, true, rdb.HMSet(ctx, "smallhash", args).Val())
	})

	t.Run("HMSET - big hash", func(t *testing.T) {
		var args []string
		for key := range bighash {
			newval := util.RandString(0, 8, util.Alpha)
			bighash[key] = newval
			args = append(args, key, newval)
		}
		require.Equal(t, true, rdb.HMSet(ctx, "bighash", args).Val())
	})

	t.Run("HMGET against non existing key and fields", func(t *testing.T) {
		var rv [][]interface{}
		cmd1 := rdb.HMGet(ctx, "doesntexist", "__123123123__", "__456456456__")
		rv = append(rv, cmd1.Val())
		cmd2 := rdb.HMGet(ctx, "smallhash", "__123123123__", "__456456456__")
		rv = append(rv, cmd2.Val())
		cmd3 := rdb.HMGet(ctx, "bighash", "__123123123__", "__456456456__")
		rv = append(rv, cmd3.Val())
		require.Equal(t, [][]interface{}{{interface{}(nil), interface{}(nil)}, {interface{}(nil), interface{}(nil)}, {interface{}(nil), interface{}(nil)}}, rv)
	})

	t.Run("HMGET against wrong type", func(t *testing.T) {
		rdb.Set(ctx, "wrongtype", "somevalue", time.Millisecond*1000)
		pattern := ".*wrong.*"
		util.ErrorRegexp(t, rdb.HMGet(ctx, "wrongtype", "field1", "field2").Err(), pattern)
	})

	t.Run("HMGET succeed field return with missing middle field", func(t *testing.T) {
		require.Equal(t, int64(2), rdb.HSet(ctx, "successreturn", "name", "1", "age", "2").Val())
		actual := rdb.HMGet(ctx, "successreturn", "name", "a", "age", "name").Val()
		require.Equal(t, []interface{}{"1", nil, "2", "1"}, actual)
	})

	t.Run("HMGET - small hash", func(t *testing.T) {
		var keys []string
		var vals []string
		for key, val := range smallhash {
			keys = append(keys, key)
			vals = append(vals, val)
		}
		var err error
		res := rdb.HMGet(ctx, "smallhash", keys...).Val()
		for i := range vals {
			if vals[i] != res[i].(string) {
				err = errors.New("$vals != $result")
				break
			}
		}
		require.NoError(t, err)
	})

	t.Run("HMGET - big hash", func(t *testing.T) {
		var keys []string
		var vals []string
		for key, val := range bighash {
			keys = append(keys, key)
			vals = append(vals, val)
		}
		var err error
		res := rdb.HMGet(ctx, "bighash", keys...).Val()
		for i := range vals {
			if vals[i] != res[i].(string) {
				err = errors.New("$vals != $result")
				break
			}
		}
		require.NoError(t, err)
	})

	t.Run("HKEYS - small hash}", func(t *testing.T) {
		expect := getKeys(smallhash)
		sort.Strings(expect)
		actual := rdb.HKeys(ctx, "smallhash").Val()
		sort.Strings(actual)
		require.Equal(t, expect, actual)
	})

	t.Run("HKEYS - big hash}", func(t *testing.T) {
		expect := getKeys(bighash)
		sort.Strings(expect)
		actual := rdb.HKeys(ctx, "bighash").Val()
		sort.Strings(actual)
		require.Equal(t, expect, actual)
	})

	t.Run("HVALS - small hash}", func(t *testing.T) {
		expect := getVals(smallhash)
		sort.Strings(expect)
		actual := rdb.HVals(ctx, "smallhash").Val()
		sort.Strings(actual)
		require.Equal(t, expect, actual)
	})

	t.Run("HVALS - big hash}", func(t *testing.T) {
		expect := getVals(bighash)
		sort.Strings(expect)
		actual := rdb.HVals(ctx, "bighash").Val()
		sort.Strings(actual)
		require.Equal(t, expect, actual)
	})

	t.Run("HVALS - field with empty string as a value", func(t *testing.T) {
		testKey := "test-hash-1"
		require.NoError(t, rdb.Del(ctx, testKey).Err())

		require.NoError(t, rdb.HSet(ctx, testKey, "field1", "some-value").Err())
		require.NoError(t, rdb.HSet(ctx, testKey, "field2", "").Err())

		require.Equal(t, []string{"some-value", ""}, rdb.HVals(ctx, testKey).Val())

		require.NoError(t, rdb.Del(ctx, testKey).Err())
	})

	t.Run("HGETALL - small hash}", func(t *testing.T) {
		gotHash, err := rdb.HGetAll(ctx, "smallhash").Result()
		require.NoError(t, err)
		require.Equal(t, smallhash, gotHash)
	})

	t.Run("HGETALL - big hash}", func(t *testing.T) {
		gotHash, err := rdb.HGetAll(ctx, "bighash").Result()
		require.NoError(t, err)
		require.Equal(t, bighash, gotHash)
	})

	t.Run("HGETALL - field with empty string as a value", func(t *testing.T) {
		testKey := "test-hash-1"
		require.NoError(t, rdb.Del(ctx, testKey).Err())

		require.NoError(t, rdb.HSet(ctx, testKey, "field1", "some-value").Err())
		require.NoError(t, rdb.HSet(ctx, testKey, "field2", "").Err())

		require.Equal(t, map[string]string{"field1": "some-value", "field2": ""}, rdb.HGetAll(ctx, testKey).Val())

		require.NoError(t, rdb.Del(ctx, testKey).Err())
	})

	t.Run("HDEL and return value", func(t *testing.T) {
		var rv []string
		rv = append(rv, fmt.Sprintf("%d", rdb.HDel(ctx, "smallhash", "nokey").Val()))
		rv = append(rv, fmt.Sprintf("%d", rdb.HDel(ctx, "bighash", "nokey").Val()))
		k := getKeys(smallhash)[0]
		rv = append(rv, fmt.Sprintf("%d", rdb.HDel(ctx, "smallhash", k).Val()))
		rv = append(rv, fmt.Sprintf("%d", rdb.HDel(ctx, "smallhash", k).Val()))
		rv = append(rv, rdb.HGet(ctx, "smallhash", k).Val())
		delete(smallhash, k)
		k = getKeys(bighash)[0]
		rv = append(rv, fmt.Sprintf("%d", rdb.HDel(ctx, "bighash", k).Val()))
		rv = append(rv, fmt.Sprintf("%d", rdb.HDel(ctx, "bighash", k).Val()))
		rv = append(rv, rdb.HGet(ctx, "bighash", k).Val())
		delete(bighash, k)
		require.Equal(t, []string{"0", "0", "1", "0", "", "1", "0", ""}, rv)
	})

	t.Run("HDEL - more than a single value", func(t *testing.T) {
		rdb.Del(ctx, "myhash")
		rdb.HMSet(ctx, "myhash", []string{"a", "1", "b", "2", "c", "3"})
		require.Equal(t, int64(0), rdb.HDel(ctx, "myhash", "x", "y").Val())
		require.Equal(t, int64(2), rdb.HDel(ctx, "myhash", "a", "c", "f").Val())
		require.Equal(t, map[string]string{"b": "2"}, rdb.HGetAll(ctx, "myhash").Val())
	})

	t.Run("HDEL - hash becomes empty before deleting all specified fields", func(t *testing.T) {
		rdb.Del(ctx, "myhash")
		rdb.HMSet(ctx, "myhash", []string{"a", "1", "b", "2", "c", "3"})
		require.Equal(t, int64(3), rdb.HDel(ctx, "myhash", "a", "b", "c", "d", "e").Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, "myhash").Val())
	})

	t.Run("HEXISTS", func(t *testing.T) {
		var rv []bool
		k := getKeys(smallhash)[0]
		rv = append(rv, rdb.HExists(ctx, "smallhash", k).Val())
		rv = append(rv, rdb.HExists(ctx, "smallhash", "nokey").Val())
		k = getKeys(bighash)[0]
		rv = append(rv, rdb.HExists(ctx, "bighash", k).Val())
		rv = append(rv, rdb.HExists(ctx, "bighash", "nokey").Val())
		require.Equal(t, []bool{true, false, true, false}, rv)
	})

	t.Run("HINCRBY against non existing database key", func(t *testing.T) {
		rdb.Del(ctx, "htest")
		require.Equal(t, int64(2), rdb.HIncrBy(ctx, "htest", "foo", 2).Val())
	})

	t.Run("HINCRBY against non existing hash key", func(t *testing.T) {
		var rv []string
		rdb.HDel(ctx, "smallhash", "tmp")
		rdb.HDel(ctx, "bighash", "tmp")
		rv = append(rv, fmt.Sprintf("%d", rdb.HIncrBy(ctx, "smallhash", "tmp", 2).Val()))
		rv = append(rv, rdb.HGet(ctx, "smallhash", "tmp").Val())
		rv = append(rv, fmt.Sprintf("%d", rdb.HIncrBy(ctx, "bighash", "tmp", 2).Val()))
		rv = append(rv, rdb.HGet(ctx, "bighash", "tmp").Val())
		require.Equal(t, []string{"2", "2", "2", "2"}, rv)
	})

	t.Run("HINCRBY against hash key created by hincrby itself", func(t *testing.T) {
		var rv []string
		rv = append(rv, fmt.Sprintf("%d", rdb.HIncrBy(ctx, "smallhash", "tmp", 3).Val()))
		rv = append(rv, rdb.HGet(ctx, "smallhash", "tmp").Val())
		rv = append(rv, fmt.Sprintf("%d", rdb.HIncrBy(ctx, "bighash", "tmp", 3).Val()))
		rv = append(rv, rdb.HGet(ctx, "bighash", "tmp").Val())
		require.Equal(t, []string{"5", "5", "5", "5"}, rv)
	})

	t.Run("HINCRBY against hash key originally set with HSET", func(t *testing.T) {
		rdb.HSet(ctx, "smallhash", "tmp", "100")
		rdb.HSet(ctx, "bighash", "tmp", "100")
		require.Equal(t, []int64{102, 102},
			[]int64{rdb.HIncrBy(ctx, "smallhash", "tmp", 2).Val(),
				rdb.HIncrBy(ctx, "bighash", "tmp", 2).Val()})
	})

	t.Run("HINCRBY over 32bit value", func(t *testing.T) {
		rdb.HSet(ctx, "smallhash", "tmp", "17179869184")
		rdb.HSet(ctx, "bighash", "tmp", "17179869184")
		require.Equal(t, []int64{17179869185, 17179869185},
			[]int64{rdb.HIncrBy(ctx, "smallhash", "tmp", 1).Val(),
				rdb.HIncrBy(ctx, "bighash", "tmp", 1).Val()})
	})

	t.Run("HINCRBY over 32bit value with over 32bit increment", func(t *testing.T) {
		rdb.HSet(ctx, "smallhash", "tmp", "17179869184")
		rdb.HSet(ctx, "bighash", "tmp", "17179869184")
		require.Equal(t, []int64{34359738368, 34359738368},
			[]int64{rdb.HIncrBy(ctx, "smallhash", "tmp", 17179869184).Val(),
				rdb.HIncrBy(ctx, "bighash", "tmp", 17179869184).Val()})
	})

	t.Run("HINCRBY fails against hash value with spaces (left)", func(t *testing.T) {
		rdb.HSet(ctx, "samllhash", "str", " 11")
		rdb.HSet(ctx, "bighash", "str", " 11")
		pattern := "ERR.*not an integer.*"
		util.ErrorRegexp(t, rdb.HIncrBy(ctx, "samllhash", "str", 1).Err(), pattern)
		util.ErrorRegexp(t, rdb.HIncrBy(ctx, "bighash", "str", 1).Err(), pattern)
	})

	t.Run("HINCRBY fails against hash value with spaces (right)", func(t *testing.T) {
		rdb.HSet(ctx, "samllhash", "str", "11 ")
		rdb.HSet(ctx, "bighash", "str", "11 ")
		pattern := "ERR.*non-integer.*"
		util.ErrorRegexp(t, rdb.HIncrBy(ctx, "samllhash", "str", 1).Err(), pattern)
		util.ErrorRegexp(t, rdb.HIncrBy(ctx, "bighash", "str", 1).Err(), pattern)
	})

	t.Run("HINCRBY can detect overflows", func(t *testing.T) {
		rdb.HSet(ctx, "hash", "n", "-9223372036854775484")
		require.Equal(t, int64(-9223372036854775485), rdb.HIncrBy(ctx, "hash", "n", -1).Val())
		pattern := ".*overflow.*"
		util.ErrorRegexp(t, rdb.HIncrBy(ctx, "hash", "n", -10000).Err(), pattern)
	})

	t.Run("HINCRBYFLOAT against non existing database key", func(t *testing.T) {
		rdb.Del(ctx, "htest")
		require.Equal(t, 2.5, rdb.HIncrByFloat(ctx, "htest", "foo", 2.5).Val())
	})

	t.Run("HINCRBYFLOAT against non existing hash key", func(t *testing.T) {
		var rv []float64
		rdb.HDel(ctx, "smallhash", "tmp")
		rdb.HDel(ctx, "bighash", "tmp")
		rv = append(rv, rdb.HIncrByFloat(ctx, "smallhash", "tmp", 2.5).Val())
		if res, err := strconv.ParseFloat(rdb.HGet(ctx, "smallhash", "tmp").Val(), strconv.IntSize); err == nil {
			rv = append(rv, res)
		}
		rv = append(rv, rdb.HIncrByFloat(ctx, "bighash", "tmp", 2.5).Val())
		if res, err := strconv.ParseFloat(rdb.HGet(ctx, "bighash", "tmp").Val(), strconv.IntSize); err == nil {
			rv = append(rv, res)
		}
		require.Equal(t, []float64{2.5, 2.5, 2.5, 2.5}, rv)
	})

	t.Run("HINCRBYFLOAT against hash key created by hincrby itself", func(t *testing.T) {
		var rv []float64
		rv = append(rv, rdb.HIncrByFloat(ctx, "smallhash", "tmp", 3.5).Val())
		if res, err := strconv.ParseFloat(rdb.HGet(ctx, "smallhash", "tmp").Val(), strconv.IntSize); err == nil {
			rv = append(rv, res)
		}
		rv = append(rv, rdb.HIncrByFloat(ctx, "bighash", "tmp", 3.5).Val())
		if res, err := strconv.ParseFloat(rdb.HGet(ctx, "bighash", "tmp").Val(), strconv.IntSize); err == nil {
			rv = append(rv, res)
		}
		require.Equal(t, []float64{6, 6, 6, 6}, rv)
	})

	t.Run("HINCRBYFLOAT against hash key originally set with HSET", func(t *testing.T) {
		var rv []float64
		rdb.HSet(ctx, "smallhash", "tmp", 100)
		rdb.HSet(ctx, "bighash", "tmp", 100)
		rv = append(rv, rdb.HIncrByFloat(ctx, "smallhash", "tmp", 2.5).Val())
		rv = append(rv, rdb.HIncrByFloat(ctx, "bighash", "tmp", 2.5).Val())
		require.Equal(t, []float64{102.5, 102.5}, rv)
	})

	t.Run("HINCRBYFLOAT over 32bit value", func(t *testing.T) {
		rdb.HSet(ctx, "smallhash", "tmp", "17179869184")
		rdb.HSet(ctx, "bighash", "tmp", "17179869184")
		require.Equal(t, []float64{17179869185, 17179869185},
			[]float64{rdb.HIncrByFloat(ctx, "smallhash", "tmp", 1).Val(),
				rdb.HIncrByFloat(ctx, "bighash", "tmp", 1).Val()})
	})

	t.Run("HINCRBYFLOAT over 32bit value with over 32bit increment", func(t *testing.T) {
		rdb.HSet(ctx, "smallhash", "tmp", "17179869184")
		rdb.HSet(ctx, "bighash", "tmp", "17179869184")
		require.Equal(t, []float64{34359738368, 34359738368},
			[]float64{rdb.HIncrByFloat(ctx, "smallhash", "tmp", 17179869184).Val(),
				rdb.HIncrByFloat(ctx, "bighash", "tmp", 17179869184).Val()})
	})

	t.Run("HINCRBYFLOAT fails against hash value with spaces (left)", func(t *testing.T) {
		rdb.HSet(ctx, "samllhash", "str", " 11")
		rdb.HSet(ctx, "bighash", "str", " 11")
		pattern := "ERR.*not.*number.*"
		util.ErrorRegexp(t, rdb.HIncrByFloat(ctx, "samllhash", "str", 1).Err(), pattern)
		util.ErrorRegexp(t, rdb.HIncrByFloat(ctx, "bighash", "str", 1).Err(), pattern)
	})

	t.Run("HINCRBYFLOAT fails against hash value with spaces (right)", func(t *testing.T) {
		rdb.HSet(ctx, "samllhash", "str", "11 ")
		rdb.HSet(ctx, "bighash", "str", "11 ")
		pattern := "ERR.*not.*number.*"
		util.ErrorRegexp(t, rdb.HIncrByFloat(ctx, "samllhash", "str", 1).Err(), pattern)
		util.ErrorRegexp(t, rdb.HIncrByFloat(ctx, "bighash", "str", 1).Err(), pattern)
	})

	t.Run("HSTRLEN against the small hash", func(t *testing.T) {
		var err error
		for _, k := range getKeys(smallhash) {
			hlen := rdb.Do(ctx, "hstrlen", "smallhash", k).Val().(int64)
			if int64(len(smallhash[k])) != hlen {
				err = fmt.Errorf("[string length %d] != [r hstrlen %d]", len(smallhash[k]), hlen)
				break
			}
		}
		require.NoError(t, err)
	})

	t.Run("HSTRLEN against the big hash", func(t *testing.T) {
		var err error
		for _, k := range getKeys(bighash) {
			hlen := rdb.Do(ctx, "hstrlen", "bighash", k).Val().(int64)
			if int64(len(bighash[k])) != hlen {
				err = fmt.Errorf("[string length %d] != [r hstrlen %d]", len(bighash[k]), hlen)
				break
			}
		}
		require.NoError(t, err)
	})

	t.Run("HSTRLEN against non existing field", func(t *testing.T) {
		var rv []int64
		rv = append(rv, rdb.Do(ctx, "hstrlen", "smallhash", "__123123123__").Val().(int64))
		rv = append(rv, rdb.Do(ctx, "hstrlen", "bighash", "__123123123__").Val().(int64))
		require.Equal(t, []int64{0, 0}, rv)
	})

	t.Run("HSTRLEN corner cases", func(t *testing.T) {
		vals := []string{"-9223372036854775808", "9223372036854775807", "9223372036854775808", "", "0", "-1", "x"}
		for _, v := range vals {
			rdb.HMSet(ctx, "smallhash", "field", v)
			rdb.HMSet(ctx, "bighash", "field", v)
			len1 := int64(len(v))
			len2 := rdb.Do(ctx, "hstrlen", "smallhash", "field").Val().(int64)
			len3 := rdb.Do(ctx, "hstrlen", "bighash", "field").Val().(int64)
			require.Equal(t, len1, len2)
			require.Equal(t, len2, len3)
		}
	})

	t.Run("Hash ziplist regression test for large keys", func(t *testing.T) {
		rdb.HSet(ctx, "hash", strings.Repeat("k", 336), "a")
		rdb.HSet(ctx, "hash", strings.Repeat("k", 336), "b")
		require.Equal(t, "b", rdb.HGet(ctx, "hash", strings.Repeat("k", 336)).Val())
	})

	for _, size := range []int64{10, 512} {
		t.Run(fmt.Sprintf("Hash fuzzing #1 - %d fields", size), func(t *testing.T) {
			for times := 0; times < 10; times++ {
				hash := make(map[string]string)
				rdb.Del(ctx, "hash")

				// Create
				for j := 0; j < int(size); j++ {
					field := util.RandomValue()
					value := util.RandomValue()
					rdb.HSet(ctx, "hash", field, value)
					hash[field] = value
				}

				// Verify
				for k, v := range hash {
					require.Equal(t, v, rdb.HGet(ctx, "hash", k).Val())
				}
				require.Equal(t, int64(len(hash)), rdb.HLen(ctx, "hash").Val())
			}
		})

		t.Run(fmt.Sprintf("Hash fuzzing #2 - %d fields", size), func(t *testing.T) {
			for times := 0; times < 10; times++ {
				hash := make(map[string]string)
				rdb.Del(ctx, "hash")
				var field string
				var value string

				// Create
				for j := 0; j < int(size); j++ {
					util.RandPath(
						func() interface{} {
							field = util.RandomValue()
							value = util.RandomValue()
							rdb.HSet(ctx, "hash", field, value)
							hash[field] = value
							return nil
						},
						func() interface{} {
							field = strconv.Itoa(int(util.RandomSignedInt(512)))
							value = strconv.Itoa(int(util.RandomSignedInt(512)))
							rdb.HSet(ctx, "hash", field, value)
							hash[field] = value
							return nil
						},
						func() interface{} {
							util.RandPath(
								func() interface{} {
									field = util.RandomValue()
									return nil
								},
								func() interface{} {
									field = strconv.Itoa(int(util.RandomSignedInt(512)))
									return nil
								},
							)
							rdb.HDel(ctx, "hash", field)
							delete(hash, field)
							return nil
						},
					)
				}
				// Verify
				for k, v := range hash {
					require.Equal(t, v, rdb.HGet(ctx, "hash", k).Val())
				}
				require.Equal(t, int64(len(hash)), rdb.HLen(ctx, "hash").Val())
			}
		})

		kvArray := []string{"a", "a", "b", "b", "c", "c", "d", "d", "e", "e", "key1", "value1", "key2", "value2", "key3", "value3", "key10", "value10", "z", "z", "x", "x"}
		t.Run("HrangeByLex BYLEX normal situation ", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "hashkey").Err())
			require.NoError(t, rdb.HMSet(ctx, "hashkey", kvArray).Err())
			require.EqualValues(t, []interface{}{"key1", "value1", "key10", "value10"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[key1", "[key10").Val())
			require.EqualValues(t, []interface{}{"key1", "value1", "key10", "value10", "key2", "value2"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[key1", "[key2").Val())
			require.EqualValues(t, []interface{}{"key1", "value1", "key10", "value10", "key2", "value2"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[key1", "(key3").Val())
			require.EqualValues(t, []interface{}{"key10", "value10", "key2", "value2", "key3", "value3"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "(key1", "[key3", "limit", 0, -1).Val())
			require.EqualValues(t, []interface{}{"key10", "value10", "key2", "value2"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "(key1", "(key3").Val())
			require.EqualValues(t, []interface{}{"a", "a", "b", "b"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "-", "[b").Val())
			require.EqualValues(t, []interface{}{"x", "x", "z", "z"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[x", "+").Val())
			require.EqualValues(t, []interface{}{"z", "z", "x", "x"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "+", "[x", "REV").Val())
			require.EqualValues(t, []interface{}{"a", "a", "b", "b", "c", "c", "d", "d", "e", "e", "key1", "value1", "key10", "value10", "key2", "value2", "key3", "value3", "x", "x", "z", "z"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "-", "+").Val())
			require.EqualValues(t, []interface{}{"z", "z", "x", "x", "key3", "value3", "key2", "value2", "key10", "value10", "key1", "value1", "e", "e", "d", "d", "c", "c", "b", "b", "a", "a"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "+", "-", "REV").Val())
		})

		t.Run("HrangeByLex BYLEX stop < start", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "hashkey").Err())
			require.NoError(t, rdb.HMSet(ctx, "hashkey", kvArray).Err())
			require.EqualValues(t, []interface{}{}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[key2", "[key1", "limit", 0, 100).Val())
			require.EqualValues(t, []interface{}{}, rdb.Do(ctx, "HrangeByLex", "hashkey", "(key1", "(key1", "limit", 0, 100).Val())
		})

		t.Run("HrangeByLex BYLEX limit", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "hashkey").Err())
			require.NoError(t, rdb.HMSet(ctx, "hashkey", kvArray).Err())
			require.EqualValues(t, []interface{}{"a", "a", "b", "b"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[a", "[z", "limit", 0, 2).Val())
			require.EqualValues(t, []interface{}{"z", "z", "x", "x"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[z", "[a", "limit", 0, 2, "REV").Val())
			require.EqualValues(t, []interface{}{"b", "b", "c", "c"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[a", "[z", "limit", 1, 2).Val())
			require.EqualValues(t, []interface{}{"x", "x", "key3", "value3"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[z", "[a", "limit", 1, 2, "REV").Val())
			require.EqualValues(t, []interface{}{}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[a", "[z", "limit", 1000, -1).Val())
			require.EqualValues(t, []interface{}{}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[a", "[z", "limit", 0, 0).Val())
			require.EqualValues(t, []interface{}{"a", "a", "b", "b", "c", "c", "d", "d", "e", "e", "key1", "value1", "key10", "value10", "key2", "value2", "key3", "value3", "x", "x", "z", "z"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[a", "[zzz", "limit", 0, 10000).Val())
		})

		t.Run("HrangeByLex BYLEX limit is negative", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "hashkey").Err())
			require.NoError(t, rdb.HMSet(ctx, "hashkey", kvArray).Err())
			require.EqualValues(t, []interface{}{"x", "x", "z", "z"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[x", "[z", "limit", 0, -100).Val())
			require.EqualValues(t, []interface{}{"z", "z", "x", "x"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[z", "[x", "limit", 0, -100, "REV").Val())
			require.EqualValues(t, []interface{}{"x", "x", "z", "z"}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[x", "[z", "limit", 0, -10).Val())
		})

		t.Run("HrangeByLex BYLEX nonexistent key", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "hashkey").Err())
			require.EqualValues(t, []interface{}{}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[a", "[z").Val())
			require.EqualValues(t, []interface{}{}, rdb.Do(ctx, "HrangeByLex", "hashkey", "[a", "[z").Val())
		})

		t.Run("HrangeByLex typo", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "hashkey").Err())
			require.NoError(t, rdb.HMSet(ctx, "hashkey", kvArray).Err())
			require.ErrorContains(t, rdb.Do(ctx, "HrangeByLex", "hashkey", "[a", "[z", "limitzz", 0, 10000).Err(), "ERR syntax")
			require.ErrorContains(t, rdb.Do(ctx, "HrangeByLex", "hashkey", "[a", "[z", "limit", 0, 10000, "BYLE").Err(), "ERR syntax")
			require.ErrorContains(t, rdb.Do(ctx, "HrangeByLex", "hashkey", "[a", "[z", "limit", 0, 10000, "RE").Err(), "ERR syntax")
			require.ErrorContains(t, rdb.Do(ctx, "HrangeByLex", "hashkey", "a", "z").Err(), "illegal")
		})

		t.Run("HrangeByLex wrong number of arguments", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "hashkey").Err())
			require.NoError(t, rdb.HMSet(ctx, "hashkey", kvArray).Err())
			require.ErrorContains(t, rdb.Do(ctx, "HrangeByLex", "hashkey", "[a", "[z", "limit", 10000, 1, 1, 1, 1).Err(), "syntax error")
			require.ErrorContains(t, rdb.Do(ctx, "HrangeByLex", "hashkey", "[a", "[z", "limit").Err(), "no more item to parse")
			require.ErrorContains(t, rdb.Do(ctx, "HrangeByLex", "hashkey", "[a").Err(), "wrong number of arguments")
			require.ErrorContains(t, rdb.Do(ctx, "HrangeByLex", "hashkey").Err(), "wrong number of arguments")
			require.ErrorContains(t, rdb.Do(ctx, "HrangeByLex").Err(), "wrong number of arguments")
		})

		t.Run("HrangeByLex - field with empty string as a value", func(t *testing.T) {
			testKey := "test-hash-1"
			require.NoError(t, rdb.Del(ctx, testKey).Err())

			require.NoError(t, rdb.HSet(ctx, testKey, "field1", "some-value").Err())
			require.NoError(t, rdb.HSet(ctx, testKey, "field2", "").Err())

			require.Equal(t, []interface{}{"field1", "some-value", "field2", ""}, rdb.Do(ctx, "HrangeByLex", testKey, "[a", "[z").Val())
		})

		t.Run("HRandField count is positive", func(t *testing.T) {
			testKey := "test-hash-1"
			require.NoError(t, rdb.Del(ctx, testKey).Err())
			require.NoError(t, rdb.HSet(ctx, testKey, "key1", "value1", "key2", "value2", "key3", "value3").Err())
			result, err := rdb.HRandField(ctx, testKey, 5).Result()
			require.NoError(t, err)
			require.Len(t, result, 3)
			require.Equal(t, []string{"key1", "key2", "key3"}, result)
			result, err = rdb.HRandField(ctx, testKey, 2).Result()
			require.NoError(t, err)
			require.Len(t, result, 2)
			require.Contains(t, []string{"key1", "key2", "key3"}, result[0])
			require.Contains(t, []string{"key1", "key2", "key3"}, result[1])
			result, err = rdb.HRandField(ctx, testKey, 0).Result()
			require.NoError(t, err)
			require.Len(t, result, 0)
			result, err = rdb.HRandField(ctx, "nonexistent-key", 1).Result()
			require.NoError(t, err)
			require.Len(t, result, 0)
			var rv [][]interface{}
			resultWithValues, err := rdb.HRandFieldWithValues(ctx, testKey, 5).Result()
			require.NoError(t, err)
			require.Len(t, resultWithValues, 3)
			for _, kv := range resultWithValues {
				keys := []interface{}{kv.Key, kv.Value}
				rv = append(rv, keys)
			}
			require.Equal(t, [][]interface{}{
				{"key1", "value1"},
				{"key2", "value2"},
				{"key3", "value3"},
			}, rv)
			// TODO: Add test to verify randomness of the selected random fields
		})

		t.Run("HRandField count is negative", func(t *testing.T) {
			testKey := "test-hash-1"
			require.NoError(t, rdb.Del(ctx, testKey).Err())
			require.NoError(t, rdb.HSet(ctx, testKey, "key1", "value1", "key2", "value2", "key3", "value3").Err())
			result, err := rdb.HRandField(ctx, testKey, -4).Result()
			require.NoError(t, err)
			require.Len(t, result, 4)
			resultWithValues, err := rdb.HRandFieldWithValues(ctx, testKey, -12).Result()
			require.NoError(t, err)
			require.Len(t, resultWithValues, 12)
			// TODO: Add test to verify randomness of the selected random fields
		})

		t.Run("HGetAll support map type", func(t *testing.T) {
			testKey := "test-hash-1"
			require.NoError(t, rdb.Del(ctx, testKey).Err())
			require.NoError(t, rdb.HSet(ctx, testKey, "key1", "value1", "key2", "value2", "key3", "value3").Err())
			result, err := rdb.HGetAll(ctx, testKey).Result()
			require.NoError(t, err)
			require.Len(t, result, 3)
			require.EqualValues(t, map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			}, result)
		})

		t.Run("Test bug with large value after compaction", func(t *testing.T) {
			testKey := "test-hash-1"
			require.NoError(t, rdb.Del(ctx, testKey).Err())

			src := rand.NewSource(time.Now().UnixNano())
			dd := make([]byte, 5000)
			for i := 1; i <= 50; i++ {
				for j := range dd {
					dd[j] = byte(src.Int63())
				}
				key := util.RandString(10, 20, util.Alpha)
				require.NoError(t, rdb.HSet(ctx, testKey, key, string(dd)).Err())
			}

			require.EqualValues(t, 50, rdb.HLen(ctx, testKey).Val())
			require.Len(t, rdb.HGetAll(ctx, testKey).Val(), 50)
			require.Len(t, rdb.HKeys(ctx, testKey).Val(), 50)
			require.Len(t, rdb.HVals(ctx, testKey).Val(), 50)

			require.NoError(t, rdb.Do(ctx, "COMPACT").Err())

			time.Sleep(5 * time.Second)

			require.EqualValues(t, 50, rdb.HLen(ctx, testKey).Val())
			require.Len(t, rdb.HGetAll(ctx, testKey).Val(), 50)
			require.Len(t, rdb.HKeys(ctx, testKey).Val(), 50)
			require.Len(t, rdb.HVals(ctx, testKey).Val(), 50)
		})
	}
}

func TestDisableExpireField(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"hash-field-expiration": "no",
	})
	defer srv.Close()

	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	ctx := context.Background()

	// can't expire fields when hash-field-expiration option is no
	expectedErrMsg := "ERR field expiration feature is disabled"
	require.ErrorContains(t, rdb.HExpire(ctx, "foo", time.Second, "f").Err(), expectedErrMsg)
	require.ErrorContains(t, rdb.HPExpire(ctx, "foo", time.Second, "f").Err(), expectedErrMsg)
	require.ErrorContains(t, rdb.HExpireAt(ctx, "foo", time.Now().Add(1*time.Second), "f").Err(), expectedErrMsg)
	require.ErrorContains(t, rdb.HPExpireAt(ctx, "foo", time.Now().Add(1*time.Second), "f").Err(), expectedErrMsg)

	rdb.HSet(ctx, "foo", "f", "v")
	require.NoError(t, rdb.ConfigSet(ctx, "hash-field-expiration", "yes").Err())
	require.Equal(t, "v", rdb.HGet(ctx, "foo", "f").Val())

	// can't expire fields on hash object whose field expiration feature is disabled
	expectedErrMsg = "can't expire fields on hash object whose field expiration feature is disabled"
	require.ErrorContains(t, rdb.HExpire(ctx, "foo", time.Second, "f").Err(), expectedErrMsg)
	require.ErrorContains(t, rdb.HPExpire(ctx, "foo", time.Second, "f").Err(), expectedErrMsg)
	require.ErrorContains(t, rdb.HExpireAt(ctx, "foo", time.Now().Add(1*time.Second), "f").Err(), expectedErrMsg)
	require.ErrorContains(t, rdb.HPExpireAt(ctx, "foo", time.Now().Add(1*time.Second), "f").Err(), expectedErrMsg)
}

func TestHashFieldExpiration(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"hash-field-expiration": "yes",
	})
	defer srv.Close()

	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	ctx := context.Background()

	t.Run("HFE expire a field of hash", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())
		require.Equal(t, true, rdb.HMSet(ctx, "hfe-key", "f1", "v1").Val())
		require.Equal(t, int64(1), rdb.Do(ctx, "HEXPIRE", "hfe-key", 1, "FIELDS", 1, "f1").Val().([]interface{})[0])

		require.LessOrEqual(t, int64(0), rdb.Do(ctx, "HTTL", "hfe-key", "FIELDS", 1, "f1").Val().([]interface{})[0])
		require.Equal(t, "v1", rdb.HGet(ctx, "hfe-key", "f1").Val())
		time.Sleep(1 * time.Second)

		require.Equal(t, int64(-2), rdb.Do(ctx, "HTTL", "hfe-key", "FIELDS", 1, "f1").Val().([]interface{})[0])
		require.Equal(t, "", rdb.HGet(ctx, "hfe-key", "f1").Val())
	})

	t.Run("HFE expireat a field of hash", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())
		require.Equal(t, true, rdb.HMSet(ctx, "hfe-key", "f1", "v1").Val())

		expireTime := time.Now().Add(1 * time.Second).Unix()
		require.Equal(t, int64(1), rdb.Do(ctx, "HEXPIREAT", "hfe-key", expireTime, "FIELDS", 1, "f1").Val().([]interface{})[0])
		require.Equal(t, expireTime, rdb.Do(ctx, "HEXPIRETIME", "hfe-key", "FIELDS", 1, "f1").Val().([]interface{})[0])
		require.Equal(t, "v1", rdb.HGet(ctx, "hfe-key", "f1").Val())
		time.Sleep(1 * time.Second)

		require.Equal(t, int64(-2), rdb.Do(ctx, "HEXPIRETIME", "hfe-key", "FIELDS", 1, "f1").Val().([]interface{})[0])
		require.Equal(t, "", rdb.HGet(ctx, "hfe-key", "f1").Val())
	})

	t.Run("HFE check the ttl of field that no associated expiration set and not exist", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())
		require.Equal(t, true, rdb.HMSet(ctx, "hfe-key", "f1", "v1").Val())

		ttl := rdb.Do(ctx, "HTTL", "hfe-key", "FIELDS", 2, "f1", "not-exist-field").Val().([]interface{})
		require.EqualValues(t, []interface{}{int64(-1), int64(-2)}, ttl)
		pttl := rdb.Do(ctx, "HPTTL", "hfe-key", "FIELDS", 2, "f1", "not-exist-field").Val().([]interface{})
		require.EqualValues(t, []interface{}{int64(-1), int64(-2)}, pttl)

		expireTime := rdb.Do(ctx, "HEXPIRETIME", "hfe-key", "FIELDS", 2, "f1", "not-exist-field").Val().([]interface{})
		require.EqualValues(t, []interface{}{int64(-1), int64(-2)}, expireTime)
		pexpireTime := rdb.Do(ctx, "HPEXPIRETIME", "hfe-key", "FIELDS", 2, "f1", "not-exist-field").Val().([]interface{})
		require.EqualValues(t, []interface{}{int64(-1), int64(-2)}, pexpireTime)
	})

	t.Run("HFE can not get expired field", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())
		fieldValue := map[string]string{
			"f1": "v1",
			"f2": "v2",
		}
		require.Equal(t, int64(2), rdb.HSet(ctx, "hfe-key", fieldValue).Val())
		require.Equal(t, int64(1), rdb.Do(ctx, "HEXPIRE", "hfe-key", 1, "FIELDS", 1, "f1").Val().([]interface{})[0])

		require.Equal(t, fieldValue, rdb.HGetAll(ctx, "hfe-key").Val())
		require.Equal(t, []interface{}{"v1", "v2"}, rdb.HMGet(ctx, "hfe-key", "f1", "f2").Val())

		time.Sleep(1 * time.Second)
		delete(fieldValue, "f1")

		require.Equal(t, fieldValue, rdb.HGetAll(ctx, "hfe-key").Val())
		require.Equal(t, []interface{}{nil, "v2"}, rdb.HMGet(ctx, "hfe-key", "f1", "f2").Val())
	})

	t.Run("HFE check hash metadata after all of fields expired", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())
		require.Equal(t, true, rdb.HMSet(ctx, "hfe-key", "f1", "v1", "f2", "v2").Val())

		require.Equal(t, "hash", rdb.Type(ctx, "hfe-key").Val())
		require.Equal(t, int64(2), rdb.HLen(ctx, "hfe-key").Val())

		rdb.Do(ctx, "HEXPIRE", "hfe-key", 1, "FIELDS", 2, "f1", "f2")
		time.Sleep(1 * time.Second)

		// if a hash object than all of fields was expired,
		// the hash object should be treated not exist.
		require.Equal(t, int64(0), rdb.HLen(ctx, "hfe-key").Val())
		require.Equal(t, "none", rdb.Type(ctx, "hfe-key").Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, "hfe-key").Val())
		require.Equal(t, time.Duration(-2), rdb.TTL(ctx, "hfe-key").Val())
		require.Equal(t, time.Duration(-2), rdb.PTTL(ctx, "hfe-key").Val())
		require.Equal(t, time.Duration(-2), rdb.ExpireTime(ctx, "hfe-key").Val())
		require.Equal(t, time.Duration(-2), rdb.PExpireTime(ctx, "hfe-key").Val())
		require.Equal(t, false, rdb.ExpireAt(ctx, "hfe-key", time.Now().Add(1*time.Second)).Val())
		require.Equal(t, false, rdb.PExpireAt(ctx, "hfe-key", time.Unix(time.Now().Unix()+1, 0)).Val())
		require.Equal(t, int64(0), rdb.Copy(ctx, "hfe-key", "dst", 0, true).Val())
		require.Equal(t, "", rdb.Dump(ctx, "hfe-key").Val())
		require.Equal(t, int64(0), rdb.Del(ctx, "hfe-key").Val())
	})

	t.Run("HFE expected 0 if delete a expired field", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())
		require.Equal(t, true, rdb.HMSet(ctx, "hfe-key", "f1", "v1", "f2", "v2").Val())
		rdb.Do(ctx, "HPEXPIRE", "hfe-key", 100, "FIELDS", 1, "f1")
		time.Sleep(500 * time.Millisecond)
		require.Equal(t, int64(0), rdb.HDel(ctx, "hfe-key", "f1").Val())
	})

	t.Run("HFE persist a field of hash", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())
		require.Equal(t, true, rdb.HMSet(ctx, "hfe-key", "f1", "v1", "f2", "v2").Val())
		rdb.Do(ctx, "HPEXPIRE", "hfe-key", 100, "FIELDS", 1, "f1")

		result := rdb.Do(ctx, "HPERSIST", "hfe-key", "FIELDS", 3, "f1", "f2", "not-exist-field").Val().([]interface{})
		require.EqualValues(t, []interface{}{int64(1), int64(-1), int64(-2)}, result)
		require.Equal(t, int64(-1), rdb.Do(ctx, "HTTL", "hfe-key", "FIELDS", 1, "f1").Val().([]interface{})[0])
	})

	t.Run("HFE expired field should not be scan", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())
		require.Equal(t, int64(2), rdb.HSet(ctx, "hfe-key", "f1", "v1", "f2", "v2").Val())

		keys, cursor := rdb.HScan(ctx, "hfe-key", 0, "*", 10).Val()
		require.Equal(t, []string{"f1", "v1", "f2", "v2"}, keys)
		require.Equal(t, uint64(0), cursor)

		rdb.Do(ctx, "HEXPIRE", "hfe-key", 1, "FIELDS", 1, "f1")
		time.Sleep(1 * time.Second)

		keys, cursor = rdb.HScan(ctx, "hfe-key", 0, "*", 10).Val()
		require.Equal(t, []string{"f2", "v2"}, keys)
		require.Equal(t, uint64(0), cursor)
	})

	t.Run("HFE expire or ttl a not hash object", func(t *testing.T) {
		require.Equal(t, "OK", rdb.Set(ctx, "k", "v", 0).Val())
		require.ErrorContains(t, rdb.Do(ctx, "HEXPIRE", "k", 1, "FIELDS", 1, "f").Err(), "WRONGTYPE")
		require.ErrorContains(t, rdb.Do(ctx, "HPEXPIRE", "k", 1, "FIELDS", 1, "f").Err(), "WRONGTYPE")
		require.ErrorContains(t, rdb.Do(ctx, "HEXPIREAT", "k", 1, "FIELDS", 1, "f").Err(), "WRONGTYPE")
		require.ErrorContains(t, rdb.Do(ctx, "HPEXPIREAT", "k", 1, "FIELDS", 1, "f").Err(), "WRONGTYPE")
		require.ErrorContains(t, rdb.Do(ctx, "HTTL", "k", "FIELDS", 1, "f").Err(), "WRONGTYPE")
		require.ErrorContains(t, rdb.Do(ctx, "HPTTL", "k", "FIELDS", 1, "f").Err(), "WRONGTYPE")
		require.ErrorContains(t, rdb.Do(ctx, "HEXPIRETIME", "k", "FIELDS", 1, "f").Err(), "WRONGTYPE")
		require.ErrorContains(t, rdb.Do(ctx, "HPEXPIRETIME", "k", "FIELDS", 1, "f").Err(), "WRONGTYPE")
	})

	t.Run("HEF syntax check", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "HEXPIRE", "k", 1, "FIELDSS", 1, "f1").Err(), "syntax error")
		require.ErrorContains(t, rdb.Do(ctx, "HTTL", "k", 1, "FIELDSS", 1, "f1").Err(), "syntax error")
		require.ErrorContains(t, rdb.Do(ctx, "HEXPIRE", "k", "FIELDSS", 1, "f1").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "HEXPIRE", "k", 1, "FIELDS", 1, "f1", "f2").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "HEXPIRE", "k", 1, "FIELDS", 2, "f1").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "HEXPIRE", "k", 1, "FIELDS", 0, "f1").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "HEXPIRE", "k", 1, "FIELDS", 0).Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "HEXPIRE", "k", 1, "FIELDS", -1, "f1").Err(), "wrong number of arguments")
	})

	t.Run("HFE expire or expireat with a pass time", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())

		require.Equal(t, int64(1), rdb.HSet(ctx, "hfe-key", "f1", "v1").Val())
		require.Equal(t, int64(2), rdb.Do(ctx, "HEXPIRE", "hfe-key", 0, "FIELDS", 1, "f1").Val().([]interface{})[0])
		require.Equal(t, int64(-2), rdb.Do(ctx, "HTTL", "hfe-key", "FIELDS", 1, "f1").Val().([]interface{})[0])
		require.Equal(t, "", rdb.HGet(ctx, "hfe-key", "f1").Val())

		require.Equal(t, int64(1), rdb.HSet(ctx, "hfe-key", "f1", "v1").Val())
		require.Equal(t, int64(2), rdb.Do(ctx, "HEXPIREAT", "hfe-key", 0, "FIELDS", 1, "f1").Val().([]interface{})[0])
		require.Equal(t, int64(-2), rdb.Do(ctx, "HTTL", "hfe-key", "FIELDS", 1, "f1").Val().([]interface{})[0])
		require.Equal(t, "", rdb.HGet(ctx, "hfe-key", "f1").Val())
	})

	t.Run("HFE Test hincrby and hincrbyfloat a field with expiration", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())
		require.Equal(t, int64(5), rdb.HIncrBy(ctx, "hfe-key", "f", 5).Val())
		require.Equal(t, int64(1), rdb.Do(ctx, "HEXPIRE", "hfe-key", 1, "FIELDS", 1, "f").Val().([]interface{})[0])
		require.Equal(t, "5", rdb.HGet(ctx, "hfe-key", "f").Val())
		require.Equal(t, float64(6.5), rdb.HIncrByFloat(ctx, "hfe-key", "f", 1.5).Val())
		f, _ := rdb.HGet(ctx, "hfe-key", "f").Float64()
		require.Equal(t, float64(6.5), f)
		time.Sleep(1 * time.Second)
		require.Equal(t, "", rdb.HGet(ctx, "hfe-key", "f").Val())
	})

	t.Run("HFE expire a field with NX/XX/GT/LT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())
		require.Equal(t, true, rdb.HMSet(ctx, "hfe-key", "f1", "v1", "f2", "v2").Val())
		require.Equal(t, int64(1), rdb.Do(ctx, "HEXPIRE", "hfe-key", 1, "FIELDS", 1, "f1").Val().([]interface{})[0])
		nxResult := rdb.Do(ctx, "HEXPIRE", "hfe-key", 1, "NX", "FIELDS", 2, "f1", "f2").Val().([]interface{})
		require.Equal(t, []interface{}{int64(0), int64(1)}, nxResult)

		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())
		require.Equal(t, true, rdb.HMSet(ctx, "hfe-key", "f1", "v1", "f2", "v2").Val())
		require.Equal(t, int64(1), rdb.Do(ctx, "HEXPIRE", "hfe-key", 1, "FIELDS", 1, "f1").Val().([]interface{})[0])
		xxResult := rdb.Do(ctx, "HEXPIRE", "hfe-key", 1, "XX", "FIELDS", 2, "f1", "f2").Val().([]interface{})
		require.Equal(t, []interface{}{int64(1), int64(0)}, xxResult)

		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())
		require.Equal(t, true, rdb.HMSet(ctx, "hfe-key", "f1", "v1", "f2", "v2").Val())
		require.Equal(t, int64(1), rdb.Do(ctx, "HEXPIRE", "hfe-key", 10, "FIELDS", 1, "f1").Val().([]interface{})[0])
		require.Equal(t, int64(1), rdb.Do(ctx, "HEXPIRE", "hfe-key", 20, "FIELDS", 1, "f2").Val().([]interface{})[0])
		gtResult := rdb.Do(ctx, "HEXPIRE", "hfe-key", 15, "GT", "FIELDS", 2, "f1", "f2").Val().([]interface{})
		require.Equal(t, []interface{}{int64(1), int64(0)}, gtResult)

		require.NoError(t, rdb.Del(ctx, "hfe-key").Err())
		require.Equal(t, true, rdb.HMSet(ctx, "hfe-key", "f1", "v1", "f2", "v2").Val())
		require.Equal(t, int64(1), rdb.Do(ctx, "HEXPIRE", "hfe-key", 10, "FIELDS", 1, "f1").Val().([]interface{})[0])
		require.Equal(t, int64(1), rdb.Do(ctx, "HEXPIRE", "hfe-key", 20, "FIELDS", 1, "f2").Val().([]interface{})[0])
		ltResult := rdb.Do(ctx, "HEXPIRE", "hfe-key", 15, "LT", "FIELDS", 2, "f1", "f2").Val().([]interface{})
		require.Equal(t, []interface{}{int64(0), int64(1)}, ltResult)
	})
}
