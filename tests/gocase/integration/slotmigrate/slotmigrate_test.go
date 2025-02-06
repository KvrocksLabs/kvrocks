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

package slotmigrate

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

type SlotMigrationState string
type SlotImportState string
type SlotMigrationType string

const (
	SlotMigrationStateStarted SlotMigrationState = "start"
	SlotMigrationStateSuccess SlotMigrationState = "success"
	SlotMigrationStateFailed  SlotMigrationState = "fail"

	SlotImportStateSuccess SlotImportState = "success"
	SlotImportStateFailed  SlotImportState = "error"

	MigrationTypeRedisCommand SlotMigrationType = "redis-command"
	MigrationTypeRawKeyValue  SlotMigrationType = "raw-key-value"
)

var testSlot = 0

func TestSlotMigrateDataType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv0.Close() }()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODEID", id0).Err())

	srv1 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv1.Close() }()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-10000\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master - 10001-16383", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("MIGRATE - Cannot migrate two slot at the same time", func(t *testing.T) {
		cnt := 20000
		slot := 0
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		otherSlot := 2
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", otherSlot, id1).Err(), "There is already a migrating job")
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.EqualValues(t, cnt, rdb1.LLen(ctx, util.SlotTable[slot]).Val())
	})

	migrateAllTypes := func(t *testing.T, migrateType SlotMigrationType, sync bool) {
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-type", string(migrateType)).Err())

		testSlot += 1
		keys := make(map[string]string, 0)
		for _, typ := range []string{"string", "expired_string", "list", "hash", "set", "zset", "bitmap", "sortint", "stream", "json"} {
			keys[typ] = fmt.Sprintf("%s_{%s}", typ, util.SlotTable[testSlot])
			require.NoError(t, rdb0.Del(ctx, keys[typ]).Err())
		}
		// type string
		require.NoError(t, rdb0.Set(ctx, keys["string"], keys["string"], 0).Err())
		require.NoError(t, rdb0.Expire(ctx, keys["string"], 10*time.Second).Err())
		// type expired string
		require.NoError(t, rdb0.Set(ctx, keys["expired_string"], keys["expired_string"], time.Second).Err())
		time.Sleep(3 * time.Second)
		require.Empty(t, rdb0.Get(ctx, keys["expired_string"]).Val())
		// type list
		require.NoError(t, rdb0.RPush(ctx, keys["list"], 0, 1, 2, 3, 4, 5).Err())
		require.NoError(t, rdb0.LPush(ctx, keys["list"], 9, 3, 7, 3, 5, 4).Err())
		require.NoError(t, rdb0.LSet(ctx, keys["list"], 5, 0).Err())
		require.NoError(t, rdb0.LInsert(ctx, keys["list"], "before", 9, 3).Err())
		require.NoError(t, rdb0.LTrim(ctx, keys["list"], 3, -3).Err())
		require.NoError(t, rdb0.RPop(ctx, keys["list"]).Err())
		require.NoError(t, rdb0.LPop(ctx, keys["list"]).Err())
		require.NoError(t, rdb0.LRem(ctx, keys["list"], 4, 3).Err())
		require.NoError(t, rdb0.Expire(ctx, keys["list"], 10*time.Second).Err())
		// type hash
		require.NoError(t, rdb0.HMSet(ctx, keys["hash"], 0, 1, 2, 3, 4, 5, 6, 7).Err())
		require.NoError(t, rdb0.HDel(ctx, keys["hash"], "2").Err())
		require.NoError(t, rdb0.Expire(ctx, keys["hash"], 10*time.Second).Err())
		// type set
		require.NoError(t, rdb0.SAdd(ctx, keys["set"], 0, 1, 2, 3, 4, 5).Err())
		require.NoError(t, rdb0.SRem(ctx, keys["set"], 1, 3).Err())
		require.NoError(t, rdb0.Expire(ctx, keys["set"], 10*time.Second).Err())
		// type zset
		require.NoError(t, rdb0.ZAdd(ctx, keys["zset"], []redis.Z{{0, "1"}, {2, "3"}, {4, "5"}}...).Err())
		require.NoError(t, rdb0.ZRem(ctx, keys["zset"], 1, 3).Err())
		require.NoError(t, rdb0.Expire(ctx, keys["zset"], 10*time.Second).Err())
		// type bitmap
		for i := 1; i < 20; i += 2 {
			require.NoError(t, rdb0.SetBit(ctx, keys["bitmap"], int64(i), 1).Err())
		}
		for i := 10000; i < 11000; i += 2 {
			require.NoError(t, rdb0.SetBit(ctx, keys["bitmap"], int64(i), 1).Err())
		}
		require.NoError(t, rdb0.Expire(ctx, keys["bitmap"], 10*time.Second).Err())
		// type sortint
		require.NoError(t, rdb0.Do(ctx, "SIADD", keys["sortint"], 2, 4, 1, 3).Err())
		require.NoError(t, rdb0.Do(ctx, "SIREM", keys["sortint"], 1).Err())
		require.NoError(t, rdb0.Expire(ctx, keys["sortint"], 10*time.Second).Err())
		// type stream
		for i := 1; i < 20; i++ {
			idxStr := strconv.FormatInt(int64(i), 10)
			require.NoError(t, rdb0.XAdd(ctx, &redis.XAddArgs{
				Stream: keys["stream"],
				ID:     idxStr + "-0",
				Values: []string{"key" + idxStr, "value" + idxStr},
			}).Err())
		}
		require.NoError(t, rdb0.Expire(ctx, keys["stream"], 10*time.Second).Err())
		require.NoError(t, rdb0.JSONSet(ctx, keys["json"], "$", `{"a": 1, "b": "hello"}`).Err())
		// check source data existence
		for _, typ := range []string{"string", "list", "hash", "set", "zset", "bitmap", "sortint", "stream", "json"} {
			require.EqualValues(t, 1, rdb0.Exists(ctx, keys[typ]).Val())
		}
		// get source data
		lv := rdb0.LRange(ctx, keys["list"], 0, -1).Val()
		hv := rdb0.HGetAll(ctx, keys["hash"]).Val()
		sv := rdb0.SMembers(ctx, keys["set"]).Val()
		zv := rdb0.ZRangeWithScores(ctx, keys["zset"], 0, -1).Val()
		siv := rdb0.Do(ctx, "SIRANGE", keys["sortint"], 0, -1).Val()
		stV := rdb0.XRange(ctx, keys["stream"], "-", "+").Val()
		streamInfo := rdb0.XInfoStream(ctx, keys["stream"]).Val()
		require.EqualValues(t, "19-0", streamInfo.LastGeneratedID)
		require.EqualValues(t, 19, streamInfo.EntriesAdded)
		require.EqualValues(t, "0-0", streamInfo.MaxDeletedEntryID)
		require.EqualValues(t, 19, streamInfo.Length)

		if !sync {
			require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", testSlot, id1).Val())
			waitForMigrateState(t, rdb0, testSlot, SlotMigrationStateSuccess)
		} else {
			require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", testSlot, id1, "sync").Val())
		}

		// check destination data
		// type string
		require.Equal(t, keys["string"], rdb1.Get(ctx, keys["string"]).Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["string"]).Val(), time.Second, 10*time.Second)
		require.Empty(t, rdb1.Get(ctx, keys["expired_string"]).Val())
		// type list
		require.EqualValues(t, lv, rdb1.LRange(ctx, keys["list"], 0, -1).Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["list"]).Val(), time.Second, 10*time.Second)
		// type hash
		require.EqualValues(t, hv, rdb1.HGetAll(ctx, keys["hash"]).Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["hash"]).Val(), time.Second, 10*time.Second)
		// type set
		require.EqualValues(t, sv, rdb1.SMembers(ctx, keys["set"]).Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["set"]).Val(), time.Second, 10*time.Second)
		// type zset
		require.EqualValues(t, zv, rdb1.ZRangeWithScores(ctx, keys["zset"], 0, -1).Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["zset"]).Val(), time.Second, 10*time.Second)
		// type bitmap
		for i := 1; i < 20; i += 2 {
			require.EqualValues(t, 1, rdb1.GetBit(ctx, keys["bitmap"], int64(i)).Val())
		}
		for i := 10000; i < 11000; i += 2 {
			require.EqualValues(t, 1, rdb1.GetBit(ctx, keys["bitmap"], int64(i)).Val())
		}
		for i := 0; i < 20; i += 2 {
			require.EqualValues(t, 0, rdb1.GetBit(ctx, keys["bitmap"], int64(i)).Val())
		}
		util.BetweenValues(t, rdb1.TTL(ctx, keys["bitmap"]).Val(), time.Second, 10*time.Second)
		// type sortint
		require.EqualValues(t, siv, rdb1.Do(ctx, "SIRANGE", keys["sortint"], 0, -1).Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["sortint"]).Val(), time.Second, 10*time.Second)
		// type stream
		require.EqualValues(t, stV, rdb1.XRange(ctx, keys["stream"], "-", "+").Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["stream"]).Val(), time.Second, 10*time.Second)
		streamInfo = rdb1.XInfoStream(ctx, keys["stream"]).Val()
		require.EqualValues(t, "19-0", streamInfo.LastGeneratedID)
		require.EqualValues(t, 19, streamInfo.EntriesAdded)
		require.EqualValues(t, "0-0", streamInfo.MaxDeletedEntryID)
		require.EqualValues(t, 19, streamInfo.Length)
		// type json
		require.Equal(t, `{"a":1,"b":"hello"}`, rdb1.JSONGet(ctx, keys["json"]).Val())
		// topology is changed on source server
		for _, typ := range []string{"string", "list", "hash", "set", "zset", "bitmap", "sortint", "stream"} {
			require.ErrorContains(t, rdb0.Exists(ctx, keys[typ]).Err(), "MOVED")
		}
	}

	migrateIncrementalStream := func(t *testing.T, migrateType SlotMigrationType) {
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-type", string(migrateType)).Err())
		testSlot += 1
		keys := make(map[string]string, 0)
		for _, typ := range []string{"stream"} {
			keys[typ] = fmt.Sprintf("%s_{%s}", typ, util.SlotTable[testSlot])
			require.NoError(t, rdb0.Del(ctx, keys[typ]).Err())
		}
		for i := 1; i < 1000; i++ {
			idxStr := strconv.FormatInt(int64(i), 10)
			require.NoError(t, rdb0.XAdd(ctx, &redis.XAddArgs{
				Stream: keys["stream"],
				ID:     idxStr + "-0",
				Values: []string{"key" + idxStr, "value" + idxStr},
			}).Err())
		}
		streamInfo := rdb0.XInfoStream(ctx, keys["stream"]).Val()
		require.EqualValues(t, "999-0", streamInfo.LastGeneratedID)
		require.EqualValues(t, 999, streamInfo.EntriesAdded)
		require.EqualValues(t, "0-0", streamInfo.MaxDeletedEntryID)
		require.EqualValues(t, 999, streamInfo.Length)

		// Slowdown the migration speed to prevent running before next increment commands
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "256").Err())
		defer func() {
			require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "4096").Err())
		}()
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", testSlot, id1).Val())
		newStreamID := "1001"
		require.NoError(t, rdb0.XAdd(ctx, &redis.XAddArgs{
			Stream: keys["stream"],
			ID:     newStreamID + "-0",
			Values: []string{"key" + newStreamID, "value" + newStreamID},
		}).Err())
		require.NoError(t, rdb0.XDel(ctx, keys["stream"], "1-0").Err())
		require.NoError(t, rdb0.Do(ctx, "XSETID", keys["stream"], "1001-0", "MAXDELETEDID", "2-0").Err())
		waitForMigrateStateInDuration(t, rdb0, testSlot, SlotMigrationStateSuccess, time.Minute)

		streamInfo = rdb1.XInfoStream(ctx, keys["stream"]).Val()
		require.EqualValues(t, "1001-0", streamInfo.LastGeneratedID)
		require.EqualValues(t, 1000, streamInfo.EntriesAdded)
		require.EqualValues(t, "2-0", streamInfo.MaxDeletedEntryID)
		require.EqualValues(t, 999, streamInfo.Length)
	}

	migrateEmptyStream := func(t *testing.T, migrateType SlotMigrationType) {
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-type", string(migrateType)).Err())

		testSlot += 1
		key := fmt.Sprintf("stream_{%s}", util.SlotTable[testSlot])

		require.NoError(t, rdb0.Del(ctx, key).Err())

		require.NoError(t, rdb0.XAdd(ctx, &redis.XAddArgs{
			Stream: key,
			ID:     "5-0",
			Values: []string{"key5", "value5"},
		}).Err())
		require.NoError(t, rdb0.XAdd(ctx, &redis.XAddArgs{
			Stream: key,
			ID:     "7-0",
			Values: []string{"key7", "value7"},
		}).Err())
		require.NoError(t, rdb0.XDel(ctx, key, "7-0").Err())
		require.NoError(t, rdb0.XDel(ctx, key, "5-0").Err())

		originInfo := rdb0.XInfoStream(ctx, key)
		originRes, err := originInfo.Result()
		require.NoError(t, err)
		require.EqualValues(t, "7-0", originRes.LastGeneratedID)
		require.EqualValues(t, 2, originRes.EntriesAdded)
		require.EqualValues(t, "7-0", originRes.MaxDeletedEntryID)
		require.EqualValues(t, 0, originRes.Length)

		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", testSlot, id1).Val())
		waitForMigrateState(t, rdb0, testSlot, SlotMigrationStateSuccess)

		require.ErrorContains(t, rdb0.Exists(ctx, key).Err(), "MOVED")

		migratedInfo := rdb1.XInfoStream(ctx, key)
		migratedRes, err := migratedInfo.Result()
		require.NoError(t, err)
		require.EqualValues(t, originRes.LastGeneratedID, migratedRes.LastGeneratedID)
		require.EqualValues(t, originRes.EntriesAdded, migratedRes.EntriesAdded)
		require.EqualValues(t, originRes.MaxDeletedEntryID, migratedRes.MaxDeletedEntryID)
		require.EqualValues(t, originRes.Length, migratedRes.Length)
	}

	migrateStreamWithDeletedEntries := func(t *testing.T, migrateType SlotMigrationType) {
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-type", string(migrateType)).Err())

		testSlot += 1
		key := fmt.Sprintf("stream_{%s}", util.SlotTable[testSlot])

		require.NoError(t, rdb0.Del(ctx, key).Err())

		for i := 1; i < 6; i++ {
			idxStr := strconv.FormatInt(int64(i), 10)
			require.NoError(t, rdb0.XAdd(ctx, &redis.XAddArgs{
				Stream: key,
				ID:     idxStr + "-0",
				Values: []string{"key" + idxStr, "value" + idxStr},
			}).Err())
		}
		require.NoError(t, rdb0.XDel(ctx, key, "1-0").Err())
		require.NoError(t, rdb0.XDel(ctx, key, "3-0").Err())

		originInfo := rdb0.XInfoStream(ctx, key)
		originRes, err := originInfo.Result()
		require.NoError(t, err)
		require.EqualValues(t, "5-0", originRes.LastGeneratedID)
		require.EqualValues(t, 5, originRes.EntriesAdded)
		require.EqualValues(t, "3-0", originRes.MaxDeletedEntryID)
		require.EqualValues(t, 3, originRes.Length)

		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", testSlot, id1).Val())
		waitForMigrateState(t, rdb0, testSlot, SlotMigrationStateSuccess)

		require.ErrorContains(t, rdb0.Exists(ctx, key).Err(), "MOVED")

		migratedInfo := rdb1.XInfoStream(ctx, key)
		migratedRes, err := migratedInfo.Result()
		require.NoError(t, err)
		require.EqualValues(t, originRes.LastGeneratedID, migratedRes.LastGeneratedID)
		require.EqualValues(t, originRes.EntriesAdded, migratedRes.EntriesAdded)
		require.EqualValues(t, originRes.MaxDeletedEntryID, migratedRes.MaxDeletedEntryID)
		require.EqualValues(t, originRes.Length, migratedRes.Length)
	}

	migrateIncrementalData := func(t *testing.T, migrateType SlotMigrationType) {
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-type", string(migrateType)).Err())
		testSlot += 1
		migratingSlot := testSlot
		hashtag := util.SlotTable[migratingSlot]
		keys := []string{
			// key for slowing migrate-speed when migrating existing data
			hashtag,
			fmt.Sprintf("{%s}_key1", hashtag),
			fmt.Sprintf("{%s}_key2", hashtag),
			fmt.Sprintf("{%s}_key3", hashtag),
			fmt.Sprintf("{%s}_key4", hashtag),
			fmt.Sprintf("{%s}_key5", hashtag),
			fmt.Sprintf("{%s}_key6", hashtag),
			fmt.Sprintf("{%s}_key7", hashtag),
			fmt.Sprintf("{%s}_key8", hashtag),
			fmt.Sprintf("{%s}_key9", hashtag),
		}
		for _, key := range keys {
			require.NoError(t, rdb0.Del(ctx, key).Err())
		}

		valuePrefix := "value"
		if migrateType == MigrationTypeRedisCommand {
			require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "64").Err())
		} else {
			// Create enough data
			valuePrefix = strings.Repeat("value", 1024)
			require.NoError(t, rdb0.ConfigSet(ctx, "migrate-batch-rate-limit-mb", "1").Err())
		}

		cnt := 2000
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, keys[0], fmt.Sprintf("%s-%d", valuePrefix, i)).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", migratingSlot, id1).Val())

		// write key that doesn't belong to this slot
		testSlot += 1
		nonMigratingSlot := testSlot
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[nonMigratingSlot]).Err())
		require.NoError(t, rdb0.Set(ctx, util.SlotTable[nonMigratingSlot], "non-migrating-value", 0).Err())

		// write increment operations include all kinds of types
		// 1. type string
		require.NoError(t, rdb0.SetEx(ctx, keys[1], 15, 10000*time.Second).Err())
		require.NoError(t, rdb0.IncrBy(ctx, keys[1], 2).Err())
		require.NoError(t, rdb0.DecrBy(ctx, keys[1], 1).Err())
		require.NoError(t, rdb0.Set(ctx, keys[2], "val", 0).Err())
		require.NoError(t, rdb0.Del(ctx, keys[2]).Err())
		require.NoError(t, rdb0.SetBit(ctx, keys[3], 10086, 1).Err())
		require.NoError(t, rdb0.Expire(ctx, keys[3], 10000*time.Second).Err())
		// verify expireat binlog could be parsed
		testSlot += 1
		slotWithExpiringKey := testSlot
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[slotWithExpiringKey]).Err())
		require.NoError(t, rdb0.Set(ctx, util.SlotTable[slotWithExpiringKey], "expiring-value", 0).Err())
		require.NoError(t, rdb0.ExpireAt(ctx, util.SlotTable[slotWithExpiringKey], time.Now().Add(100*time.Second)).Err())
		// verify del command
		testSlot += 1
		slotWithDeletedKey := testSlot
		require.NoError(t, rdb0.Set(ctx, util.SlotTable[slotWithDeletedKey], "will-be-deleted", 0).Err())
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[slotWithDeletedKey]).Err())
		// 2. type hash
		require.NoError(t, rdb0.HMSet(ctx, keys[4], "f1", "1", "f2", "2").Err())
		require.NoError(t, rdb0.HDel(ctx, keys[4], "f1").Err())
		require.NoError(t, rdb0.HIncrBy(ctx, keys[4], "f2", 2).Err())
		require.NoError(t, rdb0.HIncrBy(ctx, keys[4], "f2", -1).Err())
		// 3. type set
		require.NoError(t, rdb0.SAdd(ctx, keys[5], 1, 2).Err())
		require.NoError(t, rdb0.SRem(ctx, keys[5], 1).Err())
		// 4. type zset
		require.NoError(t, rdb0.ZAdd(ctx, keys[6], []redis.Z{{2, "m1"}}...).Err())
		require.NoError(t, rdb0.ZIncrBy(ctx, keys[6], 2, "m1").Err())
		require.NoError(t, rdb0.ZIncrBy(ctx, keys[6], -1, "m1").Err())
		require.NoError(t, rdb0.ZAdd(ctx, keys[6], []redis.Z{{3, "m3"}}...).Err())
		require.NoError(t, rdb0.ZRem(ctx, keys[6], "m3").Err())
		require.NoError(t, rdb0.ZAdd(ctx, keys[6], []redis.Z{{1, ""}}...).Err())
		require.NoError(t, rdb0.ZRem(ctx, keys[6], "").Err())
		// 5. type list
		require.NoError(t, rdb0.LPush(ctx, keys[7], "item1").Err())
		require.NoError(t, rdb0.RPush(ctx, keys[7], "item2").Err())
		require.NoError(t, rdb0.LPush(ctx, keys[7], "item3").Err())
		require.NoError(t, rdb0.RPush(ctx, keys[7], "item4").Err())
		require.Equal(t, "item3", rdb0.LPop(ctx, keys[7]).Val())
		require.Equal(t, "item4", rdb0.RPop(ctx, keys[7]).Val())
		require.NoError(t, rdb0.LPush(ctx, keys[7], "item7").Err())
		require.NoError(t, rdb0.RPush(ctx, keys[7], "item8").Err())
		require.NoError(t, rdb0.LSet(ctx, keys[7], 0, "item5").Err())
		require.NoError(t, rdb0.LInsert(ctx, keys[7], "before", "item2", "item6").Err())
		require.NoError(t, rdb0.LRem(ctx, keys[7], 1, "item7").Err())
		require.NoError(t, rdb0.LTrim(ctx, keys[7], 1, -1).Err())
		// 6. type bitmap
		for i := 1; i < 20; i += 2 {
			require.NoError(t, rdb0.SetBit(ctx, keys[8], int64(i), 1).Err())
		}
		for i := 10000; i < 11000; i += 2 {
			require.NoError(t, rdb0.SetBit(ctx, keys[8], int64(i), 1).Err())
		}
		// 7. type sortint
		require.NoError(t, rdb0.Do(ctx, "SIADD", keys[9], 2, 4, 1, 3).Err())
		require.NoError(t, rdb0.Do(ctx, "SIREM", keys[9], 2).Err())
		// check data in source server
		require.EqualValues(t, cnt, rdb0.LLen(ctx, keys[0]).Val())
		strv := rdb0.Get(ctx, keys[1]).Val()
		strt := rdb0.TTL(ctx, keys[1]).Val()
		bv := rdb0.GetBit(ctx, keys[3], 10086).Val()
		bt := rdb0.TTL(ctx, keys[3]).Val()
		hv := rdb0.HGetAll(ctx, keys[4]).Val()
		sv := rdb0.SMembers(ctx, keys[5]).Val()
		zv := rdb0.ZRangeWithScores(ctx, keys[6], 0, -1).Val()
		lv := rdb0.LRange(ctx, keys[7], 0, -1).Val()
		siv := rdb0.Do(ctx, "SIRANGE", keys[9], 0, -1).Val()
		waitForMigrateStateInDuration(t, rdb0, migratingSlot, SlotMigrationStateSuccess, time.Minute)
		waitForImportState(t, rdb1, migratingSlot, SlotImportStateSuccess)
		// check if the data is consistent
		// 1. type string
		require.EqualValues(t, cnt, rdb1.LLen(ctx, keys[0]).Val())
		require.EqualValues(t, strv, rdb1.Get(ctx, keys[1]).Val())
		require.Less(t, rdb1.TTL(ctx, keys[1]).Val()-strt, 100*time.Second)
		require.Empty(t, rdb1.Get(ctx, keys[2]).Val())
		require.EqualValues(t, bv, rdb1.GetBit(ctx, keys[3], 10086).Val())
		require.Less(t, rdb1.TTL(ctx, keys[3]).Val()-bt, 100*time.Second)
		require.ErrorContains(t, rdb1.Exists(ctx, util.SlotTable[slotWithExpiringKey]).Err(), "MOVED")
		// 2. type hash
		require.EqualValues(t, hv, rdb1.HGetAll(ctx, keys[4]).Val())
		require.EqualValues(t, "3", rdb1.HGet(ctx, keys[4], "f2").Val())
		// 3. type set
		require.EqualValues(t, sv, rdb1.SMembers(ctx, keys[5]).Val())
		// 4. type zset
		require.EqualValues(t, zv, rdb1.ZRangeWithScores(ctx, keys[6], 0, -1).Val())
		require.EqualValues(t, 3, rdb1.ZScore(ctx, keys[6], "m1").Val())
		// 5. type list
		require.EqualValues(t, lv, rdb1.LRange(ctx, keys[7], 0, -1).Val())
		// 6. type bitmap
		for i := 1; i < 20; i += 2 {
			require.EqualValues(t, 1, rdb1.GetBit(ctx, keys[8], int64(i)).Val())
		}
		for i := 10000; i < 11000; i += 2 {
			require.EqualValues(t, 1, rdb1.GetBit(ctx, keys[8], int64(i)).Val())
		}
		for i := 0; i < 20; i += 2 {
			require.EqualValues(t, 0, rdb1.GetBit(ctx, keys[8], int64(i)).Val())
		}
		// 7. type sortint
		require.EqualValues(t, siv, rdb1.Do(ctx, "SIRANGE", keys[9], 0, -1).Val())

		// not migrate if the key doesn't belong to slot
		require.Equal(t, "non-migrating-value", rdb0.Get(ctx, util.SlotTable[nonMigratingSlot]).Val())
		require.ErrorContains(t, rdb1.Exists(ctx, util.SlotTable[nonMigratingSlot]).Err(), "MOVED")
		require.EqualValues(t, 0, rdb0.Exists(ctx, util.SlotTable[slotWithDeletedKey]).Val())
	}

	testMigrationTypes := []SlotMigrationType{MigrationTypeRedisCommand, MigrationTypeRawKeyValue}

	for _, testType := range testMigrationTypes {
		t.Run(fmt.Sprintf("MIGRATE - Slot migrate all types of existing data using %s", testType), func(t *testing.T) {
			migrateAllTypes(t, testType, false)
		})

		t.Run(fmt.Sprintf("MIGRATE - Slot migrate all types of existing data (sync) using %s", testType), func(t *testing.T) {
			migrateAllTypes(t, testType, true)
		})

		t.Run(fmt.Sprintf("MIGRATE - increment sync stream from WAL using %s", testType), func(t *testing.T) {
			migrateIncrementalStream(t, testType)
		})

		t.Run(fmt.Sprintf("MIGRATE - Migrating empty stream using %s", testType), func(t *testing.T) {
			migrateEmptyStream(t, testType)
		})

		t.Run(fmt.Sprintf("MIGRATE - Migrating stream with deleted entries using %s", testType), func(t *testing.T) {
			migrateStreamWithDeletedEntries(t, testType)
		})

		t.Run(fmt.Sprintf("MIGRATE - Migrate incremental data via parsing and filtering data in WAL using %s", testType), func(t *testing.T) {
			migrateIncrementalData(t, testType)
		})
	}

	t.Run("MIGRATE - Accessing slot is forbidden on source server but not on destination server", func(t *testing.T) {
		testSlot += 1
		require.NoError(t, rdb0.Set(ctx, util.SlotTable[testSlot], 3, 0).Err())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", testSlot, id1).Val())
		waitForMigrateState(t, rdb0, testSlot, SlotMigrationStateSuccess)
		require.ErrorContains(t, rdb0.Set(ctx, util.SlotTable[testSlot], "source-value", 0).Err(), "MOVED")
		require.ErrorContains(t, rdb0.Del(ctx, util.SlotTable[testSlot]).Err(), "MOVED")
		require.ErrorContains(t, rdb0.Exists(ctx, util.SlotTable[testSlot]).Err(), "MOVED")
		require.NoError(t, rdb1.Set(ctx, util.SlotTable[testSlot], "destination-value", 0).Err())
	})

	t.Run("MIGRATE - Slot isn't forbidden writing when starting migrating", func(t *testing.T) {
		testSlot += 1
		cnt := 20000
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[testSlot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", testSlot, id1).Val())
		requireMigrateState(t, rdb0, testSlot, SlotMigrationStateStarted)
		// write during migrating
		require.EqualValues(t, cnt+1, rdb0.LPush(ctx, util.SlotTable[testSlot], cnt).Val())
		waitForMigrateState(t, rdb0, testSlot, SlotMigrationStateSuccess)
		require.Equal(t, strconv.Itoa(cnt), rdb1.LPop(ctx, util.SlotTable[testSlot]).Val())
	})

	t.Run("MIGRATE - Slot keys are not cleared after migration but cleared after setslot", func(t *testing.T) {
		testSlot += 1
		require.NoError(t, rdb0.Set(ctx, util.SlotTable[testSlot], "slot6", 0).Err())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", testSlot, id1).Val())
		waitForMigrateState(t, rdb0, testSlot, SlotMigrationStateSuccess)
		require.Equal(t, "slot6", rdb1.Get(ctx, util.SlotTable[testSlot]).Val())
		require.Contains(t, rdb0.Keys(ctx, "*").Val(), util.SlotTable[testSlot])
		require.NoError(t, rdb0.Do(ctx, "clusterx", "setslot", testSlot, "node", id1, "2").Err())
		require.NotContains(t, rdb0.Keys(ctx, "*").Val(), util.SlotTable[testSlot])
	})

	t.Run("MIGRATE - Slow migrate speed", func(t *testing.T) {
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-type", string(MigrationTypeRedisCommand)).Err())
		testSlot += 1
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "16").Err())
		require.Equal(t, map[string]string{"migrate-speed": "16"}, rdb0.ConfigGet(ctx, "migrate-speed").Val())
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[testSlot]).Err())
		// more than pipeline size(16) and max items(16) in command
		cnt := 1000
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[testSlot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", testSlot, id1).Val())

		clusterNodes := rdb0.ClusterNodes(ctx).Val()
		require.Contains(t, clusterNodes, fmt.Sprintf("[%d->-%s]", testSlot, id1))

		// should not finish 1.5s
		time.Sleep(1500 * time.Millisecond)
		requireMigrateState(t, rdb0, testSlot, SlotMigrationStateStarted)
		waitForMigrateState(t, rdb0, testSlot, SlotMigrationStateSuccess)
	})

	t.Run("MIGRATE - Data of migrated slot can't be written to source but can be written to destination", func(t *testing.T) {
		testSlot += 1
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[testSlot]).Err())
		cnt := 100
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[testSlot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", testSlot, id1).Val())
		waitForMigrateState(t, rdb0, testSlot, SlotMigrationStateSuccess)
		require.EqualValues(t, cnt, rdb1.LLen(ctx, util.SlotTable[testSlot]).Val())
		// write the migrated slot to source server
		k := fmt.Sprintf("{%s}_1", util.SlotTable[testSlot])
		require.ErrorContains(t, rdb0.Set(ctx, k, "slot17_value1", 0).Err(), "MOVED")
		// write the migrated slot to destination server
		require.NoError(t, rdb1.Set(ctx, k, "slot17_value1", 0).Err())
	})

	t.Run("MIGRATE - LMOVE (src and dst are different) via parsing WAL logs", func(t *testing.T) {
		testSlot += 1

		srcListName := fmt.Sprintf("list_src_{%s}", util.SlotTable[testSlot])
		dstListName := fmt.Sprintf("list_dst_{%s}", util.SlotTable[testSlot])

		require.NoError(t, rdb0.Del(ctx, srcListName).Err())
		require.NoError(t, rdb0.Del(ctx, dstListName).Err())

		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "512").Err())
		defer func() {
			require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "4096").Err())
		}()

		for i := 0; i < 1000; i++ {
			require.NoError(t, rdb0.RPush(ctx, srcListName, fmt.Sprintf("element%d", i)).Err())
		}

		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", testSlot, id1).Val())
		requireMigrateState(t, rdb0, testSlot, SlotMigrationStateStarted)

		for i := 0; i < 10; i++ {
			require.NoError(t, rdb0.LMove(ctx, srcListName, dstListName, "RIGHT", "LEFT").Err())
		}
		waitForMigrateState(t, rdb0, testSlot, SlotMigrationStateSuccess)

		require.ErrorContains(t, rdb0.RPush(ctx, srcListName, "element1000").Err(), "MOVED")
		require.Equal(t, int64(10), rdb1.LLen(ctx, dstListName).Val())
		require.Equal(t, "element990", rdb1.LIndex(ctx, dstListName, 0).Val())
		require.Equal(t, "element999", rdb1.LIndex(ctx, dstListName, -1).Val())
		require.Equal(t, int64(990), rdb1.LLen(ctx, srcListName).Val())
		require.Equal(t, "element0", rdb1.LIndex(ctx, srcListName, 0).Val())
		require.Equal(t, "element989", rdb1.LIndex(ctx, srcListName, -1).Val())
	})

	t.Run("MIGRATE - LMOVE (src and dst are the same) via parsing WAL logs", func(t *testing.T) {
		testSlot += 1

		srcListName := fmt.Sprintf("list_src_{%s}", util.SlotTable[testSlot])

		require.NoError(t, rdb0.Del(ctx, srcListName).Err())

		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "512").Err())
		defer func() {
			require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "4096").Err())
		}()

		srcLen := 1_000

		for i := 0; i < srcLen; i++ {
			require.NoError(t, rdb0.RPush(ctx, srcListName, fmt.Sprintf("element%d", i)).Err())
		}

		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", testSlot, id1).Val())
		requireMigrateState(t, rdb0, testSlot, SlotMigrationStateStarted)

		for i := 0; i < 10; i++ {
			require.NoError(t, rdb0.LMove(ctx, srcListName, srcListName, "RIGHT", "LEFT").Err())
		}
		waitForMigrateState(t, rdb0, testSlot, SlotMigrationStateSuccess)

		require.ErrorContains(t, rdb0.RPush(ctx, srcListName, "element1000").Err(), "MOVED")
		require.Equal(t, int64(srcLen), rdb1.LLen(ctx, srcListName).Val())
		require.EqualValues(t, []string{"element990", "element991", "element992", "element993", "element994"},
			rdb1.LRange(ctx, srcListName, 0, 4).Val())
		require.EqualValues(t, []string{"element985", "element986", "element987", "element988", "element989"},
			rdb1.LRange(ctx, srcListName, -5, -1).Val())
	})
}

func TestSlotMigrateFromSlave(t *testing.T) {
	ctx := context.Background()

	master := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { master.Close() }()
	masterClient := master.NewClient()
	defer func() { require.NoError(t, masterClient.Close()) }()
	masterID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODEID", masterID).Err())

	slave := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { slave.Close() }()
	slaveClient := slave.NewClient()
	defer func() { require.NoError(t, slaveClient.Close()) }()
	slaveID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODEID", slaveID).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-100\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("MIGRATE - Slave cannot migrate slot", func(t *testing.T) {
		require.ErrorContains(t, slaveClient.Do(ctx, "clusterx", "migrate", 1, masterID).Err(), "Can't migrate slot")
	})

	t.Run("MIGRATE - Cannot migrate slot to a slave", func(t *testing.T) {
		require.ErrorContains(t, masterClient.Do(ctx, "clusterx", "migrate", 1, slaveID).Err(), "Can't migrate slot to a slave")
	})
}

func TestSlotMigrateDestServerKilled(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv0.Close() }()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODEID", id0).Err())

	srv1 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	srv1Alive := true
	defer func() {
		if srv1Alive {
			srv1.Close()
		}
	}()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-10000\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master - 10001-16383", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	t.Run("MIGRATE - Cannot migrate slot to itself", func(t *testing.T) {
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", 1, id0).Err(), "Can't migrate slot to myself")
	})

	t.Run("MIGRATE - Fail to migrate slot if destination server is not running", func(t *testing.T) {
		slot := 1
		srv1.Close()
		srv1Alive = false
		require.NoError(t, rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Err())
		time.Sleep(50 * time.Millisecond)
		requireMigrateState(t, rdb0, slot, SlotMigrationStateFailed)
	})
}

func TestSlotMigrateDestServerKilledAgain(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv0.Close() }()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODEID", id0).Err())

	srv1 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	srv1Alive := true
	defer func() {
		if srv1Alive {
			srv1.Close()
		}
	}()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-10000\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master - 10001-16383", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("MIGRATE - Migrate slot with empty string key or value", func(t *testing.T) {
		slot := 0
		require.NoError(t, rdb0.Set(ctx, "", "slot0", 0).Err())
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[slot]).Err())
		require.NoError(t, rdb0.Set(ctx, util.SlotTable[slot], "", 0).Err())
		time.Sleep(500 * time.Millisecond)
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.Equal(t, "slot0", rdb1.Get(ctx, "").Val())
		require.Equal(t, "", rdb1.Get(ctx, util.SlotTable[slot]).Val())
		require.NoError(t, rdb1.Del(ctx, util.SlotTable[slot]).Err())
	})

	t.Run("MIGRATE - Migrate binary key-value", func(t *testing.T) {
		slot := 1
		k1 := fmt.Sprintf("\x3a\x88{%s}\x3d\xaa", util.SlotTable[slot])
		cnt := 257
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, k1, "\0000\0001").Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		k2 := fmt.Sprintf("\x49\x1f\x7f{%s}\xaf", util.SlotTable[slot])
		require.NoError(t, rdb0.Set(ctx, k2, "\0000\0001", 0).Err())
		time.Sleep(time.Second)
		waitForImportState(t, rdb1, slot, SlotImportStateSuccess)
		require.EqualValues(t, cnt, rdb1.LLen(ctx, k1).Val())
		require.Equal(t, "\0000\0001", rdb1.LPop(ctx, k1).Val())
		require.Equal(t, "\0000\0001", rdb1.Get(ctx, k2).Val())
	})

	t.Run("MIGRATE - Migrate empty slot", func(t *testing.T) {
		slot := 2
		require.NoError(t, rdb0.FlushDB(ctx).Err())
		require.NoError(t, rdb1.FlushDB(ctx).Err())
		time.Sleep(500 * time.Millisecond)
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.NoError(t, rdb1.Keys(ctx, "*").Err())
	})

	t.Run("MIGRATE - Fail to migrate slot because destination server is killed while migrating", func(t *testing.T) {
		slot := 8
		for i := 0; i < 20000; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		requireMigrateState(t, rdb0, slot, SlotMigrationStateStarted)
		srv1.Close()
		srv1Alive = false
		time.Sleep(time.Second)
		requireMigrateState(t, rdb0, slot, SlotMigrationStateFailed)
	})
}

func TestSlotMigrateSourceServerFlushedOrKilled(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	srv0Alive := true
	defer func() {
		if srv0Alive {
			srv0.Close()
		}
	}()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODEID", id0).Err())

	srv1 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv1.Close() }()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-10000\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master - 10001-16383", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("MIGRATE - Fail to migrate slot because source server is flushed", func(t *testing.T) {
		slot := 11
		for i := 0; i < 20000; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "32").Err())
		require.Equal(t, map[string]string{"migrate-speed": "32"}, rdb0.ConfigGet(ctx, "migrate-speed").Val())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateStarted)
		require.NoError(t, rdb0.FlushDB(ctx).Err())
		time.Sleep(time.Second)
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateFailed)
	})

	t.Run("MIGRATE - Fail to migrate slot because source server is killed while migrating", func(t *testing.T) {
		slot := 20
		for i := 0; i < 20000; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "32").Err())
		require.Equal(t, map[string]string{"migrate-speed": "32"}, rdb0.ConfigGet(ctx, "migrate-speed").Val())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		require.Eventually(t, func() bool {
			return slices.Contains(rdb1.Keys(ctx, "*").Val(), util.SlotTable[slot])
		}, 5*time.Second, 100*time.Millisecond)

		srv0.Close()
		srv0Alive = false
		time.Sleep(100 * time.Millisecond)
		require.NotContains(t, rdb1.Keys(ctx, "*").Val(), util.SlotTable[slot])
	})
}

func TestSlotMigrateDisablePersistClusterNodes(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{
		"cluster-enabled":               "yes",
		"persist-cluster-nodes-enabled": "no",
	})
	defer func() { srv0.Close() }()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODEID", id0).Err())

	srv1 := util.StartServer(t, map[string]string{
		"cluster-enabled":               "yes",
		"persist-cluster-nodes-enabled": "no",
	})
	defer func() { srv1.Close() }()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master -", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	slot := 1
	require.NoError(t, rdb0.Del(ctx, util.SlotTable[slot]).Err())
	require.ErrorContains(t, rdb1.Set(ctx, util.SlotTable[slot], "foobar", 0).Err(), "MOVED")

	cnt := 100
	for i := 0; i < cnt; i++ {
		require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
	}
	require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
	waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
	require.EqualValues(t, cnt, rdb1.LLen(ctx, util.SlotTable[slot]).Val())

	k := fmt.Sprintf("{%s}_1", util.SlotTable[slot])
	require.ErrorContains(t, rdb0.Set(ctx, k, "slot1_value", 0).Err(), "MOVED")
	require.Equal(t, "OK", rdb1.Set(ctx, k, "slot1_value", 0).Val())
}

func TestSlotMigrateNewNodeAndAuth(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv0.Close() }()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODEID", id0).Err())

	srv1 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv1.Close() }()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master -", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("MIGRATE - Migrate slot to newly added node", func(t *testing.T) {
		slot := 21
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[slot]).Err())
		require.ErrorContains(t, rdb1.Set(ctx, util.SlotTable[slot], "foobar", 0).Err(), "MOVED")

		cnt := 100
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.EqualValues(t, cnt, rdb1.LLen(ctx, util.SlotTable[slot]).Val())

		k := fmt.Sprintf("{%s}_1", util.SlotTable[slot])
		require.ErrorContains(t, rdb0.Set(ctx, k, "slot21_value1", 0).Err(), "MOVED")
		require.Equal(t, "OK", rdb1.Set(ctx, k, "slot21_value1", 0).Val())
	})

	t.Run("MIGRATE - Auth before migrating slot", func(t *testing.T) {
		require.NoError(t, rdb1.ConfigSet(ctx, "requirepass", "password").Err())
		cnt := 100
		slot := 22
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}

		// migrating slot will fail if no auth
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateFailed)
		require.ErrorContains(t, rdb1.Exists(ctx, util.SlotTable[slot]).Err(), "MOVED")

		// migrating slot will fail if auth with wrong password
		require.NoError(t, rdb0.ConfigSet(ctx, "requirepass", "pass").Err())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateFailed)
		require.ErrorContains(t, rdb1.Exists(ctx, util.SlotTable[slot]).Err(), "MOVED")

		// migrating slot will succeed if auth with right password
		require.NoError(t, rdb0.ConfigSet(ctx, "requirepass", "password").Err())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.EqualValues(t, 1, rdb1.Exists(ctx, util.SlotTable[slot]).Val())
		require.EqualValues(t, cnt, rdb1.LLen(ctx, util.SlotTable[slot]).Val())
	})
}

func TestSlotMigrateThreeNodes(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv0.Close() }()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODEID", id0).Err())

	srv1 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv1.Close() }()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	srv2 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv2.Close() }()
	rdb2 := srv2.NewClient()
	defer func() { require.NoError(t, rdb2.Close()) }()
	id2 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx02"
	require.NoError(t, rdb2.Do(ctx, "clusterx", "SETNODEID", id2).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-10000\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s\n", id1, srv1.Host(), srv1.Port(), id0)
	clusterNodes += fmt.Sprintf("%s %s %d master - 10001-16383", id2, srv2.Host(), srv2.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb2.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("MIGRATE - Fail to migrate slot because source server is changed to slave during migrating", func(t *testing.T) {
		slot := 10
		for i := 0; i < 10000; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id2).Val())
		requireMigrateState(t, rdb0, slot, SlotMigrationStateStarted)

		// change source server to slave by set topology
		clusterNodes := fmt.Sprintf("%s %s %d master - 0-10000\n", id1, srv1.Host(), srv1.Port())
		clusterNodes += fmt.Sprintf("%s %s %d slave %s\n", id0, srv0.Host(), srv0.Port(), id1)
		clusterNodes += fmt.Sprintf("%s %s %d master - 10001-16383", id2, srv2.Host(), srv2.Port())
		require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.NoError(t, rdb2.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())

		// check destination importing status
		waitForImportState(t, rdb2, slot, SlotImportStateFailed)
	})
}

func TestSlotMigrateSync(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv0.Close() }()
	rdb0 := srv0.NewClientWithOption(&redis.Options{PoolSize: 1})
	defer func() { require.NoError(t, rdb0.Close()) }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODEID", id0).Err())

	srv1 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv1.Close() }()
	rdb1 := srv1.NewClientWithOption(&redis.Options{PoolSize: 1})
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-8191\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master - 8192-16383", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	slot := -1
	t.Run("MIGRATE - Cannot migrate async with timeout", func(t *testing.T) {
		slot++
		require.Error(t, rdb0.Do(ctx, "clusterx", "migrate", slot, id1, "async", 1).Err())
	})

	t.Run("MIGRATE - Migrate sync with (or without) all kinds of timeouts", func(t *testing.T) {
		slot++
		// migrate sync without explicitly specify the timeout
		result := rdb0.Do(ctx, "clusterx", "migrate", slot, id1, "sync")
		require.NoError(t, result.Err())
		require.Equal(t, "OK", result.Val())

		invalidTimeouts := []interface{}{-2, -1, 1.2, "abc"}
		for _, timeout := range invalidTimeouts {
			slot++
			result := rdb0.Do(ctx, "clusterx", "migrate", slot, id1, "sync", timeout)
			require.Error(t, result.Err(), "with timeout: %v", timeout)
		}

		validTimeouts := []int{0, 10, 20, 30}
		for _, timeout := range validTimeouts {
			slot++
			err := rdb0.Do(ctx, "clusterx", "migrate", slot, id1, "sync", timeout).Err()
			// go-redis will auto-retry if occurs timeout error, so it may return the already migrated slot error,
			// so we just allow this error here to prevent the flaky test failure.
			if err != nil && !strings.Contains(err.Error(), "ERR There is already a migrating job") {
				require.NoError(t, err)
			}
		}
	})

	t.Run("MIGRATE - Migrate sync timeout", func(t *testing.T) {
		slot++
		cnt := 200000
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}

		timeout := 1
		result := rdb0.Do(ctx, "clusterx", "migrate", slot, id1, "sync", timeout)
		require.Nil(t, result.Val())
	})
}

func TestSlotMigrateTypeFallback(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{
		"cluster-enabled": "yes",
		"migrate-type":    "raw-key-value",
	})

	defer srv0.Close()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "setnodeid", id0).Err())

	srv1 := util.StartServer(t, map[string]string{
		"cluster-enabled": "yes",
		"rename-command":  "APPLYBATCH APPLYBATCH_RENAMED",
	})
	defer srv1.Close()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "setnodeid", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master -", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "setnodes", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "setnodes", clusterNodes, "1").Err())

	t.Run("MIGRATE - Fall back to redis-command migration type when the destination does not support APPLYBATCH", func(t *testing.T) {
		info, err := rdb1.Do(ctx, "command", "info", "applybatch").Slice()
		require.NoError(t, err)
		require.Len(t, info, 1)
		require.Nil(t, info[0])
		testSlot += 1
		key := util.SlotTable[testSlot]
		value := "value"
		require.NoError(t, rdb0.Set(ctx, key, value, 0).Err())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", testSlot, id1).Val())
		waitForMigrateState(t, rdb0, testSlot, SlotMigrationStateSuccess)
		require.Equal(t, value, rdb1.Get(ctx, key).Val())
	})
}

func waitForMigrateState(t testing.TB, client *redis.Client, slot int, state SlotMigrationState) {
	waitForMigrateStateInDuration(t, client, slot, state, 5*time.Second)
}

func waitForMigrateSlotRangeState(t testing.TB, client *redis.Client, slotRange string, state SlotMigrationState) {
	waitForMigrateSlotRangeStateInDuration(t, client, slotRange, state, 5*time.Second)
}

func waitForMigrateStateInDuration(t testing.TB, client *redis.Client, slot int, state SlotMigrationState, d time.Duration) {
	require.Eventually(t, func() bool {
		i := client.ClusterInfo(context.Background()).Val()
		return strings.Contains(i, fmt.Sprintf("migrating_slot(s): %d", slot)) &&
			strings.Contains(i, fmt.Sprintf("migrating_state: %s", state))
	}, d, 100*time.Millisecond)
}

func waitForMigrateSlotRangeStateInDuration(t testing.TB, client *redis.Client, slotRange string, state SlotMigrationState, d time.Duration) {
	slots := strings.Split(slotRange, "-")
	if len(slots) == 2 && slots[0] == slots[1] {
		slotRange = slots[0]
	}
	require.Eventually(t, func() bool {
		i := client.ClusterInfo(context.Background()).Val()
		return strings.Contains(i, fmt.Sprintf("migrating_slot(s): %s", slotRange)) &&
			strings.Contains(i, fmt.Sprintf("migrating_state: %s", state))
	}, d, 100*time.Millisecond)
}

func requireMigrateState(t testing.TB, client *redis.Client, slot int, state SlotMigrationState) {
	i := client.ClusterInfo(context.Background()).Val()
	require.Contains(t, i, fmt.Sprintf("migrating_slot(s): %d", slot))
	require.Contains(t, i, fmt.Sprintf("migrating_state: %s", state))
}

func requireMigrateSlotRangeState(t testing.TB, client *redis.Client, slotRange string, state SlotMigrationState) {
	i := client.ClusterInfo(context.Background()).Val()
	require.Contains(t, i, fmt.Sprintf("migrating_slot(s): %s", slotRange))
	require.Contains(t, i, fmt.Sprintf("migrating_state: %s", state))
}

func waitForImportState(t testing.TB, client *redis.Client, n int, state SlotImportState) {
	require.Eventually(t, func() bool {
		i := client.ClusterInfo(context.Background()).Val()
		return strings.Contains(i, fmt.Sprintf("importing_slot(s): %d", n)) &&
			strings.Contains(i, fmt.Sprintf("import_state: %s", state))
	}, 10*time.Second, 100*time.Millisecond)
}
func migrateSlotRangeAndSetSlot(t *testing.T, ctx context.Context, source *redis.Client, dest *redis.Client, destID string, slotRange string) {
	require.Equal(t, "OK", source.Do(ctx, "clusterx", "migrate", slotRange, destID).Val())
	waitForMigrateSlotRangeState(t, source, slotRange, SlotMigrationStateSuccess)
	sourceVersion, _ := source.Do(ctx, "clusterx", "version").Int()
	destVersion, _ := dest.Do(ctx, "clusterx", "version").Int()
	require.NoError(t, source.Do(ctx, "clusterx", "setslot", slotRange, "node", destID, sourceVersion+1).Err())
	require.NoError(t, dest.Do(ctx, "clusterx", "setslot", slotRange, "node", destID, destVersion+1).Err())
}

func TestSlotRangeMigrate(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()
	defer func() { srv0.Close() }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODEID", id0).Err())

	srv1 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	srv1Alive := true
	defer func() {
		if srv1Alive {
			srv1.Close()
		}
	}()

	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-10000\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master - 10001-16383", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	for slot := 0; slot < 500; slot++ {
		for i := 0; i < 10; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
	}

	t.Run("MIGRATE - Slot range migration basic cases", func(t *testing.T) {
		migrateSlotRangeAndSetSlot(t, ctx, rdb0, rdb1, id1, "0-9")
		nodes := rdb0.ClusterNodes(ctx).Val()
		require.Contains(t, nodes, "10-10000", "0-9 10001-16383")
	})

	t.Run("MIGRATE - Special slot range cases", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			require.NoError(t, rdb1.LPush(ctx, util.SlotTable[16383], i).Err())
		}

		migrateSlotRangeAndSetSlot(t, ctx, rdb0, rdb1, id1, "10")
		time.Sleep(1 * time.Second)
		migrateSlotRangeAndSetSlot(t, ctx, rdb0, rdb1, id1, "11-11")
		time.Sleep(1 * time.Second)
		migrateSlotRangeAndSetSlot(t, ctx, rdb1, rdb0, id0, "16383-16383")
		time.Sleep(1 * time.Second)
		nodes := rdb0.ClusterNodes(ctx).Val()
		require.Contains(t, nodes, "12-10000 16383", "0-11 10001-16382")

		errMsg := "Invalid slot range"
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "-1", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "2-1", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "-1-3", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "-1--1", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "3--1", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "4-16384", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "16384-16384", id1).Err(), errMsg)

		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "16384", id1).Err(), "Invalid slot id: out of numeric range")
	})

	t.Run("MIGRATE - Repeat migration cases", func(t *testing.T) {
		// non-overlapping
		migrateSlotRangeAndSetSlot(t, ctx, rdb0, rdb1, id1, "104-106")
		time.Sleep(1 * time.Second)
		migrateSlotRangeAndSetSlot(t, ctx, rdb0, rdb1, id1, "107-108")
		time.Sleep(1 * time.Second)
		migrateSlotRangeAndSetSlot(t, ctx, rdb0, rdb1, id1, "102-103")
		time.Sleep(1 * time.Second)
		nodes := rdb0.ClusterNodes(ctx).Val()
		require.Contains(t, nodes, "12-101 109-10000 16383", "0-11 102-108 10001-16382")

		// overlap
		errMsg := "Can't migrate slot which doesn't belong to me"
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "100-102", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "100-104", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "108-109", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "106-109", id1).Err(), errMsg)

		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "102-108", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "101-109", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "105", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "105-105", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "104-106", id1).Err(), errMsg)
	})

	t.Run("MIGRATE - Repeat migration cases, but does not immediately update the topology via setslot", func(t *testing.T) {
		// non-overlapping
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", "114-116", id1).Val())
		waitForMigrateSlotRangeState(t, rdb0, "114-116", SlotMigrationStateSuccess)
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", "117-118", id1).Val())
		waitForMigrateSlotRangeState(t, rdb0, "117-118", SlotMigrationStateSuccess)
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", "112-113", id1).Val())
		waitForMigrateSlotRangeState(t, rdb0, "112-113", SlotMigrationStateSuccess)
		for slot := 112; slot <= 118; slot++ {
			require.Contains(t, rdb0.LPush(ctx, util.SlotTable[slot], 10).Err(), "MOVED")
		}

		// overlap
		errMsg := "Can't migrate slot which has been migrated"
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "114-116", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "117-118", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "112", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "112-112", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "113", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "113-113", id1).Err(), errMsg)

		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "112-113", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "112-120", id1).Err(), errMsg)
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "110-112", id1).Err(), errMsg)
	})

	t.Run("MIGRATE - Failure cases", func(t *testing.T) {
		largeSlot := 210
		for i := 0; i < 20000; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[largeSlot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", "200-220", id1).Val())
		requireMigrateSlotRangeState(t, rdb0, "200-220", SlotMigrationStateStarted)
		srv1Alive = false
		srv1.Close()
		time.Sleep(time.Second)
		// TODO: More precise migration failure slot range
		waitForMigrateSlotRangeState(t, rdb0, "200-220", SlotMigrationStateFailed)
	})
}
