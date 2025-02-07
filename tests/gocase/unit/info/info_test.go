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

package info

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
)

func TestInfo(t *testing.T) {
	srv0 := util.StartServer(t, map[string]string{
		"cluster-enabled":             "yes",
		"histogram-bucket-boundaries": "10,20,30,50",
	})
	defer func() { srv0.Close() }()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()

	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	MustAtoi := func(t *testing.T, s string) int {
		i, err := strconv.Atoi(s)
		require.NoError(t, err)
		return i
	}

	t.Run("check last dbsize scan timestamp", func(t *testing.T) {
		require.Equal(t, "0", util.FindInfoEntry(rdb, "last_dbsize_scan_timestamp", "keyspace"))
		require.NoError(t, rdb.Do(ctx, "DBSIZE", "SCAN").Err())
		require.Eventually(t, func() bool {
			return MustAtoi(t, util.FindInfoEntry(rdb, "last_dbsize_scan_timestamp", "keyspace")) > 0
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("get rocksdb ops by INFO", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			k := fmt.Sprintf("key%d", i)
			v := fmt.Sprintf("value%d", i)
			for j := 0; j < 500; j++ {
				rdb.LPush(ctx, k, v)
				rdb.LRange(ctx, k, 0, 1)
			}
			time.Sleep(time.Second)
		}

		r := util.FindInfoEntry(rdb, "put_per_sec", "rocksdb")
		require.Greater(t, MustAtoi(t, r), 0)
		r = util.FindInfoEntry(rdb, "get_per_sec", "rocksdb")
		require.Greater(t, MustAtoi(t, r), 0)
		r = util.FindInfoEntry(rdb, "seek_per_sec", "rocksdb")
		require.Greater(t, MustAtoi(t, r), 0)
		r = util.FindInfoEntry(rdb, "next_per_sec", "rocksdb")
		require.Greater(t, MustAtoi(t, r), 0)
	})

	t.Run("get bgsave information by INFO", func(t *testing.T) {
		require.Equal(t, "0", util.FindInfoEntry(rdb, "bgsave_in_progress", "persistence"))
		require.Equal(t, "ok", util.FindInfoEntry(rdb, "last_bgsave_status", "persistence"))
		require.Equal(t, "-1", util.FindInfoEntry(rdb, "last_bgsave_time_sec", "persistence"))

		r := rdb.Do(ctx, "bgsave")
		v, err := r.Text()
		require.NoError(t, err)
		require.Equal(t, "OK", v)

		require.Eventually(t, func() bool {
			e := MustAtoi(t, util.FindInfoEntry(rdb, "bgsave_in_progress", "persistence"))
			return e == 0
		}, 5*time.Second, 100*time.Millisecond)

		lastBgsaveTime := MustAtoi(t, util.FindInfoEntry(rdb, "last_bgsave_time", "persistence"))
		require.Greater(t, lastBgsaveTime, 1640507660)
		require.Equal(t, "ok", util.FindInfoEntry(rdb, "last_bgsave_status", "persistence"))
		lastBgsaveTimeSec := MustAtoi(t, util.FindInfoEntry(rdb, "last_bgsave_time_sec", "persistence"))
		require.GreaterOrEqual(t, lastBgsaveTimeSec, 0)
		require.Less(t, lastBgsaveTimeSec, 3)
	})

	t.Run("get cluster information by INFO - cluster not enabled", func(t *testing.T) {
		require.Equal(t, "0", util.FindInfoEntry(rdb, "cluster_enabled", "cluster"))
	})

	t.Run("get cluster information by INFO - cluster enabled", func(t *testing.T) {
		require.Equal(t, "1", util.FindInfoEntry(rdb0, "cluster_enabled", "cluster"))
	})

	t.Run("get command latencies via histogram INFO - histogram-bucket-boundaries", func(t *testing.T) {
		output := util.FindInfoEntry(rdb0, "cmdstathist_info", "commandstats")

		splitValues := strings.FieldsFunc(output, func(r rune) bool {
			return r == '=' || r == ','
		})

		// expected: 10=..,20=..,30=..,50=..,inf=..,sum=...,count=..
		require.GreaterOrEqual(t, len(splitValues), 14)
		require.Contains(t, splitValues, "sum")
		require.Contains(t, splitValues, "count")
		require.Contains(t, splitValues, "inf")
	})

	t.Run("multiple sections", func(t *testing.T) {
		info := rdb.Info(ctx, "server", "cpu")
		require.NoError(t, info.Err())
		require.Contains(t, info.Val(), "# Server")
		require.Contains(t, info.Val(), "# CPU")
	})
}

func TestKeyspaceHitMiss(t *testing.T) {
	srv0 := util.StartServer(t, map[string]string{})
	defer func() { srv0.Close() }()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()

	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	require.Equal(t, "0", util.FindInfoEntry(rdb0, "keyspace_hits", "stats"))
	require.Equal(t, "0", util.FindInfoEntry(rdb0, "keyspace_misses", "stats"))
	require.NoError(t, rdb0.Set(ctx, "foo", "bar", 0).Err())

	require.NoError(t, rdb0.Get(ctx, "foo").Err())
	require.Equal(t, "1", util.FindInfoEntry(rdb0, "keyspace_hits", "stats"))
	require.Equal(t, "0", util.FindInfoEntry(rdb0, "keyspace_misses", "stats"))

	require.EqualError(t, rdb0.Get(ctx, "no_exists_key").Err(), redis.Nil.Error())
	require.Equal(t, "1", util.FindInfoEntry(rdb0, "keyspace_hits", "stats"))
	require.Equal(t, "1", util.FindInfoEntry(rdb0, "keyspace_misses", "stats"))

	require.NoError(t, rdb0.HSet(ctx, "hash", "f1", "v1").Err())
	require.Equal(t, "1", util.FindInfoEntry(rdb0, "keyspace_hits", "stats"))
	// increase by 2 because of the previous HSet will try to get the key first
	require.Equal(t, "2", util.FindInfoEntry(rdb0, "keyspace_misses", "stats"))

	require.NoError(t, rdb0.HGet(ctx, "hash", "f1").Err())
	require.Equal(t, "2", util.FindInfoEntry(rdb0, "keyspace_hits", "stats"))
	require.Equal(t, "2", util.FindInfoEntry(rdb0, "keyspace_misses", "stats"))

	require.EqualError(t, rdb0.HGet(ctx, "no_exists_hash", "f1").Err(), redis.Nil.Error())
	require.Equal(t, "2", util.FindInfoEntry(rdb0, "keyspace_hits", "stats"))
	require.Equal(t, "3", util.FindInfoEntry(rdb0, "keyspace_misses", "stats"))
}
