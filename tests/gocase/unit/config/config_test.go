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

package config

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRenameCommand(t *testing.T) {
	t.Parallel()
	srv := util.StartServer(t, map[string]string{
		"resp3-enabled":       "no",
		"rename-command KEYS": "KEYSNEW",
		"rename-command GET":  "GETNEW",
		"rename-command SET":  "SETNEW",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	require.ErrorContains(t, rdb.Keys(ctx, "*").Err(), "unknown command")
	require.ErrorContains(t, rdb.Get(ctx, "key").Err(), "unknown command")
	require.ErrorContains(t, rdb.Set(ctx, "key", "1", 0).Err(), "unknown command")
	require.Equal(t, []interface{}{}, rdb.Do(ctx, "KEYSNEW", "*").Val())
	require.NoError(t, rdb.Do(ctx, "SETNEW", "key", "1").Err())
	require.Equal(t, "1", rdb.Do(ctx, "GETNEW", "key").Val())
	val := []string{}
	for _, v := range rdb.Do(ctx, "config", "get", "rename-command").Val().([]interface{}) {
		val = append(val, v.(string))
	}
	sort.Strings(val)
	require.EqualValues(t, []string{"GET GETNEW", "KEYS KEYSNEW", "SET SETNEW", "rename-command", "rename-command", "rename-command"}, val)
}

func TestSetConfigBackupDir(t *testing.T) {
	t.Parallel()
	configs := map[string]string{}
	srv := util.StartServer(t, configs)
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	originBackupDir := filepath.Join(configs["dir"], "backup")

	r := rdb.ConfigGet(ctx, "backup-dir").Val()
	require.EqualValues(t, r["backup-dir"], originBackupDir)

	hasCompactionFiles := func(dir string) bool {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			return false
		}
		files, err := os.ReadDir(dir)
		if err != nil {
			log.Fatal(err)
		}
		return len(files) != 0
	}

	require.False(t, hasCompactionFiles(originBackupDir))

	require.NoError(t, rdb.Do(ctx, "bgsave").Err())
	time.Sleep(2000 * time.Millisecond)

	require.True(t, hasCompactionFiles(originBackupDir))

	newBackupDir := filepath.Join(configs["dir"], "backup2")

	require.False(t, hasCompactionFiles(newBackupDir))

	require.NoError(t, rdb.ConfigSet(ctx, "backup-dir", newBackupDir).Err())
	r = rdb.ConfigGet(ctx, "backup-dir").Val()
	require.EqualValues(t, r["backup-dir"], newBackupDir)

	require.NoError(t, rdb.BgSave(ctx).Err())
	time.Sleep(2000 * time.Millisecond)

	require.True(t, hasCompactionFiles(newBackupDir))
	require.True(t, hasCompactionFiles(originBackupDir))
}

func TestConfigSetCompression(t *testing.T) {
	t.Parallel()
	configs := map[string]string{}
	srv := util.StartServer(t, configs)
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	require.NoError(t, rdb.Do(ctx, "SET", "foo", "bar").Err())

	configKey := "rocksdb.compression"
	supportedCompressions := []string{"no", "snappy", "zlib", "lz4", "zstd"}
	for _, compression := range supportedCompressions {
		require.NoError(t, rdb.ConfigSet(ctx, configKey, compression).Err())
		vals, err := rdb.ConfigGet(ctx, configKey).Result()
		require.NoError(t, err)
		require.EqualValues(t, compression, vals[configKey])
	}
	require.ErrorContains(t, rdb.ConfigSet(ctx, configKey, "unsupported").Err(), "invalid enum option")
}

func TestConfigGetRESP3(t *testing.T) {
	t.Parallel()
	srv := util.StartServer(t, map[string]string{
		"resp3-enabled": "yes",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	val := rdb.ConfigGet(ctx, "resp3-enabled").Val()
	require.EqualValues(t, "yes", val["resp3-enabled"])
}

func TestStartWithoutConfigurationFile(t *testing.T) {
	t.Parallel()
	srv := util.StartServerWithCLIOptions(t, false, map[string]string{}, []string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	require.NoError(t, rdb.Do(ctx, "SET", "foo", "bar").Err())
	require.Equal(t, "bar", rdb.Do(ctx, "GET", "foo").Val())
}

func TestDynamicChangeWorkerThread(t *testing.T) {
	t.Parallel()
	configs := map[string]string{}
	srv := util.StartServer(t, configs)
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Test dynamic change worker thread", func(t *testing.T) {
		runCommands := func(workers int) {
			var wg sync.WaitGroup
			require.NoError(t, rdb.Do(ctx, "CONFIG", "SET", "workers", strconv.Itoa(workers)).Err())
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 10; j++ {
						require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
					}
				}()
			}
			wg.Wait()
		}
		// Reduce worker threads to 4
		runCommands(4)

		// Reduce worker threads to 1
		runCommands(1)

		// Increase worker threads to 12
		runCommands(12)
	})

	t.Run("Test dynamic change worker thread with blocking requests", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
		defer cancel()

		blockingTimeout := 5 * time.Second

		var wg sync.WaitGroup
		// blocking on list
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = rdb.BLPop(ctx, blockingTimeout, "list")
		}()

		// channel subscribe
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub := rdb.Subscribe(ctx, "c1", "c2", "c3")
			_, _ = sub.ReceiveTimeout(ctx, blockingTimeout)
		}()

		// pattern subscribe
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub := rdb.PSubscribe(ctx, "c1", "c2", "c3")
			_, _ = sub.ReceiveTimeout(ctx, blockingTimeout)
		}()

		// blocking on stream
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = rdb.XRead(ctx, &redis.XReadArgs{
				Streams: []string{"s1", "$"},
				Count:   1,
				Block:   blockingTimeout,
			})
		}()

		// sleep a while to make sure all blocking requests are ready
		time.Sleep(time.Second)
		require.NoError(t, rdb.Do(ctx, "CONFIG", "SET", "workers", "1").Err())
		wg.Wait()

		// We don't care about the result of these commands since we can't tell if the connection
		// is migrated or not. We just want to confirm that server works well if there have blocking
		// requests after changing worker threads.
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.Equal(t, "bar", rdb.Get(ctx, "foo").Val())
	})
}

func TestChangeProtoMaxBulkLen(t *testing.T) {
	t.Parallel()
	configs := map[string]string{}
	srv := util.StartServer(t, configs)
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	// Default value is 512MB
	vals, err := rdb.ConfigGet(ctx, "proto-max-bulk-len").Result()
	require.NoError(t, err)
	require.EqualValues(t, "536870912", vals["proto-max-bulk-len"])

	// Change to 2MB
	require.NoError(t, rdb.ConfigSet(ctx, "proto-max-bulk-len", "2097152").Err())
	vals, err = rdb.ConfigGet(ctx, "proto-max-bulk-len").Result()
	require.NoError(t, err)
	require.EqualValues(t, "2097152", vals["proto-max-bulk-len"])

	// change to 100MB
	require.NoError(t, rdb.ConfigSet(ctx, "proto-max-bulk-len", "100M").Err())
	vals, err = rdb.ConfigGet(ctx, "proto-max-bulk-len").Result()
	require.NoError(t, err)
	require.EqualValues(t, "104857600", vals["proto-max-bulk-len"])

	// Must be >= 1MB
	require.Error(t, rdb.ConfigSet(ctx, "proto-max-bulk-len", "1024").Err())
}

func TestGetConfigTxnContext(t *testing.T) {
	t.Parallel()
	srv := util.StartServer(t, map[string]string{
		"txn-context-enabled": "yes",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	val := rdb.ConfigGet(ctx, "txn-context-enabled").Val()
	require.EqualValues(t, "yes", val["txn-context-enabled"])

	// default value "no"
	srv1 := util.StartServer(t, map[string]string{})
	defer srv1.Close()

	rdb = srv1.NewClient()
	val = rdb.ConfigGet(ctx, "txn-context-enabled").Val()
	require.EqualValues(t, "no", val["txn-context-enabled"])
}

func TestGenerateConfigsMatrix(t *testing.T) {
	t.Parallel()
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
	}

	configsMatrix, err := util.GenerateConfigsMatrix(configOptions)

	require.NoError(t, err)
	require.Equal(t, 4, len(configsMatrix))
	require.Contains(t, configsMatrix, util.KvrocksServerConfigs{"txn-context-enabled": "yes", "resp3-enabled": "yes"})
	require.Contains(t, configsMatrix, util.KvrocksServerConfigs{"txn-context-enabled": "yes", "resp3-enabled": "no"})
	require.Contains(t, configsMatrix, util.KvrocksServerConfigs{"txn-context-enabled": "no", "resp3-enabled": "yes"})
	require.Contains(t, configsMatrix, util.KvrocksServerConfigs{"txn-context-enabled": "no", "resp3-enabled": "no"})
}

func TestGetConfigSkipBlockCacheDeallocationOnClose(t *testing.T) {
	t.Parallel()
	srv := util.StartServer(t, map[string]string{
		"skip-block-cache-deallocation-on-close": "yes",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	val := rdb.ConfigGet(ctx, "skip-block-cache-deallocation-on-close").Val()
	require.EqualValues(t, "yes", val["skip-block-cache-deallocation-on-close"])

	// default value "no"
	srv1 := util.StartServer(t, map[string]string{})
	defer srv1.Close()

	rdb = srv1.NewClient()
	val = rdb.ConfigGet(ctx, "skip-block-cache-deallocation-on-close").Val()
	require.EqualValues(t, "no", val["skip-block-cache-deallocation-on-close"])
}
