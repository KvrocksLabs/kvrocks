//go:build !ignore_when_tsan

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

package tdigest

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

const (
	errMsgWrongNumberArg   = "wrong number of arguments"
	errMsgParseCompression = "error parsing compression parameter"
	errMsgNeedToBePositive = "compression parameter needs to be a positive integer"
	errMsgMustInRange      = "compression must be between 1 and 1000"
	errMsgKeyAlreadyExists = "key already exists"
	errMsgKeyNotExist      = "key does not exist"
)

type tdigestInfo struct {
	Compression       int64
	Capacity          int64
	MergedNodes       int64
	UnmergedNodes     int64
	MergedWeight      int64
	UnmergedWeight    int64
	Observations      int64
	TotalCompressions int64
	// memory usgae is not useful, we do not support it now
}

func toTdigestInfo(t *testing.T, value interface{}) tdigestInfo {
	require.IsType(t, map[interface{}]interface{}{}, value)
	v := value.(map[interface{}]interface{})
	return tdigestInfo{
		Compression:       v["Compression"].(int64),
		Capacity:          v["Capacity"].(int64),
		MergedNodes:       v["Merged nodes"].(int64),
		UnmergedNodes:     v["Unmerged nodes"].(int64),
		MergedWeight:      v["Merged weight"].(int64),
		UnmergedWeight:    v["Unmerged weight"].(int64),
		Observations:      v["Observations"].(int64),
		TotalCompressions: v["Total compressions"].(int64),
	}
}

func TestTDigest(t *testing.T) {
	configOptions := []util.ConfigOptions{
		{
			Name:       "txn-context-enabled",
			Options:    []string{"yes", "no"},
			ConfigType: util.YesNo,
		},
	}

	configsMatrix, err := util.GenerateConfigsMatrix(configOptions)
	require.NoError(t, err)

	for _, configs := range configsMatrix {
		tdigestTests(t, configs)
	}
}

func tdigestTests(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("tdigest.create with different arguments", func(t *testing.T) {
		keyPrefix := "tdigest_create_"
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "hahah").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "1", "hahah").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "compression").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "compression", "hahah").Err(), errMsgParseCompression)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "compression", "0").Err(), errMsgNeedToBePositive)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "compression", "-1").Err(), errMsgNeedToBePositive)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "compression", "0.1").Err(), errMsgParseCompression)

		require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key0", "compression", "1").Err())
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key0", "compression", "1").Err(), errMsgKeyAlreadyExists)
		require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key1", "compression", "1000").Err())
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "compression", "1001").Err(), errMsgMustInRange)
	})

	t.Run("tdigest.info with different arguments", func(t *testing.T) {
		keyPrefix := "tdigest_info_"
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.INFO").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.INFO", keyPrefix+"key", "hahah").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.INFO", keyPrefix+"key").Err(), errMsgKeyNotExist)
		require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key0", "compression", "1").Err())
		{
			rsp := rdb.Do(ctx, "TDIGEST.INFO", keyPrefix+"key0")
			require.NoError(t, rsp.Err())
			info := toTdigestInfo(t, rsp.Val())
			require.EqualValues(t, 1, info.Compression)
			require.EqualValues(t, 1*6+10, info.Capacity)
			require.EqualValues(t, 0, info.MergedNodes)
			require.EqualValues(t, 0, info.UnmergedNodes)
			require.EqualValues(t, 0, info.MergedWeight)
			require.EqualValues(t, 0, info.UnmergedWeight)
			require.EqualValues(t, 0, info.Observations)
			require.EqualValues(t, 0, info.TotalCompressions)
		}

		{
			require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key1", "compression", "1000").Err())
			rsp := rdb.Do(ctx, "TDIGEST.INFO", keyPrefix+"key1")
			require.NoError(t, rsp.Err())
			info := toTdigestInfo(t, rsp.Val())
			require.EqualValues(t, 1000, info.Compression)
			require.EqualValues(t, 1024, info.Capacity) // max is 1024
			require.EqualValues(t, 0, info.MergedNodes)
			require.EqualValues(t, 0, info.UnmergedNodes)
			require.EqualValues(t, 0, info.MergedWeight)
			require.EqualValues(t, 0, info.UnmergedWeight)
			require.EqualValues(t, 0, info.Observations)
			require.EqualValues(t, 0, info.TotalCompressions)
		}
	})
}
