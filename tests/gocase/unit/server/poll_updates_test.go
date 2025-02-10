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

package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

type PollUpdatesResult struct {
	LatestSeq int64
	NextSeq   int64
	Updates   []any
}

type RESPFormat struct {
	Namespace string
	Commands  [][]string
}

func parseRESPCommands(t *testing.T, values []any) []any {
	if len(values)%2 != 0 {
		require.Fail(t, fmt.Sprintf("invalid updates length: %d", len(values)))
	}
	updates := make([]any, 0)
	for i := 0; i < len(values); i += 2 {
		namespace, ok := values[i].(string)
		require.True(t, ok)
		updateValues, ok := values[i+1].([]interface{})
		require.True(t, ok)

		commands := make([][]string, 0)
		for _, v := range updateValues {
			elements, ok := v.([]interface{})
			require.True(t, ok)
			var tokens []string
			for _, element := range elements {
				tokens = append(tokens, element.(string))
			}
			commands = append(commands, tokens)
		}
		updates = append(updates, RESPFormat{
			Namespace: namespace,
			Commands:  commands,
		})
	}
	return updates
}

func parsePollUpdatesResult(t *testing.T, m map[any]any, isRESP bool) *PollUpdatesResult {
	itemCount := 3
	require.Len(t, m, itemCount)
	var latestSeq, nextSeq int64

	updates := make([]any, 0)
	for k, v := range m {
		key := k.(string)
		switch key {
		case "latest_sequence":
			latestSeq = v.(int64)
		case "next_sequence":
			nextSeq = v.(int64)
		case "updates":
			fields := v.([]interface{})
			if isRESP {
				updates = parseRESPCommands(t, fields)
			} else {
				for _, field := range fields {
					str, ok := field.(string)
					require.True(t, ok)
					updates = append(updates, str)
				}
			}
		default:
			require.Fail(t, fmt.Sprintf("unknown key: %s", key))
		}
	}

	return &PollUpdatesResult{
		LatestSeq: latestSeq,
		NextSeq:   nextSeq,
		Updates:   updates,
	}
}

func TestPollUpdates_Basic(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{})
	defer srv0.Close()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()

	srv1 := util.StartServer(t, map[string]string{})
	defer srv1.Close()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()

	t.Run("Make sure the command POLLUPDATES works well", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			rdb0.Set(ctx, fmt.Sprintf("key-%d", i), i, 0)
		}

		updates := make([]any, 0)
		result, err := rdb0.Do(ctx, "POLLUPDATES", 0, "MAX", 6).Result()
		require.NoError(t, err)
		pollUpdates := parsePollUpdatesResult(t, result.(map[interface{}]interface{}), false)
		require.EqualValues(t, 10, pollUpdates.LatestSeq)
		require.EqualValues(t, 6, pollUpdates.NextSeq)
		require.Len(t, pollUpdates.Updates, 6)
		updates = append(updates, pollUpdates.Updates...)

		result, err = rdb0.Do(ctx, "POLLUPDATES", pollUpdates.NextSeq, "MAX", 6).Result()
		require.NoError(t, err)

		pollUpdates = parsePollUpdatesResult(t, result.(map[interface{}]interface{}), false)
		require.EqualValues(t, 10, pollUpdates.LatestSeq)
		require.EqualValues(t, 10, pollUpdates.NextSeq)
		require.Len(t, pollUpdates.Updates, 4)
		updates = append(updates, pollUpdates.Updates...)

		for i := 0; i < 10; i++ {
			batch, err := hex.DecodeString(updates[i].(string))
			require.NoError(t, err)
			applied, err := rdb1.Do(ctx, "APPLYBATCH", batch).Bool()
			require.NoError(t, err)
			require.True(t, applied)
			require.EqualValues(t, strconv.Itoa(i), rdb1.Get(ctx, fmt.Sprintf("key-%d", i)).Val())
		}
	})

	t.Run("Runs POLLUPDATES with invalid arguments", func(t *testing.T) {
		require.ErrorContains(t, rdb0.Do(ctx, "POLLUPDATES", 0, "MAX", -1).Err(),
			"ERR out of numeric range")
		require.ErrorContains(t, rdb0.Do(ctx, "POLLUPDATES", 0, "MAX", 1001).Err(),
			"ERR out of numeric range")
		require.ErrorContains(t, rdb0.Do(ctx, "POLLUPDATES", 0, "FORMAT", "COMMAND").Err(),
			"ERR invalid FORMAT option, should be RAW or RESP")
		require.ErrorContains(t, rdb0.Do(ctx, "POLLUPDATES", 12, "FORMAT", "RAW").Err(),
			"ERR next sequence is out of range")
		require.Error(t, rdb0.Do(ctx, "POLLUPDATES", 1, "FORMAT", "EXTRA").Err())
	})
}

func TestPollUpdates_WithRESPFormat(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{})
	defer srv0.Close()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()

	var pollUpdates *PollUpdatesResult
	t.Run("String type", func(t *testing.T) {
		require.NoError(t, rdb0.Set(ctx, "k0", "v0", 0).Err())
		require.NoError(t, rdb0.Set(ctx, "k1", "v1", 0).Err())
		require.NoError(t, rdb0.Del(ctx, "k1").Err())
		result, err := rdb0.Do(ctx, "POLLUPDATES", 0, "MAX", 10, "FORMAT", "RESP").Result()
		require.NoError(t, err)

		pollUpdates = parsePollUpdatesResult(t, result.(map[any]any), true)
		require.EqualValues(t, 3, pollUpdates.LatestSeq)
		require.EqualValues(t, 3, pollUpdates.NextSeq)
		require.Len(t, pollUpdates.Updates, 1)
		require.EqualValues(t, []any{RESPFormat{
			Namespace: "default",
			Commands: [][]string{
				{"SET", "k0", "v0"},
				{"SET", "k1", "v1"},
				{"DEL", "k1"},
			}},
		}, pollUpdates.Updates)
	})

	t.Run("Hash type", func(t *testing.T) {
		require.NoError(t, rdb0.HSet(ctx, "h0", "f0", "v0", "f1", "v1").Err())
		require.NoError(t, rdb0.HDel(ctx, "h0", "f1").Err())
		result, err := rdb0.Do(ctx, "POLLUPDATES", pollUpdates.NextSeq, "MAX", 10, "FORMAT", "RESP").Result()
		require.NoError(t, err)

		pollUpdates = parsePollUpdatesResult(t, result.(map[any]any), true)
		require.Len(t, pollUpdates.Updates, 1)
		require.EqualValues(t, []any{RESPFormat{
			Namespace: "default",
			Commands: [][]string{
				{"HSET", "h0", "f1", "v1"},
				{"HSET", "h0", "f0", "v0"},
				{"HDEL", "h0", "f1"},
			}},
		}, pollUpdates.Updates)
	})

	t.Run("List type", func(t *testing.T) {
		require.NoError(t, rdb0.LPush(ctx, "l0", "v0", "v1").Err())
		require.NoError(t, rdb0.LSet(ctx, "l0", 1, "v2").Err())
		result, err := rdb0.Do(ctx, "POLLUPDATES", pollUpdates.NextSeq, "MAX", 10, "FORMAT", "RESP").Result()
		require.NoError(t, err)

		pollUpdates = parsePollUpdatesResult(t, result.(map[any]any), true)
		require.Len(t, pollUpdates.Updates, 1)
		require.EqualValues(t, []any{RESPFormat{
			Namespace: "default",
			Commands: [][]string{
				{"LPUSH", "l0", "v0"},
				{"LPUSH", "l0", "v1"},
				{"LSET", "l0", "1", "v2"},
			}},
		}, pollUpdates.Updates)
	})

	t.Run("Set type", func(t *testing.T) {
		require.NoError(t, rdb0.SAdd(ctx, "s0", "v0", "v1").Err())
		require.NoError(t, rdb0.SRem(ctx, "s0", "v1").Err())
		result, err := rdb0.Do(ctx, "POLLUPDATES", pollUpdates.NextSeq, "MAX", 10, "FORMAT", "RESP").Result()
		require.NoError(t, err)

		pollUpdates = parsePollUpdatesResult(t, result.(map[any]any), true)
		require.Len(t, pollUpdates.Updates, 1)
		require.EqualValues(t, []any{RESPFormat{
			Namespace: "default",
			Commands: [][]string{
				{"SADD", "s0", "v0"},
				{"SADD", "s0", "v1"},
				{"SREM", "s0", "v1"},
			}},
		}, pollUpdates.Updates)
	})

	t.Run("ZSet type", func(t *testing.T) {
		require.NoError(t, rdb0.ZAdd(ctx, "z0", redis.Z{Member: "v0", Score: 1.2}).Err())
		require.NoError(t, rdb0.ZAdd(ctx, "z0", redis.Z{Member: "v1", Score: 1.2}).Err())
		require.NoError(t, rdb0.ZRem(ctx, "z0", "v1").Err())
		result, err := rdb0.Do(ctx, "POLLUPDATES", pollUpdates.NextSeq, "MAX", 10, "FORMAT", "RESP").Result()
		require.NoError(t, err)

		pollUpdates = parsePollUpdatesResult(t, result.(map[any]any), true)
		require.Len(t, pollUpdates.Updates, 1)
		require.EqualValues(t, []any{RESPFormat{
			Namespace: "default",
			Commands: [][]string{
				{"ZADD", "z0", "1.200000", "v0"},
				{"ZADD", "z0", "1.200000", "v1"},
				{"ZREM", "z0", "v1"},
			}},
		}, pollUpdates.Updates)
	})

	t.Run("Stream type", func(t *testing.T) {
		id, err := rdb0.XAdd(ctx, &redis.XAddArgs{
			Stream: "stream",
			Values: map[string]interface{}{"field": "value"},
		}).Result()
		require.NoError(t, err)
		require.NoError(t, rdb0.XDel(ctx, "stream", id).Err())
		result, err := rdb0.Do(ctx, "POLLUPDATES", pollUpdates.NextSeq, "MAX", 10, "FORMAT", "RESP").Result()
		require.NoError(t, err)

		pollUpdates = parsePollUpdatesResult(t, result.(map[any]any), true)
		require.Len(t, pollUpdates.Updates, 1)
		require.EqualValues(t, []any{RESPFormat{
			Commands: [][]string{
				{"XADD", "stream", id, "field", "value"},
				{"XDEL", "stream", id},
			}},
		}, pollUpdates.Updates)
	})

	t.Run("JSON type", func(t *testing.T) {
		require.NoError(t, rdb0.JSONSet(ctx, "json", "$", `{"field": "value"}`).Err())
		require.NoError(t, rdb0.JSONDel(ctx, "json", "$.field").Err())
		result, err := rdb0.Do(ctx, "POLLUPDATES", pollUpdates.NextSeq, "MAX", 10, "FORMAT", "RESP").Result()
		require.NoError(t, err)

		pollUpdates = parsePollUpdatesResult(t, result.(map[any]any), true)
		require.Len(t, pollUpdates.Updates, 1)
		require.EqualValues(t, []any{RESPFormat{
			Namespace: "default",
			Commands: [][]string{
				{"JSON.SET", "json", "$", `{"field":"value"}`},
				{"JSON.SET", "json", "$", `{}`},
			}},
		}, pollUpdates.Updates)
	})
}

func TestPollUpdates_WithStrict(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{})
	defer srv0.Close()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()

	srv1 := util.StartServer(t, map[string]string{})
	defer srv1.Close()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()

	// The latest sequence is 2 after running the HSET command, 1 for the metadata and 1 for the field
	require.NoError(t, rdb0.HSet(ctx, "h0", "f0", "v0").Err())
	// The latest sequence is 3 after running the SET command
	require.NoError(t, rdb0.Set(ctx, "k0", "v0", 0).Err())

	// PollUpdates with strict mode should return an error if the sequence number is mismatched
	err := rdb0.Do(ctx, "POLLUPDATES", 1, "MAX", 1, "STRICT").Err()
	require.ErrorContains(t, err, "ERR mismatched sequence number")

	// Works well if the sequence number is mismatched but not in strict mode
	require.NoError(t, rdb0.Do(ctx, "POLLUPDATES", 1, "MAX", 1).Err())

	result, err := rdb0.Do(ctx, "POLLUPDATES", 0, "MAX", 10, "STRICT").Result()
	require.NoError(t, err)
	pollUpdates := parsePollUpdatesResult(t, result.(map[any]any), false)
	require.EqualValues(t, 3, pollUpdates.LatestSeq)
	require.EqualValues(t, 3, pollUpdates.NextSeq)
	require.Len(t, pollUpdates.Updates, 2)

	for _, update := range pollUpdates.Updates {
		batch, err := hex.DecodeString(update.(string))
		require.NoError(t, err)
		require.NoError(t, rdb1.Do(ctx, "APPLYBATCH", batch).Err())
	}

	require.Equal(t, "v0", rdb1.Get(ctx, "k0").Val())
	require.Equal(t, "v0", rdb1.HGet(ctx, "h0", "f0").Val())
}
