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

package cms

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestCountMinSketch(t *testing.T) {
	// Define configuration options if needed.
	// Adjust or add more configurations as per your CMS requirements.
	configOptions := []util.ConfigOptions{
		{
			Name:       "txn-context-enabled",
			Options:    []string{"yes", "no"},
			ConfigType: util.YesNo,
		},
		// Add more configuration options here if necessary
	}

	// Generate all combinations of configurations
	configsMatrix, err := util.GenerateConfigsMatrix(configOptions)
	require.NoError(t, err)

	// Iterate over each configuration and run CMS tests
	for _, configs := range configsMatrix {
		testCMS(t, configs)
	}
}

// testCMS sets up the server with the given configurations and runs CMS tests
func testCMS(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	// Run individual CMS test cases
	t.Run("basic add", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "cmsA").Err())

		res := rdb.Do(ctx, "cms.initbydim", "cmsA", 100, 10)
		require.NoError(t, res.Err())
		require.Equal(t, "OK", res.Val())

		require.Equal(t, []interface{}{"width", int64(100), "depth", int64(10), "count", int64(0)}, rdb.Do(ctx, "cms.info", "cmsA").Val())

		res = rdb.Do(ctx, "cms.incrby", "cmsA", "foo", 1)
		require.NoError(t, res.Err())
		addCnt, err := res.Result()

		require.NoError(t, err)
		require.Equal(t, string("OK"), addCnt)

		card, err := rdb.Do(ctx, "cms.query", "cmsA", "foo").Result()
		require.NoError(t, err)
		require.Equal(t, []interface{}([]interface{}{"1"}), card, "The queried count for 'foo' should be 1")
	})

	t.Run("cms.initbyprob - Initialization with Probability Parameters", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "cmsB").Err())

		res := rdb.Do(ctx, "cms.initbyprob", "cmsB", "0.001", "0.1")
		require.NoError(t, res.Err())
		require.Equal(t, "OK", res.Val())

		infoRes := rdb.Do(ctx, "cms.info", "cmsB")
		require.NoError(t, infoRes.Err())
		infoSlice, ok := infoRes.Val().([]interface{})
		require.True(t, ok, "Expected cms.info to return a slice")

		infoMap := make(map[string]interface{})
		for i := 0; i < len(infoSlice); i += 2 {
			key, ok1 := infoSlice[i].(string)
			value, ok2 := infoSlice[i+1].(int64)
			require.True(t, ok1 && ok2, "Expected cms.info keys to be strings and values to be int64")
			infoMap[key] = value
		}

		require.Equal(t, int64(2000), infoMap["width"])
		require.Equal(t, int64(4), infoMap["depth"])
		require.Equal(t, int64(0), infoMap["count"])
	})

	t.Run("cms.incrby - Basic Increment Operations", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "cmsA").Err())
		res := rdb.Do(ctx, "cms.initbydim", "cmsA", "100", "10")
		require.NoError(t, res.Err())
		require.Equal(t, "OK", res.Val())

		elements := map[string]string{"apple": "7", "orange": "15", "mango": "3"}
		for key, count := range elements {
			res = rdb.Do(ctx, "cms.incrby", "cmsA", key, count)
			require.NoError(t, res.Err())
			require.Equal(t, "OK", res.Val())
		}

		for key, expected := range elements {
			res = rdb.Do(ctx, "cms.query", "cmsA", key)
			require.NoError(t, res.Err())
			countSlice, ok := res.Val().([]interface{})
			require.True(t, ok, "Expected cms.query to return a slice")
			require.Len(t, countSlice, 1, "Expected cms.query to return a single count")
			count, ok := countSlice[0].(string)
			require.True(t, ok, "Expected count to be a string")
			require.Equal(t, expected, count, fmt.Sprintf("Count for key '%s' mismatch", key))
		}

		// Verify total count
		infoRes := rdb.Do(ctx, "cms.info", "cmsA")
		require.NoError(t, infoRes.Err())
		infoSlice, ok := infoRes.Val().([]interface{})
		require.True(t, ok, "Expected cms.info to return a slice")

		// Convert the slice to a map for easier access
		infoMap := make(map[string]interface{})
		for i := 0; i < len(infoSlice); i += 2 {
			key, ok1 := infoSlice[i].(string)
			value, ok2 := infoSlice[i+1].(int64)
			require.True(t, ok1 && ok2, "Expected cms.info keys to be strings and values to be int64")
			infoMap[key] = value
		}

		total := int64(0)
		for _, cntStr := range elements {
			cnt, err := strconv.ParseInt(cntStr, 10, 64)
			require.NoError(t, err, "Failed to parse count string to int64")
			total += cnt
		}
		require.Equal(t, total, infoMap["count"], "Total count mismatch")
	})

	// Increment operation on a non-existent CMS
	t.Run("cms.incrby - Increment Non-Existent CMS", func(t *testing.T) {
		res := rdb.Do(ctx, "cms.incrby", "nonexistent_cms", "apple", "5")
		require.Error(t, res.Err())
	})

	t.Run("cms.query - Query Non-Existent CMS", func(t *testing.T) {
		// Attempt to query a CMS that doesn't exist
		res := rdb.Do(ctx, "cms.query", "nonexistent_cms", "foo")
		require.Error(t, res.Err())
		require.Contains(t, res.Err().Error(), "ERR NotFound:")
	})

	// Query for non-existent element
	t.Run("cms.query - Query Non-Existent Element", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "cmsA").Err())
		res := rdb.Do(ctx, "cms.initbydim", "cmsA", "100", "10")
		require.NoError(t, res.Err())
		require.Equal(t, "OK", res.Val())

		// Query a non-existent element
		res = rdb.Do(ctx, "cms.query", "cmsA", "nonexistent")
		require.NoError(t, res.Err())
		countSlice, ok := res.Val().([]interface{})
		require.True(t, ok, "Expected cms.query to return a slice")
		require.Len(t, countSlice, 1, "Expected cms.query to return a single count")
		count, ok := countSlice[0].(string)
		require.True(t, ok, "Expected count to be a string")
		require.Equal(t, "0", count, "Non-existent element should return count '0'")
	})

	// Merging CMS structures
	t.Run("cms.merge - Basic Merge Operation", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "cmsA").Err())
		require.NoError(t, rdb.Do(ctx, "DEL", "cmsB").Err())

		res := rdb.Do(ctx, "cms.initbydim", "cmsA", "100", "10")
		require.NoError(t, res.Err())
		require.Equal(t, "OK", res.Val())

		res = rdb.Do(ctx, "cms.initbydim", "cmsB", "100", "10")
		require.NoError(t, res.Err())
		require.Equal(t, "OK", res.Val())

		// Increment elements in cmsA
		elementsA := map[string]string{"apple": "7", "orange": "15", "mango": "3"}
		for key, count := range elementsA {
			res = rdb.Do(ctx, "cms.incrby", "cmsA", key, count)
			require.NoError(t, res.Err())
			require.Equal(t, "OK", res.Val())
		}

		// Increment elements in cmsB
		elementsB := map[string]string{"banana": "5", "apple": "4", "grape": "6"}
		for key, count := range elementsB {
			res = rdb.Do(ctx, "cms.incrby", "cmsB", key, count)
			require.NoError(t, res.Err())
			require.Equal(t, "OK", res.Val())
		}

		// Merge cmsB into cmsA with weights
		res = rdb.Do(ctx, "cms.merge", "cmsA", "1", "cmsB", "WEIGHTS", "1")
		require.NoError(t, res.Err())
		require.Equal(t, "OK", res.Val())

		// Query counts after merge
		expectedCounts := map[string]string{"apple": "11", "orange": "15", "mango": "3", "banana": "5", "grape": "6"}
		for key, expected := range expectedCounts {
			res = rdb.Do(ctx, "cms.query", "cmsA", key)
			require.NoError(t, res.Err())
			countSlice, ok := res.Val().([]interface{})
			require.True(t, ok, "Expected cms.query to return a slice")
			require.Len(t, countSlice, 1, "Expected cms.query to return a single count")
			count, ok := countSlice[0].(string)
			require.True(t, ok, "Expected count to be a string")
			require.Equal(t, expected, count, fmt.Sprintf("Count for key '%s' mismatch after merge", key))
		}

		infoRes := rdb.Do(ctx, "cms.info", "cmsA")
		require.NoError(t, infoRes.Err())
		infoSlice, ok := infoRes.Val().([]interface{})
		require.True(t, ok, "Expected cms.info to return a slice")

		infoMap := make(map[string]interface{})
		for i := 0; i < len(infoSlice); i += 2 {
			key, ok1 := infoSlice[i].(string)
			value, ok2 := infoSlice[i+1].(int64)
			require.True(t, ok1 && ok2, "Expected cms.info keys to be strings and values to be int64")
			infoMap[key] = value
		}

		expectedTotal := int64(40)
		require.Equal(t, expectedTotal, infoMap["count"], "Total count mismatch after merge")
	})

	t.Run("cms.merge - Merge with Uninitialized Destination CMS", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "cmsA").Err())
		require.NoError(t, rdb.Do(ctx, "DEL", "cmsB").Err())

		// Initialize only the source CMS
		res := rdb.Do(ctx, "cms.initbydim", "cmsB", "100", "10")
		require.NoError(t, res.Err())
		require.Equal(t, "OK", res.Val())

		// Attempt to merge cmsB into cmsA without initializing cmsA
		res = rdb.Do(ctx, "cms.merge", "cmsA", "1", "cmsB", "WEIGHTS", "1")
		require.Error(t, res.Err(), "Merging into an uninitialized destination CMS should return an error")
		require.Contains(t, res.Err().Error(), "Destination CMS does not exist.", "Expected error message to contain 'Destination CMS does not exist.'")
	})

	t.Run("cms.merge - Merge with Uninitialized Source CMS", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "cmsA").Err())
		require.NoError(t, rdb.Do(ctx, "DEL", "cmsB").Err())

		// Initialize only the destination CMS
		res := rdb.Do(ctx, "cms.initbydim", "cmsA", "100", "10")
		require.NoError(t, res.Err())
		require.Equal(t, "OK", res.Val())

		// Attempt to merge a non-initialized cmsB into cmsA
		res = rdb.Do(ctx, "cms.merge", "cmsA", "1", "cmsB", "WEIGHTS", "1")
		require.Error(t, res.Err(), "Merging from an uninitialized source CMS should return an error")
		require.Contains(t, res.Err().Error(), "Source CMS key not found.", "Expected error message to contain 'Source CMS key not found.'")
	})

	t.Run("cms.merge - Merge with Both Destination and Source CMS Uninitialized", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "cmsA").Err())
		require.NoError(t, rdb.Do(ctx, "DEL", "cmsB").Err())

		// Attempt to merge two non-initialized CMSes
		res := rdb.Do(ctx, "cms.merge", "cmsA", "1", "cmsB", "WEIGHTS", "1")
		require.Error(t, res.Err(), "Merging with both destination and source CMS uninitialized should return an error")
		errMsg := res.Err().Error()
		require.Contains(t, errMsg, "Destination CMS does not exist.")
	})
}
