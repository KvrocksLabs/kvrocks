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

package list

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// We need a value larger than list-max-ziplist-value to make sure
// the list has the right encoding when it is swapped in again.
var largeValue = map[string]string{
	"zipList":    "hello",
	"linkedList": strings.Repeat("hello", 4),
}

func TestLTRIM(t *testing.T) {
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
		testLTRIM(t, configs)
	}
}

func testLTRIM(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := "myList"
	startLen := int64(32)

	for typ, value := range largeValue {
		t.Run(fmt.Sprintf("LTRIM stress testing - %s", typ), func(t *testing.T) {
			var myList []string
			require.NoError(t, rdb.Del(ctx, key).Err())
			require.NoError(t, rdb.RPush(ctx, key, value).Err())
			myList = append(myList, value)

			for i := int64(0); i < startLen; i++ {
				s := strconv.FormatInt(rand.Int63(), 10)
				require.NoError(t, rdb.RPush(ctx, key, s).Err())
				myList = append(myList, s)
			}

			for i := 0; i < 1000; i++ {
				lo := int64(rand.Float64() * float64(startLen))
				hi := int64(float64(lo) + rand.Float64()*float64(startLen))

				myList = myList[lo:min(int(hi+1), len(myList))]
				require.NoError(t, rdb.LTrim(ctx, key, lo, hi).Err())
				require.Equal(t, myList, rdb.LRange(ctx, key, 0, -1).Val(), "failed trim")

				starting := rdb.LLen(ctx, key).Val()
				for j := starting; j < startLen; j++ {
					s := strconv.FormatInt(rand.Int63(), 10)
					require.NoError(t, rdb.RPush(ctx, key, s).Err())
					myList = append(myList, s)
					require.Equal(t, myList, rdb.LRange(ctx, key, 0, -1).Val(), "failed append match")
				}
			}
		})
	}
}

func TestZipList(t *testing.T) {
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
		testZipList(t, configs)
	}
}

func testZipList(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClientWithOption(&redis.Options{
		ReadTimeout: 30 * time.Second,
		MaxRetries:  -1, // disable retry
	})
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Explicit regression for a list bug", func(t *testing.T) {
		key := "l"
		myList := []string{
			"49376042582", "BkG2o\\pIC]4YYJa9cJ4GWZalG[4tin;1D2whSkCOW`mX;SFXGyS8sedcff3fQI^tgPCC@^Nu1J6o]meM@Lko]t_jRyo<xSJ1oObDYd`ppZuW6P@fS278YaOx=s6lvdFlMbP0[SbkI^Kr\\HBXtuFaA^mDx:yzS4a[skiiPWhT<nNfAf=aQVfclcuwDrfe;iVuKdNvB9kbfq>tK?tH[\\EvWqS]b`o2OCtjg:?nUTwdjpcUm]y:pg5q24q7LlCOwQE^",
		}
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.RPush(ctx, key, myList[0]).Err())
		require.NoError(t, rdb.RPush(ctx, key, myList[1]).Err())
		require.Equal(t, myList[0], rdb.LIndex(ctx, key, 0).Val())
		require.Equal(t, myList[1], rdb.LIndex(ctx, key, 1).Val())
	})

	t.Run("Regression for quicklist #3343 bug", func(t *testing.T) {
		key := "myList"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.LPush(ctx, key, "401").Err())
		require.NoError(t, rdb.LPush(ctx, key, "392").Err())

		require.NoError(t, rdb.RPush(ctx, key, fmt.Sprintf("%s\"%s\"", strings.Repeat("x", 5105), "799")).Err())
		require.NoError(t, rdb.LSet(ctx, key, -1, fmt.Sprintf("%s\"%s\"", strings.Repeat("x", 1014), "702")).Err())

		require.NoError(t, rdb.LPop(ctx, key).Err())

		require.NoError(t, rdb.LSet(ctx, key, -1, fmt.Sprintf("%s\"%s\"", strings.Repeat("x", 4149), "852")).Err())

		require.NoError(t, rdb.LInsert(ctx, key, "before", "401", fmt.Sprintf("%s\"%s\"", strings.Repeat("x", 9927), "12")).Err())
		require.NoError(t, rdb.LRange(ctx, key, 0, -1).Err())
		require.Equal(t, rdb.Ping(ctx).Val(), "PONG")
	})

	t.Run("Stress tester for #3343-alike bugs", func(t *testing.T) {
		key := "key"
		require.NoError(t, rdb.Del(ctx, key).Err())
		for i := 0; i < 10000; i++ {
			op := rand.Int63n(6)
			randCnt := 5 - rand.Int63n(10)
			var ele string
			if rand.Int31n(2) == 0 {
				ele = fmt.Sprintf("%d", rand.Int63n(1000))
			} else {
				ele = fmt.Sprintf("%s%d", strings.Repeat("x", int(rand.Int63n(10000))), 1)
			}
			switch op {
			case 0:
				require.NoError(t, rdb.LPush(ctx, key, ele).Err())
			case 1:
				require.NoError(t, rdb.RPush(ctx, key, ele).Err())
			case 2:
				rdb.LPop(ctx, key)
			case 3:
				rdb.RPop(ctx, key)
			case 4:
				rdb.LSet(ctx, key, randCnt, ele)
			case 5:
				otherEle := fmt.Sprintf("%d", rand.Int63n(1000))
				var where string
				if rand.Int31n(2) == 0 {
					where = "before"
				} else {
					where = "after"
				}
				require.NoError(t, rdb.LInsert(ctx, key, where, otherEle, ele).Err())
			}
		}
	})

	t.Run("ziplist implementation: value encoding and backlink", func(t *testing.T) {
		iterations := 100
		for j := 0; j < iterations; j++ {
			key := fmt.Sprintf("l1-%d", j)
			require.NoError(t, rdb.Del(ctx, key).Err())
			var lis []string
			for i := 0; i < 200; i++ {
				op := rand.Int63n(7)
				data := ""
				switch op {
				case 0:
					data = strings.Repeat("x", int(rand.Int63n(1000000)))
				case 1:
					data = fmt.Sprintf("%d", rand.Int63n(65536))
				case 2:
					data = fmt.Sprintf("%d", rand.Int63n(4294967296))
				case 3:
					data = fmt.Sprintf("%d", rand.Uint64())
				case 4:
					data = fmt.Sprintf("-%d", rand.Int63n(65536))
					if data == "-0" {
						data = "0"
					}
				case 5:
					data = fmt.Sprintf("-%d", rand.Int63n(4294967296))
					if data == "-0" {
						data = "0"
					}
				case 6:
					data = fmt.Sprintf("-%d", rand.Uint64())
					if data == "-0" {
						data = "0"
					}
				}
				lis = append(lis, data)
				require.NoError(t, rdb.RPush(ctx, key, data).Err())
			}
			require.Equal(t, int64(len(lis)), rdb.LLen(ctx, key).Val())

			for i := 199; i >= 0; i-- {
				require.Equal(t, lis[i], rdb.LIndex(ctx, key, int64(i)).Val())
			}
		}
	})

	t.Run("ziplist implementation: encoding stress testing", func(t *testing.T) {
		for j := 0; j < 200; j++ {
			key := fmt.Sprintf("l2-%d", j)
			require.NoError(t, rdb.Del(ctx, key).Err())
			var lis []string
			l := int(rand.Int63n(400))
			for i := 0; i < l; i++ {
				rv := util.RandomValue()
				util.RandPathNoResult(
					func() {
						lis = append(lis, rv)
						require.NoError(t, rdb.RPush(ctx, key, rv).Err())
					},
					func() {
						lis = append([]string{rv}, lis...)
						require.NoError(t, rdb.LPush(ctx, key, rv).Err())
					},
				)
			}
			require.Equal(t, int64(len(lis)), rdb.LLen(ctx, key).Val())
			for i := 0; i < l; i++ {
				require.Equal(t, lis[i], rdb.LIndex(ctx, key, int64(i)).Val())
			}
		}
	})
}

func TestList(t *testing.T) {
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

	for _, configs := range configsMatrix {
		testList(t, configs)
	}
}

func testList(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("LPUSH, RPUSH, LLENGTH, LINDEX, LPOP - ziplist", func(t *testing.T) {
		// first lpush then rpush
		require.EqualValues(t, 1, rdb.LPush(ctx, "myziplist1", "aa").Val())
		require.EqualValues(t, 2, rdb.RPush(ctx, "myziplist1", "bb").Val())
		require.EqualValues(t, 3, rdb.RPush(ctx, "myziplist1", "cc").Val())
		require.EqualValues(t, 3, rdb.LLen(ctx, "myziplist1").Val())
		require.Equal(t, "aa", rdb.LIndex(ctx, "myziplist1", 0).Val())
		require.Equal(t, "bb", rdb.LIndex(ctx, "myziplist1", 1).Val())
		require.Equal(t, "cc", rdb.LIndex(ctx, "myziplist1", 2).Val())
		require.Equal(t, "", rdb.LIndex(ctx, "myziplist1", 3).Val())
		require.Equal(t, "cc", rdb.RPop(ctx, "myziplist1").Val())
		require.Equal(t, "aa", rdb.LPop(ctx, "myziplist1").Val())

		// first rpush then lpush
		require.EqualValues(t, 1, rdb.RPush(ctx, "myziplist2", "a").Val())
		require.EqualValues(t, 2, rdb.LPush(ctx, "myziplist2", "b").Val())
		require.EqualValues(t, 3, rdb.LPush(ctx, "myziplist2", "c").Val())
		require.EqualValues(t, 3, rdb.LLen(ctx, "myziplist2").Val())
		require.Equal(t, "c", rdb.LIndex(ctx, "myziplist2", 0).Val())
		require.Equal(t, "b", rdb.LIndex(ctx, "myziplist2", 1).Val())
		require.Equal(t, "a", rdb.LIndex(ctx, "myziplist2", 2).Val())
		require.Equal(t, "", rdb.LIndex(ctx, "myziplist2", 3).Val())
		require.Equal(t, "a", rdb.RPop(ctx, "myziplist2").Val())
		require.Equal(t, "c", rdb.LPop(ctx, "myziplist2").Val())
	})

	t.Run("LPUSH, RPUSH, LLENGTH, LINDEX, LPOP - regular list", func(t *testing.T) {
		// first lpush then rpush
		require.EqualValues(t, 1, rdb.LPush(ctx, "mylist1", largeValue["linkedList"]).Val())
		require.EqualValues(t, 2, rdb.RPush(ctx, "mylist1", "b").Val())
		require.EqualValues(t, 3, rdb.RPush(ctx, "mylist1", "c").Val())
		require.EqualValues(t, 3, rdb.LLen(ctx, "mylist1").Val())
		require.Equal(t, largeValue["linkedList"], rdb.LIndex(ctx, "mylist1", 0).Val())
		require.Equal(t, "b", rdb.LIndex(ctx, "mylist1", 1).Val())
		require.Equal(t, "c", rdb.LIndex(ctx, "mylist1", 2).Val())
		require.Equal(t, "", rdb.LIndex(ctx, "mylist1", 3).Val())
		require.Equal(t, "c", rdb.RPop(ctx, "mylist1").Val())
		require.Equal(t, largeValue["linkedList"], rdb.LPop(ctx, "mylist1").Val())

		// first rpush then lpush
		require.EqualValues(t, 1, rdb.RPush(ctx, "mylist2", largeValue["linkedList"]).Val())
		require.EqualValues(t, 2, rdb.LPush(ctx, "mylist2", "b").Val())
		require.EqualValues(t, 3, rdb.LPush(ctx, "mylist2", "c").Val())
		require.EqualValues(t, 3, rdb.LLen(ctx, "mylist2").Val())
		require.Equal(t, "c", rdb.LIndex(ctx, "mylist2", 0).Val())
		require.Equal(t, "b", rdb.LIndex(ctx, "mylist2", 1).Val())
		require.Equal(t, largeValue["linkedList"], rdb.LIndex(ctx, "mylist2", 2).Val())
		require.Equal(t, "", rdb.LIndex(ctx, "mylist2", 3).Val())
		require.Equal(t, largeValue["linkedList"], rdb.RPop(ctx, "mylist2").Val())
		require.Equal(t, "c", rdb.LPop(ctx, "mylist2").Val())
	})

	t.Run("R/LPOP against empty list", func(t *testing.T) {
		require.Equal(t, "", rdb.LPop(ctx, "non-existing-list").Val())
	})

	t.Run("Variadic RPUSH/LPUSH", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mylist").Err())
		require.EqualValues(t, 4, rdb.LPush(ctx, "mylist", "a", "b", "c", "d").Val())
		require.EqualValues(t, 8, rdb.RPush(ctx, "mylist", "1", "2", "3", "4").Val())
		require.Equal(t, []string{"d", "c", "b", "a", "1", "2", "3", "4"}, rdb.LRange(ctx, "mylist", 0, -1).Val())
	})

	t.Run("DEL a list", func(t *testing.T) {
		require.EqualValues(t, 1, rdb.Del(ctx, "mylist2").Val())
		require.EqualValues(t, 0, rdb.Exists(ctx, "mylist2").Val())
		require.EqualValues(t, 0, rdb.LLen(ctx, "mylist2").Val())
	})

	createList := func(key string, entries ...interface{}) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		for _, entry := range entries {
			require.NoError(t, rdb.RPush(ctx, key, entry).Err())
		}
	}

	for listType, large := range largeValue {
		t.Run(fmt.Sprintf("BLPOP, BRPOP: single existing list - %s", listType), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			createList("blist", []string{"a", "b", large, "c", "d"})
			require.NoError(t, rd.WriteArgs("blpop", "blist", "1"))
			rd.MustReadStrings(t, []string{"blist", "a"})
			require.NoError(t, rd.WriteArgs("brpop", "blist", "1"))
			rd.MustReadStrings(t, []string{"blist", "d"})
			require.NoError(t, rd.WriteArgs("blpop", "blist", "1"))
			rd.MustReadStrings(t, []string{"blist", "b"})
			require.NoError(t, rd.WriteArgs("brpop", "blist", "1"))
			rd.MustReadStrings(t, []string{"blist", "c"})
		})

		t.Run(fmt.Sprintf("BLPOP, BRPOP: multiple existing lists - %s", listType), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			createList("blist1", []string{"a", large, "c"})
			createList("blist2", []string{"d", large, "f"})
			require.NoError(t, rd.WriteArgs("blpop", "blist1", "blist2", "1"))
			rd.MustReadStrings(t, []string{"blist1", "a"})
			require.NoError(t, rd.WriteArgs("brpop", "blist1", "blist2", "1"))
			rd.MustReadStrings(t, []string{"blist1", "c"})
			require.EqualValues(t, 1, rdb.LLen(ctx, "blist1").Val())
			require.EqualValues(t, 3, rdb.LLen(ctx, "blist2").Val())
			require.NoError(t, rd.WriteArgs("blpop", "blist2", "blist2", "1"))
			rd.MustReadStrings(t, []string{"blist2", "d"})
			require.NoError(t, rd.WriteArgs("brpop", "blist2", "blist2", "1"))
			rd.MustReadStrings(t, []string{"blist2", "f"})
			require.EqualValues(t, 1, rdb.LLen(ctx, "blist1").Val())
			require.EqualValues(t, 1, rdb.LLen(ctx, "blist2").Val())
		})

		t.Run(fmt.Sprintf("BLPOP, BRPOP: second list has an entry - %s", listType), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, "blist1").Err())
			createList("blist2", []string{"d", large, "f"})
			require.NoError(t, rd.WriteArgs("blpop", "blist1", "blist2", "1"))
			rd.MustReadStrings(t, []string{"blist2", "d"})
			require.NoError(t, rd.WriteArgs("brpop", "blist1", "blist2", "1"))
			rd.MustReadStrings(t, []string{"blist2", "f"})
			require.EqualValues(t, 0, rdb.LLen(ctx, "blist1").Val())
			require.EqualValues(t, 1, rdb.LLen(ctx, "blist2").Val())
		})
	}

	t.Run("BLPOP with same key multiple times should work (redis issue #801)", func(t *testing.T) {
		rd := srv.NewTCPClient()
		defer func() { require.NoError(t, rd.Close()) }()
		require.NoError(t, rdb.Del(ctx, "list1", "list2").Err())
		require.NoError(t, rd.WriteArgs("blpop", "list1", "list2", "list2", "list1", "0"))
		require.NoError(t, rdb.LPush(ctx, "list1", "a").Err())
		rd.MustReadStrings(t, []string{"list1", "a"})
		require.NoError(t, rd.WriteArgs("blpop", "list1", "list2", "list2", "list1", "0"))
		require.NoError(t, rdb.LPush(ctx, "list2", "b").Err())
		rd.MustReadStrings(t, []string{"list2", "b"})
		require.NoError(t, rdb.LPush(ctx, "list1", "a").Err())
		require.NoError(t, rdb.LPush(ctx, "list2", "b").Err())
		require.NoError(t, rd.WriteArgs("blpop", "list1", "list2", "list2", "list1", "0"))
		rd.MustReadStrings(t, []string{"list1", "a"})
		require.NoError(t, rd.WriteArgs("blpop", "list1", "list2", "list2", "list1", "0"))
		rd.MustReadStrings(t, []string{"list2", "b"})
	})

	t.Run("BLPOP with variadic LPUSH", func(t *testing.T) {
		rd := srv.NewTCPClient()
		defer func() { require.NoError(t, rd.Close()) }()
		require.NoError(t, rdb.Del(ctx, "blist", "target").Err())
		require.NoError(t, rd.WriteArgs("blpop", "blist", "0"))
		require.EqualValues(t, 2, rdb.LPush(ctx, "blist", "foo", "bar").Val())
		rd.MustReadStrings(t, []string{"blist", "bar"})
		require.Equal(t, "foo", rdb.LRange(ctx, "blist", 0, -1).Val()[0])
	})

	for _, popType := range []string{"blpop", "brpop"} {
		t.Run(fmt.Sprintf("%s: with single empty list argument", popType), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, "blist1").Err())
			require.NoError(t, rd.WriteArgs(popType, "blist1", "0"))
			require.NoError(t, rdb.RPush(ctx, "blist1", "foo").Err())
			rd.MustReadStrings(t, []string{"blist1", "foo"})
			require.EqualValues(t, 0, rdb.Exists(ctx, "blist1").Val())
		})

		t.Run(fmt.Sprintf("%s: with negative timeout", popType), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rd.WriteArgs(popType, "blist1", "-1"))
			rd.MustMatch(t, ".*negative.*")
		})

		t.Run(fmt.Sprintf("%s: with zero timeout should block indefinitely", popType), func(t *testing.T) {
			// To test this, use a timeout of 0 and wait a second.
			// The blocking pop should still be waiting for a push.
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rd.WriteArgs(popType, "blist1", "0"))
			require.NoError(t, rdb.RPush(ctx, "blist1", "foo").Err())
			rd.MustReadStrings(t, []string{"blist1", "foo"})
		})

		t.Run(fmt.Sprintf("%s: second argument is not a list", popType), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, "blist1", "blist2").Err())
			require.NoError(t, rdb.Set(ctx, "blist2", "nolist", 0).Err())
			require.NoError(t, rd.WriteArgs(popType, "blist1", "blist2", "1"))
			rd.MustMatch(t, ".*WRONGTYPE.*")
		})

		t.Run(fmt.Sprintf("%s: timeout", popType), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, "blist1", "blist2").Err())
			require.NoError(t, rd.WriteArgs(popType, "blist1", "blist2", "1"))
			rd.MustMatch(t, "")
		})

		t.Run(fmt.Sprintf("%s: arguments are empty", popType), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, "blist1", "blist2").Err())
			require.NoError(t, rd.WriteArgs(popType, "blist1", "blist2", "4"))
			require.NoError(t, rdb.RPush(ctx, "blist1", "foo").Err())
			rd.MustReadStrings(t, []string{"blist1", "foo"})
			require.EqualValues(t, 0, rdb.Exists(ctx, "blist1").Val())
			require.EqualValues(t, 0, rdb.Exists(ctx, "blist2").Val())
			require.NoError(t, rd.WriteArgs(popType, "blist1", "blist2", "1"))
			require.NoError(t, rdb.RPush(ctx, "blist2", "foo").Err())
			rd.MustReadStrings(t, []string{"blist2", "foo"})
			require.EqualValues(t, 0, rdb.Exists(ctx, "blist1").Val())
			require.EqualValues(t, 0, rdb.Exists(ctx, "blist2").Val())
		})
	}

	t.Run("LPUSHX, RPUSHX - generic", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "xlist").Err())
		require.EqualValues(t, 0, rdb.LPushX(ctx, "xlist", "a").Val())
		require.EqualValues(t, 0, rdb.LLen(ctx, "xlist").Val())
		require.EqualValues(t, 0, rdb.RPushX(ctx, "xlist", "a").Val())
		require.EqualValues(t, 0, rdb.LLen(ctx, "xlist").Val())
	})

	for listType, large := range largeValue {
		t.Run(fmt.Sprintf("LPUSHX, RPUSHX - %s", listType), func(t *testing.T) {
			createList("xlist", []string{large, "c"})
			require.EqualValues(t, 3, rdb.RPushX(ctx, "xlist", "d").Val())
			require.EqualValues(t, 4, rdb.LPushX(ctx, "xlist", "a").Val())
			require.EqualValues(t, 6, rdb.RPushX(ctx, "xlist", "42", "x").Val())
			require.EqualValues(t, 9, rdb.LPushX(ctx, "xlist", "y3", "y2", "y1").Val())
			require.Equal(t, []string{"y1", "y2", "y3", "a", large, "c", "d", "42", "x"}, rdb.LRange(ctx, "xlist", 0, -1).Val())
		})

		t.Run(fmt.Sprintf("LINSERT - %s", listType), func(t *testing.T) {
			createList("xlist", []string{"a", large, "c", "d"})
			require.EqualValues(t, 5, rdb.LInsert(ctx, "xlist", "before", "c", "zz").Val())
			require.Equal(t, []string{"a", large, "zz", "c", "d"}, rdb.LRange(ctx, "xlist", 0, 10).Val())
			require.EqualValues(t, 6, rdb.LInsert(ctx, "xlist", "after", "c", "yy").Val())
			require.Equal(t, []string{"a", large, "zz", "c", "yy", "d"}, rdb.LRange(ctx, "xlist", 0, 10).Val())
			require.EqualValues(t, 7, rdb.LInsert(ctx, "xlist", "after", "d", "dd").Val())
			require.EqualValues(t, -1, rdb.LInsert(ctx, "xlist", "after", "bad", "ddd").Val())
			require.Equal(t, []string{"a", large, "zz", "c", "yy", "d", "dd"}, rdb.LRange(ctx, "xlist", 0, 10).Val())
			require.EqualValues(t, 8, rdb.LInsert(ctx, "xlist", "before", "a", "aa").Val())
			require.EqualValues(t, -1, rdb.LInsert(ctx, "xlist", "before", "bad", "aaa").Val())
			require.Equal(t, []string{"aa", "a", large, "zz", "c", "yy", "d", "dd"}, rdb.LRange(ctx, "xlist", 0, 10).Val())

			// check inserting integer encoded value
			require.EqualValues(t, 9, rdb.LInsert(ctx, "xlist", "before", "aa", "42").Val())
			require.Equal(t, "42", rdb.LRange(ctx, "xlist", 0, 0).Val()[0])
		})
	}

	t.Run("LINSERT raise error on bad syntax", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.LInsert(ctx, "xlist", "aft3r", "aa", "42").Err(), ".*syntax.*error.*")
	})

	listType := "quicklist"
	for _, num := range []int{250, 500} {
		checkNumberedListConsistency := func(key string) {
			l := rdb.LLen(ctx, key).Val()
			for i := 0; i < int(l); i++ {
				require.Equal(t, strconv.Itoa(i), rdb.LIndex(ctx, key, int64(i)).Val())
				require.Equal(t, strconv.Itoa(int(l)-i-1), rdb.LIndex(ctx, key, int64(int(l)-i-1)).Val())
			}
		}

		checkRandomAccessConsistency := func(key string) {
			l := rdb.LLen(ctx, key).Val()
			for i := 0; i < int(l); i++ {
				k := rand.Intn(int(l))
				require.Equal(t, strconv.Itoa(k), rdb.LIndex(ctx, key, int64(k)).Val())
				require.Equal(t, strconv.Itoa(int(l)-k-1), rdb.LIndex(ctx, key, int64(int(l)-k-1)).Val())
			}
		}

		t.Run(fmt.Sprintf("LINDEX consistency test - %s", listType), func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "mylist").Err())
			for i := 0; i < num; i++ {
				require.NoError(t, rdb.RPush(ctx, "mylist", strconv.Itoa(i)).Err())
			}
			checkNumberedListConsistency("mylist")
		})

		t.Run(fmt.Sprintf("LINDEX random access - %s", listType), func(t *testing.T) {
			checkRandomAccessConsistency("mylist")
		})

		t.Run(fmt.Sprintf("Check if list is still ok after a DEBUG RELOAD - %s", listType), func(t *testing.T) {
			checkNumberedListConsistency("mylist")
			checkRandomAccessConsistency("mylist")
		})
	}
	t.Run("LLEN against non-list value error", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mylist").Err())
		require.NoError(t, rdb.Set(ctx, "mylist", "foobar", 0).Err())
		require.ErrorContains(t, rdb.LLen(ctx, "mylist").Err(), "WRONGTYPE")
	})

	t.Run("LLEN against non existing key", func(t *testing.T) {
		require.EqualValues(t, 0, rdb.LLen(ctx, "not-a-key").Val())
	})

	t.Run("LINDEX against non-list value error", func(t *testing.T) {
		require.ErrorContains(t, rdb.LIndex(ctx, "mylist", 0).Err(), "WRONGTYPE")
	})

	t.Run("LINDEX against non existing key", func(t *testing.T) {
		require.Equal(t, "", rdb.LIndex(ctx, "not-a-key", 10).Val())
	})

	t.Run("LPUSH against non-list value error", func(t *testing.T) {
		require.ErrorContains(t, rdb.LPush(ctx, "mylist", 0).Err(), "WRONGTYPE")
	})

	t.Run("RPUSH against non-list value error", func(t *testing.T) {
		require.ErrorContains(t, rdb.RPush(ctx, "mylist", 0).Err(), "WRONGTYPE")
	})

	for listType, large := range largeValue {
		t.Run(fmt.Sprintf("RPOPLPUSH base case - %s", listType), func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "mylist1", "mylist2").Err())
			createList("mylist1", []string{"a", large, "c", "d"})
			require.Equal(t, "d", rdb.RPopLPush(ctx, "mylist1", "mylist2").Val())
			require.Equal(t, "c", rdb.RPopLPush(ctx, "mylist1", "mylist2").Val())
			require.Equal(t, []string{"a", large}, rdb.LRange(ctx, "mylist1", 0, -1).Val())
			require.Equal(t, []string{"c", "d"}, rdb.LRange(ctx, "mylist2", 0, -1).Val())
		})

		t.Run(fmt.Sprintf("RPOPLPUSH with the same list as src and dst - %s", listType), func(t *testing.T) {
			createList("mylist", []string{"a", large, "c"})
			require.Equal(t, []string{"a", large, "c"}, rdb.LRange(ctx, "mylist", 0, -1).Val())
			require.Equal(t, "c", rdb.RPopLPush(ctx, "mylist", "mylist").Val())
			require.Equal(t, []string{"c", "a", large}, rdb.LRange(ctx, "mylist", 0, -1).Val())
		})

		for otherListType, otherLarge := range largeValue {
			t.Run(fmt.Sprintf("RPOPLPUSH with %s source and existing target %s", listType, otherListType), func(t *testing.T) {
				createList("srclist", []string{"a", "b", "c", large})
				createList("dstlist", []string{otherLarge})
				require.Equal(t, large, rdb.RPopLPush(ctx, "srclist", "dstlist").Val())
				require.Equal(t, "c", rdb.RPopLPush(ctx, "srclist", "dstlist").Val())
				require.Equal(t, []string{"a", "b"}, rdb.LRange(ctx, "srclist", 0, -1).Val())
				require.Equal(t, []string{"c", large, otherLarge}, rdb.LRange(ctx, "dstlist", 0, -1).Val())
			})
		}
	}

	t.Run("RPOPLPUSH against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "srclist", "dstlist").Err())
		require.Equal(t, "", rdb.RPopLPush(ctx, "srclist", "dstlist").Val())
		require.EqualValues(t, 0, rdb.Exists(ctx, "srclist").Val())
		require.EqualValues(t, 0, rdb.Exists(ctx, "dstlist").Val())
	})

	t.Run("RPOPLPUSH against non list src key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "srclist", "dstlist").Err())
		require.NoError(t, rdb.Set(ctx, "srclist", "x", 0).Err())
		require.ErrorContains(t, rdb.RPopLPush(ctx, "srclist", "dstlist").Err(), "WRONGTYPE")
		require.Equal(t, "string", rdb.Type(ctx, "srclist").Val())
		require.EqualValues(t, 0, rdb.Exists(ctx, "newlist").Val())
	})

	t.Run("RPOPLPUSH against non list dst key", func(t *testing.T) {
		createList("srclist", []string{"a", "b", "c", "d"})
		require.NoError(t, rdb.Set(ctx, "dstlist", "x", 0).Err())
		require.ErrorContains(t, rdb.RPopLPush(ctx, "srclist", "dstlist").Err(), "WRONGTYPE")
		require.Equal(t, "string", rdb.Type(ctx, "dstlist").Val())
		require.Equal(t, []string{"a", "b", "c", "d"}, rdb.LRange(ctx, "srclist", 0, -1).Val())
	})

	t.Run("RPOPLPUSH against non existing src key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "srclist", "dstlist").Err())
		require.Equal(t, "", rdb.RPopLPush(ctx, "srclist", "dstlist").Val())
	})

	for listType, large := range largeValue {
		t.Run(fmt.Sprintf("Basic LPOP/RPOP - %s", listType), func(t *testing.T) {
			createList("mylist", []string{large, "1", "2"})

			require.Equal(t, large, rdb.LPop(ctx, "mylist").Val())
			require.Equal(t, "2", rdb.RPop(ctx, "mylist").Val())
			require.Equal(t, "1", rdb.LPop(ctx, "mylist").Val())
			require.EqualValues(t, 0, rdb.LLen(ctx, "mylist").Val())

			// pop on empty list
			require.Equal(t, "", rdb.LPop(ctx, "mylist").Val())
			require.Equal(t, "", rdb.LPop(ctx, "mylist").Val())
		})
	}

	t.Run("LPOP/RPOP against non list value", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "notalist", "foo", 0).Err())
		require.ErrorContains(t, rdb.LPop(ctx, "notalist").Err(), "WRONGTYPE")
		require.ErrorContains(t, rdb.RPop(ctx, "notalist").Err(), "WRONGTYPE")
	})

	t.Run("LPOP/RPOP with wrong number of arguments", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "lpop", "key", "1", "1").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "lpop", "key", "2", "2").Err(), "wrong number of arguments")
	})

	t.Run("RPOP/LPOP with the optional count argument", func(t *testing.T) {
		require.EqualValues(t, 7, rdb.LPush(ctx, "listcount", "aa", "bb", "cc", "dd", "ee", "ff", "gg").Val())
		require.Equal(t, []string{"gg"}, rdb.LPopCount(ctx, "listcount", 1).Val())
		require.Equal(t, []string{"ff", "ee"}, rdb.LPopCount(ctx, "listcount", 2).Val())
		require.Equal(t, []string{"aa", "bb"}, rdb.RPopCount(ctx, "listcount", 2).Val())
		require.Equal(t, []string{"cc"}, rdb.RPopCount(ctx, "listcount", 1).Val())
		require.Equal(t, []string{"dd"}, rdb.LPopCount(ctx, "listcount", 123).Val())
		util.ErrorRegexp(t, rdb.LPopCount(ctx, "forbatqaz", -123).Err(), ".*ERR.*range.*")
	})

	t.Run("LPOP/RPOP with the count 0 returns an empty array", func(t *testing.T) {
		require.NoError(t, rdb.LPush(ctx, "listcount", "zero").Err())
		require.Equal(t, []string{}, rdb.LPopCount(ctx, "listcount", 0).Val())
		require.Equal(t, []string{}, rdb.RPopCount(ctx, "listcount", 0).Val())
	})

	t.Run("LPOP/RPOP against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "non_existing_key").Err())
		require.EqualError(t, rdb.LPop(ctx, "non_existing_key").Err(), redis.Nil.Error())
		require.EqualError(t, rdb.RPop(ctx, "non_existing_key").Err(), redis.Nil.Error())
	})

	t.Run("LPOP/RPOP with <count> against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "non_existing_key").Err())
		require.EqualError(t, rdb.LPopCount(ctx, "non_existing_key", 0).Err(), redis.Nil.Error())
		require.EqualError(t, rdb.LPopCount(ctx, "non_existing_key", 1).Err(), redis.Nil.Error())
		require.EqualError(t, rdb.RPopCount(ctx, "non_existing_key", 0).Err(), redis.Nil.Error())
		require.EqualError(t, rdb.RPopCount(ctx, "non_existing_key", 1).Err(), redis.Nil.Error())
	})

	listType = "quicklist"
	for _, num := range []int{250, 500} {
		t.Run(fmt.Sprintf("Mass RPOP/LPOP - %s", listType), func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "mylist").Err())
			sum1 := 0
			for i := 0; i < num; i++ {
				require.NoError(t, rdb.LPush(ctx, "mylist", strconv.Itoa(i)).Err())
				sum1 += i
			}
			sum2 := 0
			for i := 0; i < num/2; i++ {
				if v1, err := strconv.Atoi(rdb.LPop(ctx, "mylist").Val()); err == nil {
					sum2 += v1
				}
				if v2, err := strconv.Atoi(rdb.RPop(ctx, "mylist").Val()); err == nil {
					sum2 += v2
				}
			}
			require.Equal(t, sum1, sum2)
		})
	}

	for listType, large := range largeValue {
		t.Run(fmt.Sprintf("LRANGE basics - %s", listType), func(t *testing.T) {
			createList("mylist", []string{large, "1", "2", "3", "4", "5", "6", "7", "8", "9"})
			require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8"}, rdb.LRange(ctx, "mylist", 1, -2).Val())
			require.Equal(t, []string{"7", "8", "9"}, rdb.LRange(ctx, "mylist", -3, -1).Val())
			require.Equal(t, []string{"4"}, rdb.LRange(ctx, "mylist", 4, 4).Val())
		})

		t.Run(fmt.Sprintf("LRANGE inverted indexes - %s", listType), func(t *testing.T) {
			createList("mylist", []string{large, "1", "2", "3", "4", "5", "6", "7", "8", "9"})
			require.Equal(t, []string{}, rdb.LRange(ctx, "mylist", 6, 2).Val())
		})

		t.Run(fmt.Sprintf("LRANGE out of range indexes including the full list - $type - %s", listType), func(t *testing.T) {
			createList("mylist", []string{large, "1", "2", "3"})
			require.Equal(t, []string{large, "1", "2", "3"}, rdb.LRange(ctx, "mylist", -1000, 1000).Val())
		})

		t.Run(fmt.Sprintf("LRANGE out of range negative end index - %s", listType), func(t *testing.T) {
			createList("mylist", []string{large, "1", "2", "3"})
			require.Equal(t, []string{large}, rdb.LRange(ctx, "mylist", 0, -4).Val())
			require.Equal(t, []string{}, rdb.LRange(ctx, "mylist", 0, -5).Val())
		})
	}

	t.Run("LRANGE against non existing key", func(t *testing.T) {
		require.Equal(t, []string{}, rdb.LRange(ctx, "nosuchkey", 0, 1).Val())
	})

	for listType, large := range largeValue {
		trimList := func(listType string, minInt, maxInt int64) []string {
			require.NoError(t, rdb.Del(ctx, "mylist").Err())
			createList("mylist", []string{"1", "2", "3", "4", large})
			require.NoError(t, rdb.LTrim(ctx, "mylist", minInt, maxInt).Err())
			return rdb.LRange(ctx, "mylist", 0, -1).Val()
		}

		t.Run(fmt.Sprintf("LTRIM basics - %s", listType), func(t *testing.T) {
			require.Equal(t, []string{"1"}, trimList(listType, 0, 0))
			require.Equal(t, []string{"1", "2"}, trimList(listType, 0, 1))
			require.Equal(t, []string{"1", "2", "3"}, trimList(listType, 0, 2))
			require.Equal(t, []string{"2", "3"}, trimList(listType, 1, 2))
			require.Equal(t, []string{"2", "3", "4", large}, trimList(listType, 1, -1))
			require.Equal(t, []string{"2", "3", "4"}, trimList(listType, 1, -2))
			require.Equal(t, []string{"4", large}, trimList(listType, -2, -1))
			require.Equal(t, []string{large}, trimList(listType, -1, -1))
			require.Equal(t, []string{"1", "2", "3", "4", large}, trimList(listType, -5, -1))
			require.Equal(t, []string{"1", "2", "3", "4", large}, trimList(listType, -10, 10))
			require.Equal(t, []string{"1", "2", "3", "4", large}, trimList(listType, 0, 5))
			require.Equal(t, []string{"1", "2", "3", "4", large}, trimList(listType, 0, 10))
		})

		t.Run(fmt.Sprintf("LTRIM out of range negative end index - %s", listType), func(t *testing.T) {
			require.Equal(t, []string{"1"}, trimList(listType, 0, -5))
			require.Equal(t, []string{}, trimList(listType, 0, -6))
		})

		t.Run(fmt.Sprintf("LTRIM lrem elements after ltrim list - %s", listType), func(t *testing.T) {
			createList("myotherlist", []string{"0", "1", "2", "3", "4", "3", "6", "7", "3", "9"})
			require.Equal(t, "OK", rdb.LTrim(ctx, "myotherlist", 2, -3).Val())
			require.Equal(t, []string{"2", "3", "4", "3", "6", "7"}, rdb.LRange(ctx, "myotherlist", 0, -1).Val())
			require.EqualValues(t, 2, rdb.LRem(ctx, "myotherlist", 4, "3").Val())
			require.Equal(t, []string{"2", "4", "6", "7"}, rdb.LRange(ctx, "myotherlist", 0, -1).Val())
		})

		t.Run(fmt.Sprintf("LTRIM linsert elements after ltrim list - %s", listType), func(t *testing.T) {
			createList("myotherlist1", []string{"0", "1", "2", "3", "4", "3", "6", "7", "3", "9"})
			require.Equal(t, "OK", rdb.LTrim(ctx, "myotherlist1", 2, -3).Val())
			require.Equal(t, []string{"2", "3", "4", "3", "6", "7"}, rdb.LRange(ctx, "myotherlist1", 0, -1).Val())
			require.EqualValues(t, -1, rdb.LInsert(ctx, "myotherlist1", "before", "9", "0").Val())
			require.EqualValues(t, 7, rdb.LInsert(ctx, "myotherlist1", "before", "4", "0").Val())
			require.Equal(t, []string{"2", "3", "0", "4", "3", "6", "7"}, rdb.LRange(ctx, "myotherlist1", 0, -1).Val())
		})
	}

	for listType, large := range largeValue {
		t.Run(fmt.Sprintf("LSET - %s", listType), func(t *testing.T) {
			createList("mylist", []string{"99", "98", large, "96", "95"})
			require.NoError(t, rdb.LSet(ctx, "mylist", 1, "foo").Err())
			require.NoError(t, rdb.LSet(ctx, "mylist", -1, "bar").Err())
			require.Equal(t, []string{"99", "foo", large, "96", "bar"}, rdb.LRange(ctx, "mylist", 0, -1).Val())
		})

		t.Run(fmt.Sprintf("LSET out of range index - %s", listType), func(t *testing.T) {
			util.ErrorRegexp(t, rdb.LSet(ctx, "mylist", 10, "foo").Err(), "ERR.*range.*")
		})
	}

	t.Run("LSET against non existing key", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.LSet(ctx, "nosuchkey", 10, "foo").Err(), ".*no such key.*")
	})

	t.Run("LSET against non list value", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "nolist", "foobar", 0).Err())
		require.ErrorContains(t, rdb.LSet(ctx, "nolist", 0, "foo").Err(), "WRONGTYPE")
	})

	for listType, e := range largeValue {
		t.Run(fmt.Sprintf("LREM remove all the occurrences - %s", listType), func(t *testing.T) {
			createList("mylist", []string{e, "foo", "bar", "foobar", "foobared", "zap", "bar", "test", "foo"})
			require.EqualValues(t, 2, rdb.LRem(ctx, "mylist", 0, "bar").Val())
			require.Equal(t, []string{e, "foo", "foobar", "foobared", "zap", "test", "foo"}, rdb.LRange(ctx, "mylist", 0, -1).Val())
		})

		t.Run(fmt.Sprintf("LREM remove the first occurrence - %s", listType), func(t *testing.T) {
			require.EqualValues(t, 1, rdb.LRem(ctx, "mylist", 1, "foo").Val())
			require.Equal(t, []string{e, "foobar", "foobared", "zap", "test", "foo"}, rdb.LRange(ctx, "mylist", 0, -1).Val())
		})

		t.Run(fmt.Sprintf("LREM remove non existing element - %s", listType), func(t *testing.T) {
			require.EqualValues(t, 0, rdb.LRem(ctx, "mylist", 1, "nosuchelement").Val())
			require.Equal(t, []string{e, "foobar", "foobared", "zap", "test", "foo"}, rdb.LRange(ctx, "mylist", 0, -1).Val())
		})

		t.Run(fmt.Sprintf("LREM starting from tail with negative count - %s", listType), func(t *testing.T) {
			createList("mylist", []string{e, "foo", "bar", "foobar", "foobared", "zap", "bar", "test", "foo", "foo"})
			require.EqualValues(t, 1, rdb.LRem(ctx, "mylist", -1, "bar").Val())
			require.Equal(t, []string{e, "foo", "bar", "foobar", "foobared", "zap", "test", "foo", "foo"}, rdb.LRange(ctx, "mylist", 0, -1).Val())
		})

		t.Run(fmt.Sprintf("LREM starting from tail with negative count (2) - %s", listType), func(t *testing.T) {
			require.EqualValues(t, 2, rdb.LRem(ctx, "mylist", -2, "foo").Val())
			require.Equal(t, []string{e, "foo", "bar", "foobar", "foobared", "zap", "test"}, rdb.LRange(ctx, "mylist", 0, -1).Val())
		})

		t.Run(fmt.Sprintf("LREM deleting objects that may be int encoded - %s", listType), func(t *testing.T) {
			createList("myotherlist", e, 1, 2, 3)
			require.EqualValues(t, 1, rdb.LRem(ctx, "myotherlist", 1, 2).Val())
			require.EqualValues(t, 3, rdb.LLen(ctx, "myotherlist").Val())
		})

		t.Run(fmt.Sprintf("LREM remove elements in repeating list - %s", listType), func(t *testing.T) {
			createList("myotherlist", e, "a", "b", "c", "d", "e", "f", "a", "f", "a", "f")
			require.EqualValues(t, 1, rdb.LRem(ctx, "myotherlist", 1, "f").Val())
			require.Equal(t, []string{e, "a", "b", "c", "d", "e", "a", "f", "a", "f"}, rdb.LRange(ctx, "myotherlist", 0, -1).Val())
			require.EqualValues(t, 2, rdb.LRem(ctx, "myotherlist", 0, "f").Val())
			require.Equal(t, []string{e, "a", "b", "c", "d", "e", "a", "a"}, rdb.LRange(ctx, "myotherlist", 0, -1).Val())
		})
	}

	t.Run("Test LMOVE on different keys", func(t *testing.T) {
		require.NoError(t, rdb.RPush(ctx, "list1{t}", "1").Err())
		require.NoError(t, rdb.RPush(ctx, "list1{t}", "2").Err())
		require.NoError(t, rdb.RPush(ctx, "list1{t}", "3").Err())
		require.NoError(t, rdb.RPush(ctx, "list1{t}", "4").Err())
		require.NoError(t, rdb.RPush(ctx, "list1{t}", "5").Err())
		require.NoError(t, rdb.LMove(ctx, "list1{t}", "list2{t}", "RIGHT", "LEFT").Err())
		require.NoError(t, rdb.LMove(ctx, "list1{t}", "list2{t}", "LEFT", "RIGHT").Err())
		require.EqualValues(t, 3, rdb.LLen(ctx, "list1{t}").Val())
		require.EqualValues(t, 2, rdb.LLen(ctx, "list2{t}").Val())
		require.Equal(t, []string{"2", "3", "4"}, rdb.LRange(ctx, "list1{t}", 0, -1).Val())
		require.Equal(t, []string{"5", "1"}, rdb.LRange(ctx, "list2{t}", 0, -1).Val())
	})

	for _, from := range []string{"LEFT", "RIGHT"} {
		for _, to := range []string{"LEFT", "RIGHT"} {
			t.Run(fmt.Sprintf("LMOVE %s %s on the list node", from, to), func(t *testing.T) {
				rd := srv.NewTCPClient()
				defer func() { require.NoError(t, rd.Close()) }()
				require.NoError(t, rdb.Del(ctx, "target_key{t}").Err())
				require.NoError(t, rdb.RPush(ctx, "target_key{t}", 1).Err())
				createList("list{t}", []string{"a", "b", "c", "d"})
				require.NoError(t, rd.WriteArgs("lmove", "list{t}", "target_key{t}", from, to))
				r, err1 := rd.ReadLine()
				require.Equal(t, "$1", r)
				require.NoError(t, err1)
				elem, err2 := rd.ReadLine()
				require.NoError(t, err2)
				if from == "RIGHT" {
					require.Equal(t, elem, "d")
					require.Equal(t, []string{"a", "b", "c"}, rdb.LRange(ctx, "list{t}", 0, -1).Val())
				} else {
					require.Equal(t, elem, "a")
					require.Equal(t, []string{"b", "c", "d"}, rdb.LRange(ctx, "list{t}", 0, -1).Val())
				}
				if to == "RIGHT" {
					require.Equal(t, elem, rdb.RPop(ctx, "target_key{t}").Val())
				} else {
					require.Equal(t, elem, rdb.LPop(ctx, "target_key{t}").Val())
				}
			})
		}
	}

	t.Run("Test BLMOVE on different keys", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "list1{t}").Err())
		require.NoError(t, rdb.Del(ctx, "list2{t}").Err())
		require.NoError(t, rdb.RPush(ctx, "list1{t}", "1").Err())
		require.NoError(t, rdb.RPush(ctx, "list1{t}", "2").Err())
		require.NoError(t, rdb.RPush(ctx, "list1{t}", "3").Err())
		require.NoError(t, rdb.RPush(ctx, "list1{t}", "4").Err())
		require.NoError(t, rdb.RPush(ctx, "list1{t}", "5").Err())
		require.NoError(t, rdb.BLMove(ctx, "list1{t}", "list2{t}", "RIGHT", "LEFT", time.Millisecond*1000).Err())
		require.NoError(t, rdb.BLMove(ctx, "list1{t}", "list2{t}", "LEFT", "RIGHT", time.Millisecond*1000).Err())
		require.EqualValues(t, 3, rdb.LLen(ctx, "list1{t}").Val())
		require.EqualValues(t, 2, rdb.LLen(ctx, "list2{t}").Val())
		require.Equal(t, []string{"2", "3", "4"}, rdb.LRange(ctx, "list1{t}", 0, -1).Val())
		require.Equal(t, []string{"5", "1"}, rdb.LRange(ctx, "list2{t}", 0, -1).Val())
	})

	for _, from := range []string{"LEFT", "RIGHT"} {
		for _, to := range []string{"LEFT", "RIGHT"} {
			t.Run(fmt.Sprintf("BLMOVE %s %s on the list node", from, to), func(t *testing.T) {
				rd := srv.NewTCPClient()
				defer func() { require.NoError(t, rd.Close()) }()
				require.NoError(t, rdb.Del(ctx, "target_key{t}").Err())
				require.NoError(t, rdb.RPush(ctx, "target_key{t}", 1).Err())
				createList("list{t}", []string{"a", "b", "c", "d"})
				require.NoError(t, rd.WriteArgs("blmove", "list{t}", "target_key{t}", from, to, "1"))
				r, err1 := rd.ReadLine()
				require.Equal(t, "$1", r)
				require.NoError(t, err1)
				elem, err2 := rd.ReadLine()
				require.NoError(t, err2)
				if from == "RIGHT" {
					require.Equal(t, elem, "d")
					require.Equal(t, []string{"a", "b", "c"}, rdb.LRange(ctx, "list{t}", 0, -1).Val())
				} else {
					require.Equal(t, elem, "a")
					require.Equal(t, []string{"b", "c", "d"}, rdb.LRange(ctx, "list{t}", 0, -1).Val())
				}
				if to == "RIGHT" {
					require.Equal(t, elem, rdb.RPop(ctx, "target_key{t}").Val())
				} else {
					require.Equal(t, elem, rdb.LPop(ctx, "target_key{t}").Val())
				}
			})
		}
	}

	t.Run("Test BLMOVE block behaviour", func(t *testing.T) {
		rd := srv.NewTCPClient()
		defer func() { require.NoError(t, rd.Close()) }()
		require.NoError(t, rdb.Del(ctx, "blist", "target").Err())
		require.NoError(t, rd.WriteArgs("blmove", "blist", "target", "left", "right", "0"))
		require.EqualValues(t, 2, rdb.LPush(ctx, "blist", "foo", "bar").Val())
		rd.MustRead(t, "$3")
		require.Equal(t, "bar", rdb.LRange(ctx, "target", 0, -1).Val()[0])
	})

	t.Run("LPOS rank negation overflow", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mylist").Err())
		util.ErrorRegexp(t, rdb.Do(ctx, "LPOS", "mylist", "foo", "RANK", "-9223372036854775808").Err(), ".*rank would overflow.*")
	})

	for listType, large := range largeValue {
		t.Run(fmt.Sprintf("LPOS basic usage - %s", listType), func(t *testing.T) {
			createList("mylist", []string{"a", "b", "c", large, "2", "3", "c", "c"})
			require.Equal(t, int64(0), rdb.LPos(ctx, "mylist", "a", redis.LPosArgs{}).Val())
			require.Equal(t, int64(2), rdb.LPos(ctx, "mylist", "c", redis.LPosArgs{}).Val())
		})

		t.Run("LPOS RANK option", func(t *testing.T) {
			require.Equal(t, int64(2), rdb.LPos(ctx, "mylist", "c", redis.LPosArgs{Rank: 1}).Val())
			require.Equal(t, int64(6), rdb.LPos(ctx, "mylist", "c", redis.LPosArgs{Rank: 2}).Val())
			require.Error(t, rdb.LPos(ctx, "mylist", "c", redis.LPosArgs{Rank: 4}).Err())
			require.Equal(t, int64(7), rdb.LPos(ctx, "mylist", "c", redis.LPosArgs{Rank: -1}).Val())
			require.Equal(t, int64(6), rdb.LPos(ctx, "mylist", "c", redis.LPosArgs{Rank: -2}).Val())
			err := rdb.Do(ctx, "LPOS", "mylist", "c", "RANK", 0).Err()
			require.Error(t, err)
			require.True(t, strings.Contains(err.Error(), "RANK can't be zero"))
		})

		t.Run("LPOS COUNT option", func(t *testing.T) {
			require.Equal(t, []int64{2, 6, 7}, rdb.LPosCount(ctx, "mylist", "c", 0, redis.LPosArgs{}).Val())
			require.Equal(t, []int64{2}, rdb.LPosCount(ctx, "mylist", "c", 1, redis.LPosArgs{}).Val())
			require.Equal(t, []int64{2, 6}, rdb.LPosCount(ctx, "mylist", "c", 2, redis.LPosArgs{}).Val())
			require.Equal(t, []int64{2, 6, 7}, rdb.LPosCount(ctx, "mylist", "c", 100, redis.LPosArgs{}).Val())
		})

		t.Run("LPOS COUNT + RANK option", func(t *testing.T) {
			require.Equal(t, []int64{6, 7}, rdb.LPosCount(ctx, "mylist", "c", 0, redis.LPosArgs{Rank: 2}).Val())
			require.Equal(t, []int64{7, 6}, rdb.LPosCount(ctx, "mylist", "c", 2, redis.LPosArgs{Rank: -1}).Val())
		})

		t.Run("LPOS non existing key", func(t *testing.T) {
			require.Empty(t, rdb.LPosCount(ctx, "mylistxxx", "c", 0, redis.LPosArgs{Rank: 2}).Val())
		})

		t.Run("LPOS no match", func(t *testing.T) {
			require.Empty(t, rdb.LPosCount(ctx, "mylist", "x", 2, redis.LPosArgs{Rank: -1}).Val())
			require.Empty(t, rdb.LPos(ctx, "mylist", "x", redis.LPosArgs{Rank: -1}).Val())
		})

		t.Run("LPOS MAXLEN", func(t *testing.T) {
			require.Equal(t, []int64{0}, rdb.LPosCount(ctx, "mylist", "a", 0, redis.LPosArgs{MaxLen: 1}).Val())
			require.Empty(t, rdb.LPosCount(ctx, "mylist", "c", 0, redis.LPosArgs{MaxLen: 1}).Val())
			require.Equal(t, []int64{2}, rdb.LPosCount(ctx, "mylist", "c", 0, redis.LPosArgs{MaxLen: 3}).Val())
			require.Equal(t, []int64{7, 6}, rdb.LPosCount(ctx, "mylist", "c", 0, redis.LPosArgs{MaxLen: 3, Rank: -1}).Val())
			require.Equal(t, []int64{6}, rdb.LPosCount(ctx, "mylist", "c", 0, redis.LPosArgs{MaxLen: 7, Rank: 2}).Val())
		})

		t.Run("LPOS when RANK is greater than matches", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "mylist").Err())
			require.NoError(t, rdb.LPush(ctx, "mylist", "a").Err())
			require.Empty(t, rdb.LPosCount(ctx, "mylist", "b", 10, redis.LPosArgs{Rank: 5}).Val())
		})
	}

	for _, direction := range []string{"LEFT", "RIGHT"} {
		key1 := "lmpop-list1"
		key2 := "lmpop-list2"
		rdb.Del(ctx, key1, key2)
		require.EqualValues(t, 5, rdb.LPush(ctx, key1, "one", "two", "three", "four", "five").Val())
		require.EqualValues(t, 5, rdb.LPush(ctx, key2, "ONE", "TWO", "THREE", "FOUR", "FIVE").Val())

		t.Run(fmt.Sprintf("LMPOP test oneKey countSingle %s", direction), func(t *testing.T) {
			result := rdb.LMPop(ctx, direction, 1, key1)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key1, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"five"}, resultVal)
			} else {
				require.Equal(t, []string{"one"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("LMPOP test oneKey countMulti %s", direction), func(t *testing.T) {
			result := rdb.LMPop(ctx, direction, 2, key1)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key1, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"four", "three"}, resultVal)
			} else {
				require.Equal(t, []string{"two", "three"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("LMPOP test oneKey countTooMuch %s", direction), func(t *testing.T) {
			result := rdb.LMPop(ctx, direction, 10, key1)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key1, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"two", "one"}, resultVal)
			} else {
				require.Equal(t, []string{"four", "five"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("LMPOP test oneKey empty %s", direction), func(t *testing.T) {
			require.EqualError(t, rdb.LMPop(ctx, direction, 1, key1).Err(), redis.Nil.Error())
		})

		require.EqualValues(t, 2, rdb.LPush(ctx, key1, "six", "seven").Val())
		t.Run(fmt.Sprintf("LMPOP test firstKey countOver %s", direction), func(t *testing.T) {
			result := rdb.LMPop(ctx, direction, 10, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key1, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"seven", "six"}, resultVal)
			} else {
				require.Equal(t, []string{"six", "seven"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("LMPOP test secondKey countSingle %s", direction), func(t *testing.T) {
			result := rdb.LMPop(ctx, direction, 1, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key2, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"FIVE"}, resultVal)
			} else {
				require.Equal(t, []string{"ONE"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("LMPOP test secondKey countMulti %s", direction), func(t *testing.T) {
			result := rdb.LMPop(ctx, direction, 2, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key2, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"FOUR", "THREE"}, resultVal)
			} else {
				require.Equal(t, []string{"TWO", "THREE"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("LMPOP test secondKey countOver %s", direction), func(t *testing.T) {
			result := rdb.LMPop(ctx, direction, 10, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key2, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"TWO", "ONE"}, resultVal)
			} else {
				require.Equal(t, []string{"FOUR", "FIVE"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("LMPOP test bothKey empty %s", direction), func(t *testing.T) {
			require.EqualError(t, rdb.LMPop(ctx, direction, 1, key1, key2).Err(), redis.Nil.Error())
		})

		t.Run(fmt.Sprintf("LMPOP test dummyKey empty %s", direction), func(t *testing.T) {
			require.EqualError(t, rdb.LMPop(ctx, direction, 1, "dummy1", "dummy2").Err(), redis.Nil.Error())
		})

		lmpopNoCount := func(c *redis.Client, ctx context.Context, direction string, keys ...string) *redis.KeyValuesCmd {
			args := make([]interface{}, 2+len(keys), 5+len(keys))
			args[0] = "lmpop"
			args[1] = len(keys)
			for i, key := range keys {
				args[2+i] = key
			}
			args = append(args, strings.ToLower(direction))
			cmd := redis.NewKeyValuesCmd(ctx, args...)
			_ = c.Process(ctx, cmd)
			return cmd
		}
		rdb.Del(ctx, key1, key2)
		require.EqualValues(t, 2, rdb.LPush(ctx, key1, "one", "two").Val())
		require.EqualValues(t, 2, rdb.LPush(ctx, key2, "ONE", "TWO").Val())

		t.Run(fmt.Sprintf("LMPOP test oneKey noCount one %s", direction), func(t *testing.T) {
			result := lmpopNoCount(rdb, ctx, direction, key1)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key1, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"two"}, resultVal)
			} else {
				require.Equal(t, []string{"one"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("LMPOP test firstKey noCount one %s", direction), func(t *testing.T) {
			result := lmpopNoCount(rdb, ctx, direction, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key1, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"one"}, resultVal)
			} else {
				require.Equal(t, []string{"two"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("LMPOP test oneKey noCount empty %s", direction), func(t *testing.T) {
			require.EqualError(t, lmpopNoCount(rdb, ctx, direction, key1).Err(), redis.Nil.Error())
		})

		t.Run(fmt.Sprintf("LMPOP test secondKey noCount one %s", direction), func(t *testing.T) {
			result := lmpopNoCount(rdb, ctx, direction, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key2, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"TWO"}, resultVal)
			} else {
				require.Equal(t, []string{"ONE"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("LMPOP test secondKey noCount one %s", direction), func(t *testing.T) {
			result := lmpopNoCount(rdb, ctx, direction, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key2, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"ONE"}, resultVal)
			} else {
				require.Equal(t, []string{"TWO"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("LMPOP test bothKey noCount empty %s", direction), func(t *testing.T) {
			require.EqualError(t, lmpopNoCount(rdb, ctx, direction, key1, key2).Err(), redis.Nil.Error())
		})
	}

	for _, direction := range []string{"LEFT", "RIGHT"} {
		key1 := "blmpop-list1"
		key2 := "blmpop-list2"
		rdb.Del(ctx, key1, key2)
		require.EqualValues(t, 5, rdb.LPush(ctx, key1, "one", "two", "three", "four", "five").Val())
		require.EqualValues(t, 5, rdb.LPush(ctx, key2, "ONE", "TWO", "THREE", "FOUR", "FIVE").Val())

		zeroTimeout := time.Second * 0

		// TEST SUIT #1: non-blocking scenario (at least one queried list is not empty).
		// In these cases, the behavior should be the same to LMPOP.
		t.Run(fmt.Sprintf("BLMPOP test unblocked oneKey countSingle %s", direction), func(t *testing.T) {
			result := rdb.BLMPop(ctx, zeroTimeout, direction, 1, key1)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key1, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"five"}, resultVal)
			} else {
				require.Equal(t, []string{"one"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("BLMPOP test unblocked oneKey countMulti %s", direction), func(t *testing.T) {
			result := rdb.BLMPop(ctx, zeroTimeout, direction, 2, key1)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key1, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"four", "three"}, resultVal)
			} else {
				require.Equal(t, []string{"two", "three"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("BLMPOP test unblocked oneKey countTooMuch %s", direction), func(t *testing.T) {
			result := rdb.BLMPop(ctx, zeroTimeout, direction, 10, key1)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key1, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"two", "one"}, resultVal)
			} else {
				require.Equal(t, []string{"four", "five"}, resultVal)
			}
		})

		require.EqualValues(t, 2, rdb.LPush(ctx, key1, "six", "seven").Val())
		t.Run(fmt.Sprintf("BLMPOP test unblocked firstKey countOver %s", direction), func(t *testing.T) {
			result := rdb.BLMPop(ctx, zeroTimeout, direction, 10, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key1, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"seven", "six"}, resultVal)
			} else {
				require.Equal(t, []string{"six", "seven"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("BLMPOP test unblocked secondKey countSingle %s", direction), func(t *testing.T) {
			result := rdb.BLMPop(ctx, zeroTimeout, direction, 1, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key2, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"FIVE"}, resultVal)
			} else {
				require.Equal(t, []string{"ONE"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("BLMPOP test unblocked secondKey countMulti %s", direction), func(t *testing.T) {
			result := rdb.BLMPop(ctx, zeroTimeout, direction, 2, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key2, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"FOUR", "THREE"}, resultVal)
			} else {
				require.Equal(t, []string{"TWO", "THREE"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("BLMPOP test unblocked secondKey countOver %s", direction), func(t *testing.T) {
			result := rdb.BLMPop(ctx, zeroTimeout, direction, 10, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key2, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"TWO", "ONE"}, resultVal)
			} else {
				require.Equal(t, []string{"FOUR", "FIVE"}, resultVal)
			}
		})

		blmpopNoCount := func(c *redis.Client, ctx context.Context, timeout string, direction string, keys ...string) *redis.KeyValuesCmd {
			args := make([]interface{}, 3+len(keys), 6+len(keys))
			args[0] = "blmpop"
			args[1] = timeout
			args[2] = len(keys)
			for i, key := range keys {
				args[3+i] = key
			}
			args = append(args, strings.ToLower(direction))
			cmd := redis.NewKeyValuesCmd(ctx, args...)
			_ = c.Process(ctx, cmd)
			return cmd
		}
		rdb.Del(ctx, key1, key2)
		require.EqualValues(t, 2, rdb.LPush(ctx, key1, "one", "two").Val())
		require.EqualValues(t, 2, rdb.LPush(ctx, key2, "ONE", "TWO").Val())

		t.Run(fmt.Sprintf("BLMPOP test unblocked oneKey noCount one %s", direction), func(t *testing.T) {
			result := blmpopNoCount(rdb, ctx, "0", direction, key1)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key1, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"two"}, resultVal)
			} else {
				require.Equal(t, []string{"one"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("BLMPOP test unblocked firstKey noCount one %s", direction), func(t *testing.T) {
			result := blmpopNoCount(rdb, ctx, "0", direction, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key1, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"one"}, resultVal)
			} else {
				require.Equal(t, []string{"two"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("BLMPOP test unblocked secondKey noCount one %s", direction), func(t *testing.T) {
			result := blmpopNoCount(rdb, ctx, "0", direction, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key2, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"TWO"}, resultVal)
			} else {
				require.Equal(t, []string{"ONE"}, resultVal)
			}
		})

		t.Run(fmt.Sprintf("BLMPOP test unblocked secondKey noCount one %s", direction), func(t *testing.T) {
			result := blmpopNoCount(rdb, ctx, "0", direction, key1, key2)
			resultKey, resultVal := result.Val()
			require.NoError(t, result.Err())
			require.EqualValues(t, key2, resultKey)
			if direction == "LEFT" {
				require.Equal(t, []string{"ONE"}, resultVal)
			} else {
				require.Equal(t, []string{"TWO"}, resultVal)
			}
		})

		// TEST SUIT #2: blocking scenario, but data reaches within timeout.
		t.Run(fmt.Sprintf("BLMPOP test blocked served oneKey countSingle %s", direction), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, key1, key2).Err())
			require.NoError(t, rd.WriteArgs("blmpop", "1", "1", key1, direction, "count", "1"))
			require.NoError(t, rdb.RPush(ctx, key1, "ONE", "TWO").Err())
			if direction == "LEFT" {
				rd.MustReadStringsWithKey(t, key1, []string{"ONE"})
			} else {
				rd.MustReadStringsWithKey(t, key1, []string{"TWO"})
			}
			require.EqualValues(t, 1, rdb.Exists(ctx, key1).Val())
		})

		t.Run(fmt.Sprintf("BLMPOP test blocked served oneKey countMulti %s", direction), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, key1, key2).Err())
			require.NoError(t, rd.WriteArgs("blmpop", "1", "1", key1, direction, "count", "2"))
			require.NoError(t, rdb.RPush(ctx, key1, "ONE", "TWO").Err())
			if direction == "LEFT" {
				rd.MustReadStringsWithKey(t, key1, []string{"ONE", "TWO"})
			} else {
				rd.MustReadStringsWithKey(t, key1, []string{"TWO", "ONE"})
			}
			require.EqualValues(t, 0, rdb.Exists(ctx, key1).Val())
		})

		t.Run(fmt.Sprintf("BLMPOP test blocked served oneKey countOver %s", direction), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, key1, key2).Err())
			require.NoError(t, rd.WriteArgs("blmpop", "1", "1", key1, direction, "count", "10"))
			require.NoError(t, rdb.RPush(ctx, key1, "ONE", "TWO").Err())
			if direction == "LEFT" {
				rd.MustReadStringsWithKey(t, key1, []string{"ONE", "TWO"})
			} else {
				rd.MustReadStringsWithKey(t, key1, []string{"TWO", "ONE"})
			}
			require.EqualValues(t, 0, rdb.Exists(ctx, key1).Val())
		})

		t.Run(fmt.Sprintf("BLMPOP test blocked served firstKey countOver %s", direction), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, key1, key2).Err())
			require.NoError(t, rd.WriteArgs("blmpop", "1", "2", key1, key2, direction, "count", "2"))
			require.NoError(t, rdb.RPush(ctx, key1, "ONE", "TWO").Err())
			if direction == "LEFT" {
				rd.MustReadStringsWithKey(t, key1, []string{"ONE", "TWO"})
			} else {
				rd.MustReadStringsWithKey(t, key1, []string{"TWO", "ONE"})
			}
			require.EqualValues(t, 0, rdb.Exists(ctx, key1).Val())
		})

		t.Run(fmt.Sprintf("BLMPOP test blocked served secondKey countOver %s", direction), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, key1, key2).Err())
			require.NoError(t, rd.WriteArgs("blmpop", "1", "2", key1, key2, direction, "count", "2"))
			require.NoError(t, rdb.RPush(ctx, key2, "one", "two").Err())
			if direction == "LEFT" {
				rd.MustReadStringsWithKey(t, key2, []string{"one", "two"})
			} else {
				rd.MustReadStringsWithKey(t, key2, []string{"two", "one"})
			}
			require.EqualValues(t, 0, rdb.Exists(ctx, key2).Val())
		})

		t.Run(fmt.Sprintf("BLMPOP test blocked served bothKey FIFO countOver %s", direction), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, key1, key2).Err())
			require.NoError(t, rd.WriteArgs("blmpop", "1", "2", key1, key2, direction, "count", "2"))
			// https://github.com/apache/kvrocks/issues/2617
			// WriteArgs are required to be executed first
			time.Sleep(100 * time.Millisecond)

			require.NoError(t, rdb.RPush(ctx, key1, "ONE", "TWO").Err())
			require.NoError(t, rdb.RPush(ctx, key2, "one", "two").Err())
			if direction == "LEFT" {
				rd.MustReadStringsWithKey(t, key1, []string{"ONE", "TWO"})
			} else {
				rd.MustReadStringsWithKey(t, key1, []string{"TWO", "ONE"})
			}
			require.EqualValues(t, 0, rdb.Exists(ctx, key1).Val())
			require.EqualValues(t, 2, rdb.LLen(ctx, key2).Val())
		})

		t.Run(fmt.Sprintf("BLMPOP test blocked served secondKey noCount %s", direction), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, key1, key2).Err())
			require.NoError(t, rd.WriteArgs("blmpop", "1", "2", key1, key2, direction))
			require.NoError(t, rdb.RPush(ctx, key2, "one", "two").Err())
			if direction == "LEFT" {
				rd.MustReadStringsWithKey(t, key2, []string{"one"})
			} else {
				rd.MustReadStringsWithKey(t, key2, []string{"two"})
			}
			require.EqualValues(t, 1, rdb.LLen(ctx, key2).Val())
		})

		// TEST SUIT #3: blocking scenario, and timeout is triggered.
		t.Run(fmt.Sprintf("BLMPOP test blocked timeout %s", direction), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, key1, key2).Err())
			require.NoError(t, rd.WriteArgs("blmpop", "1", "2", key1, key2, direction))
			rd.MustMatch(t, "")
		})

		// TEST SUIT #4: blocking scenario, and timeout is 0 (permanently blocked).
		t.Run(fmt.Sprintf("BLMPOP test blocked infinitely served secondKey countOver %s", direction), func(t *testing.T) {
			rd := srv.NewTCPClient()
			defer func() { require.NoError(t, rd.Close()) }()
			require.NoError(t, rdb.Del(ctx, key1, key2).Err())
			require.NoError(t, rd.WriteArgs("blmpop", "0", "2", key1, key2, direction, "count", "2"))
			require.NoError(t, rdb.RPush(ctx, key2, "one", "two").Err())
			if direction == "LEFT" {
				rd.MustReadStringsWithKey(t, key2, []string{"one", "two"})
			} else {
				rd.MustReadStringsWithKey(t, key2, []string{"two", "one"})
			}
			require.EqualValues(t, 0, rdb.Exists(ctx, key2).Val())
		})
	}
}
