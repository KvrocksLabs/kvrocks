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

package json

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestJson(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("JSON.SET and JSON.GET basics", func(t *testing.T) {
		require.Error(t, rdb.Do(ctx, "JSON.SET", "a").Err())
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", ` {"x":1, "y":2} `).Err())
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a").Val(), `{"x":1,"y":2}`)

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$.y", `233`).Err())
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a").Val(), `{"x":1,"y":233}`)

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `[[1], [2]]`).Err())
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$[*][0]", "3").Err())
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a").Val(), `[[3],[3]]`)

		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a", "$").Val(), `[[[3],[3]]]`)
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a", "$[0]").Val(), `[[3]]`)
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a", "$[0][0]").Val(), `[3]`)

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"x":1,"y":{"x":{"y":2},"y":3}}`).Err())
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a").Val(), `{"x":1,"y":{"x":{"y":2},"y":3}}`)
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a", "$").Val(), `[{"x":1,"y":{"x":{"y":2},"y":3}}]`)
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a", "$..x").Val(), `[1,{"y":2}]`)
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a", "$..x", "$..y").Val(), `{"$..x":[1,{"y":2}],"$..y":[{"x":{"y":2},"y":3},3,2]}`)

		require.Equal(t, rdb.Do(ctx, "JSON.GET", "no-such-key").Val(), nil)
		require.Equal(t, rdb.Type(ctx, "a").Val(), "ReJSON-RL")
	})

	t.Run("JSON.GET with options", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", ` {"x":1, "y":2} `).Err())
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a", "INDENT", " ").Val(), `{ "x":1, "y":2}`)
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a", "INDENT", " ", "SPACE", " ").Val(), `{ "x": 1, "y": 2}`)
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a", "NEWLINE", "\n").Val(), "{\n\"x\":1,\n\"y\":2\n}")
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a", "NEWLINE", "\n", "INDENT", " ", "SPACE", " ").Val(), "{\n \"x\": 1,\n \"y\": 2\n}")
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a", "INDENT", " ", "$").Val(), `[ {  "x":1,  "y":2 }]`)
	})

	t.Run("JSON storage format CBOR", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"x":1, "y":2}`).Err())
		require.Equal(t, "json", rdb.Do(ctx, "JSON.INFO", "a").Val().([]interface{})[1])

		require.NoError(t, rdb.Do(ctx, "CONFIG", "SET", "json-storage-format", "cbor").Err())
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "b", "$", `{"x":1, "y":2}`).Err())
		require.Equal(t, "cbor", rdb.Do(ctx, "JSON.INFO", "b").Val().([]interface{})[1])
		require.Equal(t, `{"x":1,"y":2}`, rdb.Do(ctx, "JSON.GET", "b").Val())
		require.Equal(t, `{"x":1,"y":2}`, rdb.Do(ctx, "JSON.GET", "a").Val())
	})

	t.Run("JSON.ARRAPPEND basics", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "SET", "a", `1`).Err())
		require.Error(t, rdb.Do(ctx, "JSON.ARRAPPEND", "a", "$", `1`).Err())
		require.NoError(t, rdb.Do(ctx, "DEL", "a").Err())

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", ` {"x":1, "y": {"x":1} } `).Err())
		require.Equal(t, []interface{}{}, rdb.Do(ctx, "JSON.ARRAPPEND", "a", "$..k", `1`).Val())
		require.Error(t, rdb.Do(ctx, "JSON.ARRAPPEND", "a", "$").Err())
		require.Error(t, rdb.Do(ctx, "JSON.ARRAPPEND", "a", "$", ` 1, 2, 3`).Err())
		require.Error(t, rdb.Do(ctx, "JSON.ARRAPPEND", "a", "$", `1`, ` 1, 2, 3`).Err())
		require.Equal(t, []interface{}{nil, nil}, rdb.Do(ctx, "JSON.ARRAPPEND", "a", "$..x", `1`).Val())

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", ` {"x":1, "y": {"x":[]} } `).Err())
		require.Equal(t, []interface{}{int64(1), nil}, rdb.Do(ctx, "JSON.ARRAPPEND", "a", "$..x", `1`).Val())
		require.Equal(t, `[{"x":1,"y":{"x":[1]}}]`, rdb.Do(ctx, "JSON.GET", "a", "$").Val())

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", ` {"x":[], "y":[]} `).Err())
		require.Equal(t, []interface{}{int64(1)}, rdb.Do(ctx, "JSON.ARRAPPEND", "a", "$.x", `1`).Val())
		require.Equal(t, `{"x":[1],"y":[]}`, rdb.Do(ctx, "JSON.GET", "a").Val())
		require.Equal(t, []interface{}{int64(4)}, rdb.Do(ctx, "JSON.ARRAPPEND", "a", "$.x", `1`, `2`, `3`).Val())
		require.Equal(t, []interface{}{int64(1)}, rdb.Do(ctx, "JSON.ARRAPPEND", "a", "$.y", ` {"x":[], "y":[]} `).Val())
		require.Equal(t, `[{"x":[1,1,2,3],"y":[{"x":[],"y":[]}]}]`, rdb.Do(ctx, "JSON.GET", "a", "$").Val())

		require.Equal(t, []interface{}{int64(2), int64(6)}, rdb.Do(ctx, "JSON.ARRAPPEND", "a", "$..x", `1`, `2`).Val())
		require.Equal(t, `[[1,2]]`, rdb.Do(ctx, "JSON.GET", "a", "$.y[0].x").Val())
		require.Equal(t, `[]`, rdb.Do(ctx, "JSON.GET", "a", "$.x.x").Val())
		require.Equal(t, `[{"x":[1,1,2,3,1,2],"y":[{"x":[1,2],"y":[]}]}]`, rdb.Do(ctx, "JSON.GET", "a", "$").Val())
	})

	t.Run("JSON.TYPE basics", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"b":true,"x":1, "y":1.2, "z": {"x":[1,2,3], "y": null}, "v":{"x":"y"}}`).Err())

		types, err := rdb.Do(ctx, "JSON.TYPE", "a").StringSlice()
		require.NoError(t, err)
		require.Equal(t, []string{"object"}, types)

		types, err = rdb.Do(ctx, "JSON.TYPE", "a", "$").StringSlice()
		require.NoError(t, err)
		require.Equal(t, []string{"object"}, types)

		types, err = rdb.Do(ctx, "JSON.TYPE", "a", "$..x").StringSlice()
		require.NoError(t, err)
		require.EqualValues(t, []string{"integer", "string", "array"}, types)

		types, err = rdb.Do(ctx, "JSON.TYPE", "a", "$..y").StringSlice()
		require.NoError(t, err)
		require.EqualValues(t, []string{"number", "null"}, types)

		types, err = rdb.Do(ctx, "JSON.TYPE", "a", "$.b").StringSlice()
		require.NoError(t, err)
		require.EqualValues(t, []string{"boolean"}, types)

		types, err = rdb.Do(ctx, "JSON.TYPE", "a", "$.no_exists").StringSlice()
		require.NoError(t, err)
		require.EqualValues(t, []string{}, types)

		_, err = rdb.Do(ctx, "JSON.TYPE", "not_exists", "$").StringSlice()
		require.EqualError(t, err, redis.Nil.Error())
	})

	t.Run("Clear JSON values", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "bb", "$", `{"obj":{"a":1, "b":2}, "arr":[1,2,3], "str": "foo", "bool": true, "int": 42, "float": 3.14}`).Err())

		require.NoError(t, rdb.Do(ctx, "JSON.CLEAR", "bb", "$").Err())
		require.Equal(t, `{}`, rdb.Do(ctx, "JSON.GET", "bb").Val())

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "bb", "$", `{"obj":{"a":1, "b":2}, "arr":[1,2,3], "str": "foo", "bool": true, "int": 42, "float": 3.14}`).Err())
		require.NoError(t, rdb.Do(ctx, "JSON.CLEAR", "bb", "$.obj").Err())
		require.Equal(t, `{"arr":[1,2,3],"bool":true,"float":3.14,"int":42,"obj":{},"str":"foo"}`, rdb.Do(ctx, "JSON.GET", "bb").Val())

		require.NoError(t, rdb.Do(ctx, "JSON.CLEAR", "bb", "$.arr").Err())
		require.Equal(t, `{"arr":[],"bool":true,"float":3.14,"int":42,"obj":{},"str":"foo"}`, rdb.Do(ctx, "JSON.GET", "bb").Val())

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "bb", "$", `{"obj":{"a":1, "b":2}, "arr":[1,2,3], "str": "foo", "bool": true, "int": 42, "float": 3.14}`).Err())
		require.NoError(t, rdb.Do(ctx, "JSON.CLEAR", "bb", "$.*").Err())
		require.Equal(t, `{"arr":[],"bool":true,"float":0,"int":0,"obj":{},"str":"foo"}`, rdb.Do(ctx, "JSON.GET", "bb").Val())

		_, err := rdb.Do(ctx, "JSON.CLEAR", "bb", "$.some").Result()
		require.NoError(t, err)
	})

	t.Run("JSON.ARRLEN basics", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "a").Err())
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"a1":[1,2],"a2":[[1,5,7],[8],[9]],"i":1,"d":1.0,"s":"string","o":{"a3":[1,1,1]}}`).Err())

		lens, err := rdb.Do(ctx, "JSON.ARRLEN", "a", "$.a1").Uint64Slice()
		require.NoError(t, err)
		require.EqualValues(t, []uint64{2}, lens)

		lens, err = rdb.Do(ctx, "JSON.ARRLEN", "a", "$.a2").Uint64Slice()
		require.NoError(t, err)
		require.EqualValues(t, []uint64{3}, lens)

		lens, err = rdb.Do(ctx, "JSON.ARRLEN", "a", "$.a2[0]").Uint64Slice()
		require.NoError(t, err)
		require.EqualValues(t, []uint64{3}, lens)

		require.EqualValues(t, []interface{}{nil}, rdb.Do(ctx, "JSON.ARRLEN", "a", "$.i").Val())
		require.EqualValues(t, []interface{}{nil}, rdb.Do(ctx, "JSON.ARRLEN", "a", "$.d").Val())
		require.EqualValues(t, []interface{}{nil}, rdb.Do(ctx, "JSON.ARRLEN", "a", "$.s").Val())
		require.EqualValues(t, []interface{}{nil}, rdb.Do(ctx, "JSON.ARRLEN", "a", "$.o").Val())

		lens, err = rdb.Do(ctx, "JSON.ARRLEN", "a", "$.o.a3").Uint64Slice()
		require.NoError(t, err)
		require.EqualValues(t, []uint64{3}, lens)

		_, err = rdb.Do(ctx, "JSON.ARRLEN", "not_exists", "$.*").Uint64Slice()
		require.EqualError(t, err, redis.Nil.Error())

		lens, err = rdb.Do(ctx, "JSON.ARRLEN", "a", "$.not_exists").Uint64Slice()
		require.NoError(t, err)
		require.EqualValues(t, []uint64{}, lens)
	})

	t.Run("JSON.OBJKEYS basics", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a").Err())
		// key no exists
		require.EqualError(t, rdb.Do(ctx, "JSON.OBJKEYS", "not_exists", "$").Err(), redis.Nil.Error())
		// key not json
		require.NoError(t, rdb.Do(ctx, "SET", "no_json", "1").Err())
		require.Error(t, rdb.Do(ctx, "JSON.OBJKEYS", "no_json", "$").Err())
		// json path no exists
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"a1":[1,2]}`).Err())
		require.EqualValues(t, []interface{}{}, rdb.Do(ctx, "JSON.OBJKEYS", "a", "$.not_exists").Val())
		// json path not object
		require.EqualValues(t, []interface{}{nil}, rdb.Do(ctx, "JSON.OBJKEYS", "a", "$.a1").Val())
		// default path
		require.EqualValues(t, []interface{}{[]interface{}{"a1"}}, rdb.Do(ctx, "JSON.OBJKEYS", "a").Val())
		// json path has one object
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"a1":{"b":1,"c":1}}`).Err())
		require.EqualValues(t, []interface{}{[]interface{}{"b", "c"}}, rdb.Do(ctx, "JSON.OBJKEYS", "a", "$.a1").Val())
		// json path has many object
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"a":{"a1":{"b":1,"c":1}},"b":{"a1":{"e":1,"f":1}}}`).Err())
		require.EqualValues(t, []interface{}{[]interface{}{"b", "c"}, []interface{}{"e", "f"}}, rdb.Do(ctx, "JSON.OBJKEYS", "a", "$..a1").Val())
		// json path has many object and one is not object
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"a":{"a1":{"b":1,"c":1}},"b":{"a1":[1]},"c":{"a1":{"e":1,"f":1}}}`).Err())
		require.EqualValues(t, []interface{}{[]interface{}{"b", "c"}, interface{}(nil), []interface{}{"e", "f"}}, rdb.Do(ctx, "JSON.OBJKEYS", "a", "$..a1").Val())
	})

	t.Run("JSON.ARRPOP basics", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `[3,"str",2.1,{},[5,6]]`).Err())
		require.EqualValues(t, []interface{}{"[5,6]"}, rdb.Do(ctx, "JSON.ARRPOP", "a").Val())
		require.EqualValues(t, []interface{}{"2.1"}, rdb.Do(ctx, "JSON.ARRPOP", "a", "$", "-2").Val())
		require.EqualValues(t, []interface{}{"{}"}, rdb.Do(ctx, "JSON.ARRPOP", "a", "$", "3").Val())
		require.EqualValues(t, []interface{}{`"str"`}, rdb.Do(ctx, "JSON.ARRPOP", "a", "$", "1").Val())
		require.EqualValues(t, []interface{}{"3"}, rdb.Do(ctx, "JSON.ARRPOP", "a", "$", "0").Val())
		require.EqualValues(t, []interface{}{nil}, rdb.Do(ctx, "JSON.ARRPOP", "a", "$").Val())

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "o", "$", `{"o":{"x":1},"s":"str","i":1,"d":2.2}`).Err())
		require.EqualValues(t, []interface{}{nil}, rdb.Do(ctx, "JSON.ARRPOP", "o", "$.o", 1).Val())
		require.EqualValues(t, []interface{}{nil}, rdb.Do(ctx, "JSON.ARRPOP", "o", "$.s", -1).Val())
		require.EqualValues(t, []interface{}{nil}, rdb.Do(ctx, "JSON.ARRPOP", "o", "$.i", 0).Val())
		require.EqualValues(t, []interface{}{nil}, rdb.Do(ctx, "JSON.ARRPOP", "o", "$.d", 2).Val())

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `[[0,1],[3,{"x":2.0}],"str",[4,[5,"6"]]]`).Err())
		require.EqualValues(t, []interface{}{`[5,"6"]`, nil, `{"x":2.0}`, "1"}, rdb.Do(ctx, "JSON.ARRPOP", "a", "$.*").Val())

		require.ErrorContains(t, rdb.Do(ctx, "JSON.ARRPOP", "a", "$", "str").Err(), "not started as an integer")
		require.ErrorContains(t, rdb.Do(ctx, "JSON.ARRPOP", "a", "$", "2str").Err(), "encounter non-integer characters")
		require.ErrorContains(t, rdb.Do(ctx, "JSON.ARRPOP", "a", "$", "-1str").Err(), "encounter non-integer characters")
		require.ErrorContains(t, rdb.Do(ctx, "JSON.ARRPOP", "a", "$", "0", "1").Err(), "wrong number of arguments")
	})

	t.Run("JSON.TOGGLE basics", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `true`).Err())
		require.EqualValues(t, []interface{}{int64(0)}, rdb.Do(ctx, "JSON.TOGGLE", "a", "$").Val())
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a").Val(), `false`)

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"bool":true}`).Err())
		require.EqualValues(t, []interface{}{int64(0)}, rdb.Do(ctx, "JSON.TOGGLE", "a", "$.bool").Val())
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a").Val(), `{"bool":false}`)

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"bool":true,"bools":{"bool":true}}`).Err())
		require.EqualValues(t, []interface{}{int64(0)}, rdb.Do(ctx, "JSON.TOGGLE", "a", "$.bool").Val())
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a").Val(), `{"bool":false,"bools":{"bool":true}}`)

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"bool":true,"bools":{"bool":true}}`).Err())
		require.EqualValues(t, []interface{}{int64(0), int64(0)}, rdb.Do(ctx, "JSON.TOGGLE", "a", "$..bool").Val())
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a").Val(), `{"bool":false,"bools":{"bool":false}}`)

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"bool":false,"bools":{"bool":true}}`).Err())
		require.EqualValues(t, []interface{}{int64(1), int64(0)}, rdb.Do(ctx, "JSON.TOGGLE", "a", "$..bool").Val())
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a").Val(), `{"bool":true,"bools":{"bool":false}}`)

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"incorrectbool":99,"bools":{"bool":true},"bool":{"bool":false}}`).Err())
		require.EqualValues(t, []interface{}{nil, int64(1), int64(0)}, rdb.Do(ctx, "JSON.TOGGLE", "a", "$..bool").Val())
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a").Val(), `{"bool":{"bool":true},"bools":{"bool":false},"incorrectbool":99}`)

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `[99,true,99]`).Err())
		require.EqualValues(t, []interface{}{nil, int64(0), nil}, rdb.Do(ctx, "JSON.TOGGLE", "a", "$..*").Val())
		require.Equal(t, rdb.Do(ctx, "JSON.GET", "a").Val(), `[99,false,99]`)
	})

	t.Run("JSON.ARRINDEX basics", func(t *testing.T) {
		arrIndexCmd := "JSON.ARRINDEX"
		require.NoError(t, rdb.Do(ctx, "SET", "a", `1`).Err())
		require.Error(t, rdb.Do(ctx, arrIndexCmd, "a", "$", `1`).Err())
		require.NoError(t, rdb.Do(ctx, "DEL", "a").Err())

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", ` {"x":1, "y": {"x":1} } `).Err())
		require.Equal(t, []interface{}{}, rdb.Do(ctx, arrIndexCmd, "a", "$..k", `1`).Val())
		require.Error(t, rdb.Do(ctx, arrIndexCmd, "a", "$").Err())
		require.Error(t, rdb.Do(ctx, arrIndexCmd, "a", "$", ` 1, 2, 3`).Err())

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"arr":[0,1,2,3,2,1,0]}`).Err())
		require.Equal(t, []interface{}{int64(0)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `0`).Val())
		require.Equal(t, []interface{}{int64(3)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `3`).Val())
		require.Equal(t, []interface{}{int64(-1)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `4`).Val())
		require.Equal(t, []interface{}{int64(6)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `0`, 1).Val())
		require.Equal(t, []interface{}{int64(6)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `0`, -1).Val())
		require.Equal(t, []interface{}{int64(6)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `0`, 6).Val())
		require.Equal(t, []interface{}{int64(6)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `0`, 4, -0).Val())
		require.Equal(t, []interface{}{int64(-1)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `0`, 5, -1).Val())
		require.Equal(t, []interface{}{int64(6)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `0`, 5, 0).Val())
		require.Equal(t, []interface{}{int64(-1)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `2`, -2, 6).Val())
		require.Equal(t, []interface{}{int64(-1)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `"foo"`).Val())

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"arr":[0,1,2,3,4,2,1,0]}`).Err())

		require.Equal(t, []interface{}{int64(3)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `3`).Val())
		require.Equal(t, []interface{}{int64(5)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `2`, 3).Val())
		require.Equal(t, []interface{}{int64(1)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `1`).Val())
		require.Equal(t, []interface{}{int64(2)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `2`, 1, 4).Val())
		require.Equal(t, []interface{}{int64(-1)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `6`).Val())
		require.Equal(t, []interface{}{int64(-1)}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr", `3`, 0, 2).Val())

		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a", "$", `{"arr":[0,1,2]}`).Err())
		require.Equal(t, []interface{}{nil, nil, nil}, rdb.Do(ctx, arrIndexCmd, "a", "$.arr.*", `1`).Val())
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "a1", "$", `{"arr":[[1],[2],[3]]}`).Err())
		require.Equal(t, []interface{}{int64(0), int64(-1), int64(-1)}, rdb.Do(ctx, arrIndexCmd, "a1", "$.arr.*", `1`).Val())
	})
}
