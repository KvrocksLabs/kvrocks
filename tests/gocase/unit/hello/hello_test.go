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

package hello

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestHello(t *testing.T) {
	srv := util.StartServer(t, map[string]string{"resp3-enabled": "no"})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("hello with wrong protocol", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "1")
		require.ErrorContains(t, r.Err(), "NOPROTO unsupported protocol version")
	})

	t.Run("hello with protocol 2", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "2")
		rList := r.Val().([]interface{})
		require.EqualValues(t, "version", rList[2])
		require.EqualValues(t, "4.0.0", rList[3])
		require.EqualValues(t, "proto", rList[4])
		require.EqualValues(t, 2, rList[5])
	})

	t.Run("hello with protocol 3", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "3")
		rList := r.Val().([]interface{})
		require.EqualValues(t, "version", rList[2])
		require.EqualValues(t, "4.0.0", rList[3])
		require.EqualValues(t, "proto", rList[4])
		require.EqualValues(t, 2, rList[5])
	})

	t.Run("hello with wrong protocol", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "5")
		require.ErrorContains(t, r.Err(), "NOPROTO unsupported protocol version")
	})

	t.Run("hello with non protocol", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "AUTH")
		require.ErrorContains(t, r.Err(), "Protocol version is not an integer or out of range")
	})

	t.Run("hello with non protocol", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "2", "SETNAME", "kvrocks")
		rList := r.Val().([]interface{})
		require.EqualValues(t, "proto", rList[4])
		require.EqualValues(t, 2, rList[5])

		r = rdb.Do(ctx, "CLIENT", "GETNAME")
		require.EqualValues(t, r.Val(), "kvrocks")
	})
}

func TestEnableRESP3(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"resp3-enabled": "yes",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	r, err := rdb.Do(ctx, "HELLO", "2").Result()
	require.NoError(t, err)
	rList := r.([]interface{})
	require.EqualValues(t, "proto", rList[4])
	require.EqualValues(t, 2, rList[5])

	r, err = rdb.Do(ctx, "HELLO", "3").Result()
	require.NoError(t, err)
	rMap := r.(map[interface{}]interface{})
	require.EqualValues(t, rMap["proto"], 3)
}

func TestHelloWithAuth(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"requirepass": "foobar",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("AUTH fails when a wrong password is given", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "3", "AUTH", "wrong!")
		require.ErrorContains(t, r.Err(), "Invalid password")
	})

	t.Run("AUTH fails when a wrong username is given", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "3", "AUTH", "wrong!", "foobar")
		require.ErrorContains(t, r.Err(), "Invalid password")
	})

	t.Run("Arbitrary command gives an error when AUTH is required", func(t *testing.T) {
		r := rdb.Set(ctx, "foo", "bar", 0)
		require.ErrorContains(t, r.Err(), "NOAUTH Authentication required.")
	})

	t.Run("AUTH succeeds when the right password is given", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "3", "AUTH", "foobar")
		t.Log(r)
	})

	t.Run("AUTH succeeds when the right username and password are given", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "3", "AUTH", "default", "foobar")
		t.Log(r)
	})

	t.Run("Once AUTH succeeded we can actually send commands to the server", func(t *testing.T) {
		require.Equal(t, "OK", rdb.Set(ctx, "foo", 100, 0).Val())
		require.EqualValues(t, 101, rdb.Incr(ctx, "foo").Val())
	})

	t.Run("hello with non protocol", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "2", "AUTH", "foobar", "SETNAME", "kvrocks")
		rList := r.Val().([]interface{})
		require.EqualValues(t, "proto", rList[4])
		require.EqualValues(t, 2, rList[5])

		r = rdb.Do(ctx, "CLIENT", "GETNAME")
		require.EqualValues(t, r.Val(), "kvrocks")
	})

	t.Run("hello with non protocol", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "2", "AUTH", "default", "foobar", "SETNAME", "kvrocks")
		rList := r.Val().([]interface{})
		require.EqualValues(t, "proto", rList[4])
		require.EqualValues(t, 2, rList[5])

		r = rdb.Do(ctx, "CLIENT", "GETNAME")
		require.EqualValues(t, r.Val(), "kvrocks")
	})
}

func TestHelloWithAuthByGoRedis(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"requirepass": "foobar",
	})
	defer srv.Close()

	t.Run("hello with auth sent by go-redis", func(t *testing.T) {
		rdb := srv.NewClientWithOption(&redis.Options{
			Password: "foobar",
		})
		defer func() { require.NoError(t, rdb.Close()) }()

		require.Equal(t, "PONG", rdb.Ping(context.Background()).Val())
	})
}
