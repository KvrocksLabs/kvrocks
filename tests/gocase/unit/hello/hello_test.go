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

package auth

import (
	"context"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestHello(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("hello with wrong protocol", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "1")
		require.ErrorContains(t, r.Err(), "-NOPROTO unsupported protocol version")
	})

	t.Run("hello with protocol 2", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "2")
		rList := r.Val().([]interface{})
		require.EqualValues(t, rList[2], "proto")
		require.EqualValues(t, rList[3], 2)
	})

	t.Run("hello with protocol 3", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "3")
		rList := r.Val().([]interface{})
		require.EqualValues(t, rList[2], "proto")
		require.EqualValues(t, rList[3], 2)
	})

	t.Run("hello with wrong protocol", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "5")
		require.ErrorContains(t, r.Err(), "-NOPROTO unsupported protocol version")
	})

	t.Run("hello with non protocol", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "AUTH")
		require.ErrorContains(t, r.Err(), "Protocol version is not an integer or out of range")
	})

	t.Run("hello with non protocol", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "2", "SETNAME", "kvrocks")
		rList := r.Val().([]interface{})
		require.EqualValues(t, rList[2], "proto")
		require.EqualValues(t, rList[3], 2)

		r = rdb.Do(ctx, "CLIENT", "GETNAME")
		require.EqualValues(t, r.Val(), "kvrocks")
	})
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
		require.ErrorContains(t, r.Err(), "invalid password")
	})

	t.Run("Arbitrary command gives an error when AUTH is required", func(t *testing.T) {
		r := rdb.Set(ctx, "foo", "bar", 0)
		require.ErrorContains(t, r.Err(), "NOAUTH Authentication required.")
	})

	t.Run("AUTH succeeds when the right password is given", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "3", "AUTH", "foobar")
		t.Log(r)
	})

	t.Run("Once AUTH succeeded we can actually send commands to the server", func(t *testing.T) {
		require.Equal(t, "OK", rdb.Set(ctx, "foo", 100, 0).Val())
		require.EqualValues(t, 101, rdb.Incr(ctx, "foo").Val())
	})

	t.Run("hello with non protocol", func(t *testing.T) {
		r := rdb.Do(ctx, "HELLO", "2", "AUTH", "foobar", "SETNAME", "kvrocks")
		rList := r.Val().([]interface{})
		require.EqualValues(t, rList[2], "proto")
		require.EqualValues(t, rList[3], 2)

		r = rdb.Do(ctx, "CLIENT", "GETNAME")
		require.EqualValues(t, r.Val(), "kvrocks")
	})
}
