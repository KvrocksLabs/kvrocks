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

package scripting

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestScripting(t *testing.T) {
	srv := util.StartServer(t, map[string]string{"resp3-enabled": "no"})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("EVAL - numkeys can't be negative", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.Do(ctx, "EVAL", `return redis.call('PING');`, "-1").Err(), ".*can't be negative.*")
	})

	t.Run("EVAL - Does Lua interpreter replies to our requests?", func(t *testing.T) {
		r := rdb.Eval(ctx, `return 'hello'`, []string{})
		require.NoError(t, r.Err())
		require.Equal(t, "hello", r.Val())
	})

	t.Run("EVAL - Lua integer -> Redis protocol type conversion", func(t *testing.T) {
		r := rdb.Eval(ctx, `return 100.5`, []string{})
		require.NoError(t, r.Err())
		require.Equal(t, int64(100), r.Val())
	})

	t.Run("EVAL - Lua string -> Redis protocol type conversion", func(t *testing.T) {
		r := rdb.Eval(ctx, `return 'hello world'`, []string{})
		require.NoError(t, r.Err())
		require.Equal(t, "hello world", r.Val())
	})

	t.Run("EVAL - Lua true boolean -> Redis protocol type conversion", func(t *testing.T) {
		r := rdb.Eval(ctx, `return true`, []string{})
		require.NoError(t, r.Err())
		require.Equal(t, int64(1), r.Val())
	})

	t.Run("EVAL - Lua false boolean -> Redis protocol type conversion", func(t *testing.T) {
		r := rdb.Eval(ctx, `return false`, []string{})
		require.EqualError(t, r.Err(), redis.Nil.Error())
		require.Nil(t, r.Val())
	})

	t.Run("EVAL - Lua status code reply -> Redis protocol type conversion", func(t *testing.T) {
		r := rdb.Eval(ctx, `return {ok='fine'}`, []string{})
		require.NoError(t, r.Err())
		require.Equal(t, "fine", r.Val())
	})

	t.Run("EVAL - Lua error reply -> Redis protocol type conversion", func(t *testing.T) {
		r := rdb.Eval(ctx, `return {err='this is an error'}`, []string{})
		require.EqualError(t, r.Err(), "this is an error")
		require.Nil(t, r.Val())
	})

	t.Run("Script return recursive object", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.WriteArgs("EVAL", `return "hello"`, "0"))
		c.MustRead(t, "$5")
		c.MustRead(t, "hello")
	})

	t.Run("EVAL - Lua table -> Redis protocol type conversion", func(t *testing.T) {
		r := rdb.Eval(ctx, `return {1,2,3,'ciao',{1,2}}`, []string{})
		require.NoError(t, r.Err())
		require.Equal(t, "[1 2 3 ciao [1 2]]", fmt.Sprintf("%v", r.Val()))
	})

	t.Run("EVAL - Are the KEYS and ARGV arrays populated correctly?", func(t *testing.T) {
		r := rdb.Eval(ctx, `return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}`, []string{"a", "b"}, "c", "d")
		require.NoError(t, r.Err())
		require.Equal(t, "[a b c d]", fmt.Sprintf("%v", r.Val()))
	})

	t.Run("EVAL - is Lua able to call Redis API?", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "myval", 0).Err())
		r := rdb.Eval(ctx, `return redis.call('get',KEYS[1])`, []string{"mykey"})
		require.NoError(t, r.Err())
		require.Equal(t, "myval", r.Val())
	})

	t.Run("EVALSHA - Can we call a SHA1 if already defined?", func(t *testing.T) {
		r := rdb.EvalSha(ctx, "fd758d1589d044dd850a6f05d52f2eefd27f033f", []string{"mykey"})
		require.NoError(t, r.Err())
		require.Equal(t, "myval", r.Val())
	})

	t.Run("EVALSHA - Can we call a SHA1 in uppercase?", func(t *testing.T) {
		r := rdb.EvalSha(ctx, "FD758D1589D044DD850A6F05D52F2EEFD27F033F", []string{"mykey"})
		require.NoError(t, r.Err())
		require.Equal(t, "myval", r.Val())
	})

	t.Run("EVALSHA - Do we get an error on invalid SHA1?", func(t *testing.T) {
		r := rdb.EvalSha(ctx, "NotValidShaSUM", []string{})
		util.ErrorRegexp(t, r.Err(), "NOSCRIPT.*")
		require.Nil(t, r.Val())
	})

	t.Run("EVALSHA - Do we get an error on non defined SHA1?", func(t *testing.T) {
		r := rdb.EvalSha(ctx, "ffd632c7d33e571e9f24556ebed26c3479a87130", []string{})
		util.ErrorRegexp(t, r.Err(), "NOSCRIPT.*")
		require.Nil(t, r.Val())
	})

	t.Run("EVAL - Redis integer -> Lua type conversion", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", 0, 0).Err())
		r := rdb.Eval(ctx, `
local foo = redis.pcall('incr',KEYS[1])
return {type(foo),foo}
`, []string{"x"})
		require.NoError(t, r.Err())
		require.Equal(t, "[number 1]", fmt.Sprintf("%v", r.Val()))
	})

	t.Run("EVAL - Redis bulk -> Lua type conversion", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "myval", 0).Err())
		r := rdb.Eval(ctx, `
local foo = redis.pcall('get',KEYS[1])
return {type(foo),foo}
`, []string{"mykey"})
		require.NoError(t, r.Err())
		require.Equal(t, "[string myval]", fmt.Sprintf("%v", r.Val()))
	})

	t.Run("EVAL - Redis multi bulk -> Lua type conversion", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mylist").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", "a", "b", "c").Err())
		r := rdb.Eval(ctx, `
local foo = redis.pcall('lrange',KEYS[1],0,-1)
return {type(foo),foo[1],foo[2],foo[3],# foo}
`, []string{"mylist"})
		require.NoError(t, r.Err())
		require.Equal(t, "[table a b c 3]", fmt.Sprintf("%v", r.Val()))
	})

	t.Run("EVAL - Redis status reply -> Lua type conversion", func(t *testing.T) {
		r := rdb.Eval(ctx, `
local foo = redis.pcall('set',KEYS[1],'myval')
return {type(foo),foo['ok']}
`, []string{"mykey"})
		require.NoError(t, r.Err())
		require.Equal(t, "[table OK]", fmt.Sprintf("%v", r.Val()))
	})

	t.Run("EVAL - Redis error reply -> Lua type conversion", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "myval", 0).Err())
		r := rdb.Eval(ctx, `
local foo = redis.pcall('incr',KEYS[1])
return {type(foo),foo['err']}
`, []string{"mykey"})
		require.NoError(t, r.Err())
		require.Equal(t, "[table Invalid argument: value is not an integer or out of range]", fmt.Sprintf("%v", r.Val()))
	})

	t.Run("EVAL - Redis nil bulk reply -> Lua type conversion", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		r := rdb.Eval(ctx, `
local foo = redis.pcall('get',KEYS[1])
return {type(foo),foo == false}
`, []string{"mykey"})
		require.NoError(t, r.Err())
		require.Equal(t, "[boolean 1]", fmt.Sprintf("%v", r.Val()))
	})

	t.Run("EVAL - Scripts can't run certain commands", func(t *testing.T) {
		r := rdb.Eval(ctx, `return redis.pcall('shutdown')`, []string{})
		require.ErrorContains(t, r.Err(), "not allowed")
	})

	t.Run("EVAL - Scripts can run blocking commands and get immediate result", func(t *testing.T) {
		r := rdb.Eval(ctx, `return redis.pcall('blpop', KEYS[1], 0)`, []string{"key_for_blpop_script"})
		require.Equal(t, r.Val(), nil)
		require.ErrorContains(t, r.Err(), "nil")
	})

	t.Run("EVAL - Scripts can run certain commands", func(t *testing.T) {
		r := rdb.Eval(ctx, `redis.pcall('randomkey'); return redis.pcall('set','x','ciao')`, []string{})
		require.NoError(t, r.Err())
		require.Equal(t, "OK", r.Val())
	})

	t.Run("EVAL - No arguments to redis.call/pcall is considered an error", func(t *testing.T) {
		r := rdb.Eval(ctx, `return redis.call()`, []string{})
		require.ErrorContains(t, r.Err(), "one argument")
	})

	t.Run("EVAL - redis.call variant raises a Lua error on Redis cmd error", func(t *testing.T) {
		r := rdb.Eval(ctx, `redis.call('nosuchcommand')`, []string{})
		require.ErrorContains(t, r.Err(), "Unknown Redis")
		r = rdb.Eval(ctx, `redis.call('get','a','b','c')`, []string{})
		require.ErrorContains(t, r.Err(), "number of args")
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		r = rdb.Eval(ctx, `redis.call('lpush',KEYS[1],'val')`, []string{"foo"})
		require.ErrorContains(t, r.Err(), "against a key")
	})

	t.Run("EVAL - JSON numeric decoding", func(t *testing.T) {
		r := rdb.Eval(ctx, `
return
  table.concat(
    cjson.decode(
      "[0.0, -5e3, -1, 0.3e-3, 1023.2, 0e10]"), " ")
`, []string{})
		require.NoError(t, r.Err())
		require.Equal(t, "0 -5000 -1 0.0003 1023.2 0", fmt.Sprintf("%v", r.Val()))
	})

	t.Run("EVAL - JSON string decoding", func(t *testing.T) {
		r := rdb.Eval(ctx, `
local decoded = cjson.decode('{"keya": "a", "keyb": "b"}')
return {decoded.keya, decoded.keyb}
`, []string{})
		require.NoError(t, r.Err())
		require.Equal(t, "[a b]", fmt.Sprintf("%v", r.Val()))
	})

	t.Run("EVAL - cmsgpack can pack double?", func(t *testing.T) {
		r := rdb.Eval(ctx, `
local encoded = cmsgpack.pack(0.1)
local h = ""
for i = 1, #encoded do
	h = h .. string.format("%02x",string.byte(encoded,i))
end
return h
`, []string{})
		require.NoError(t, r.Err())
		require.Equal(t, "cb3fb999999999999a", r.Val())
	})

	t.Run("EVAL - cmsgpack can pack negative int64?", func(t *testing.T) {
		r := rdb.Eval(ctx, `
local encoded = cmsgpack.pack(-1099511627776)
local h = ""
for i = 1, #encoded do
	h = h .. string.format("%02x",string.byte(encoded,i))
end
return h
`, []string{})
		require.NoError(t, r.Err())
		require.Equal(t, "d3ffffff0000000000", r.Val())
	})

	t.Run("EVAL - cmsgpack can pack and unpack circular references?", func(t *testing.T) {
		r := rdb.Eval(ctx, `
local a = {x=nil,y=5}
local b = {x=a}
a['x'] = b
local encoded = cmsgpack.pack(a)
local h = ""
-- cmsgpack encodes to a depth of 16, but can't encode
-- references, so the encoded object has a deep copy recursive
-- depth of 16.
for i = 1, #encoded do
	h = h .. string.format("%02x",string.byte(encoded,i))
end
-- when unpacked, re.x.x != re because the unpack creates
-- individual tables down to a depth of 16.
-- (that's why the encoded output is so large)
local re = cmsgpack.unpack(encoded)
assert(re)
assert(re.x)
assert(re.x.x.y == re.y)
assert(re.x.x.x.x.y == re.y)
assert(re.x.x.x.x.x.x.y == re.y)
assert(re.x.x.x.x.x.x.x.x.x.x.y == re.y)
-- maximum working depth:
assert(re.x.x.x.x.x.x.x.x.x.x.x.x.x.x.y == re.y)
-- now the last x would be b above and has no y
assert(re.x.x.x.x.x.x.x.x.x.x.x.x.x.x.x)
-- so, the final x.x is at the depth limit and was assigned nil
assert(re.x.x.x.x.x.x.x.x.x.x.x.x.x.x.x.x == nil)
assert(h == "82a17881a17882a17881a17882a17881a17882a17881a17882a17881a17882a17881a17882a17881a17882a17881a178c0a17905a17905a17905a17905a17905a17905a17905a17905" or h == "82a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a178c0")
return {re.x.x.x.x.x.x.x.x.y == re.y, re.y == 5}
`, []string{})
		require.NoError(t, r.Err())
		require.Equal(t, "[1 1]", fmt.Sprintf("%v", r.Val()))
	})

	t.Run("EVAL - Numerical sanity check from bitop", func(t *testing.T) {
		r := rdb.Eval(ctx, `
assert(0x7fffffff == 2147483647, "broken hex literals");
assert(0xffffffff == -1 or 0xffffffff == 2^32-1,
	"broken hex literals");
assert(tostring(-1) == "-1", "broken tostring()");
assert(tostring(0xffffffff) == "-1" or
	tostring(0xffffffff) == "4294967295",
	"broken tostring()")
`, []string{})
		require.EqualError(t, r.Err(), redis.Nil.Error())
		require.Nil(t, r.Val())
	})

	t.Run("EVAL - Verify minimal bitop functionality", func(t *testing.T) {
		r := rdb.Eval(ctx, `
assert(bit.tobit(1) == 1);
assert(bit.band(1) == 1);
assert(bit.bxor(1,2) == 3);
assert(bit.bor(1,2,4,8,16,32,64,128) == 255)
`, []string{})
		require.EqualError(t, r.Err(), redis.Nil.Error())
		require.Nil(t, r.Val())
	})

	t.Run("EVAL - Able to parse trailing comments", func(t *testing.T) {
		r := rdb.Eval(ctx, `return 'hello' --trailing comment`, []string{})
		require.NoError(t, r.Err())
		require.Equal(t, "hello", r.Val())
	})

	t.Run("EVAL does not leak in the Lua stack", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", 0, 0).Err())

		// use a non-blocking client to speed up the loop.
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()

		for i := 0; i < 10000; i++ {
			require.NoError(t, c.WriteArgs("EVAL", `return redis.call("incr",KEYS[1])`, "1", "x"))
		}
		for i := 0; i < 10000; i++ {
			_, err := c.ReadLine()
			require.NoError(t, err)
		}

		require.EqualValues(t, "10000", rdb.Get(ctx, "x").Val())
	})

	t.Run("SCRIPTING FLUSH - is able to clear the scripts cache?", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "myval", 0).Err())
		r := rdb.EvalSha(ctx, "fd758d1589d044dd850a6f05d52f2eefd27f033f", []string{"mykey"})
		require.NoError(t, r.Err())
		require.Equal(t, "myval", r.Val())
		require.NoError(t, rdb.ScriptFlush(ctx).Err())
		r = rdb.EvalSha(ctx, "fd758d1589d044dd850a6f05d52f2eefd27f033f", []string{"mykey"})
		util.ErrorRegexp(t, r.Err(), "NOSCRIPT.*")
	})

	t.Run("SCRIPT EXISTS - can detect already defined scripts?", func(t *testing.T) {
		r1 := rdb.Eval(ctx, "return 1+1", []string{})
		require.NoError(t, r1.Err())
		require.Equal(t, int64(2), r1.Val())
		r2 := rdb.ScriptLoad(ctx, "return 1+2")
		require.NoError(t, r2.Err())
		r3 := rdb.ScriptExists(ctx, "a27e7e8a43702b7046d4f6a7ccf5b60cef6b9bd9", "a27e7e8a43702b7046d4f6a7ccf5b60cef6b9bda", r2.Val())
		require.NoError(t, r3.Err())
		require.Equal(t, []bool{false, false, true}, r3.Val())
	})

	t.Run("SCRIPT LOAD - should return SHA as the bulk string", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.WriteArgs("script", "load", "return 'bulk string'"))
		c.MustRead(t, "$40")
		c.MustRead(t, "04b85c6fe6dbd424de3fb5914509afa7597570f2")
	})

	t.Run("SCRIPT LOAD - is able to register scripts in the scripting cache", func(t *testing.T) {
		r1 := rdb.ScriptLoad(ctx, "return 'loaded'")
		require.NoError(t, r1.Err())
		require.Equal(t, "b534286061d4b9e4026607613b95c06c06015ae8", r1.Val())
		r2 := rdb.EvalSha(ctx, "b534286061d4b9e4026607613b95c06c06015ae8", []string{})
		require.NoError(t, r2.Err())
		require.Equal(t, "loaded", r2.Val())
	})

	t.Run("Globals protection reading an undeclared global variable", func(t *testing.T) {
		r2 := rdb.Eval(ctx, `return a`, []string{})
		util.ErrorRegexp(t, r2.Err(), ".*ERR.*attempted to access .* global.*")
	})

	t.Run("Globals protection setting an undeclared global variable", func(t *testing.T) {
		r2 := rdb.Eval(ctx, `a=10`, []string{})
		util.ErrorRegexp(t, r2.Err(), ".*ERR.*attempted to create global.*")
	})

	t.Run("Test an example script DECR_IF_GT", func(t *testing.T) {
		scriptDecrIfGt := `
local current

current = redis.call('get',KEYS[1])
if not current then return nil end
if current > ARGV[1] then
	return redis.call('decr',KEYS[1])
else
	return redis.call('get',KEYS[1])
end
`
		require.NoError(t, rdb.Set(ctx, "foo", 5, 0).Err())
		r := rdb.Eval(ctx, scriptDecrIfGt, []string{"foo"}, 2)
		require.NoError(t, r.Err())
		require.Equal(t, int64(4), r.Val())
		r = rdb.Eval(ctx, scriptDecrIfGt, []string{"foo"}, 2)
		require.NoError(t, r.Err())
		require.Equal(t, int64(3), r.Val())
		r = rdb.Eval(ctx, scriptDecrIfGt, []string{"foo"}, 2)
		require.NoError(t, r.Err())
		require.Equal(t, int64(2), r.Val())
		r = rdb.Eval(ctx, scriptDecrIfGt, []string{"foo"}, 2)
		require.NoError(t, r.Err())
		require.Equal(t, "2", r.Val())
		r = rdb.Eval(ctx, scriptDecrIfGt, []string{"foo"}, 2)
		require.NoError(t, r.Err())
		require.Equal(t, "2", r.Val())
	})

	t.Run("Scripting engine PRNG can be seeded correctly", func(t *testing.T) {
		rand1 := rdb.Eval(ctx, `
math.randomseed(ARGV[1]); return tostring(math.random())
`, []string{}, 10).Val()
		rand2 := rdb.Eval(ctx, `
math.randomseed(ARGV[1]); return tostring(math.random())
`, []string{}, 10).Val()
		rand3 := rdb.Eval(ctx, `
math.randomseed(ARGV[1]); return tostring(math.random())
`, []string{}, 20).Val()
		require.Equal(t, rand1, rand2)
		require.NotEqual(t, rand2, rand3)
	})

	t.Run("In the context of Lua the output of random commands gets ordered", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "myset").Err())
		require.NoError(t, rdb.SAdd(ctx, "myset", "a", "b", "c", "d", "e", "f", "g", "h", "i", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "z", "aa", "aaa", "azz").Err())
		r := rdb.Eval(ctx, `return redis.call('smembers',KEYS[1])`, []string{"myset"})
		require.NoError(t, r.Err())
		require.Equal(t, "[a aa aaa azz b c d e f g h i l m n o p q r s t u v z]", fmt.Sprintf("%v", r.Val()))
	})

	t.Run("Make sure redis.log() works", func(t *testing.T) {
		require.EqualError(t, rdb.Eval(ctx, `return redis.log(redis.LOG_DEBUG, 'debug level');`, []string{}).Err(), redis.Nil.Error())
		require.EqualError(t, rdb.Eval(ctx, `return redis.log(redis.LOG_VERBOSE, 'debug level');`, []string{}).Err(), redis.Nil.Error())
		require.EqualError(t, rdb.Eval(ctx, `return redis.log(redis.LOG_NOTICE, 'debug level');`, []string{}).Err(), redis.Nil.Error())
		require.EqualError(t, rdb.Eval(ctx, `return redis.log(redis.LOG_WARNING, 'debug level');`, []string{}).Err(), redis.Nil.Error())
	})

	t.Run("EVAL_RO - successful case", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		r := rdb.Do(ctx, "EVAL_RO", `return redis.call('get', KEYS[1]);`, "1", "foo")
		require.NoError(t, r.Err())
		require.Equal(t, "bar", r.Val())
	})

	t.Run("EVALSHA_RO - successful case", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		r := rdb.Do(ctx, "EVALSHA_RO", "796941151549c416aa77522fb347487236c05e46", "1", "foo")
		require.NoError(t, r.Err())
		require.Equal(t, "bar", r.Val())
	})

	t.Run("EVAL_RO - cannot run write commands", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		r := rdb.Do(ctx, "EVAL_RO", `redis.call('del', KEYS[1]);`, "1", "foo")
		util.ErrorRegexp(t, r.Err(), "ERR .* Write commands are not allowed from read-only scripts")
	})

	t.Run("EVALSHA_RO - cannot run write commands", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		// sha1 of `redis.call('del', KEYS[1]);`
		r := rdb.Do(ctx, "EVALSHA_RO", "a1e63e1cd1bd1d5413851949332cfb9da4ee6dc0", "1", "foo")
		util.ErrorRegexp(t, r.Err(), "ERR .* Write commands are not allowed from read-only scripts")
	})

	t.Run("EVAL - cannot use redis.setresp(3) if RESP3 is disabled", func(t *testing.T) {
		r := rdb.Eval(ctx, `redis.setresp(3);`, []string{})
		util.ErrorRegexp(t, r.Err(), ".*ERR.*You need set resp3-enabled to yes to enable RESP3.*")
	})
}

func TestScriptingMasterSlave(t *testing.T) {
	master := util.StartServer(t, map[string]string{})
	defer master.Close()
	masterClient := master.NewClient()
	defer func() { require.NoError(t, masterClient.Close()) }()

	slave := util.StartServer(t, map[string]string{})
	defer slave.Close()
	slaveClient := slave.NewClient()
	defer func() { require.NoError(t, slaveClient.Close()) }()

	ctx := context.Background()

	util.SlaveOf(t, slaveClient, master)
	util.WaitForSync(t, slaveClient)

	t.Run("SCRIPTING: script load on master, read on slave", func(t *testing.T) {
		sha := masterClient.ScriptLoad(ctx, `return 'script loaded'`).Val()
		require.Equal(t, "4167ea82ed9c381c7659f7cf93f394219147e8c4", sha)
		util.WaitForOffsetSync(t, masterClient, slaveClient, 5*time.Second)
		require.Equal(t, []bool{true}, masterClient.ScriptExists(ctx, sha).Val())
		require.Equal(t, []bool{true}, slaveClient.ScriptExists(ctx, sha).Val())

		require.NoError(t, masterClient.ScriptFlush(ctx).Err())
		util.WaitForOffsetSync(t, masterClient, slaveClient, 5*time.Second)
		require.Equal(t, []bool{false}, masterClient.ScriptExists(ctx, sha).Val())
		require.Equal(t, []bool{false}, slaveClient.ScriptExists(ctx, sha).Val())
	})
}

func TestScriptingWithRESP3(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"resp3-enabled": "yes",
	})
	defer srv.Close()

	rdb := srv.NewClient()
	defer func() {
		require.NoError(t, rdb.Close())
	}()

	ctx := context.Background()
	t.Run("EVAL - Redis protocol type map conversion", func(t *testing.T) {
		rdb.HSet(ctx, "myhash", "f1", "v1")
		rdb.HSet(ctx, "myhash", "f2", "v2")
		val, err := rdb.Eval(ctx, `redis.setresp(3); return redis.call('hgetall', KEYS[1])`, []string{"myhash"}).Result()
		require.NoError(t, err)
		require.Equal(t, map[interface{}]interface{}{"f1": "v1", "f2": "v2"}, val)
	})

	t.Run("EVAL - Redis protocol type set conversion", func(t *testing.T) {
		require.NoError(t, rdb.SAdd(ctx, "myset", "m0", "m1", "m2").Err())
		val, err := rdb.Eval(ctx, `redis.setresp(3); return redis.call('smembers', KEYS[1])`, []string{"myset"}).StringSlice()
		require.NoError(t, err)
		slices.Sort(val)
		require.EqualValues(t, []string{"m0", "m1", "m2"}, val)
	})

	t.Run("EVAL - Redis protocol type double conversion", func(t *testing.T) {
		require.NoError(t, rdb.ZAdd(ctx, "mydouble", redis.Z{Member: "z0", Score: 1.5}).Err())
		val, err := rdb.Eval(ctx, `redis.setresp(3); return redis.call('zscore', KEYS[1], KEYS[2])`, []string{"mydouble", "z0"}).Result()
		require.NoError(t, err)
		require.EqualValues(t, 1.5, val)
	})

	t.Run("EVAL - Redis protocol type bignumber conversion", func(t *testing.T) {
		val, err := rdb.Eval(ctx, `redis.setresp(3); return redis.call('debug', 'protocol', 'bignum')`, []string{}).Result()
		require.NoError(t, err)

		bignum, _ := big.NewInt(0).SetString("1234567999999999999999999999999999999", 10)
		require.EqualValues(t, bignum, val)
	})

	t.Run("EVAL - Redis protocol type boolean conversion", func(t *testing.T) {
		val, err := rdb.Eval(ctx, `redis.setresp(3); return redis.call('debug', 'protocol', 'true')`, []string{}).Result()
		require.NoError(t, err)
		require.EqualValues(t, true, val)

		val, err = rdb.Eval(ctx, `redis.setresp(3); return redis.call('debug', 'protocol', 'false')`, []string{}).Result()
		require.NoError(t, err)
		require.EqualValues(t, false, val)
	})

	t.Run("EVAL - Redis protocol type verbatim conversion", func(t *testing.T) {
		val, err := rdb.Eval(ctx, `return redis.call('debug', 'protocol', 'verbatim')`, []string{}).Result()
		require.NoError(t, err)

		require.EqualValues(t, "verbatim string", val)
	})

	t.Run("EVAL - lua redis.setresp function", func(t *testing.T) {
		err := rdb.Eval(ctx, `return redis.setresp(2, 3);`, []string{}).Err()
		util.ErrorRegexp(t, err, ".*ERR.*requires one argument.*")

		err = rdb.Eval(ctx, `return redis.setresp(4);`, []string{}).Err()
		util.ErrorRegexp(t, err, ".*ERR.*RESP version must be 2 or 3.*")

		// set to RESP3
		err = rdb.Eval(ctx, `return redis.setresp(3);`, []string{}).Err()
		require.ErrorIs(t, err, redis.Nil)

		rdb.HSet(ctx, "hash0", "f1", "v1")
		vals, err := rdb.Eval(ctx, `redis.setresp(3); return redis.call('hgetall', KEYS[1])`, []string{"hash0"}).Result()
		require.NoError(t, err)
		// return as a map in RESP3
		require.EqualValues(t, map[interface{}]interface{}{"f1": "v1"}, vals)

		// set to RESP2
		err = rdb.Eval(ctx, `return redis.setresp(2);`, []string{}).Err()
		require.ErrorIs(t, err, redis.Nil)

		vals, err = rdb.Eval(ctx, `return redis.call('hgetall', KEYS[1])`, []string{"hash0"}).Result()
		require.NoError(t, err)
		// return as an array in RESP2
		require.EqualValues(t, []interface{}{"f1", "v1"}, vals)
	})
}

func TestEvalScriptFlags(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Eval extract-flags-error", func(t *testing.T) {
		r := rdb.Do(ctx, "EVAL",
			`#!lua name=mylib
		return 'extract-flags'`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")

		r = rdb.Do(ctx, "EVAL",
			`#!lua flags=no-writes name=mylib
		return 'extract-flags'`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")

		r = rdb.Do(ctx, "EVAL",
			`#!lua erroroption=no-writes
		return 'extract-flags'`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")

		r = rdb.Do(ctx, "EVAL",
			`#!lua flags=invalid-flag
		return 'extract-flags'`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR Unknown flag given:*")

		r = rdb.Do(ctx, "EVAL",
			`#!lua flags=no-writes,invalid-flag
		return 'extract-flags'`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR Unknown flag given:*")

		r = rdb.Do(ctx, "EVAL",
			`#!lua flags=no-writes no-cluster
		return 'extract-flags'`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")

		r = rdb.Do(ctx, "EVAL",
			`#!lua flags=no-writes flags=no-cluster
		return 'extract-flags'`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR Redundant flags in script shebang")

		r = rdb.Do(ctx, "EVAL",
			`#!errorengine flags=no-writes
		return 'extract-flags'`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR Unexpected engine in script shebang:*")

		r = rdb.Do(ctx, "EVAL",
			`#!luaflags=no-writes
		return 'extract-flags'`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR Unexpected engine in script shebang:*")

		r = rdb.Do(ctx, "EVAL",
			`#!lua xxflags=no-writes
		return 'extract-flags'`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")
	})

	t.Run("SCRIPT LOAD extract-flags-error", func(t *testing.T) {
		r := rdb.Do(ctx, "SCRIPT", "LOAD",
			`#!lua name=mylib
		return 'extract-flags'`)
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")

		r = rdb.Do(ctx, "SCRIPT", "LOAD",
			`#!lua flags=no-writes name=mylib
		return 'extract-flags'`)
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")

		r = rdb.Do(ctx, "SCRIPT", "LOAD",
			`#!lua erroroption=no-writes
		return 'extract-flags'`)
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")

		r = rdb.Do(ctx, "SCRIPT", "LOAD",
			`#!lua flags=invalid-flag
		return 'extract-flags'`)
		util.ErrorRegexp(t, r.Err(), "ERR Unknown flag given::*")

		r = rdb.Do(ctx, "SCRIPT", "LOAD",
			`#!lua flags=no-writes,invalid-flag
		return 'extract-flags'`)
		util.ErrorRegexp(t, r.Err(), "ERR Unknown flag given::*")

		r = rdb.Do(ctx, "SCRIPT", "LOAD",
			`#!lua flags=no-writes no-cluster
		return 'extract-flags'`)
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")

		r = rdb.Do(ctx, "SCRIPT", "LOAD",
			`#!lua flags=no-writes flags=no-cluster
		return 'extract-flags'`)
		util.ErrorRegexp(t, r.Err(), "ERR Redundant flags in script shebang")

		r = rdb.Do(ctx, "SCRIPT", "LOAD",
			`#!errorengine flags=no-writes
		return 'extract-flags'`)
		util.ErrorRegexp(t, r.Err(), "ERR Unexpected engine in script shebang:*")

		r = rdb.Do(ctx, "SCRIPT", "LOAD",
			`#!luaflags=no-writes
		return 'extract-flags'`)
		util.ErrorRegexp(t, r.Err(), "ERR Unexpected engine in script shebang:*")

		r = rdb.Do(ctx, "SCRIPT", "LOAD",
			`#!lua xxflags=no-writes
		return 'extract-flags'`)
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")
	})

	t.Run("no-writes", func(t *testing.T) {
		r := rdb.Do(ctx, "EVAL",
			`#!lua flags=no-writes
		return redis.call('set', 'k1','v1');`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR .* Write commands are not allowed from read-only scripts")

		r = rdb.Do(ctx, "EVAL", `return redis.call('set', 'k2','v2');`, "0")
		require.NoError(t, r.Err())

		r = rdb.Do(ctx, "EVAL",
			`#!lua
		return redis.call('set', 'k3','v3');`, "0")
		require.NoError(t, r.Err())

		r = rdb.Do(ctx, "EVAL_RO",
			`return redis.call('set', 'k4','v4');`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR .* Write commands are not allowed from read-only scripts")

		r = rdb.Do(ctx, "EVAL_RO",
			`#!lua
		return redis.call('set', 'k5','v5');`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR .* Write commands are not allowed from read-only scripts")

		r = rdb.Do(ctx, "EVAL_RO",
			`#!lua flags=no-writes
		return redis.call('set', 'k6','v6');`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR .* Write commands are not allowed from read-only scripts")
	})

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

	t.Run("no-cluster", func(t *testing.T) {
		r := rdb0.Do(ctx, "EVAL",
			`#!lua flags=no-cluster
		return redis.call('set', 'k','v');`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR .* Can not run script on cluster, 'no-cluster' flag is set")

		// Only valid in cluster mode
		r = rdb.Do(ctx, "EVAL",
			`#!lua flags=no-cluster
		return redis.call('set', 'k','v');`, "0")
		require.NoError(t, r.Err())

		// Scripts without #! can run commands that access keys belonging to different cluster hash slots,
		// but ones with #! inherit the default flags, so they cannot.
		r = rdb0.Do(ctx, "EVAL", `return redis.call('set', 'k','v');`, "0")
		require.NoError(t, r.Err())

		r = rdb0.Do(ctx, "EVAL",
			`#!lua
		return redis.call('set', 'k','v');`, "0")
		require.NoError(t, r.Err())
	})

	t.Run("allow-cross-slot-keys", func(t *testing.T) {
		// Node0: bar-slot = 5061, test-slot = 6918
		// Node1: foo-slot = 12182
		// Different slots of different nodes are not affected by allow-cross-slot-keys,
		// and different slots of the same node can be allowed
		r := rdb0.Do(ctx, "EVAL",
			`#!lua flags=allow-cross-slot-keys
		redis.call('set', 'bar','value_bar');
		return redis.call('set', 'test', 'value_test');`, "0")
		require.NoError(t, r.Err())

		r = rdb0.Do(ctx, "EVAL",
			`#!lua flags=allow-cross-slot-keys
		redis.call('set', 'foo','value_foo');
		return redis.call('set', 'bar', 'value_bar');`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR .* Script attempted to access a non local key in a cluster node script")

		// There is a shebang prefix #!lua but crossslot is not allowed when flags are not set
		r = rdb0.Do(ctx, "EVAL",
			`#!lua
		redis.call('get', 'bar');
		return redis.call('get', 'test');`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR .* Script attempted to access keys that do not hash to the same slot")

		r = rdb0.Do(ctx, "EVAL",
			`#!lua
		redis.call('get', 'foo');
		return redis.call('get', 'bar');`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR .* Script attempted to access a non local key in a cluster node script")

		// Old style: CrossSlot is allowed when there is neither #!lua nor flags set
		r = rdb0.Do(ctx, "EVAL",
			`redis.call('get', 'bar');
		return redis.call('get', 'test');`, "0")
		require.NoError(t, r.Err())

		r = rdb0.Do(ctx, "EVAL",
			`redis.call('get', 'foo');
		return redis.call('get', 'bar');`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR .* Script attempted to access a non local key in a cluster node script")

		// Pre-declared keys are not affected by allow-cross-slot-keys
		r = rdb0.Do(ctx, "EVAL",
			`#!lua flags=allow-cross-slot-keys
		local key = redis.call('get', KEY[1]);
		return redis.call('get', KEY[2]);`, "2", "bar", "test")
		require.EqualError(t, r.Err(), "CROSSSLOT Attempted to access keys that don't hash to the same slot")
	})

	t.Run("mixed use", func(t *testing.T) {
		r := rdb0.Do(ctx, "EVAL",
			`#!lua flags=no-writes,no-cluster
		return redis.call('get', 'key_a');`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR .* Can not run script on cluster, 'no-cluster' flag is set")

		r = rdb.Do(ctx, "EVAL",
			`#!lua flags=no-writes,no-cluster
		return redis.call('set', 'key_a', 'value_a');`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR .* Write commands are not allowed from read-only scripts")

		err := rdb.Set(ctx, "key_a", "value_a", 0).Err()
		require.NoError(t, err)
		r = rdb.Do(ctx, "EVAL",
			`#!lua flags=no-writes,no-cluster
		return redis.call('get', 'key_a');`, "0")
		require.NoError(t, r.Err())

		r = rdb0.Do(ctx, "EVAL",
			`#!lua flags=no-writes,allow-cross-slot-keys
		redis.call('get', 'bar');
		return redis.call('get', 'test');`, "0")
		require.NoError(t, r.Err())

		r = rdb0.Do(ctx, "EVAL",
			`#!lua flags=no-writes,allow-cross-slot-keys
		redis.call('set', 'bar', 'value');
		return redis.call('set', 'test', 'value');`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR .* Write commands are not allowed from read-only scripts")

		r = rdb0.Do(ctx, "EVAL",
			`#!lua flags=no-writes,allow-cross-slot-keys
		redis.call('get', 'bar');
		return redis.call('get', 'foo');`, "0")
		util.ErrorRegexp(t, r.Err(), "ERR .* Script attempted to access a non local key in a cluster node script")

	})
}
