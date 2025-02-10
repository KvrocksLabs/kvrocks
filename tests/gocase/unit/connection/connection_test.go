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

package connection

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestConnection(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	t.Run("HTTP requests will be dropped", func(t *testing.T) {
		_, err := http.Get(fmt.Sprintf("http://%s", srv.HostPort())) //nolint:bodyclose
		require.Error(t, err)
		require.True(t, srv.LogFileMatches(t, "HTTP request.*Connection aborted"), "should contain HTTP drop log")

		c := srv.NewTCPClient()
		err = c.Write("GET / HTTP/1.1\r\nHOST: example.com\r\n")
		require.NoError(t, err)
		_, err = c.ReadLine()
		require.Error(t, err)
	})
}
