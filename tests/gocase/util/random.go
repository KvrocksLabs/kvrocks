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

package util

import (
	"fmt"
	"math/rand"
)

func RandPath[T any](funcs ...func() T) T {
	index := rand.Int31n(int32(len(funcs)))
	return funcs[index]()
}

func RandPathNoResult(funcs ...func()) {
	index := rand.Int31n(int32(len(funcs)))
	funcs[index]()
}

func randomSignedInt(max int) int32 {
	i := rand.Int31n(int32(max))
	if rand.Float64() > 0.5 {
		return -i
	}
	return i
}

func RandomValue() string {
	return RandPath(
		// Small enough to likely collide
		func() string {
			return fmt.Sprintf("%d", randomSignedInt(1000))
		},
		// 32 bit compressible signed/unsigned
		func() string {
			return RandPath(
				func() string {
					return fmt.Sprintf("%d", rand.Int63n(2000000000))
				},
				func() string {
					return fmt.Sprintf("%d", rand.Int63n(4000000000))
				},
			)
		},
		// 64 bit
		func() string {
			return fmt.Sprintf("%d", rand.Int63n(1000000000000))
		},
		// Random string
		func() string {
			return RandPath(
				func() string {
					// TODO implement random string
					return ""
				},
			)
		},
	)
}
