# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM alpine:3.16 as build

ARG MORE_BUILD_ARGS

# workaround tzdata install hanging
ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apk update && apk add git gcc g++ make cmake autoconf automake libtool python3 linux-headers curl openssl openssl-dev libexecinfo-dev
WORKDIR /kvrocks

COPY . .
RUN ./x.py build -DENABLE_OPENSSL=ON -DCMAKE_BUILD_TYPE=Release -j $(nproc) $MORE_BUILD_ARGS

RUN curl -O https://download.redis.io/releases/redis-6.2.7.tar.gz && \
    tar -xzvf redis-6.2.7.tar.gz && \
    mkdir tools && \
    cd redis-6.2.7 && \
    make redis-cli && \
    mv src/redis-cli /kvrocks/tools/redis-cli

FROM alpine:3.16

RUN apk upgrade && apk add openssl libexecinfo

WORKDIR /kvrocks

HEALTHCHECK --interval=5s --timeout=1s --start-period=120s --retries=3 CMD echo PING | nc 127.0.0.1 6666 || exit 1

RUN mkdir /var/run/kvrocks && mkdir /var/lib/kvrocks

RUN addgroup -S kvrocks && adduser -D -H -S -G kvrocks kvrocks

RUN chown kvrocks:kvrocks /var/run/kvrocks && chown kvrocks:kvrocks /var/lib/kvrocks

USER kvrocks

COPY --from=build /kvrocks/build/kvrocks ./bin/
COPY --from=build /kvrocks/tools/redis-cli ./bin/

ENV PATH="$PATH:/kvrocks/bin"

VOLUME /var/lib/kvrocks

RUN chown kvrocks:kvrocks /var/lib/kvrocks

COPY ./LICENSE ./NOTICE ./DISCLAIMER ./
COPY ./licenses ./licenses
COPY ./kvrocks.conf /var/lib/kvrocks/

EXPOSE 6666:6666

ENTRYPOINT ["/bin/kvrocks", "-c", "/var/lib/kvrocks/kvrocks.conf", "--dir", "/var/lib/kvrocks", "--pidfile", "/var/run/kvrocks/kvrocks.pid"]
