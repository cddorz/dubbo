/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.reactive.handler;

import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.reactive.calls.ReactorServerCalls;
import org.apache.dubbo.rpc.stub.StubMethodHandler;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The handler of OneToMany() method for stub invocation.
 */
public class OneToManyMethodHandler<T, R> implements StubMethodHandler<T, R> {

    private final Function<Mono<T>, Flux<R>> func;

    public OneToManyMethodHandler(Function<Mono<T>, Flux<R>> func) {
        this.func = func;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CompletableFuture<?> invoke(Object[] arguments) {
        T request = (T) arguments[0];
        StreamObserver<R> responseObserver = (StreamObserver<R>) arguments[1];
        return ReactorServerCalls.oneToMany(request, responseObserver, func);
    }
}
