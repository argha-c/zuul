/*
 * Copyright 2020 Netflix, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

package com.netflix.netty.common.throttle;

import static com.netflix.netty.common.throttle.MaxInboundConnectionsHandler.CONNECTION_THROTTLED_EVENT;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.zuul.netty.server.http2.DummyChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MaxInboundConnectionsHandlerTest {

    @Mock
    private Registry registry;
    @Mock
    private Counter counter;

    private final String listener = "test-conn-throttled";

    @Before
    public void setup() {
        when(registry.counter("server.connections.throttled", "id", listener)).thenReturn(counter);
    }

    @Test
    public void incrementCounterOnConnectionThrottledEvent() {
        final EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(new DummyChannelHandler());
        channel.pipeline().addLast(new MaxInboundConnectionsHandler(registry, listener, 100));

        channel.pipeline().context(DummyChannelHandler.class).fireUserEventTriggered(CONNECTION_THROTTLED_EVENT);

        verify(counter, times(1)).increment();
    }
}
