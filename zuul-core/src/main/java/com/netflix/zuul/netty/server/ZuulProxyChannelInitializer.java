/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.zuul.netty.server;

import com.netflix.netty.common.channel.config.ChannelConfig;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;

/**
 * Extended ZuulServerChannelInitializer that adds ProxyChannelHandler to the pipeline
 * for direct proxy handling, bypassing the filter system for better performance.
 */
public class ZuulProxyChannelInitializer extends ZuulServerChannelInitializer {

    public ZuulProxyChannelInitializer(
            String metricId, ChannelConfig channelConfig, ChannelConfig channelDependencies, ChannelGroup channels) {
        super(metricId, channelConfig, channelDependencies, channels);
    }

    @Override
    protected void addZuulHandlers(final ChannelPipeline pipeline) {
        // Add the standard Zuul handlers first
        super.addZuulHandlers(pipeline);
        
        // Add the ProxyChannelHandler after the filter chain for direct proxy handling
        // This allows requests to be handled either by filters or directly by the proxy handler
        pipeline.addAfter(
            "ZuulFilterChainHandler", 
            ProxyChannelHandler.CHANNEL_HANDLER_NAME, 
            new ProxyChannelHandler()
        );
    }
}