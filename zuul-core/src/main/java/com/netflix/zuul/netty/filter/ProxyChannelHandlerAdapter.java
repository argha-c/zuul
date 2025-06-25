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

package com.netflix.zuul.netty.filter;

import com.netflix.zuul.context.CommonContextKeys;
import com.netflix.zuul.filters.FilterType;
import com.netflix.zuul.filters.SyncZuulFilterAdapter;
import io.netty.handler.codec.http.HttpContent;
import com.netflix.zuul.message.ZuulMessage;
import com.netflix.zuul.message.http.HttpRequestMessage;
import com.netflix.zuul.message.http.HttpResponseMessage;
import com.netflix.zuul.message.http.HttpResponseMessageImpl;
import com.netflix.zuul.netty.filter.FilterRunner;
import com.netflix.zuul.netty.server.ProxyChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter that bridges between the Zuul filter system and the new ProxyChannelHandler.
 * This allows ProxyChannelHandler to be used within the existing filter framework
 * while providing the benefits of low-level Netty channel handling.
 */
public class ProxyChannelHandlerAdapter extends SyncZuulFilterAdapter<HttpRequestMessage, HttpResponseMessage> {
    
    private static final Logger logger = LoggerFactory.getLogger(ProxyChannelHandlerAdapter.class);
    
    private final HttpRequestMessage zuulRequest;
    private final ChannelHandlerContext channelHandlerContext;
    private final FilterRunner<HttpResponseMessage, HttpResponseMessage> nextStage;
    private ProxyChannelHandler proxyChannelHandler;
    private boolean handlerAdded = false;

    public ProxyChannelHandlerAdapter(
            HttpRequestMessage zuulRequest,
            ChannelHandlerContext channelHandlerContext,
            FilterRunner<HttpResponseMessage, HttpResponseMessage> nextStage) {
        this.zuulRequest = zuulRequest;
        this.channelHandlerContext = channelHandlerContext;
        this.nextStage = nextStage;
    }

    @Override
    public HttpResponseMessage apply(HttpRequestMessage request) {
        try {
            // Create and add the ProxyChannelHandler to the pipeline if not already added
            if (!handlerAdded) {
                proxyChannelHandler = new ProxyChannelHandler();
                ChannelPipeline pipeline = channelHandlerContext.pipeline();
                
                // Add the handler before the current filter chain handler
                pipeline.addBefore(
                    "ZuulFilterChainHandler", 
                    ProxyChannelHandler.CHANNEL_HANDLER_NAME, 
                    proxyChannelHandler
                );
                handlerAdded = true;
            }
            
            // The ProxyChannelHandler will handle the request asynchronously
            // and call invokeNext when the response is ready
            return null; // Return null to indicate async processing
            
        } catch (Exception e) {
            logger.error("Error in ProxyChannelHandlerAdapter", e);
            return HttpResponseMessageImpl.defaultErrorResponse(request);
        }
    }

    @Override
    public HttpContent processContentChunk(ZuulMessage zuulReq, HttpContent chunk) {
        // Let the ProxyChannelHandler handle content chunks directly
        if (proxyChannelHandler != null) {
            try {
                proxyChannelHandler.channelRead(channelHandlerContext, chunk);
                return null; // Indicate that we've handled the chunk
            } catch (Exception e) {
                logger.error("Error processing content chunk in ProxyChannelHandler", e);
                return chunk; // Return the chunk to let the caller handle it
            }
        }
        return chunk;
    }

    @Override
    public String filterName() {
        return "ProxyChannelHandlerAdapter";
    }

    @Override
    public FilterType filterType() {
        return FilterType.ENDPOINT;
    }

    @Override
    public int filterOrder() {
        return 0;
    }

    @Override
    public boolean shouldFilter(HttpRequestMessage msg) {
        return true;
    }

    @Override
    public HttpResponseMessage getDefaultOutput(HttpRequestMessage input) {
        return HttpResponseMessageImpl.defaultErrorResponse(input);
    }

    /**
     * Called when the ProxyChannelHandler has a response ready
     */
    public void invokeNext(HttpResponseMessage response) {
        if (nextStage != null) {
            nextStage.filter(response);
        }
    }

    /**
     * Finish method for cleanup, similar to ProxyEndpoint.finish()
     */
    public void finish(boolean error) {
        if (proxyChannelHandler != null) {
            proxyChannelHandler.finish(error);
        }
    }

    /**
     * Get the underlying ProxyChannelHandler
     */
    public ProxyChannelHandler getProxyChannelHandler() {
        return proxyChannelHandler;
    }
}