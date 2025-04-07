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

package org.apache.shenyu.springboot.starter.plugin.mcp.server;

import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.WebFluxSseServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpServerTransportProvider;
import org.apache.shenyu.plugin.api.ShenyuPlugin;
import org.apache.shenyu.plugin.base.handler.PluginDataHandler;
import org.apache.shenyu.plugin.mcp.server.McpServerFilter;
import org.apache.shenyu.plugin.mcp.server.McpServerPlugin;
import org.apache.shenyu.plugin.mcp.server.ShenyuMcpToolsProvider;
import org.apache.shenyu.plugin.mcp.server.handler.McpServerPluginDataHandler;
import org.springframework.ai.mcp.McpToolUtils;
import org.springframework.ai.mcp.server.autoconfigure.McpServerProperties;
import org.springframework.ai.tool.ToolCallbacks;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.web.reactive.DispatcherHandler;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.server.WebFilter;

/**
 * The type Mock plugin configuration.
 */
@Configuration
@ConditionalOnProperty(value = {"shenyu.plugins.mcp.server.enabled"}, havingValue = "true", matchIfMissing = true)
public class McpServerPluginConfiguration {

    /**
     * Mock plugin.
     *
     * @return the shenyu plugin
     */
    @Bean
    public ShenyuPlugin mcpServerPlugin() {
        return new McpServerPlugin();
    }

    /**
     * Mock plugin data handler.
     *
     * @return the plugin data handler
     */
    @Bean
    public PluginDataHandler mcpServerPluginDataHandler() {
        return new McpServerPluginDataHandler();
    }
    
    @Bean
    public RouterFunction<?> mcpRouterFunction(WebFluxSseServerTransportProvider transportProvider) {
        return transportProvider.getRouterFunction();
    }
    
    /**
     * Health filter.
     *
     * @param dispatcherHandler the dispatcher handler
     * @param mcpServerProperties the mcp server properties
     * @return the web filter
     */
    @Bean
    @Order(-99)
    @ConditionalOnProperty(name = "shenyu.mcp.server.enabled", havingValue = "true", matchIfMissing = true)
    public WebFilter mcpFilter(final DispatcherHandler dispatcherHandler, final McpServerProperties mcpServerProperties) {
        return new McpServerFilter(dispatcherHandler, mcpServerProperties.getSseMessageEndpoint());
    }
    
    @Bean
    public McpSyncServer mcpServer(McpServerTransportProvider transportProvider) { // @formatter:off
        ShenyuMcpToolsProvider shenyuMcpToolsProvider = new ShenyuMcpToolsProvider();
        // Configure server capabilities with resource support
        var capabilities = McpSchema.ServerCapabilities.builder()
                .tools(true) // Tool support with list changes notifications
                .logging() // Logging support
                .build();
        
        // Create the server with both tool and resource capabilities
        McpSyncServer server = McpServer.sync(transportProvider)
                .serverInfo("MCP Demo Weather Server", "1.0.0")
                .capabilities(capabilities)
                .tools(McpToolUtils.toSyncToolSpecifications(ToolCallbacks.from(shenyuMcpToolsProvider))) // Add @Tools
                .build();
        
        return server; // @formatter:on
    } // @formatter:on
}
