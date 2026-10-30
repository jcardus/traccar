/*
 * Copyright 2025 - 2026 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpAsyncServer;
import io.modelcontextprotocol.server.McpAsyncServerExchange;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import reactor.core.publisher.Mono;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.List;

@Singleton
public class McpServerHolder implements AutoCloseable {

    public static final String PATH = "/api/mcp";

    private static final McpSchema.ToolAnnotations READ_ONLY_ANNOTATIONS = new McpSchema.ToolAnnotations(
            null, true, false, true, false, null);

    private final ObjectMapper objectMapper;
    private final McpApiInvoker apiInvoker;

    private final HttpServletStreamableServerTransportProvider transport;
    private final McpAsyncServer server;

    @Inject
    public McpServerHolder(ObjectMapper objectMapper, McpToolRegistry toolRegistry, McpApiInvoker apiInvoker) {

        this.objectMapper = objectMapper;
        this.apiInvoker = apiInvoker;

        transport = HttpServletStreamableServerTransportProvider.builder()
                .mcpEndpoint(PATH)
                .jsonMapper(new JacksonMcpJsonMapper(objectMapper))
                .contextExtractor(this::extractTransportContext)
                .build();

        var capabilities = McpSchema.ServerCapabilities.builder()
                .tools(true)
                .resources(false, true)
                .prompts(true)
                .build();

        server = McpServer.async(transport)
                .serverInfo("traccar-mcp", "1.0.0")
                .instructions(McpResultLink.INSTRUCTIONS)
                .capabilities(capabilities)
                .tools(toolRegistry.getTools().stream().map(this::createApiTool).toList())
                .resourceTemplates(resultTemplate())
                .build();
    }

    private McpTransportContext extractTransportContext(HttpServletRequest request) {
        var contextData = new HashMap<String, Object>();
        Object userId = request.getAttribute(McpAuthFilter.ATTRIBUTE_USER_ID);
        if (userId != null) {
            contextData.put(McpAuthFilter.ATTRIBUTE_USER_ID, userId);
        }
        Object authorization = request.getAttribute(McpAuthFilter.ATTRIBUTE_AUTHORIZATION);
        if (authorization != null) {
            contextData.put(McpAuthFilter.ATTRIBUTE_AUTHORIZATION, authorization);
        }
        if (contextData.isEmpty()) {
            return McpTransportContext.EMPTY;
        }
        return McpTransportContext.create(contextData);
    }

    private McpServerFeatures.AsyncToolSpecification createApiTool(McpToolRegistry.McpApiTool tool) {

        var toolSchemaBuilder = McpSchema.Tool.builder(tool.name(), tool.inputSchema())
                .title(tool.title())
                .annotations(READ_ONLY_ANNOTATIONS);
        String description = McpResultLink.description(tool.path());
        if (description != null) {
            toolSchemaBuilder.description(description);
        }
        var toolSchema = toolSchemaBuilder.build();

        return McpServerFeatures.AsyncToolSpecification.builder()
                .tool(toolSchema)
                .callHandler((context, request) -> {
                    try {
                        String authorization =
                                (String) context.transportContext().get(McpAuthFilter.ATTRIBUTE_AUTHORIZATION);
                        String body = apiInvoker.get(tool.path(), request.arguments(), authorization);
                        if (!McpResultLink.returnsLink(tool.path(), request.arguments())) {
                            return Mono.just(McpSchema.CallToolResult.builder().addTextContent(body).build());
                        }
                        String uri = McpResultLink.uri(tool.path(), request.arguments());
                        String summary = McpResultLink.summary(
                                objectMapper, tool.path(), request.arguments(), body);
                        var link = McpSchema.ResourceLink.builder()
                                .uri(uri)
                                .name(McpResultLink.name(tool.path()))
                                .mimeType("application/json")
                                .description(summary)
                                .build();
                        return Mono.just(McpSchema.CallToolResult.builder()
                                .addTextContent(summary)
                                .addContent(link)
                                .build());
                    } catch (RuntimeException e) {
                        return Mono.just(McpSchema.CallToolResult.builder()
                                .addTextContent(e.getMessage())
                                .isError(true)
                                .build());
                    }
                })
                .build();
    }

    private McpServerFeatures.AsyncResourceTemplateSpecification resultTemplate() {
        var template = McpSchema.ResourceTemplate.builder("traccar://{kind}?{query}", "traccar-result")
                .title("Traccar positions")
                .description("JSON array of positions for a traccar:// URI. "
                        + "Call resources/read with that URI. The tool result is only a summary.")
                .mimeType("application/json")
                .build();
        return new McpServerFeatures.AsyncResourceTemplateSpecification(template, this::readResult);
    }

    private Mono<McpSchema.ReadResourceResult> readResult(
            McpAsyncServerExchange exchange, McpSchema.ReadResourceRequest request) {
        try {
            McpResultLink.Target target = McpResultLink.parse(request.uri());
            String authorization =
                    (String) exchange.transportContext().get(McpAuthFilter.ATTRIBUTE_AUTHORIZATION);
            String body = apiInvoker.get(target.path(), target.arguments(), authorization);
            var contents = McpSchema.TextResourceContents.builder(request.uri(), body)
                    .mimeType("application/json")
                    .build();
            return Mono.just(McpSchema.ReadResourceResult.builder(List.of(contents)).build());
        } catch (RuntimeException e) {
            return Mono.error(e);
        }
    }

    public HttpServlet getServlet() {
        return transport;
    }

    @Override
    public void close() throws Exception {
        server.close();
    }

}
