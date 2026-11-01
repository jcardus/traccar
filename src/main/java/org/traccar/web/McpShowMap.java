/*
 * Copyright 2026 Joaquim Cardeira (joaquim_cardeira@hotmail.com)
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
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.spec.McpSchema;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class McpShowMap {

    public static final String NAME = "show_map";
    public static final String MIME_TYPE = "text/html;profile=mcp-app";

    private static final String CDN = "https://cdn.jsdelivr.net";
    private static final String TILES = "https://tiles.openfreemap.org";
    private static final String DESCRIPTION =
            "Shows the user a map. You don't see it, and the result is a summary.";

    private static volatile String page;

    private McpShowMap() {
    }

    public static McpServerFeatures.AsyncToolSpecification tool(ObjectMapper mapper, McpApiInvoker invoker) {
        var schema = McpSchema.Tool.builder(NAME, inputSchema())
                .title("Show map")
                .description(DESCRIPTION)
                .annotations(new McpSchema.ToolAnnotations(null, true, false, true, false, null))
                .meta(Map.of("ui", Map.of("resourceUri", McpMapPayload.RESOURCE_URI, "csp", csp())))
                .build();
        return McpServerFeatures.AsyncToolSpecification.builder()
                .tool(schema)
                .callHandler((context, request) -> {
                    try {
                        String authorization = (String) context.transportContext()
                                .get(McpAuthFilter.ATTRIBUTE_AUTHORIZATION);
                        Map<String, Object> arguments = request.arguments() == null
                                ? Map.of() : request.arguments();
                        McpMapPayload.Built built = McpMapPayload.build(
                                mapper, arguments, (path, query) -> invoker.get(path, query, authorization));
                        return Mono.just(McpSchema.CallToolResult.builder()
                                .addTextContent(built.text())
                                .structuredContent(built.content())
                                .build());
                    } catch (RuntimeException error) {
                        return Mono.just(McpSchema.CallToolResult.builder()
                                .addTextContent(message(error))
                                .isError(true)
                                .build());
                    }
                })
                .build();
    }

    public static McpServerFeatures.AsyncResourceSpecification resource() {
        var resource = McpSchema.Resource.builder(McpMapPayload.RESOURCE_URI, "traccar-map")
                .title("Traccar map")
                .description("Live map of positions, markers, and small shapes.")
                .mimeType(MIME_TYPE)
                .meta(Map.of("ui", Map.of("csp", csp())))
                .build();
        return new McpServerFeatures.AsyncResourceSpecification(resource, (exchange, request) -> {
            var contents = McpSchema.TextResourceContents.builder(McpMapPayload.RESOURCE_URI, page())
                    .mimeType(MIME_TYPE)
                    .meta(Map.of("ui", Map.of("csp", csp())))
                    .build();
            return Mono.just(McpSchema.ReadResourceResult.builder(List.of(contents)).build());
        });
    }

    static String page() {
        String current = page;
        if (current != null) {
            return current;
        }
        synchronized (McpShowMap.class) {
            if (page == null) {
                page = readPage();
            }
            return page;
        }
    }

    private static String readPage() {
        try (InputStream input = McpShowMap.class.getResourceAsStream("map.html")) {
            if (input == null) {
                throw new IllegalStateException("The map page is missing.");
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException error) {
            throw new IllegalStateException("The map page is missing.", error);
        }
    }

    private static Map<String, Object> csp() {
        return Map.of(
                "connectDomains", List.of(CDN, TILES),
                "resourceDomains", List.of(CDN, TILES),
                "workerDomains", List.of("blob:", CDN));
    }

    private static Map<String, Object> inputSchema() {
        Map<String, Object> layer = new LinkedHashMap<>();
        layer.put("type", "object");
        layer.put("properties", Map.of(
                "uri", Map.of(
                        "type", "string",
                        "description", "A traccar:// link from a positions or route result."),
                "data", Map.of(
                        "type", "object",
                        "description", "Inline GeoJSON."),
                "paint", Map.of(
                        "type", "object",
                        "description", "MapLibre paint.")));
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", Map.of(
                "summary", Map.of("type", "string", "description", "Short label for the map."),
                "layers", Map.of("type", "array", "items", layer),
                "markers", Map.of(
                        "type", "array",
                        "items", Map.of(
                                "type", "object",
                                "properties", Map.of(
                                        "position", Map.of(
                                                "type", "array",
                                                "items", Map.of("type", "number"),
                                                "description", "[longitude, latitude]"),
                                        "label", Map.of("type", "string"),
                                        "color", Map.of("type", "string")))),
                "basemap", Map.of(
                        "type", "string",
                        "enum", List.of("liberty", "bright", "positron", "dark", "fiord"))));
        return schema;
    }

    private static String message(RuntimeException error) {
        String text = error.getMessage() == null ? "Could not draw the map." : error.getMessage();
        String line = text.split("\n", 2)[0];
        if (line.contains("[{") || line.length() > 300) {
            return "Could not draw the map.";
        }
        return line;
    }

}
