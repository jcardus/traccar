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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class McpMapPayloadTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testDrawsPositionsMarkersAndAShape() {
        McpMapPayload.Built built = McpMapPayload.build(objectMapper, Map.of(
                "layers", List.of(
                        Map.of("uri", "traccar://route?deviceId=7&from=2026-10-29T08:00:00Z"
                                + "&to=2026-10-29T09:00:00Z",
                                "paint", Map.of("line-color", "#e11d48")),
                        Map.of("data", Map.of(
                                "type", "Feature",
                                "geometry", Map.of(
                                        "type", "Polygon",
                                        "coordinates", List.of(List.of(
                                                List.of(-9.2, 38.7),
                                                List.of(-9.1, 38.7),
                                                List.of(-9.1, 38.8),
                                                List.of(-9.2, 38.7)))),
                                "properties", Map.of()))),
                "markers", List.of(Map.of("position", List.of(-9.1393, 38.7223), "label", "Van"))),
                this::fetch);

        assertTrue(built.text().contains("Markers: 1."));
        assertTrue(built.text().contains("Positions: 3."));
        assertTrue(built.text().contains("Shape drawn."));
        assertFalse(built.text().contains("38.7223"));
        assertFalse(built.text().contains("-9.1393"));

        assertEquals("https://tiles.openfreemap.org/styles/liberty", built.content().get("style"));
        List<?> markers = (List<?>) built.content().get("markers");
        assertEquals(1, markers.size());
        Map<?, ?> marker = assertInstanceOf(Map.class, markers.get(0));
        assertEquals(List.of(-9.1393, 38.7223), marker.get("position"));
        assertEquals("Van", marker.get("label"));

        List<?> layers = (List<?>) built.content().get("layers");
        assertEquals(2, layers.size());
        assertEquals("line", ((Map<?, ?>) layers.get(0)).get("type"));
        assertEquals("#e11d48", ((Map<?, ?>) ((Map<?, ?>) layers.get(0)).get("paint")).get("line-color"));
        assertEquals("fill", ((Map<?, ?>) layers.get(1)).get("type"));

        List<?> bounds = assertInstanceOf(List.class, built.content().get("bounds"));
        assertEquals(4, bounds.size());
    }

    @Test
    public void testRejectsALayerWithoutUriOrData() {
        assertThrows(IllegalArgumentException.class, () -> McpMapPayload.build(
                objectMapper,
                Map.of("layers", List.of(Map.of("paint", Map.of()))),
                (path, arguments) -> "[]"));
    }

    @Test
    public void testRejectsAnEmptyMap() {
        assertThrows(IllegalArgumentException.class, () -> McpMapPayload.build(
                objectMapper, Map.of("summary", "Nothing"), this::fetch));
    }

    @Test
    public void testDrawsAPointAsACircle() {
        McpMapPayload.Built built = McpMapPayload.build(objectMapper, Map.of(
                "layers", List.of(Map.of("data", Map.of(
                        "type", "Point", "coordinates", List.of(-9.14, 38.72))))),
                this::fetch);

        List<?> layers = (List<?>) built.content().get("layers");
        assertEquals("circle", ((Map<?, ?>) layers.get(0)).get("type"));
        assertEquals(List.of(-9.14, 38.72, -9.14, 38.72), built.content().get("bounds"));
    }

    @Test
    public void testPageIncludesTheMapLibrary() {
        String page = McpShowMap.page();
        assertTrue(page.contains("maplibre-gl@6.12.0"));
        assertTrue(page.contains("@modelcontextprotocol/ext-apps@2.0.4"));
        assertTrue(page.contains("id=\"map\""));
        var resource = McpShowMap.resource().resource();
        assertEquals(McpMapPayload.RESOURCE_URI, resource.uri());
        assertEquals(McpShowMap.MIME_TYPE, resource.mimeType());
    }

    private String fetch(String path, Map<String, Object> arguments) {
        return switch (path) {
            case "/reports/route" -> "[{\"deviceId\":7,\"latitude\":38.70,\"longitude\":-9.20,"
                    + "\"fixTime\":\"2026-10-29T08:00:00Z\",\"speed\":10,\"valid\":true},"
                    + "{\"deviceId\":7,\"latitude\":38.71,\"longitude\":-9.19,"
                    + "\"fixTime\":\"2026-10-29T08:30:00Z\",\"speed\":20,\"valid\":true},"
                    + "{\"deviceId\":7,\"latitude\":38.72,\"longitude\":-9.18,"
                    + "\"fixTime\":\"2026-10-29T09:00:00Z\",\"speed\":0,\"valid\":false}]";
            default -> "[]";
        };
    }

}
