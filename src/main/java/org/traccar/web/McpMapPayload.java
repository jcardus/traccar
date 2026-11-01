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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class McpMapPayload {

    public static final String RESOURCE_URI = "ui://traccar/map.html";
    private static final String TILES = "https://tiles.openfreemap.org/styles/";
    private static final Set<String> BASE_MAPS = Set.of("liberty", "bright", "positron", "dark", "fiord");
    // traccar-web's geometry color.
    private static final String COLOR = "#3bb2d0";

    private McpMapPayload() {
    }

    @FunctionalInterface
    public interface Fetcher {
        String get(String path, Map<String, Object> arguments);
    }

    public record Built(String text, Map<String, Object> content) {
    }

    public static Built build(ObjectMapper mapper, Map<String, Object> arguments, Fetcher fetcher) {
        Map<String, Object> input = arguments == null ? Map.of() : arguments;
        Canvas canvas = new Canvas();
        Object layers = input.get("layers");
        if (layers instanceof List<?> list) {
            for (Object item : list) {
                drawLayer(mapper, canvas, object(item), fetcher);
            }
        }
        addMarkers(canvas, input.get("markers"));
        if (!canvas.bounds.present()) {
            throw new IllegalArgumentException("Nothing to draw. Pass layers or markers.");
        }
        String summary = text(input.get("summary"));
        String body = String.join(" ", canvas.notes);
        String message = summary == null ? body : summary + " " + body;
        return new Built(message, content(input, canvas));
    }

    private static void drawLayer(
            ObjectMapper mapper, Canvas canvas, Map<String, Object> layer, Fetcher fetcher) {
        if (layer.get("uri") != null) {
            drawPositions(mapper, canvas, layer, fetcher);
        } else if (layer.get("data") != null) {
            drawGeoJson(mapper, canvas, layer);
        } else {
            throw new IllegalArgumentException("Each layer needs a traccar:// uri or GeoJSON data.");
        }
    }

    private static void drawPositions(
            ObjectMapper mapper, Canvas canvas, Map<String, Object> layer, Fetcher fetcher) {
        String uri = text(layer.get("uri"));
        if (uri == null) {
            throw new IllegalArgumentException("A positions layer needs a traccar:// URI.");
        }
        McpResultLink.Target target = McpResultLink.parse(uri);
        JsonNode positions = array(mapper, fetcher.get(target.path(), target.arguments()));
        if (positions.size() < 2) {
            throw new IllegalArgumentException("The link has fewer than two positions.");
        }
        List<List<Double>> coordinates = new ArrayList<>();
        for (JsonNode position : positions) {
            double longitude = position.path("longitude").asDouble();
            double latitude = position.path("latitude").asDouble();
            coordinates.add(List.of(longitude, latitude));
            canvas.bounds.include(longitude, latitude);
        }
        Map<String, Object> geometry = new LinkedHashMap<>();
        geometry.put("type", "LineString");
        geometry.put("coordinates", coordinates);
        Map<String, Object> properties = Map.of();
        canvas.source(
                "line",
                collection(List.of(feature(geometry, properties))),
                paint(layer, "line"),
                Map.of("line-join", "round", "line-cap", "round"));
        canvas.note("Positions: " + positions.size() + ".");
    }

    private static void drawGeoJson(ObjectMapper mapper, Canvas canvas, Map<String, Object> layer) {
        Object data = layer.get("data");
        if (!(data instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("A geojson layer needs a data object.");
        }
        JsonNode node = mapper.valueToTree(data);
        String type = layerType(geometryType(node));
        canvas.source(type, data, paint(layer, type), Map.of());
        includeNode(canvas, node);
        canvas.note("Shape drawn.");
    }

    private static String layerType(String geometryType) {
        if (geometryType == null) {
            throw new IllegalArgumentException("The GeoJSON data has no geometry.");
        }
        return switch (geometryType) {
            case "Polygon", "MultiPolygon" -> "fill";
            case "Point", "MultiPoint" -> "circle";
            default -> "line";
        };
    }

    private static String geometryType(JsonNode node) {
        if (node == null) {
            return null;
        }
        if (node.has("coordinates")) {
            return node.path("type").asText(null);
        }
        if (node.has("geometry")) {
            return geometryType(node.get("geometry"));
        }
        for (String key : List.of("features", "geometries")) {
            for (JsonNode child : node.path(key)) {
                String type = geometryType(child);
                if (type != null) {
                    return type;
                }
            }
        }
        return null;
    }

    private static void addMarkers(Canvas canvas, Object value) {
        if (!(value instanceof List<?> markers)) {
            return;
        }
        int count = 0;
        for (Object item : markers) {
            Map<String, Object> marker = object(item);
            if (!(marker.get("position") instanceof List<?> position) || position.size() < 2) {
                throw new IllegalArgumentException("A marker position is [longitude, latitude].");
            }
            double longitude = number(position.get(0));
            double latitude = number(position.get(1));
            Map<String, Object> drawn = new LinkedHashMap<>();
            drawn.put("position", List.of(longitude, latitude));
            String label = text(marker.get("label"));
            if (label != null) {
                drawn.put("label", label);
            }
            String color = text(marker.get("color"));
            if (color != null) {
                drawn.put("color", color);
            }
            canvas.markers.add(drawn);
            canvas.bounds.include(longitude, latitude);
            count += 1;
        }
        if (count > 0) {
            canvas.note("Markers: " + count + ".");
        }
    }

    private static Map<String, Object> content(Map<String, Object> input, Canvas canvas) {
        String basemap = text(input.get("basemap"));
        if (basemap == null) {
            basemap = "liberty";
        }
        if (!BASE_MAPS.contains(basemap)) {
            throw new IllegalArgumentException("Unknown basemap.");
        }
        Map<String, Object> content = new LinkedHashMap<>();
        content.put("style", TILES + basemap);
        content.put("sources", canvas.sources);
        content.put("layers", canvas.layers);
        content.put("markers", canvas.markers);
        Bounds bounds = canvas.bounds;
        content.put("bounds", List.of(bounds.west, bounds.south, bounds.east, bounds.north));
        return content;
    }

    private static Map<String, Object> feature(Map<String, Object> geometry, Map<String, Object> properties) {
        Map<String, Object> feature = new LinkedHashMap<>();
        feature.put("type", "Feature");
        feature.put("geometry", geometry);
        feature.put("properties", properties);
        return feature;
    }

    private static Map<String, Object> collection(List<Map<String, Object>> features) {
        Map<String, Object> collection = new LinkedHashMap<>();
        collection.put("type", "FeatureCollection");
        collection.put("features", features);
        return collection;
    }

    private static void includeNode(Canvas canvas, JsonNode node) {
        if (node == null) {
            return;
        }
        if (node.has("coordinates")) {
            includeNodePositions(canvas, node.get("coordinates"));
        }
        if (node.has("features")) {
            node.get("features").forEach(feature -> includeNode(canvas, feature));
        }
        if (node.has("geometry")) {
            includeNode(canvas, node.get("geometry"));
        }
        if (node.has("geometries")) {
            node.get("geometries").forEach(geometry -> includeNode(canvas, geometry));
        }
    }

    private static void includeNodePositions(Canvas canvas, JsonNode node) {
        if (node == null || !node.isArray() || node.isEmpty()) {
            return;
        }
        if (node.get(0).isNumber() && node.size() >= 2) {
            canvas.bounds.include(node.get(0).asDouble(), node.get(1).asDouble());
            return;
        }
        node.forEach(child -> includeNodePositions(canvas, child));
    }

    private static Map<String, Object> paint(Map<String, Object> layer, String type) {
        Map<String, Object> paint = mapOrEmpty(layer.get("paint"));
        if (!paint.isEmpty()) {
            return paint;
        }
        if ("fill".equals(type)) {
            return Map.of("fill-color", COLOR, "fill-outline-color", COLOR, "fill-opacity", 0.1);
        }
        if ("circle".equals(type)) {
            return Map.of("circle-color", COLOR, "circle-radius", 6);
        }
        return Map.of("line-color", COLOR, "line-width", 2);
    }

    private static JsonNode array(ObjectMapper mapper, String body) {
        try {
            JsonNode root = mapper.readTree(body);
            if (root == null || !root.isArray()) {
                throw new IllegalArgumentException("Not a JSON array.");
            }
            return root;
        } catch (IOException e) {
            throw new IllegalArgumentException("Not JSON.");
        }
    }

    private static Map<String, Object> object(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("Each layer must be an object.");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        map.forEach((key, item) -> result.put(String.valueOf(key), item));
        return result;
    }

    private static Map<String, Object> mapOrEmpty(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            return Map.of();
        }
        Map<String, Object> result = new LinkedHashMap<>();
        map.forEach((key, item) -> result.put(String.valueOf(key), item));
        return result;
    }

    private static double number(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        throw new IllegalArgumentException("A coordinate must be a number.");
    }

    private static String text(Object value) {
        if (value instanceof String text && !text.isBlank()) {
            return text;
        }
        return null;
    }

    private static final class Bounds {
        private double west = Double.POSITIVE_INFINITY;
        private double south = Double.POSITIVE_INFINITY;
        private double east = Double.NEGATIVE_INFINITY;
        private double north = Double.NEGATIVE_INFINITY;

        private void include(double longitude, double latitude) {
            west = Math.min(west, longitude);
            south = Math.min(south, latitude);
            east = Math.max(east, longitude);
            north = Math.max(north, latitude);
        }

        private boolean present() {
            return west <= east && south <= north;
        }
    }

    private static final class Canvas {
        private final Map<String, Object> sources = new LinkedHashMap<>();
        private final List<Map<String, Object>> layers = new ArrayList<>();
        private final List<Map<String, Object>> markers = new ArrayList<>();
        private final List<String> notes = new ArrayList<>();
        private final Bounds bounds = new Bounds();
        private int index;

        private void source(String type, Object data, Map<String, Object> paint, Map<String, Object> layout) {
            String id = "data-" + index;
            index += 1;
            Map<String, Object> source = new LinkedHashMap<>();
            source.put("type", "geojson");
            source.put("data", data);
            sources.put(id, source);
            Map<String, Object> layer = new LinkedHashMap<>();
            layer.put("id", id);
            layer.put("type", type);
            layer.put("source", id);
            if (!paint.isEmpty()) {
                layer.put("paint", paint);
            }
            if (!layout.isEmpty()) {
                layer.put("layout", layout);
            }
            layers.add(layer);
        }

        private void note(String sentence) {
            notes.add(sentence);
        }
    }

}
