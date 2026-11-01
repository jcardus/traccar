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
import org.traccar.helper.DistanceCalculator;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class McpMapPayload {

    public static final String RESOURCE_URI = "ui://traccar/map-v2.html";

    private static final int MAX_INLINE_CHARS = 12_000;
    private static final double KNOTS_TO_KMH = 1.852;
    private static final String TILES = "https://tiles.openfreemap.org/styles/";
    private static final Set<String> BASEMAPS = Set.of("liberty", "bright", "positron", "dark", "fiord");
    private static final Set<String> LAYER_TYPES = Set.of(
            "fill", "line", "circle", "symbol", "fill-extrusion", "heatmap");

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
        String summary = text(input.get("summary"));
        String body = canvas.notes.isEmpty() ? "Map drawn." : String.join(" ", canvas.notes);
        String message = summary == null ? body : summary + " " + body;
        return new Built(message, content(input, canvas));
    }

    private static void drawLayer(
            ObjectMapper mapper, Canvas canvas, Map<String, Object> layer, Fetcher fetcher) {
        String kind = text(layer.get("kind"));
        if (kind == null) {
            throw new IllegalArgumentException("Each layer needs a kind: positions or geojson.");
        }
        switch (kind) {
            case "positions" -> drawPositions(mapper, canvas, layer, fetcher);
            case "geojson" -> drawGeoJson(mapper, canvas, layer);
            default -> throw new IllegalArgumentException("Layer kind must be positions or geojson.");
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
        if (latest(target)) {
            drawLatest(canvas, positions);
            return;
        }
        List<Fix> fixes = fixes(positions);
        if (fixes.size() < 2) {
            throw new IllegalArgumentException("The track has fewer than two fixes.");
        }
        List<List<Double>> coordinates = new ArrayList<>();
        for (Fix fix : fixes) {
            coordinates.add(List.of(fix.longitude(), fix.latitude()));
            canvas.bounds.include(fix.longitude(), fix.latitude());
        }
        Map<String, Object> geometry = new LinkedHashMap<>();
        geometry.put("type", "LineString");
        geometry.put("coordinates", coordinates);
        Map<String, Object> properties = Map.of();
        canvas.source(
                "line",
                collection(List.of(feature(geometry, properties))),
                paint(layer, "line"),
                layout(layer));
        canvas.note(trackNote(fixes));
    }

    private static boolean latest(McpResultLink.Target target) {
        return McpResultLink.POSITIONS.equals(target.path())
                && target.arguments().get("from") == null
                && target.arguments().get("to") == null;
    }

    private static void drawLatest(Canvas canvas, JsonNode positions) {
        int count = 0;
        for (JsonNode position : positions) {
            if (position.path("valid").isBoolean() && !position.path("valid").asBoolean()) {
                continue;
            }
            double latitude = position.path("latitude").asDouble(Double.NaN);
            double longitude = position.path("longitude").asDouble(Double.NaN);
            if (!located(latitude, longitude)) {
                continue;
            }
            canvas.marker(longitude, latitude, markerLabel(position));
            count += 1;
        }
        canvas.note(count == 0 ? "No positions to draw." : "Latest positions: " + count + ".");
    }

    private static String markerLabel(JsonNode position) {
        String deviceId = position.path("deviceId").isNull() ? "" : position.path("deviceId").asText("");
        JsonNode route = position.path("attributes").path("route");
        String line = route.isMissingNode() || route.isNull() ? "" : route.asText("");
        if (deviceId.isEmpty()) {
            return line.isEmpty() ? null : line;
        }
        return line.isEmpty() ? deviceId : deviceId + " " + line;
    }

    private static void drawGeoJson(ObjectMapper mapper, Canvas canvas, Map<String, Object> layer) {
        Object data = layer.get("data");
        if (!(data instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("A geojson layer needs a data object.");
        }
        String encoded;
        try {
            encoded = mapper.writeValueAsString(data);
        } catch (IOException e) {
            throw new IllegalArgumentException("A geojson layer needs a data object.");
        }
        if (encoded.length() > MAX_INLINE_CHARS) {
            throw new IllegalArgumentException(
                    "That shape is too large to inline. Pass a traccar:// URI on a positions layer.");
        }
        String type = text(layer.get("type"));
        if (type == null) {
            type = "line";
        }
        if (!LAYER_TYPES.contains(type)) {
            throw new IllegalArgumentException("Unknown GeoJSON layer type.");
        }
        canvas.source(type, data, paint(layer, type), mapOrEmpty(layer.get("layout")));
        includeNode(canvas, mapper.valueToTree(data));
        canvas.note("Shape drawn.");
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
            if (!located(latitude, longitude)) {
                throw new IllegalArgumentException("A marker position is [longitude, latitude].");
            }
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
        if (!BASEMAPS.contains(basemap)) {
            throw new IllegalArgumentException("Unknown basemap.");
        }
        Map<String, Object> content = new LinkedHashMap<>();
        content.put("style", TILES + basemap);
        content.put("sources", canvas.sources);
        content.put("layers", canvas.layers);
        content.put("markers", canvas.markers);
        content.put("camera", camera(input, canvas.bounds));
        return content;
    }

    private static Map<String, Object> camera(Map<String, Object> input, Bounds bounds) {
        Object centerValue = input.get("center");
        Object zoom = input.get("zoom");
        if (centerValue instanceof List<?> center && center.size() >= 2 && zoom instanceof Number) {
            Map<String, Object> camera = new LinkedHashMap<>();
            camera.put("center", List.of(number(center.get(0)), number(center.get(1))));
            camera.put("zoom", ((Number) zoom).doubleValue());
            camera.put("bearing", numberOrZero(input.get("bearing")));
            camera.put("pitch", numberOrZero(input.get("pitch")));
            return camera;
        }
        if (bounds.present()) {
            return Map.of("bounds", List.of(bounds.west, bounds.south, bounds.east, bounds.north));
        }
        Map<String, Object> camera = new LinkedHashMap<>();
        camera.put("center", List.of(0.0, 20.0));
        camera.put("zoom", 1);
        camera.put("bearing", 0);
        camera.put("pitch", 0);
        return camera;
    }

    private static String trackNote(List<Fix> fixes) {
        double distance = 0;
        double maxSpeed = 0;
        for (int i = 0; i < fixes.size(); i++) {
            maxSpeed = Math.max(maxSpeed, fixes.get(i).speedKnots() * KNOTS_TO_KMH);
            if (i > 0) {
                Fix previous = fixes.get(i - 1);
                Fix current = fixes.get(i);
                distance += DistanceCalculator.distance(
                        previous.latitude(), previous.longitude(), current.latitude(), current.longitude());
            }
        }
        long minutes = 0;
        if (fixes.get(0).timeMs() > 0 && fixes.get(fixes.size() - 1).timeMs() > 0) {
            minutes = Math.round((fixes.get(fixes.size() - 1).timeMs() - fixes.get(0).timeMs()) / 60000.0);
        }
        return String.format(
                Locale.US,
                "Track: %d points, %.1f km, %d min, max %.0f km/h.",
                fixes.size(),
                distance / 1000.0,
                minutes,
                maxSpeed);
    }

    private static List<Fix> fixes(JsonNode positions) {
        List<Fix> fixes = new ArrayList<>();
        for (JsonNode position : positions) {
            if (position.path("valid").isBoolean() && !position.path("valid").asBoolean()) {
                continue;
            }
            double latitude = position.path("latitude").asDouble(Double.NaN);
            double longitude = position.path("longitude").asDouble(Double.NaN);
            if (!located(latitude, longitude)) {
                continue;
            }
            long timeMs = 0;
            String fixTime = position.path("fixTime").asText("");
            if (!fixTime.isEmpty()) {
                try {
                    timeMs = Instant.parse(fixTime).toEpochMilli();
                } catch (RuntimeException ignored) {
                    timeMs = 0;
                }
            }
            fixes.add(new Fix(latitude, longitude, position.path("speed").asDouble(0), timeMs));
        }
        return fixes;
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
            return Map.of("fill-color", "#2563eb", "fill-opacity", 0.25);
        }
        if ("circle".equals(type)) {
            return Map.of("circle-color", "#2563eb", "circle-radius", 6);
        }
        return Map.of("line-color", "#2563eb", "line-width", 4);
    }

    private static Map<String, Object> layout(Map<String, Object> layer) {
        Map<String, Object> layout = mapOrEmpty(layer.get("layout"));
        if (!layout.isEmpty()) {
            return layout;
        }
        return Map.of("line-join", "round", "line-cap", "round");
    }

    private static JsonNode array(ObjectMapper mapper, String body) {
        try {
            JsonNode root = mapper.readTree(body);
            if (root == null || !root.isArray()) {
                throw new IllegalArgumentException("Traccar did not return a JSON array.");
            }
            return root;
        } catch (IOException e) {
            throw new IllegalArgumentException("Traccar did not return JSON.");
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

    private static double numberOrZero(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        return 0;
    }

    private static String text(Object value) {
        if (value instanceof String text && !text.isBlank()) {
            return text;
        }
        return null;
    }

    private static boolean located(double latitude, double longitude) {
        return Double.isFinite(latitude) && Double.isFinite(longitude)
                && Math.abs(latitude) <= 90 && Math.abs(longitude) <= 180;
    }

    private record Fix(double latitude, double longitude, double speedKnots, long timeMs) {
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

        private void marker(double longitude, double latitude, String label) {
            Map<String, Object> marker = new LinkedHashMap<>();
            marker.put("position", List.of(longitude, latitude));
            if (label != null && !label.isBlank()) {
                marker.put("label", label);
            }
            markers.add(marker);
            bounds.include(longitude, latitude);
        }

        private void note(String sentence) {
            notes.add(sentence);
        }
    }

}
