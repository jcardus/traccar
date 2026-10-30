/*
 * Copyright 2026 Anton Tananaev (anton@traccar.org)
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
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public final class McpResultLink {

    public static final String POSITIONS = "/positions";
    public static final String ROUTE = "/reports/route";

    private static final String SCHEME = "traccar://";
    private static final int DEVICE_LIMIT = 8;
    private static final String POSITION_LIST =
            "a summary of the position list and a resource link for the client. "
                    + "The summary has the count, devices, time span, start and end, "
                    + "and the included fields and attributes.";
    private static final Set<String> POSITION_KEYS = Set.of("deviceId", "from", "to", "id");
    private static final Set<String> REPORT_KEYS = Set.of("deviceId", "groupId", "from", "to");

    private McpResultLink() {
    }

    public record Target(String path, Map<String, Object> arguments) {
    }

    public static boolean returnsLink(String path, Map<String, Object> arguments) {
        if (ROUTE.equals(path)) {
            return true;
        }
        if (!POSITIONS.equals(path) || arguments == null) {
            return false;
        }
        return arguments.get("from") != null || arguments.get("to") != null;
    }

    public static String description(String path) {
        if (POSITIONS.equals(path)) {
            return "Returns the latest position of each device as JSON. When from or to is set, returns "
                    + POSITION_LIST;
        }
        if (ROUTE.equals(path)) {
            return "Returns " + POSITION_LIST;
        }
        return null;
    }

    public static String name(String path) {
        if (ROUTE.equals(path) || POSITIONS.equals(path)) {
            return "positions.json";
        }
        return null;
    }

    public static String uri(String path, Map<String, Object> arguments) {
        return SCHEME + kind(path) + "?" + query(arguments);
    }

    public static Target parse(String uri) {
        if (uri == null || !uri.startsWith(SCHEME)) {
            throw new IllegalArgumentException("Unknown resource");
        }
        int queryStart = uri.indexOf('?');
        if (queryStart < SCHEME.length()) {
            throw new IllegalArgumentException("Unknown resource");
        }
        String kind = uri.substring(SCHEME.length(), queryStart);
        String path = path(kind);
        if (path == null) {
            throw new IllegalArgumentException("Unknown resource");
        }
        String query = uri.substring(queryStart + 1);
        if (query.isEmpty()) {
            throw new IllegalArgumentException("Unknown resource");
        }
        Set<String> allowed = POSITIONS.equals(path) ? POSITION_KEYS : REPORT_KEYS;
        Map<String, List<String>> values = new LinkedHashMap<>();
        for (String part : query.split("&", -1)) {
            int separator = part.indexOf('=');
            if (separator <= 0) {
                throw new IllegalArgumentException("Unknown resource");
            }
            String key = decode(part.substring(0, separator));
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException("Unknown resource");
            }
            String value = decode(part.substring(separator + 1));
            values.computeIfAbsent(key, ignored -> new ArrayList<>()).add(value);
        }
        Map<String, Object> arguments = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : values.entrySet()) {
            List<String> entryValues = entry.getValue();
            arguments.put(entry.getKey(), entryValues.size() == 1 ? entryValues.get(0) : entryValues);
        }
        return new Target(path, arguments);
    }

    public static String summary(ObjectMapper mapper, String path, Map<String, Object> arguments, String body) {
        StringBuilder text = new StringBuilder();
        List<JsonNode> positions = positions(mapper, body);
        if (positions == null) {
            text.append("Positions");
            appendOwners(text, arguments);
            text.append(window(arguments));
        } else if (positions.isEmpty()) {
            text.append("0 positions");
            appendOwners(text, arguments);
            text.append(window(arguments));
        } else {
            text.append(contents(positions, arguments));
        }
        text.append(". The client can read the positions at ").append(uri(path, arguments)).append('.');
        return text.toString();
    }

    private static List<JsonNode> positions(ObjectMapper mapper, String body) {
        try {
            JsonNode root = mapper.readTree(body);
            if (root == null || !root.isArray()) {
                return null;
            }
            List<JsonNode> positions = new ArrayList<>();
            root.forEach(positions::add);
            return positions;
        } catch (IOException e) {
            return null;
        }
    }

    private static String contents(List<JsonNode> positions, Map<String, Object> arguments) {
        Map<String, DeviceSpan> spans = new LinkedHashMap<>();
        Set<String> fields = new TreeSet<>();
        Set<String> attributes = new TreeSet<>();
        for (JsonNode position : positions) {
            collectIncluded(position, fields, attributes);
            String deviceId = text(position.get("deviceId"));
            DeviceSpan span = spans.computeIfAbsent(deviceId == null ? "" : deviceId, DeviceSpan::new);
            span.add(position);
        }
        List<DeviceSpan> devices = new ArrayList<>(spans.values());
        StringBuilder text = new StringBuilder();
        if (devices.size() == 1) {
            text.append(oneDevice(devices.get(0), arguments));
        } else {
            int total = 0;
            for (DeviceSpan span : devices) {
                total += span.count;
            }
            text.append(counted(total, "position", "positions"));
            text.append(" for ");
            if (devices.size() > DEVICE_LIMIT) {
                text.append(counted(devices.size(), "device", "devices"));
            } else {
                text.append(deviceList(devices));
            }
            int shown = Math.min(devices.size(), DEVICE_LIMIT);
            for (int i = 0; i < shown; i++) {
                text.append(". ").append(deviceLine(devices.get(i), arguments));
            }
            int hidden = devices.size() - shown;
            if (hidden > 0) {
                text.append(". And ").append(counted(hidden, "more device", "more devices"));
            }
        }
        appendIncluded(text, fields, attributes);
        return text.toString();
    }

    private static void collectIncluded(JsonNode position, Set<String> fields, Set<String> attributes) {
        if (position == null || !position.isObject()) {
            return;
        }
        position.fields().forEachRemaining(entry -> {
            if ("attributes".equals(entry.getKey())) {
                JsonNode value = entry.getValue();
                if (value != null && value.isObject()) {
                    value.fields().forEachRemaining(attribute -> {
                        if (present(attribute.getValue())) {
                            attributes.add(attribute.getKey());
                        }
                    });
                }
            } else if (present(entry.getValue())) {
                fields.add(entry.getKey());
            }
        });
    }

    private static boolean present(JsonNode value) {
        if (value == null || value.isNull() || value.isMissingNode()) {
            return false;
        }
        if (value.isTextual()) {
            return !value.asText().isBlank();
        }
        if (value.isContainerNode()) {
            return value.size() > 0;
        }
        return true;
    }

    private static void appendIncluded(StringBuilder text, Set<String> fields, Set<String> attributes) {
        if (!fields.isEmpty()) {
            text.append(". Fields: ").append(String.join(", ", fields));
        }
        if (!attributes.isEmpty()) {
            text.append(". Attributes: ").append(String.join(", ", attributes));
        }
    }

    private static String oneDevice(DeviceSpan span, Map<String, Object> arguments) {
        StringBuilder text = new StringBuilder(counted(span.count, "position", "positions"));
        if (span.invalid > 0) {
            text.append(" (").append(span.invalid).append(" invalid)");
        }
        if (span.deviceId != null) {
            text.append(" for device ").append(span.deviceId);
        } else {
            appendOwners(text, arguments);
        }
        appendWhenWhere(text, span, arguments);
        return text.toString();
    }

    private static String deviceLine(DeviceSpan span, Map<String, Object> arguments) {
        StringBuilder text = new StringBuilder();
        if (span.deviceId != null) {
            text.append("Device ").append(span.deviceId).append(": ");
        }
        text.append(counted(span.count, "position", "positions"));
        if (span.invalid > 0) {
            text.append(" (").append(span.invalid).append(" invalid)");
        }
        appendWhenWhere(text, span, arguments);
        return text.toString();
    }

    private static void appendWhenWhere(StringBuilder text, DeviceSpan span, Map<String, Object> arguments) {
        if (span.startTime != null && span.endTime != null && !span.startTime.equals(span.endTime)) {
            text.append(" from ").append(span.startTime).append(" to ").append(span.endTime);
        } else if (span.startTime != null) {
            text.append(" at ").append(span.startTime);
        } else {
            text.append(window(arguments));
        }
        boolean moved = span.startPoint && span.endPoint
                && (Double.compare(span.startLat, span.endLat) != 0
                || Double.compare(span.startLon, span.endLon) != 0);
        if (moved) {
            text.append(". Start ").append(place(span.startLat, span.startLon, span.startAddress));
            text.append(". End ").append(place(span.endLat, span.endLon, span.endAddress));
        } else if (span.startPoint) {
            text.append(". At ").append(place(span.startLat, span.startLon, span.startAddress));
        } else if (span.endPoint) {
            text.append(". At ").append(place(span.endLat, span.endLon, span.endAddress));
        }
    }

    private static String deviceList(List<DeviceSpan> spans) {
        List<String> ids = new ArrayList<>();
        for (DeviceSpan span : spans) {
            if (span.deviceId != null) {
                ids.add(span.deviceId);
            }
        }
        if (ids.isEmpty()) {
            return counted(spans.size(), "device", "devices");
        }
        return (ids.size() == 1 ? "device " : "devices ") + String.join(", ", ids);
    }

    private static String place(double latitude, double longitude, String address) {
        String point = String.format(Locale.ROOT, "%.5f, %.5f", latitude, longitude);
        return address == null ? point : point + " (" + address + ")";
    }

    private static void appendOwners(StringBuilder text, Map<String, Object> arguments) {
        String owners = owners(arguments);
        if (owners != null) {
            text.append(" for ").append(owners);
        }
    }

    private static String text(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        String value = node.asText();
        return value == null || value.isBlank() ? null : value;
    }

    private static double number(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode() || !node.isNumber()) {
            return Double.NaN;
        }
        return node.asDouble();
    }

    private static String timeKey(JsonNode time) {
        if (time == null || time.isNull() || time.isMissingNode()) {
            return null;
        }
        if (time.isNumber()) {
            return String.format(Locale.ROOT, "%020d", time.longValue());
        }
        String value = time.asText();
        return value.isBlank() ? null : value;
    }

    private static String counted(int count, String one, String many) {
        if (count < 0) {
            return null;
        }
        return count + " " + (count == 1 ? one : many);
    }

    private static String owners(Map<String, Object> arguments) {
        if (arguments == null) {
            return null;
        }
        String devices = labels(arguments.get("deviceId"), "device", "devices");
        String groups = labels(arguments.get("groupId"), "group", "groups");
        if (devices != null && groups != null) {
            return devices + " and " + groups;
        }
        return devices != null ? devices : groups;
    }

    private static String labels(Object value, String one, String many) {
        List<String> values = strings(value);
        if (values.isEmpty()) {
            return null;
        }
        return (values.size() == 1 ? one : many) + " " + String.join(", ", values);
    }

    private static List<String> strings(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof List<?> list) {
            List<String> values = new ArrayList<>();
            for (Object item : list) {
                if (item != null) {
                    values.add(String.valueOf(item));
                }
            }
            return values;
        }
        return List.of(String.valueOf(value));
    }

    private static String window(Map<String, Object> arguments) {
        if (arguments == null) {
            return "";
        }
        Object from = arguments.get("from");
        Object to = arguments.get("to");
        if (from != null && to != null) {
            return " from " + from + " to " + to;
        }
        if (from != null) {
            return " from " + from;
        }
        if (to != null) {
            return " to " + to;
        }
        return "";
    }

    private static String kind(String path) {
        if (ROUTE.equals(path)) {
            return "route";
        }
        return "positions";
    }

    private static String path(String kind) {
        return switch (kind) {
            case "positions" -> POSITIONS;
            case "route" -> ROUTE;
            default -> null;
        };
    }

    private static String query(Map<String, Object> arguments) {
        if (arguments == null || arguments.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Object> entry : arguments.entrySet()) {
            for (String value : strings(entry.getValue())) {
                if (builder.length() > 0) {
                    builder.append('&');
                }
                builder.append(encode(entry.getKey())).append('=').append(encode(value));
            }
        }
        return builder.toString();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static final class DeviceSpan {

        private final String deviceId;
        private int count;
        private int invalid;
        private String startKey;
        private String endKey;
        private String startTime;
        private String endTime;
        private boolean startPoint;
        private boolean endPoint;
        private double startLat;
        private double startLon;
        private double endLat;
        private double endLon;
        private String startAddress;
        private String endAddress;

        private DeviceSpan(String deviceId) {
            this.deviceId = deviceId.isEmpty() ? null : deviceId;
        }

        private void add(JsonNode position) {
            count++;
            JsonNode valid = position.get("valid");
            if (valid != null && valid.isBoolean() && !valid.asBoolean()) {
                invalid++;
            }
            String key = timeKey(position.get("fixTime"));
            if (key == null) {
                return;
            }
            String time = position.get("fixTime").asText();
            if (startKey == null || key.compareTo(startKey) < 0) {
                startKey = key;
                startTime = time;
                copyPoint(position, true);
            }
            if (endKey == null || key.compareTo(endKey) >= 0) {
                endKey = key;
                endTime = time;
                copyPoint(position, false);
            }
        }

        private void copyPoint(JsonNode position, boolean start) {
            double latitude = number(position.get("latitude"));
            double longitude = number(position.get("longitude"));
            boolean hasPoint = !Double.isNaN(latitude) && !Double.isNaN(longitude);
            String address = text(position.get("address"));
            if (start) {
                startPoint = hasPoint;
                startLat = latitude;
                startLon = longitude;
                startAddress = address;
            } else {
                endPoint = hasPoint;
                endLat = latitude;
                endLon = longitude;
                endAddress = address;
            }
        }

    }

}
