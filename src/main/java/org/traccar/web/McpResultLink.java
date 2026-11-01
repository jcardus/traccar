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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public final class McpResultLink {

    public static final String POSITIONS = "/positions";
    public static final String ROUTE = "/reports/route";

    private static final String SCHEME = "traccar";
    private static final String DESCRIPTION = "Returns a summary and a traccar:// link to the position list. "
            + "show_map draws the link, and resources/read returns the list.";
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
        if (POSITIONS.equals(path) || ROUTE.equals(path)) {
            return DESCRIPTION;
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
        return SCHEME + "://" + kind(path) + "?" + query(arguments);
    }

    public static Target parse(String uri) {
        URI parsed = URI.create(uri);
        String path = SCHEME.equals(parsed.getScheme()) ? path(parsed.getHost()) : null;
        String query = parsed.getRawQuery();
        if (path == null || query == null) {
            throw new IllegalArgumentException("Unknown resource");
        }
        Set<String> allowed = POSITIONS.equals(path) ? POSITION_KEYS : REPORT_KEYS;
        Map<String, List<String>> values = new LinkedHashMap<>();
        for (String part : query.split("&")) {
            String[] pair = part.split("=", 2);
            String key = decode(pair[0]);
            if (pair.length < 2 || !allowed.contains(key)) {
                throw new IllegalArgumentException("Unknown resource");
            }
            values.computeIfAbsent(key, ignored -> new ArrayList<>()).add(decode(pair[1]));
        }
        return new Target(path, new LinkedHashMap<>(values));
    }

    public static String summary(ObjectMapper mapper, Map<String, Object> arguments, String body) {
        StringBuilder text = new StringBuilder();
        JsonNode positions = positions(mapper, body);
        text.append(counted(positions.size(), "position", "positions"));
        appendDevices(text, positions, arguments);
        text.append(window(arguments));
        appendIncluded(text, positions);
        return text.append('.').toString();
    }

    private static JsonNode positions(ObjectMapper mapper, String body) {
        try {
            return mapper.readTree(body);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static void appendDevices(StringBuilder text, JsonNode positions, Map<String, Object> arguments) {
        Set<String> devices = new TreeSet<>();
        for (JsonNode position : positions) {
            String deviceId = text(position.get("deviceId"));
            if (deviceId != null) {
                devices.add(deviceId);
            }
        }
        if (devices.size() == 1) {
            text.append(" for device ").append(devices.iterator().next());
        } else if (devices.size() > 1) {
            text.append(" for ").append(counted(devices.size(), "device", "devices"));
        } else {
            appendOwners(text, arguments);
        }
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
            return !value.isEmpty();
        }
        return true;
    }

    private static void appendIncluded(StringBuilder text, JsonNode positions) {
        Set<String> fields = new TreeSet<>();
        Set<String> attributes = new TreeSet<>();
        positions.forEach(position -> collectIncluded(position, fields, attributes));
        if (!fields.isEmpty()) {
            text.append(". Fields: ").append(String.join(", ", fields));
        }
        if (!attributes.isEmpty()) {
            text.append(". Attributes: ").append(String.join(", ", attributes));
        }
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

    private static String counted(int count, String one, String many) {
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
                if (!builder.isEmpty()) {
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

}
