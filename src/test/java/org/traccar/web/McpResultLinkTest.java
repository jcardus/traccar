package org.traccar.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class McpResultLinkTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testLatestPositionsStayInline() {
        assertFalse(McpResultLink.returnsLink(McpResultLink.POSITIONS, Map.of("deviceId", 7)));
        assertFalse(McpResultLink.returnsLink("/devices", Map.of()));
        assertFalse(McpResultLink.returnsLink("/reports/combined", Map.of()));
    }

    @Test
    public void testLinkedResultsReturnLink() {
        assertTrue(McpResultLink.returnsLink(McpResultLink.POSITIONS, Map.of("from", "2026-10-29T08:00:00Z")));
        assertTrue(McpResultLink.returnsLink(McpResultLink.ROUTE, Map.of()));
        assertEquals("positions.json", McpResultLink.name(McpResultLink.POSITIONS));
        assertEquals("positions.json", McpResultLink.name(McpResultLink.ROUTE));
    }

    @Test
    public void testUriRoundTrip() {
        Map<String, Object> arguments = new LinkedHashMap<>();
        arguments.put("deviceId", List.of(7, 8));
        arguments.put("from", "2026-10-29T08:00:00Z");
        arguments.put("to", "2026-10-29T18:00:00Z");
        arguments.put("note", "a/b");

        String rejected = McpResultLink.uri(McpResultLink.ROUTE, arguments);
        assertThrows(IllegalArgumentException.class, () -> McpResultLink.parse(rejected));

        arguments.remove("note");
        String uri = McpResultLink.uri(McpResultLink.ROUTE, arguments);
        McpResultLink.Target target = McpResultLink.parse(uri);

        assertEquals(McpResultLink.ROUTE, target.path());
        assertEquals(List.of("7", "8"), target.arguments().get("deviceId"));
        assertEquals(List.of("2026-10-29T08:00:00Z"), target.arguments().get("from"));
        assertEquals(List.of("2026-10-29T18:00:00Z"), target.arguments().get("to"));
    }

    @Test
    public void testParseRejectsOtherEndpoints() {
        assertThrows(IllegalArgumentException.class, () -> McpResultLink.parse("traccar://devices?all=true"));
        assertThrows(IllegalArgumentException.class, () -> McpResultLink.parse("traccar://combined?deviceId=7"));
        assertThrows(IllegalArgumentException.class, () -> McpResultLink.parse("https://example.com/positions"));
    }

    @Test
    public void testSummaryDescribesPositions() {
        Map<String, Object> arguments = new LinkedHashMap<>();
        arguments.put("deviceId", 7);
        arguments.put("from", "2026-10-29T00:00:00Z");
        arguments.put("to", "2026-10-29T23:00:00Z");
        String body = "["
                + "{\"deviceId\":7,\"fixTime\":\"2026-10-29T08:00:00Z\",\"latitude\":38.5,\"longitude\":-9.25,"
                + "\"speed\":0,\"course\":null,\"valid\":true,\"attributes\":{\"totalDistance\":1000}},"
                + "{\"deviceId\":7,\"fixTime\":\"2026-10-29T09:00:00Z\",\"latitude\":38.75,\"longitude\":-9.5,"
                + "\"speed\":27,\"valid\":false,\"address\":\"Lisbon\","
                + "\"attributes\":{\"totalDistance\":3500,\"ignition\":false,\"battery\":null}}"
                + "]";

        String summary = McpResultLink.summary(objectMapper, arguments, body);

        assertEquals(
                "2 positions for device 7 from 2026-10-29T00:00:00Z to 2026-10-29T23:00:00Z. "
                        + "Fields: address, deviceId, fixTime, latitude, longitude, speed, valid. "
                        + "Attributes: ignition, totalDistance.",
                summary);
        assertEquals(
                "0 positions for device 7 from 2026-10-29T00:00:00Z to 2026-10-29T23:00:00Z.",
                McpResultLink.summary(objectMapper, arguments, "[]"));
    }

    @Test
    public void testSummaryCountsDevices() {
        Map<String, Object> arguments = Map.of("groupId", 3);
        String body = "["
                + "{\"deviceId\":7,\"fixTime\":\"2026-10-29T08:00:00Z\",\"latitude\":38.5,\"longitude\":-9.25},"
                + "{\"deviceId\":7,\"fixTime\":\"2026-10-29T09:00:00Z\",\"latitude\":38.75,\"longitude\":-9.5},"
                + "{\"deviceId\":8,\"fixTime\":\"2026-10-29T10:00:00Z\",\"latitude\":41.0,\"longitude\":-8.0}"
                + "]";

        String summary = McpResultLink.summary(objectMapper, arguments, body);

        assertEquals(
                "3 positions for 2 devices. "
                        + "Fields: deviceId, fixTime, latitude, longitude.",
                summary);
    }

}
