package org.sensorhub.oshconnect.net;

import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaDefinitionsTest {
    /**
     * The recordSchema of a live Axis camera's video data stream, as the node
     * returns it for {@code schema?obsFormat=application/swe+binary} (it offers
     * no swe+json schema).
     */
    private static final String VIDEO_RECORD_SCHEMA = """
            {
              "type": "DataRecord",
              "name": "video1",
              "definition": "http://sensorml.com/ont/swe/property/VideoFrame",
              "fields": [
                {
                  "type": "Time",
                  "name": "time",
                  "definition": "http://www.opengis.net/def/property/OGC/0/SamplingTime",
                  "uom": { "href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian" }
                },
                {
                  "type": "DataArray",
                  "name": "img",
                  "definition": "http://sensorml.com/ont/swe/property/RasterImage",
                  "elementCount": {
                    "type": "Count",
                    "name": "elementCount",
                    "definition": "http://sensorml.com/ont/swe/property/GridHeight",
                    "value": 480
                  },
                  "elementType": {
                    "type": "DataArray",
                    "name": "row",
                    "elementCount": {
                      "type": "Count",
                      "name": "elementCount",
                      "definition": "http://sensorml.com/ont/swe/property/GridWidth",
                      "value": 704
                    },
                    "elementType": {
                      "type": "DataRecord",
                      "name": "pixel",
                      "fields": [
                        { "type": "Count", "name": "red", "definition": "http://sensorml.com/ont/swe/property/RedChannel" },
                        { "type": "Count", "name": "green", "definition": "http://sensorml.com/ont/swe/property/GreenChannel" },
                        { "type": "Count", "name": "blue", "definition": "http://sensorml.com/ont/swe/property/BlueChannel" }
                      ]
                    }
                  }
                }
              ]
            }
            """;

    @Test
    void collectsComponentDefinitionsAtAnyDepth() {
        var definitions = ConSysApiClientExtras.schemaDefinitions(JsonParser.parseString(VIDEO_RECORD_SCHEMA));
        assertEquals(Set.of(
                "http://sensorml.com/ont/swe/property/VideoFrame",
                "http://www.opengis.net/def/property/OGC/0/SamplingTime",
                "http://sensorml.com/ont/swe/property/RasterImage",
                "http://sensorml.com/ont/swe/property/RedChannel",
                "http://sensorml.com/ont/swe/property/GreenChannel",
                "http://sensorml.com/ont/swe/property/BlueChannel"
        ), definitions);
    }

    @Test
    void walksVectorCoordinatesAndChoiceItems() {
        var schema = """
                {
                  "type": "DataChoice", "definition": "urn:choice",
                  "items": [
                    { "type": "Vector", "definition": "urn:vector",
                      "coordinates": [ { "type": "Quantity", "name": "lat", "definition": "urn:lat" } ] }
                  ]
                }
                """;
        assertEquals(Set.of("urn:choice", "urn:vector", "urn:lat"),
                ConSysApiClientExtras.schemaDefinitions(JsonParser.parseString(schema)));
    }
}
