package com.google.edwmigration.dumper.application.dumper.connector.cloudera;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Utility class for the cloudera connector
 */
public class ClouderaConnectorUtils {

    public static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
                    .disable(SerializationFeature.INDENT_OUTPUT);
}
