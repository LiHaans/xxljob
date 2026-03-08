package com.lihaans.minio;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class JsonObjectNameExtractor {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public List<String> extract(String jsonLine, String arrayFieldPath, String objectNameField) throws Exception {
        JsonNode root = objectMapper.readTree(jsonLine);
        JsonNode arrayNode = resolvePath(root, arrayFieldPath);
        if (arrayNode == null || !arrayNode.isArray()) {
            return Collections.emptyList();
        }

        List<String> result = new ArrayList<String>();
        Iterator<JsonNode> iterator = arrayNode.elements();
        while (iterator.hasNext()) {
            JsonNode item = iterator.next();
            if (item.isTextual()) {
                String v = trimToNull(item.asText());
                if (v != null) {
                    result.add(v);
                }
            } else if (item.isObject()) {
                JsonNode objNameNode = item.get(objectNameField);
                if (objNameNode != null && objNameNode.isValueNode()) {
                    String v = trimToNull(objNameNode.asText());
                    if (v != null) {
                        result.add(v);
                    }
                }
            }
        }
        return result;
    }

    private JsonNode resolvePath(JsonNode root, String path) {
        String[] parts = path.split("\\.");
        JsonNode current = root;
        for (String part : parts) {
            if (current == null) {
                return null;
            }
            current = current.get(part);
        }
        return current;
    }

    private String trimToNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}
