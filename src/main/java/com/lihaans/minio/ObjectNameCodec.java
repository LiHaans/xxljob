package com.lihaans.minio;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public final class ObjectNameCodec {
    private ObjectNameCodec() {
    }

    public static String sanitize(String objectName) {
        if (objectName == null) {
            return null;
        }
        String trimmed = objectName.trim();
        if (trimmed.isEmpty()) {
            return trimmed;
        }
        return trimmed;
    }

    public static String urlEncodeUtf8(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }
}
