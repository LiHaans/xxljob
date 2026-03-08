package com.lihaans.minio;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class ConfigLoader {
    private ConfigLoader() {
    }

    public static AppConfig load(String path) throws IOException {
        Properties properties = new Properties();
        InputStream in = new FileInputStream(path);
        try {
            properties.load(in);
        } finally {
            in.close();
        }
        return AppConfig.from(properties);
    }
}
