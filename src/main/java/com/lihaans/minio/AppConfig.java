package com.lihaans.minio;

import io.minio.MinioClient;

import java.util.Properties;

public class AppConfig {
    private final String inputDir;
    private final String inputSuffix;
    private final String arrayFieldPath;
    private final String objectNameField;
    private final boolean skipExistingTarget;
    private final String targetKeyPrefix;
    private final int readerThreads;
    private final int transferThreads;
    private final int queueCapacity;
    private final long progressLogIntervalSeconds;

    private final StorageConfig source;
    private final StorageConfig target;

    public AppConfig(String inputDir,
                     String inputSuffix,
                     String arrayFieldPath,
                     String objectNameField,
                     boolean skipExistingTarget,
                     String targetKeyPrefix,
                     int readerThreads,
                     int transferThreads,
                     int queueCapacity,
                     long progressLogIntervalSeconds,
                     StorageConfig source,
                     StorageConfig target) {
        this.inputDir = inputDir;
        this.inputSuffix = inputSuffix;
        this.arrayFieldPath = arrayFieldPath;
        this.objectNameField = objectNameField;
        this.skipExistingTarget = skipExistingTarget;
        this.targetKeyPrefix = targetKeyPrefix;
        this.readerThreads = readerThreads;
        this.transferThreads = transferThreads;
        this.queueCapacity = queueCapacity;
        this.progressLogIntervalSeconds = progressLogIntervalSeconds;
        this.source = source;
        this.target = target;
    }

    public static AppConfig from(Properties p) {
        String inputDir = required(p, "input.dir");
        String inputSuffix = p.getProperty("input.suffix", ".jsonl.gz");
        String arrayFieldPath = required(p, "json.arrayFieldPath");
        String objectNameField = p.getProperty("json.objectNameField", "objectName");
        boolean skipExistingTarget = Boolean.parseBoolean(p.getProperty("action.skipExistingTarget", "false"));
        String targetKeyPrefix = p.getProperty("action.targetKeyPrefix", "");
        int readerThreads = positiveInt(p, "reader.threads", 4);
        int transferThreads = positiveInt(p, "transfer.threads", 32);
        int queueCapacity = positiveInt(p, "transfer.queueCapacity", 5000);
        long progressLogIntervalSeconds = positiveLong(p, "progress.logIntervalSeconds", 30L);

        StorageConfig source = StorageConfig.from(p, "source");
        StorageConfig target = StorageConfig.from(p, "target");

        return new AppConfig(inputDir, inputSuffix, arrayFieldPath, objectNameField,
                skipExistingTarget, targetKeyPrefix, readerThreads, transferThreads, queueCapacity,
                progressLogIntervalSeconds, source, target);
    }

    private static String required(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required config: " + key);
        }
        return v.trim();
    }

    private static int positiveInt(Properties p, String key, int defaultValue) {
        String raw = p.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        int value = Integer.parseInt(raw.trim());
        if (value <= 0) {
            throw new IllegalArgumentException("Config must be > 0: " + key);
        }
        return value;
    }

    private static long positiveLong(Properties p, String key, long defaultValue) {
        String raw = p.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        long value = Long.parseLong(raw.trim());
        if (value <= 0L) {
            throw new IllegalArgumentException("Config must be > 0: " + key);
        }
        return value;
    }

    public String getInputDir() {
        return inputDir;
    }

    public String getInputSuffix() {
        return inputSuffix;
    }

    public String getArrayFieldPath() {
        return arrayFieldPath;
    }

    public String getObjectNameField() {
        return objectNameField;
    }

    public boolean isSkipExistingTarget() {
        return skipExistingTarget;
    }

    public String getTargetKeyPrefix() {
        return targetKeyPrefix;
    }

    public int getReaderThreads() {
        return readerThreads;
    }

    public int getTransferThreads() {
        return transferThreads;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public long getProgressLogIntervalSeconds() {
        return progressLogIntervalSeconds;
    }

    public StorageConfig getSource() {
        return source;
    }

    public StorageConfig getTarget() {
        return target;
    }

    public static class StorageConfig {
        private final String endpoint;
        private final String accessKey;
        private final String secretKey;
        private final String bucket;
        private final String region;

        public StorageConfig(String endpoint, String accessKey, String secretKey, String bucket, String region) {
            this.endpoint = endpoint;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.bucket = bucket;
            this.region = region;
        }

        static StorageConfig from(Properties p, String prefix) {
            return new StorageConfig(
                    required(p, prefix + ".endpoint"),
                    required(p, prefix + ".accessKey"),
                    required(p, prefix + ".secretKey"),
                    required(p, prefix + ".bucket"),
                    p.getProperty(prefix + ".region", "").trim()
            );
        }

        public MinioClient buildClient() {
            MinioClient.Builder builder = MinioClient.builder()
                    .endpoint(endpoint)
                    .credentials(accessKey, secretKey);
            if (region != null && !region.isEmpty()) {
                builder.region(region);
            }
            return builder.build();
        }

        public String getBucket() {
            return bucket;
        }
    }
}
