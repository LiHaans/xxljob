package com.lihaans.minio;

public class Main {
    public static void main(String[] args) throws Exception {
        String configPath = null;
        for (int i = 0; i < args.length; i++) {
            if ("--config".equals(args[i]) && i + 1 < args.length) {
                configPath = args[i + 1];
                i++;
            }
        }

        if (configPath == null) {
            System.err.println("Usage: java -jar minio-transfer-tool-1.0.0.jar --config /path/to/config.properties");
            System.exit(1);
            return;
        }

        AppConfig config = ConfigLoader.load(configPath);
        JsonObjectNameExtractor extractor = new JsonObjectNameExtractor();
        MinioTransferService transferService = new MinioTransferService(config);
        JsonlGzProcessor processor = new JsonlGzProcessor(config, extractor, transferService);

        long start = System.currentTimeMillis();
        TransferStats stats = processor.process();
        long costMs = System.currentTimeMillis() - start;

        System.out.println("Done. costMs=" + costMs + ", stats=" + stats);
    }
}
