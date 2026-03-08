package com.lihaans.minio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class JsonlGzProcessor {
    private final AppConfig config;
    private final JsonObjectNameExtractor extractor;
    private final MinioTransferService transferService;

    public JsonlGzProcessor(AppConfig config, JsonObjectNameExtractor extractor, MinioTransferService transferService) {
        this.config = config;
        this.extractor = extractor;
        this.transferService = transferService;
    }

    public TransferStats process() throws IOException {
        final TransferStats stats = new TransferStats();
        final Path root = Paths.get(config.getInputDir());
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (file.getFileName().toString().endsWith(config.getInputSuffix())) {
                    stats.incFilesScanned();
                    processFile(file, stats);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return stats;
    }

    private void processFile(Path file, TransferStats stats) throws IOException {
        System.out.println("Processing file: " + file);
        InputStream in = Files.newInputStream(file);
        try {
            GZIPInputStream gzipIn = new GZIPInputStream(in);
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(gzipIn, StandardCharsets.UTF_8));
                String line;
                while ((line = reader.readLine()) != null) {
                    stats.incLinesRead();
                    try {
                        List<String> objectNames = extractor.extract(line, config.getArrayFieldPath(), config.getObjectNameField());
                        for (String objectName : objectNames) {
                            stats.incObjectRefsFound();
                            MinioTransferService.TransferResult result = transferService.transfer(objectName);
                            handleResult(result, stats);
                        }
                    } catch (Exception e) {
                        stats.incFailed();
                        System.err.println("Failed to parse/process line in file " + file + ": " + e.getMessage());
                    }
                }
            } finally {
                gzipIn.close();
            }
        } finally {
            in.close();
        }
    }

    private void handleResult(MinioTransferService.TransferResult result, TransferStats stats) {
        if ("TRANSFERRED".equals(result.getStatus())) {
            stats.incTransferred();
            System.out.println("Transferred: " + result.getObjectName());
        } else if ("MISSING".equals(result.getStatus())) {
            stats.incMissing();
            System.out.println("Missing, ignored: " + result.getObjectName());
        } else if ("SKIPPED_EXISTING".equals(result.getStatus())) {
            stats.incSkippedExisting();
            System.out.println("Skipped existing target: " + result.getObjectName());
        } else {
            stats.incFailed();
            System.err.println("Failed transfer: " + result.getObjectName() + ", error=" +
                    (result.getException() == null ? "unknown" : result.getException().getMessage()));
        }
    }
}
