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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

public class JsonlGzProcessor {
    private static final String POISON_PILL = "__POISON_PILL__";

    private final AppConfig config;
    private final JsonObjectNameExtractor extractor;
    private final MinioTransferService transferService;

    public JsonlGzProcessor(AppConfig config, JsonObjectNameExtractor extractor, MinioTransferService transferService) {
        this.config = config;
        this.extractor = extractor;
        this.transferService = transferService;
    }

    public TransferStats process() throws IOException, InterruptedException {
        final TransferStats stats = new TransferStats();
        final List<Path> files = collectInputFiles();
        final BlockingQueue<String> queue = new ArrayBlockingQueue<String>(config.getQueueCapacity());
        final ExecutorService transferExecutor = Executors.newFixedThreadPool(
                config.getTransferThreads(),
                new NamedThreadFactory("minio-transfer-worker-")
        );
        final ExecutorService readerExecutor = Executors.newFixedThreadPool(
                config.getReaderThreads(),
                new NamedThreadFactory("jsonl-reader-worker-")
        );
        final Thread progressThread = createProgressThread(stats, queue);
        final CountDownLatch readerDone = new CountDownLatch(files.size());

        progressThread.start();
        startTransferWorkers(transferExecutor, queue, stats);
        startReaderWorkers(readerExecutor, files, queue, stats, readerDone);

        try {
            readerExecutor.shutdown();
            while (!readerExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                System.out.println("Waiting readers to finish... remaining=" + readerDone.getCount() + ", queueSize=" + queue.size());
            }
        } finally {
            for (int i = 0; i < config.getTransferThreads(); i++) {
                putUninterruptibly(queue, POISON_PILL);
            }
            transferExecutor.shutdown();
            while (!transferExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                System.out.println("Waiting transfer workers to finish... queueSize=" + queue.size() + ", stats=" + stats);
            }
            progressThread.interrupt();
            progressThread.join(TimeUnit.SECONDS.toMillis(3));
            logProgress(stats, queue.size(), true);
        }
        return stats;
    }

    private List<Path> collectInputFiles() throws IOException {
        final List<Path> files = new ArrayList<Path>();
        final Path root = Paths.get(config.getInputDir());
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (file.getFileName().toString().endsWith(config.getInputSuffix())) {
                    files.add(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return files;
    }

    private void startReaderWorkers(ExecutorService readerExecutor,
                                    List<Path> files,
                                    final BlockingQueue<String> queue,
                                    final TransferStats stats,
                                    final CountDownLatch readerDone) {
        for (final Path file : files) {
            readerExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    stats.incFilesScanned();
                    try {
                        processFile(file, stats, queue);
                    } catch (Exception e) {
                        stats.incFailed();
                        System.err.println("Failed processing file " + file + ": " + e.getMessage());
                    } finally {
                        readerDone.countDown();
                    }
                }
            });
        }
    }

    private void startTransferWorkers(ExecutorService transferExecutor,
                                      final BlockingQueue<String> queue,
                                      final TransferStats stats) {
        for (int i = 0; i < config.getTransferThreads(); i++) {
            transferExecutor.submit(new TransferWorker(queue, stats));
        }
    }

    private void processFile(Path file, TransferStats stats, BlockingQueue<String> queue) throws IOException {
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
                            putUninterruptibly(queue, objectName);
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
        } else if ("MISSING".equals(result.getStatus())) {
            stats.incMissing();
            System.out.println("Missing, ignored: source=" + result.getSourceObjectName());
        } else if ("SKIPPED_EXISTING".equals(result.getStatus())) {
            stats.incSkippedExisting();
        } else {
            stats.incFailed();
            System.err.println("Failed transfer: source=" + result.getSourceObjectName() + ", target=" + result.getTargetObjectName() + ", error=" +
                    (result.getException() == null ? "unknown" : result.getException().getMessage()));
        }
    }

    private void putUninterruptibly(BlockingQueue<String> queue, String value) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    queue.put(value);
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private Thread createProgressThread(final TransferStats stats, final BlockingQueue<String> queue) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        TimeUnit.SECONDS.sleep(config.getProgressLogIntervalSeconds());
                        logProgress(stats, queue.size(), false);
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "minio-transfer-progress");
        thread.setDaemon(true);
        return thread;
    }

    private void logProgress(TransferStats stats, int queueSize, boolean finished) {
        String prefix = finished ? "Final progress" : "Progress";
        long done = stats.getTransferred() + stats.getMissing() + stats.getSkippedExisting() + stats.getFailed();
        System.out.println(prefix + ": done=" + done +
                ", queueSize=" + queueSize +
                ", filesScanned=" + stats.getFilesScanned() +
                ", linesRead=" + stats.getLinesRead() +
                ", objectRefsFound=" + stats.getObjectRefsFound() +
                ", transferred=" + stats.getTransferred() +
                ", missing=" + stats.getMissing() +
                ", skippedExisting=" + stats.getSkippedExisting() +
                ", failed=" + stats.getFailed());
    }

    private class TransferWorker implements Runnable {
        private final BlockingQueue<String> queue;
        private final TransferStats stats;

        private TransferWorker(BlockingQueue<String> queue, TransferStats stats) {
            this.queue = queue;
            this.stats = stats;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    String objectName = queue.take();
                    if (POISON_PILL.equals(objectName)) {
                        return;
                    }
                    MinioTransferService.TransferResult result = transferService.transfer(objectName);
                    handleResult(result, stats);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private int index = 1;

        private NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public synchronized Thread newThread(Runnable r) {
            Thread thread = new Thread(r, prefix + index++);
            thread.setDaemon(false);
            return thread;
        }
    }
}
