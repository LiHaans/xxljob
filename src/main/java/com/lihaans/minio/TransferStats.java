package com.lihaans.minio;

import java.util.concurrent.atomic.AtomicLong;

public class TransferStats {
    private final AtomicLong filesScanned = new AtomicLong();
    private final AtomicLong linesRead = new AtomicLong();
    private final AtomicLong objectRefsFound = new AtomicLong();
    private final AtomicLong transferred = new AtomicLong();
    private final AtomicLong missing = new AtomicLong();
    private final AtomicLong skippedExisting = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    public void incFilesScanned() { filesScanned.incrementAndGet(); }
    public void incLinesRead() { linesRead.incrementAndGet(); }
    public void incObjectRefsFound() { objectRefsFound.incrementAndGet(); }
    public void incTransferred() { transferred.incrementAndGet(); }
    public void incMissing() { missing.incrementAndGet(); }
    public void incSkippedExisting() { skippedExisting.incrementAndGet(); }
    public void incFailed() { failed.incrementAndGet(); }

    public long getFilesScanned() { return filesScanned.get(); }
    public long getLinesRead() { return linesRead.get(); }
    public long getObjectRefsFound() { return objectRefsFound.get(); }
    public long getTransferred() { return transferred.get(); }
    public long getMissing() { return missing.get(); }
    public long getSkippedExisting() { return skippedExisting.get(); }
    public long getFailed() { return failed.get(); }

    @Override
    public String toString() {
        return "TransferStats{" +
                "filesScanned=" + filesScanned.get() +
                ", linesRead=" + linesRead.get() +
                ", objectRefsFound=" + objectRefsFound.get() +
                ", transferred=" + transferred.get() +
                ", missing=" + missing.get() +
                ", skippedExisting=" + skippedExisting.get() +
                ", failed=" + failed.get() +
                '}';
    }
}
