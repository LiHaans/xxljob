package com.lihaans.minio;

public class TransferStats {
    private long filesScanned;
    private long linesRead;
    private long objectRefsFound;
    private long transferred;
    private long missing;
    private long skippedExisting;
    private long failed;

    public void incFilesScanned() { filesScanned++; }
    public void incLinesRead() { linesRead++; }
    public void incObjectRefsFound() { objectRefsFound++; }
    public void incTransferred() { transferred++; }
    public void incMissing() { missing++; }
    public void incSkippedExisting() { skippedExisting++; }
    public void incFailed() { failed++; }

    @Override
    public String toString() {
        return "TransferStats{" +
                "filesScanned=" + filesScanned +
                ", linesRead=" + linesRead +
                ", objectRefsFound=" + objectRefsFound +
                ", transferred=" + transferred +
                ", missing=" + missing +
                ", skippedExisting=" + skippedExisting +
                ", failed=" + failed +
                '}';
    }
}
