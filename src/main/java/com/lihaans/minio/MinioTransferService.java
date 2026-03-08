package com.lihaans.minio;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.ErrorResponse;

import java.io.InputStream;

public class MinioTransferService {
    private final AppConfig config;
    private final MinioClient sourceClient;
    private final MinioClient targetClient;

    public MinioTransferService(AppConfig config) {
        this.config = config;
        this.sourceClient = config.getSource().buildClient();
        this.targetClient = config.getTarget().buildClient();
    }

    public TransferResult transfer(String sourceObjectName) {
        String targetObjectName = buildTargetObjectName(sourceObjectName);
        try {
            if (config.isSkipExistingTarget() && targetExists(targetObjectName)) {
                return TransferResult.skippedExisting(targetObjectName);
            }

            InputStream inputStream = sourceClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(config.getSource().getBucket())
                            .object(sourceObjectName)
                            .build()
            );
            try {
                targetClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(config.getTarget().getBucket())
                                .object(targetObjectName)
                                .stream(inputStream, -1, 10 * 1024 * 1024)
                                .build()
                );
            } finally {
                inputStream.close();
            }
            return TransferResult.transferred(targetObjectName);
        } catch (ErrorResponseException e) {
            ErrorResponse errorResponse = e.errorResponse();
            String code = errorResponse != null ? errorResponse.code() : null;
            if ("NoSuchKey".equals(code) || "NoSuchObject".equals(code) || "NoSuchFile".equals(code)) {
                return TransferResult.missing(sourceObjectName);
            }
            return TransferResult.failed(sourceObjectName, e);
        } catch (Exception e) {
            return TransferResult.failed(sourceObjectName, e);
        }
    }

    private boolean targetExists(String targetObjectName) {
        try {
            targetClient.statObject(
                    StatObjectArgs.builder()
                            .bucket(config.getTarget().getBucket())
                            .object(targetObjectName)
                            .build()
            );
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private String buildTargetObjectName(String sourceObjectName) {
        String prefix = config.getTargetKeyPrefix();
        if (prefix == null || prefix.isEmpty()) {
            return sourceObjectName;
        }
        if (prefix.endsWith("/")) {
            return prefix + sourceObjectName;
        }
        return prefix + "/" + sourceObjectName;
    }

    public static class TransferResult {
        private final String status;
        private final String objectName;
        private final Exception exception;

        private TransferResult(String status, String objectName, Exception exception) {
            this.status = status;
            this.objectName = objectName;
            this.exception = exception;
        }

        public static TransferResult transferred(String objectName) {
            return new TransferResult("TRANSFERRED", objectName, null);
        }

        public static TransferResult missing(String objectName) {
            return new TransferResult("MISSING", objectName, null);
        }

        public static TransferResult skippedExisting(String objectName) {
            return new TransferResult("SKIPPED_EXISTING", objectName, null);
        }

        public static TransferResult failed(String objectName, Exception exception) {
            return new TransferResult("FAILED", objectName, exception);
        }

        public String getStatus() {
            return status;
        }

        public String getObjectName() {
            return objectName;
        }

        public Exception getException() {
            return exception;
        }
    }
}
