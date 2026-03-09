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
                return TransferResult.skippedExisting(sourceObjectName, targetObjectName);
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
            return TransferResult.transferred(sourceObjectName, targetObjectName);
        } catch (ErrorResponseException e) {
            ErrorResponse errorResponse = e.errorResponse();
            String code = errorResponse != null ? errorResponse.code() : null;
            if ("NoSuchKey".equals(code) || "NoSuchObject".equals(code) || "NoSuchFile".equals(code)) {
                return TransferResult.missing(sourceObjectName, targetObjectName);
            }
            return TransferResult.failed(sourceObjectName, targetObjectName, e);
        } catch (Exception e) {
            return TransferResult.failed(sourceObjectName, targetObjectName, e);
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
        private final String sourceObjectName;
        private final String targetObjectName;
        private final Exception exception;

        private TransferResult(String status, String sourceObjectName, String targetObjectName, Exception exception) {
            this.status = status;
            this.sourceObjectName = sourceObjectName;
            this.targetObjectName = targetObjectName;
            this.exception = exception;
        }

        public static TransferResult transferred(String sourceObjectName, String targetObjectName) {
            return new TransferResult("TRANSFERRED", sourceObjectName, targetObjectName, null);
        }

        public static TransferResult missing(String sourceObjectName, String targetObjectName) {
            return new TransferResult("MISSING", sourceObjectName, targetObjectName, null);
        }

        public static TransferResult skippedExisting(String sourceObjectName, String targetObjectName) {
            return new TransferResult("SKIPPED_EXISTING", sourceObjectName, targetObjectName, null);
        }

        public static TransferResult failed(String sourceObjectName, String targetObjectName, Exception exception) {
            return new TransferResult("FAILED", sourceObjectName, targetObjectName, exception);
        }

        public String getStatus() {
            return status;
        }

        public String getSourceObjectName() {
            return sourceObjectName;
        }

        public String getTargetObjectName() {
            return targetObjectName;
        }

        public Exception getException() {
            return exception;
        }
    }
}
