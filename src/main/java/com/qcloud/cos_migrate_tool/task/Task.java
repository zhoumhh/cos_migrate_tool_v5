package com.qcloud.cos_migrate_tool.task;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.amazonaws.services.s3.transfer.PersistableUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;
import com.qcloud.cos.utils.Md5Utils;
import com.qcloud.cos_migrate_tool.config.CommonConfig;
import com.qcloud.cos_migrate_tool.record.RecordDb;
import com.qcloud.cos_migrate_tool.record.RecordDb.QUERY_RESULT;
import com.qcloud.cos_migrate_tool.record.RecordElement;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Task implements Runnable {
    private Semaphore semaphore;
    public static final Logger log = LoggerFactory.getLogger(Task.class);
    protected static Semaphore mutex = new Semaphore(1);

    protected TransferManager smallFileTransfer;
    protected TransferManager bigFileTransfer;
    protected long smallFileThreshold;
    private RecordDb recordDb;
    protected CommonConfig config;
    QUERY_RESULT query_result;



    public Task(Semaphore semaphore, CommonConfig config, TransferManager smallFileTransfer,
            TransferManager bigFileTransfer, RecordDb recordDb) {
        super();
        this.semaphore = semaphore;
        this.config = config;
        this.smallFileTransfer = smallFileTransfer;
        this.bigFileTransfer = bigFileTransfer;
        this.smallFileThreshold = config.getSmallFileThreshold();
        this.recordDb = recordDb;
    }

    public boolean isExist(RecordElement recordElement, boolean isCompareValue) {
        
        query_result = recordDb.queryRecord(recordElement);
        if (query_result == RecordDb.QUERY_RESULT.ALL_EQ) {
            String printMsg = String.format("[skip] task_info: %s", recordElement.buildKey());
            System.out.println(printMsg);
            log.info("skip! task_info: [key: {}], [value: {}]", recordElement.buildKey(),
                    recordElement.buildValue());
            return true;
        }

        if (!isCompareValue && (query_result == RecordDb.QUERY_RESULT.VALUE_NOT_EQ)) {
            String printMsg = String.format("[skip] task_info: %s", recordElement.buildKey());
            System.out.println(printMsg);
            log.info("skip! not compare value, task_info: [key: {}], [value: {}]",
                    recordElement.buildKey(), recordElement.buildValue());
            return true;
        }
        
        return false;
    }

    public void saveRecord(RecordElement recordElement) {
        recordDb.saveRecord(recordElement);
    }

    public void saveRequestId(String key, String requestId) {
        recordDb.saveRequestId(key, requestId);
    }

    private void printTransferProgress(TransferProgress progress, String key) {
        long byteSent = progress.getBytesTransferred();
        long byteTotal = progress.getTotalBytesToTransfer();
        double pct = 100.0;
        if (byteTotal != 0) {
            pct = progress.getPercentTransferred();
        }
        String printMsg = String.format(
                "[UploadInProgress] [key: %s] [byteSent/ byteTotal/ percentage: %d/ %d/ %.2f%%]",
                key, byteSent, byteTotal, pct);
        log.info(printMsg);
        System.out.println(printMsg);
    }

    public String showTransferProgressAndGetRequestId(Upload upload, boolean multipart, String key,
            long mtime) throws InterruptedException {
        boolean pointSaveFlag = false;
        long printCount = 0;
        TransferProgress progress = upload.getProgress();
        do {
            ++printCount;
            Thread.sleep(100);

            long byteSent = progress.getBytesTransferred();
            if (printCount % 20 == 0) {
                printTransferProgress(progress, key);
            }

        } while (upload.isDone() == false);
        // 结束后在打印下进度
        printTransferProgress(progress, key);
        UploadResult uploadResult = upload.waitForUploadResult();
        return "null";
    }

    private boolean isMultipartUploadIdValid(String bucketName, String cosKey, String uploadId) {
        ListPartsRequest listPartsRequest = new ListPartsRequest(bucketName, cosKey, uploadId);
        try {
            this.bigFileTransfer.getAmazonS3Client().listParts(listPartsRequest);
            return true;
        } catch (AmazonServiceException e) {
            return false;
        }

    }

    private String uploadBigFile(PutObjectRequest putObjectRequest) throws InterruptedException {
        String bucketName = putObjectRequest.getBucketName();
        String cosKey = putObjectRequest.getKey();
        String localPath = putObjectRequest.getFile().getAbsolutePath();
        long mtime = putObjectRequest.getFile().lastModified();
        long partSize = this.bigFileTransfer.getConfiguration().getMinimumUploadPartSize();
        long mutlipartUploadThreshold =
                this.bigFileTransfer.getConfiguration().getMultipartUploadThreshold();

        String multipartId = this.recordDb.queryMultipartUploadSavePoint(bucketName, cosKey,
                localPath, mtime, partSize, mutlipartUploadThreshold);
        Upload upload = null;
        // 如果multipartId不为Null, 则表示存在断点, 使用续传.
        if (multipartId != null && isMultipartUploadIdValid(bucketName, cosKey, multipartId)) {
            PersistableUpload persistableUpload = new PersistableUpload(bucketName, cosKey,
                    localPath, multipartId, partSize, mutlipartUploadThreshold);
            upload = this.bigFileTransfer.resumeUpload(persistableUpload);
        } else {
            upload = this.bigFileTransfer.upload(putObjectRequest);
        }
        return showTransferProgressAndGetRequestId(upload, true, cosKey, mtime);
    }



    private String uploadSmallFile(PutObjectRequest putObjectRequest) throws InterruptedException {
        Upload upload = smallFileTransfer.upload(putObjectRequest);
        File file = putObjectRequest.getFile();
        long mtime = 0;
        if(file != null){
            mtime = file.lastModified();
        }
        return showTransferProgressAndGetRequestId(upload, false, putObjectRequest.getKey(), mtime);
    }


    public String uploadFile(String bucketName, String cosPath, File localFile,
            StorageClass storageClass, boolean entireMd5Attached, ObjectMetadata objectMetadata,
            AccessControlList acl) throws Exception {
        PutObjectRequest putObjectRequest;
        // Adjust for S3 SDK
        while (cosPath.startsWith("/")) {
            cosPath = cosPath.substring(1);
        }
        if(localFile.isDirectory()) {
            if(!cosPath.endsWith("/")) {
                cosPath += "/";
            }
            byte[] contentByteArray = new byte[0];
            objectMetadata.setContentType("application/x-directory");
            objectMetadata.setContentLength(0);
            putObjectRequest = new PutObjectRequest(bucketName, cosPath, new ByteArrayInputStream(contentByteArray),
                    objectMetadata);
        } else {
            putObjectRequest = new PutObjectRequest(bucketName, cosPath, localFile);
        }
        putObjectRequest.setStorageClass(storageClass);

        if (acl != null) {
            putObjectRequest.setAccessControlList(acl);
        }

        if (entireMd5Attached && !localFile.isDirectory()) {
            String md5 = Md5Utils.md5Hex(localFile);
            String upyunTag = objectMetadata.getUserMetaDataOf("upyun-etag");
            if (upyunTag != null) {
                if (!md5.equalsIgnoreCase(upyunTag)) {
                    String exceptionMsg = String.format("md5 is not match upyun[%s] local[%s]",
                            upyunTag, md5);
                    throw new Exception(exceptionMsg);
                }
            }
            objectMetadata.addUserMetadata("md5", md5);
        }

        if (config.getEncryptionType().equals("sse-cos")) {
            objectMetadata.setServerSideEncryption("AES256");
        }

        putObjectRequest.setMetadata(objectMetadata);
        int retryTime = 0;
        final int maxRetry = 5;

        while (retryTime < maxRetry) {
            try {
                String requestId;
                if (localFile.length() >= smallFileThreshold) {
                    requestId = uploadBigFile(putObjectRequest);
                } else {
                    requestId = uploadSmallFile(putObjectRequest);
                }
                
                return requestId;
            } catch (Exception e) {
                log.warn("upload failed, ready to retry. retryTime:" + retryTime, e);
                ++retryTime;
                if (retryTime >= maxRetry) {
                    throw e;
                } else {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(200, 1000));
                }
            }
        }
        return null;
    }

    public boolean isExistOnCOS(TransferManager transferManager, RecordElement recordElement, String bucketName, String cosPath) {
        try {
            transferManager.getAmazonS3Client().getObjectMetadata(bucketName, cosPath);
            String printMsg = String.format("[skip] file on cos, task_info: %s", recordElement.buildKey());
            System.out.println(printMsg);
            log.info("skip! file on cos, task_info: [key: {}], [value: {}]", recordElement.buildKey(),
                    recordElement.buildValue());
            return true;
        } catch (AmazonServiceException e) {
            if (e.getStatusCode() == 404) {
                return false;
            } else if (e.getStatusCode() == 503) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }

            throw e;
        }
    }

    public abstract void doTask();

    private void checkTimeWindows() throws InterruptedException {
        int timeWindowBegin = config.getTimeWindowBegin();
        int timeWindowEnd = config.getTimeWindowEnd();
        while (true) {
            DateTime dateTime = DateTime.now();
            int minuteOfDay = dateTime.getMinuteOfDay();
            if (minuteOfDay >= timeWindowBegin && minuteOfDay <= timeWindowEnd) {
                return;
            }

            if (mutex.tryAcquire()) {
                String printTips = String.format(
                        "currentTime %s, wait next time window [%02d:%02d, %02d:%02d]",
                        dateTime.toString("yyyy-MM-dd HH:mm:ss"), timeWindowBegin / 60,
                        timeWindowBegin % 60, timeWindowEnd / 60, timeWindowEnd % 60);
                System.out.println(printTips);
                System.out.println(
                        "---------------------------------------------------------------------");
                log.info(printTips);
                Thread.sleep(60000);
                mutex.release();
            } else {
                Thread.sleep(60000);
            }
        }
    }

    public void run() {
        try {
            checkTimeWindows();
            doTask();
        } catch (InterruptedException e) {
            log.error("task is interrupted", e);
        } catch (Exception e) {
            log.error("unknown exception occur", e);
        } finally {
            semaphore.release();
        }
    }
}
