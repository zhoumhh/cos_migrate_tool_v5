package com.qcloud.cos_migrate_tool.task;

import java.io.File;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.model.CopyResult;
import com.qcloud.cos_migrate_tool.config.CopyBucketConfig;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.record.MigrateCopyBucketRecordElement;
import com.qcloud.cos_migrate_tool.record.RecordDb;

public class MigrateCopyBucketTask extends Task {
    private final AmazonS3 srcCOSClient;
    private final String destRegion;
    private final String destBucketName;
    private final String destKey;
    private final String srcRegion;
    private final String srcEndpointSuffx;
    private final String srcBucketName;
    private final String srcKey;
    private final StorageClass srcStorageClass;
    private long srcSize;
    private String srcEtag;
    private CopyBucketConfig copyBucketConfig;

    public MigrateCopyBucketTask(Semaphore semaphore, CopyBucketConfig config,
            TransferManager smallFileTransfer, TransferManager bigFileTransfer, RecordDb recordDb,
            AmazonS3 srcCOSClient, String srcKey, long srcSize, String srcEtag, StorageClass srcStorageClass,
            String destKey) {
        super(semaphore, config, smallFileTransfer, bigFileTransfer, recordDb);
        this.srcCOSClient = srcCOSClient;
        this.destRegion = config.getRegion();
        this.destBucketName = config.getBucketName();
        while (destKey.startsWith("/")) {
            destKey = destKey.substring(1);
        }
        this.destKey = destKey;
        this.srcRegion = config.getSrcRegion();
        this.srcEndpointSuffx = config.getSrcEndpointSuffix();
        this.srcBucketName = config.getSrcBucket();
        this.srcKey = srcKey;
        this.srcStorageClass = srcStorageClass;
        this.srcSize = srcSize;
        this.srcEtag = srcEtag;
        this.copyBucketConfig = (CopyBucketConfig)config;
    }


    private void transferFileForNotAllowedCopyObject(MigrateCopyBucketRecordElement copyElement) {
        String downloadTempPath =
                config.getTempFolderPath() + ThreadLocalRandom.current().nextLong();
        File downloadTempFile = new File(downloadTempPath);
        try {
            ObjectMetadata objectMetadata = srcCOSClient
                    .getObject(new GetObjectRequest(srcBucketName, srcKey), downloadTempFile);
            String requestId = uploadFile(destBucketName, destKey, downloadTempFile,
                    config.getStorageClass(), config.isEntireFileMd5Attached(), objectMetadata, null);
            saveRecord(copyElement);
            saveRequestId(destKey, requestId);
            if (this.query_result == RecordDb.QUERY_RESULT.KEY_NOT_EXIST) {
                TaskStatics.instance.addSuccessCnt();
            } else {
                TaskStatics.instance.addUpdateCnt();
            }
            String printMsg = String.format("[ok] [requestid: %s], task_info: %s",
                    requestId == null ? "NULL" : requestId,
                    copyElement.buildKey());
            System.out.println(printMsg);
            log.info(printMsg);
        } catch (Exception e) {
            String printMsg = String.format("[fail] task_info: %s",
                    copyElement.buildKey());
            System.out.println(printMsg);
            log.error("fail! task_info: [key: {}], [value: {}], exception: {}",
                    copyElement.buildKey(),
                    copyElement.buildValue(), e.toString());
            TaskStatics.instance.addFailCnt();
        }
    }


    @Override
    public void doTask() {
        
        if (srcEtag.isEmpty()) {
            ObjectMetadata objectMetadata = srcCOSClient.getObjectMetadata(srcBucketName, srcKey);
            srcEtag = objectMetadata.getETag();
            this.srcSize = objectMetadata.getContentLength();
        }
 
        MigrateCopyBucketRecordElement migrateCopyBucketRecordElement =
                new MigrateCopyBucketRecordElement(destRegion, destBucketName, destKey, srcRegion,
                        srcBucketName, srcKey, srcSize, srcEtag, srcStorageClass.toString());
        if (isExist(migrateCopyBucketRecordElement, true)) {
            TaskStatics.instance.addSkipCnt();
            return;
        }

        if (copyBucketConfig.getSrcStorageClass() != null) {
            if (!srcStorageClass.toString().equalsIgnoreCase(copyBucketConfig.getSrcStorageClass().toString())) {
                TaskStatics.instance.addSkipCnt();
                log.info("[skip] file [{}], storage class: [{}]", srcKey, srcStorageClass);
                return;
            }
        }

        if (config.skipSamePath()) {
            try {
                if (isExistOnCOS(smallFileTransfer, migrateCopyBucketRecordElement, destBucketName, destKey)) {
                    TaskStatics.instance.addSkipCnt();
                    return;
                }
            } catch (Exception e) {
                String printMsg = String.format("[fail] task_info: %s", migrateCopyBucketRecordElement.buildKey());
                System.err.println(printMsg);
                log.error("[fail] task_info: {}, exception: {}", migrateCopyBucketRecordElement.buildKey(), e.toString());
                TaskStatics.instance.addFailCnt();
                return;
            }
        }

        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(srcBucketName, srcKey, destBucketName, destKey);

        if (srcBucketName.equals(destBucketName) && ( destKey.equals("/" + srcKey) || srcKey.equals(destKey)) ) {
            ObjectMetadata newObjectMetadata = new ObjectMetadata();
            newObjectMetadata.addUserMetadata("x-amz-metadata-directive", "Replaced");
            copyObjectRequest.setNewObjectMetadata(newObjectMetadata);
        }
        
        copyObjectRequest.setStorageClass(this.config.getStorageClass());
        
        try {
            Copy copy = smallFileTransfer.copy(copyObjectRequest, srcCOSClient, null);
            CopyResult copyResult = copy.waitForCopyResult();
            String requestId = "null";
            saveRecord(migrateCopyBucketRecordElement);
            saveRequestId(destKey, requestId);
            if (this.query_result == RecordDb.QUERY_RESULT.KEY_NOT_EXIST) {
                TaskStatics.instance.addSuccessCnt();
            } else {
                TaskStatics.instance.addUpdateCnt();
            }
            String printMsg = String.format("[ok] [requestid: %s], task_info: %s",
                    requestId == null ? "NULL" : requestId,
                    migrateCopyBucketRecordElement.buildKey());
            System.out.println(printMsg);
            log.info(printMsg);
        } catch (Exception e) {
            if (e instanceof AmazonServiceException) {
                if (((AmazonServiceException) e).getStatusCode() == 405) {
                    log.info(
                            "try to transfer file for not allowed copy object, task_info: [key: {}], [value: {}], exception: {}",
                            migrateCopyBucketRecordElement.buildKey(),
                            migrateCopyBucketRecordElement.buildValue(), e.toString());
                    transferFileForNotAllowedCopyObject(migrateCopyBucketRecordElement);
                    return;
                }
            }
            String printMsg = String.format("[fail] task_info: %s",
                    migrateCopyBucketRecordElement.buildKey());
            System.out.println(printMsg);
            log.error("fail! task_info: [key: {}], [value: {}], exception: {}",
                    migrateCopyBucketRecordElement.buildKey(),
                    migrateCopyBucketRecordElement.buildValue(), e.toString());
            TaskStatics.instance.addFailCnt();
        }
    }


}
