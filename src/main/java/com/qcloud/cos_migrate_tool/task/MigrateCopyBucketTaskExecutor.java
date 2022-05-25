package com.qcloud.cos_migrate_tool.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.util.SdkHttpUtils;
import com.qcloud.cos.utils.UrlEncoderUtils;
import com.qcloud.cos_migrate_tool.config.CopyBucketConfig;
import com.qcloud.cos_migrate_tool.config.MigrateType;
import com.qcloud.cos_migrate_tool.meta.TaskStatics;
import com.qcloud.cos_migrate_tool.utils.SystemUtils;
import com.qcloud.cos_migrate_tool.utils.VersionInfoUtils;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrateCopyBucketTaskExecutor extends TaskExecutor {
    private static final Logger log = LoggerFactory.getLogger(MigrateCopyBucketTaskExecutor.class);

    private AmazonS3 srcCosClient;
    private String srcRegion;
    private String srcBucketName;
    private String srcCosPath;
    private String srcFileList;

    public MigrateCopyBucketTaskExecutor(CopyBucketConfig config) {
        super(MigrateType.MIGRATE_FROM_COS_BUCKET_COPY, config);
        String src_token = ((CopyBucketConfig) config).getSrcToken();
        AWSCredentials srcCred = null;
        if (src_token != null && !src_token.isEmpty()) {
            log.info("Use temporary token to list object");
            System.out.println("Use temporary token to list object");
            srcCred = new BasicSessionCredentials(config.getSrcAk(), config.getSrcSk(), src_token);
        } else {
            srcCred = new BasicAWSCredentials(config.getSrcAk(), config.getSrcSk());
        }
        ClientConfiguration clientConfig = new ClientConfiguration();
        if (config.isEnableHttps()) {
            clientConfig.setProtocol(Protocol.HTTPS);
        } else {
            clientConfig.setProtocol(Protocol.HTTP);
        }

        clientConfig.setConnectionTimeout(5000);
        clientConfig.setSocketTimeout(5000);

        String endpoint;
        if (config.getSrcEndpointSuffix() != null) {
            endpoint = config.getSrcEndpointSuffix();
        } else {
            endpoint = "cos." + config.getRegion() + ".myqcloud.com";
        }

        this.srcCosClient = AmazonS3ClientBuilder.standard()
                .disableChunkedEncoding()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, config.getSrcRegion()))
                .withCredentials(new AWSStaticCredentialsProvider(srcCred))
                .withClientConfiguration(clientConfig)
                .build();

        this.srcRegion = config.getSrcRegion();
        this.srcBucketName = config.getSrcBucket();
        this.srcCosPath = config.getSrcCosPath();
        // For S3 SDK
        while (this.srcCosPath.startsWith("/")) {
            this.srcCosPath = this.srcCosPath.substring(1);
        }
        this.srcFileList = config.getSrcFileList();
    }

    @Override
    protected String buildTaskDbComment() {
        String comment = String.format(
                "[time: %s], [destRegion: %s], [destBucketName: %s], [destCosFolder: %s], [srcRegion: %s], [srcFileList:%s], [srcBucketName: %s], [srcFolder: %s], [smallTaskExecutor: %d]\n",
                SystemUtils.getCurrentDateTime(), config.getRegion(), config.getBucketName(),
                config.getCosPath(), srcRegion, srcFileList, srcBucketName, srcCosPath,
                this.smallFileUploadExecutorNum);
        return comment;
    }

    @Override
    protected String buildTaskDbFolderPath() {
        String temp = String.format(
                "[destCosFolder: %s], [srcRegion: %s], [srcBucket: %s], [srcCosFolder: %s]",
                config.getCosPath(), ((CopyBucketConfig) config).getSrcRegion(),
                ((CopyBucketConfig) config).getSrcBucket(),
                ((CopyBucketConfig) config).getSrcCosPath());
        String dbFolderPath = String.format("db/migrate_copy_bucket/%s/%s", config.getBucketName(),
                DigestUtils.md5Hex(temp));
        return dbFolderPath;
    }

    @Override
    public void buildTask() {

        int lastDelimiter = srcCosPath.lastIndexOf("/") + 1;

        if (!srcFileList.isEmpty()) {
            File file = new File(srcFileList);
            if (!file.isFile() || !file.exists()) {
                String printMsg = String.format("file[%s] not exist or not file", srcFileList);
                log.error(printMsg);
                System.out.println(printMsg);
            }

            InputStreamReader read = null;
            try {
                read = new InputStreamReader(new FileInputStream(file));
            } catch (FileNotFoundException e1) {
                e1.printStackTrace();
                return;
            }

            BufferedReader bufferedReader = new BufferedReader(read);
            String srcKey = null;

            try {
                while ((srcKey = bufferedReader.readLine()) != null) {

                    srcKey = UrlEncoderUtils.urlDecode(srcKey);

                    String copyDestKey = null;
                    if (srcKey.startsWith("/")) {
                        copyDestKey = config.getCosPath() + srcKey.substring(1);
                        srcKey = srcKey.substring(1);
                    } else {
                        copyDestKey = config.getCosPath() + srcKey;
                    }

                    // set storage class to Standard just for a non-null value needed
                    // no side effect
                    MigrateCopyBucketTask task =
                            new MigrateCopyBucketTask(semaphore, (CopyBucketConfig) config,
                                    smallFileTransferManager, bigFileTransferManager, recordDb,
                                    srcCosClient, srcKey, 0, "", StorageClass.Standard, copyDestKey);

                    AddTask(task);

                }

                TaskStatics.instance.setListFinished(true);

            } catch (IOException e) {
                log.error(e.toString());
                TaskStatics.instance.setListFinished(false);
                e.printStackTrace();
            } catch (Exception e) {
                log.error(e.toString());
                e.printStackTrace();
                TaskStatics.instance.setListFinished(false);
            }

            try {
                bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                read.close();
            } catch (IOException e) {
                e.printStackTrace();
            }


        } else {
            String nextMarker = "";
            String[] progress = this.recordDb.getListProgress();
            if (config.isResume() && progress != null) {
                nextMarker = progress[1];
            }

            ListObjectsRequest listObjectsRequest =
                    new ListObjectsRequest(srcBucketName, srcCosPath, nextMarker, null, 1000);

            ObjectListing objectListing;
            int retry_num = 0;

            do {
                try {
                    while (true) {
                        listObjectsRequest.setMarker(nextMarker);
                        objectListing = srcCosClient.listObjects(listObjectsRequest);
                        List<S3ObjectSummary> cosObjectSummaries =
                                objectListing.getObjectSummaries();

                        for (S3ObjectSummary cosObjectSummary : cosObjectSummaries) {
                            String srcKey = cosObjectSummary.getKey();
                            String srcEtag = cosObjectSummary.getETag();
                            long srcSize = cosObjectSummary.getSize();
                            String keyName = srcKey.substring(lastDelimiter);
                            String copyDestKey;
                            if (config.getCosPath().length() == 0 || config.getCosPath().endsWith("/")) {
                                copyDestKey = config.getCosPath() + keyName;
                            } else {
                                copyDestKey = config.getCosPath() + "/" + keyName;
                            }

                            MigrateCopyBucketTask task = new MigrateCopyBucketTask(semaphore,
                                    (CopyBucketConfig) config, smallFileTransferManager,
                                    bigFileTransferManager, recordDb, srcCosClient, srcKey, srcSize,
                                    srcEtag, StorageClass.fromValue(cosObjectSummary.getStorageClass()),
                                    copyDestKey);

                            AddTask(task);
                        }
                        nextMarker = objectListing.getNextMarker();
                        if (nextMarker != null) {
                            this.recordDb.saveListProgress(srcCosPath, nextMarker);
                        }
                        if (!objectListing.isTruncated()) {
                            break;
                        }
                    }

                    TaskStatics.instance.setListFinished(true);

                    return;

                } catch (Exception e) {
                    log.error("List cos bucket occur a exception:{}", e.toString());
                    TaskStatics.instance.setListFinished(false);
                }

                ++retry_num;

            } while (retry_num < 20);
        }

    }

    @Override
    public void waitTaskOver() {
        super.waitTaskOver();
        srcCosClient.shutdown();
    }

}
