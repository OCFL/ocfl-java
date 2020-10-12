/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package edu.wisc.library.ocfl.ibm;

import com.ibm.cloud.objectstorage.SdkClientException;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.model.AmazonS3Exception;
import com.ibm.cloud.objectstorage.services.s3.model.DeleteObjectsRequest;
import com.ibm.cloud.objectstorage.services.s3.model.GetObjectRequest;
import com.ibm.cloud.objectstorage.services.s3.model.HeadBucketRequest;
import com.ibm.cloud.objectstorage.services.s3.model.ListObjectsV2Request;
import com.ibm.cloud.objectstorage.services.s3.model.ListObjectsV2Result;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cloud.objectstorage.services.s3.model.PutObjectRequest;
import com.ibm.cloud.objectstorage.services.s3.transfer.TransferManager;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.storage.cloud.CloudClient;
import edu.wisc.library.ocfl.core.storage.cloud.CloudObjectKey;
import edu.wisc.library.ocfl.core.storage.cloud.KeyNotFoundException;
import edu.wisc.library.ocfl.core.storage.cloud.ListResult;
import edu.wisc.library.ocfl.core.util.UncheckedFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Client implementation for IBM's Cloud Object Storage. Note, COS is unable to handle common special characters in object
 * keys. To get around this problem, keys are URL encoded and decoded going in and out of this class.
 */
public class IbmCosClient implements CloudClient {

    private static final Logger LOG = LoggerFactory.getLogger(IbmCosClient.class);

    private static final String NO_SUCH_KEY = "NoSuchKey";

    private static final int KB = 1024;
    private static final int MB = 1024 * KB;
    private static final long GB = 1024 * MB;
    private static final long TB = 1024 * GB;

    private static final long MAX_FILE_BYTES = 5 * TB;
    private static final int MAX_PART_BYTES = 100 * MB;

    // TODO experiment with performance with async client
    private final AmazonS3 s3Client;
    private final TransferManager transferManager;
    private final String bucket;
    private final String repoPrefix;
    private final CloudObjectKey.Builder keyBuilder;

    /**
     * Used to create a new IbmCosClient instance.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @see IbmCosClient#builder()
     *
     * @param s3Client cos client
     * @param transferManager transfer manager
     * @param bucket s3 bucket
     */
    public IbmCosClient(AmazonS3 s3Client, TransferManager transferManager, String bucket) {
        this(s3Client, transferManager, bucket, "");
    }

    /**
     * @see IbmCosClient#builder()
     *
     * @param s3Client cos client
     * @param transferManager transfer manager
     * @param bucket s3 bucket
     * @param prefix key prefix
     */
    public IbmCosClient(AmazonS3 s3Client, TransferManager transferManager, String bucket, String prefix) {
        this.s3Client = Enforce.notNull(s3Client, "s3Client cannot be null");
        this.transferManager = Enforce.notNull(transferManager, "transferManager cannot be null");
        this.bucket = Enforce.notBlank(bucket, "bucket cannot be blank");
        this.repoPrefix = sanitizeRepoPrefix(Enforce.notNull(prefix, "prefix cannot be null"));
        this.keyBuilder = CloudObjectKey.builder().prefix(repoPrefix);
    }

    private static String sanitizeRepoPrefix(String repoPrefix) {
        return repoPrefix.substring(0, indexLastNonSlash(repoPrefix));
    }

    private static int indexLastNonSlash(String string) {
        for (int i = string.length(); i > 0; i--) {
            if (string.charAt(i - 1) != '/') {
                return i;
            }
        }
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String bucket() {
        return bucket;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String prefix() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    public CloudObjectKey uploadFile(Path srcPath, String dstPath) {
        return uploadFile(srcPath, dstPath, null);
    }

    /**
     * {@inheritDoc}
     */
    public CloudObjectKey uploadFile(Path srcPath, String dstPath, byte[] md5digest) {
        var dstKey = keyBuilder.buildFromPath(dstPath);
        var fileSize = UncheckedFiles.size(srcPath);

        LOG.debug("Uploading {} to bucket {} key {} size {}", srcPath, bucket, dstKey, fileSize);

        if (fileSize >= MAX_FILE_BYTES) {
            throw new IllegalArgumentException(String.format("Cannot store file %s because it exceeds the maximum file size.", srcPath));
        }

        var metadata = new ObjectMetadata();

        var request = new PutObjectRequest(bucket, dstKey.getKey(), srcPath.toFile()).withMetadata(metadata);

        if (fileSize > MAX_PART_BYTES) {
            try {
                transferManager.upload(request).waitForUploadResult();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            s3Client.putObject(request);
        }

        return dstKey;
    }

    /**
     * {@inheritDoc}
     */
    public CloudObjectKey uploadBytes(String dstPath, byte[] bytes, String contentType) {
        var dstKey = keyBuilder.buildFromPath(dstPath);
        LOG.debug("Writing string to bucket {} key {}", bucket, dstKey);

        var metadata = new ObjectMetadata();
        metadata.setContentType(contentType);
        metadata.setContentLength(bytes.length);

        s3Client.putObject(new PutObjectRequest(bucket, dstKey.getKey(), new ByteArrayInputStream(bytes), metadata));

        return dstKey;
    }

    /**
     * {@inheritDoc}
     */
    public CloudObjectKey copyObject(String srcPath, String dstPath) {
        var srcKey = keyBuilder.buildFromPath(srcPath);
        var dstKey = keyBuilder.buildFromPath(dstPath);

        LOG.debug("Copying {} to {} in bucket {}", srcKey, dstKey, bucket);

        try {
            s3Client.copyObject(bucket, srcKey.getKey(), bucket, dstKey.getKey());
        } catch (AmazonS3Exception e) {
            if (NO_SUCH_KEY.equals(e.getErrorCode())) {
                throw new KeyNotFoundException(e);
            }
            throw e;
        } catch (SdkClientException e) {
            // TODO verify class and message
            if (e.getMessage().contains("copy source is larger than the maximum allowable size")) {
                transferManager.copy(bucket, srcKey.getKey(), bucket, dstKey.getKey());
            } else {
                throw e;
            }
        }

        return dstKey;
    }

    /**
     * {@inheritDoc}
     */
    public Path downloadFile(String srcPath, Path dstPath) {
        var srcKey = keyBuilder.buildFromPath(srcPath);

        LOG.debug("Downloading bucket {} key {} to {}", bucket, srcKey, dstPath);

        try {
            s3Client.getObject(new GetObjectRequest(bucket, srcKey.getKey()), dstPath.toFile());
        } catch (AmazonS3Exception e) {
            if (NO_SUCH_KEY.equals(e.getErrorCode())) {
                throw new KeyNotFoundException(e);
            }
            throw e;
        }

        return dstPath;
    }

    /**
     * {@inheritDoc}
     */
    public InputStream downloadStream(String srcPath) {
        var srcKey = keyBuilder.buildFromPath(srcPath);

        LOG.debug("Streaming bucket {} key {}", bucket, srcKey);

        try {
            return s3Client.getObject(bucket, srcKey.getKey()).getObjectContent();
        } catch (AmazonS3Exception e) {
            if (NO_SUCH_KEY.equals(e.getErrorCode())) {
                throw new KeyNotFoundException(String.format("Key %s not found in bucket %s.", srcKey, bucket), e);
            }
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    public String downloadString(String srcPath) {
        try (var stream = downloadStream(srcPath)) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public ListResult list(String prefix) {
        var prefixedPrefix = keyBuilder.buildFromPath(prefix);
        return toListResult(s3Client.listObjectsV2(bucket, prefixedPrefix.getKey()));
    }

    /**
     * {@inheritDoc}
     */
    public ListResult listDirectory(String path) {
        var prefix = keyBuilder.buildFromPath(path).getKey();

        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix = prefix + "/";
        }

        LOG.debug("Listing directory {} in bucket {}", prefix, bucket);

        return toListResult(s3Client.listObjectsV2(new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(prefix)
                .withDelimiter("/")));
    }

    /**
     * {@inheritDoc}
     */
    public void deletePath(String path) {
        LOG.debug("Deleting path {} in bucket {}", path, bucket);

        var keys = list(path).getObjects().stream()
                .map(ListResult.ObjectListing::getKey)
                .collect(Collectors.toList());

        deleteObjectsInternal(keys);
    }

    /**
     * {@inheritDoc}
     */
    public void deleteObjects(Collection<String> objectPaths) {
        if (!objectPaths.isEmpty()) {
            var objectKeys = objectPaths.stream().map(keyBuilder::buildFromPath)
                    .collect(Collectors.toList());
            deleteObjectsInternal(objectKeys);
        }
    }

    private void deleteObjectsInternal(Collection<CloudObjectKey> objectKeys) {
        LOG.debug("Deleting objects in bucket {}: {}", bucket, objectKeys);

        if (!objectKeys.isEmpty()) {
            s3Client.deleteObjects(new DeleteObjectsRequest(bucket)
                    .withKeys(objectKeys.stream().map(CloudObjectKey::getKey).toArray(String[]::new)));
        }
    }

    /**
     * {@inheritDoc}
     */
    public void safeDeleteObjects(String... objectKeys) {
        safeDeleteObjects(Arrays.asList(objectKeys));
    }

    /**
     * {@inheritDoc}
     */
    public void safeDeleteObjects(Collection<String> objectKeys) {
        try {
            deleteObjects(objectKeys);
        } catch (RuntimeException e) {
            LOG.error("Failed to cleanup objects in bucket {}: {}", bucket, objectKeys, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean bucketExists() {
        try {
            s3Client.headBucket(new HeadBucketRequest(bucket));
            return true;
        } catch (AmazonS3Exception e) {
            if ("404 Not Found".equals(e.getErrorCode()) || "NoSuchBucket".equals(e.getErrorCode())) {
                return false;
            }
            throw e;
        }
    }

    private ListResult toListResult(ListObjectsV2Result s3Result) {
        var prefixLength = s3Result.getPrefix() == null ? 0 : s3Result.getPrefix().length();
        var repoPrefixLength = repoPrefix.isBlank() ? 0 : repoPrefix.length() + 1;

        var objects = s3Result.getObjectSummaries().stream().map(o -> {
            var key = o.getKey();
            return new ListResult.ObjectListing()
                    .setKey(keyBuilder.buildFromKey(key))
                    .setKeySuffix(key.substring(prefixLength));
        }).collect(Collectors.toList());

        var dirs = s3Result.getCommonPrefixes().stream().map(path -> {
            return new ListResult.DirectoryListing()
                    .setPath(path.substring(repoPrefixLength));
        }).collect(Collectors.toList());

        return new ListResult()
                .setObjects(objects)
                .setDirectories(dirs);
    }

    public static class Builder {

        private AmazonS3 s3Client;
        private TransferManager transferManager;
        private String bucket;
        private String repoPrefix;

        /**
         * The IBM CoS client
         *
         * @param s3Client ibm cos client
         * @return builder
         */
        public Builder s3Client(AmazonS3 s3Client) {
            this.s3Client = Enforce.notNull(s3Client, "s3Client cannot be null");
            return this;
        }

        /**
         * The IBM CoS transfer manager
         *
         * @param transferManager transfer manager
         * @return builder
         */
        public Builder transferManager(TransferManager transferManager) {
            this.transferManager = Enforce.notNull(transferManager, "transferManager cannot be null");
            return this;
        }

        /**
         * The S3 bucket to use. Required.
         *
         * @param bucket s3 bucket
         * @return builder
         */
        public Builder bucket(String bucket) {
            this.bucket = Enforce.notBlank(bucket, "bucket cannot be blank");
            return this;
        }

        /**
         * The key prefix to use for the repository. Optional.
         *
         * @param repoPrefix key prefix
         * @return builder
         */
        public Builder repoPrefix(String repoPrefix) {
            this.repoPrefix = repoPrefix;
            return this;
        }

        public IbmCosClient build() {
            var prefix = repoPrefix == null ? "" : repoPrefix;
            return new IbmCosClient(s3Client, transferManager, bucket, prefix);
        }

    }

}
