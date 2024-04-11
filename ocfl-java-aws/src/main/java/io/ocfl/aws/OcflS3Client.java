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

package io.ocfl.aws;

import io.ocfl.api.OcflRepository;
import io.ocfl.api.exception.OcflIOException;
import io.ocfl.api.util.Enforce;
import io.ocfl.core.storage.cloud.CloudClient;
import io.ocfl.core.storage.cloud.CloudObjectKey;
import io.ocfl.core.storage.cloud.HeadResult;
import io.ocfl.core.storage.cloud.KeyNotFoundException;
import io.ocfl.core.storage.cloud.ListResult;
import io.ocfl.core.util.UncheckedFiles;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * CloudClient implementation that uses Amazon's S3 synchronous v2 client
 */
public class OcflS3Client implements CloudClient {

    private static final Logger LOG = LoggerFactory.getLogger(OcflS3Client.class);

    private static final long EIGHT_MB = 8 * 1024 * 1024;

    private final S3AsyncClient s3Client;
    private final S3TransferManager transferManager;
    private final String bucket;
    private final String repoPrefix;
    private final CloudObjectKey.Builder keyBuilder;

    private final BiConsumer<String, PutObjectRequest.Builder> putObjectModifier;

    /**
     * Used to create a new OcflS3Client instance.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @see OcflS3Client#builder()
     *
     * @param s3Client aws sdk s3 client
     * @param bucket s3 bucket
     */
    public OcflS3Client(S3AsyncClient s3Client, String bucket) {
        this(s3Client, bucket, null, null, null);
    }

    /**
     * @see OcflS3Client#builder()
     *
     * @param s3Client aws sdk s3 client, not null
     * @param bucket s3 bucket, not null
     * @param prefix key prefix, may be null
     * @param transferManager aws sdk s3 transfer manager, not null
     * @param putObjectModifier hook for modifying putObject requests, may be null
     */
    public OcflS3Client(
            S3AsyncClient s3Client,
            String bucket,
            String prefix,
            S3TransferManager transferManager,
            BiConsumer<String, PutObjectRequest.Builder> putObjectModifier) {
        this.s3Client = Enforce.notNull(s3Client, "s3Client cannot be null");
        this.bucket = Enforce.notBlank(bucket, "bucket cannot be blank");
        this.repoPrefix = sanitizeRepoPrefix(prefix == null ? "" : prefix);
        this.transferManager = Enforce.notNull(transferManager, "transferManager cannot be null");
        this.keyBuilder = CloudObjectKey.builder().prefix(repoPrefix);
        this.putObjectModifier = putObjectModifier != null ? putObjectModifier : (k, b) -> {};
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
     * Nothing to do
     */
    @Override
    public void close() {
        // noop
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
        return repoPrefix;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<CloudObjectKey> uploadFileAsync(Path srcPath, String dstPath) {
        return uploadFileAsync(srcPath, dstPath, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<CloudObjectKey> uploadFileAsync(Path srcPath, String dstPath, String contentType) {
        var fileSize = UncheckedFiles.size(srcPath);
        var dstKey = keyBuilder.buildFromPath(dstPath);

        LOG.debug("Uploading {} to bucket {} key {} size {}", srcPath, bucket, dstKey, fileSize);

        var builder = PutObjectRequest.builder().contentType(contentType);

        putObjectModifier.accept(dstKey.getKey(), builder);

        if (fileSize >= EIGHT_MB) {
            var upload = transferManager.uploadFile(req -> req.source(srcPath)
                    .putObjectRequest(
                            builder.bucket(bucket).key(dstKey.getKey()).build())
                    .build());
            return new UploadFuture(upload.completionFuture(), srcPath, dstKey);
        } else {
            var upload = s3Client.putObject(
                    builder.bucket(bucket).key(dstKey.getKey()).build(), srcPath);
            return new UploadFuture(upload, srcPath, dstKey);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloudObjectKey uploadFile(Path srcPath, String dstPath) {
        return uploadFile(srcPath, dstPath, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloudObjectKey uploadFile(Path srcPath, String dstPath, String contentType) {
        var future = uploadFileAsync(srcPath, dstPath, contentType);
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw (RuntimeException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OcflS3Exception("Failed ot upload " + srcPath, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloudObjectKey uploadBytes(String dstPath, byte[] bytes, String contentType) {
        var dstKey = keyBuilder.buildFromPath(dstPath);
        LOG.debug("Writing bytes to bucket {} key {}", bucket, dstKey);

        var builder = PutObjectRequest.builder().contentType(contentType);

        putObjectModifier.accept(dstKey.getKey(), builder);

        try {
            s3Client.putObject(builder.bucket(bucket).key(dstKey.getKey()).build(), AsyncRequestBody.fromBytes(bytes))
                    .join();
        } catch (RuntimeException e) {
            throw new OcflS3Exception("Failed to upload bytes to " + dstKey, OcflS3Util.unwrapCompletionEx(e));
        }

        return dstKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloudObjectKey copyObject(String srcPath, String dstPath) {
        var srcKey = keyBuilder.buildFromPath(srcPath);
        var dstKey = keyBuilder.buildFromPath(dstPath);

        LOG.debug("Copying {} to {} in bucket {}", srcKey, dstKey, bucket);

        try {
            var copy = s3Client.copyObject(req -> req.destinationBucket(bucket)
                    .destinationKey(dstKey.getKey())
                    .sourceBucket(bucket)
                    .sourceKey(srcKey.getKey())
                    .build());

            copy.join();
        } catch (RuntimeException e) {
            var cause = OcflS3Util.unwrapCompletionEx(e);
            if (wasNotFound(cause)) {
                throw new KeyNotFoundException("Key " + srcKey + " not found in bucket " + bucket, cause);
            }
            throw new OcflS3Exception("Failed to copy object from " + srcKey + " to " + dstKey, cause);
        }

        return dstKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Path downloadFile(String srcPath, Path dstPath) {
        var srcKey = keyBuilder.buildFromPath(srcPath);
        LOG.debug("Downloading from bucket {} key {} to {}", bucket, srcKey, dstPath);

        try {
            transferManager
                    .downloadFile(req -> req.getObjectRequest(getReq ->
                                    getReq.bucket(bucket).key(srcKey.getKey()).build())
                            .destination(dstPath)
                            .build())
                    .completionFuture()
                    .join();
        } catch (RuntimeException e) {
            var cause = OcflS3Util.unwrapCompletionEx(e);
            if (wasNotFound(cause)) {
                throw new KeyNotFoundException("Key " + srcKey + " not found in bucket " + bucket, cause);
            }
            throw new OcflS3Exception("Failed to download " + srcKey + " to " + dstPath, cause);
        }

        return dstPath;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream downloadStream(String srcPath) {
        var srcKey = keyBuilder.buildFromPath(srcPath);
        LOG.debug("Streaming from bucket {} key {}", bucket, srcKey);

        try {
            return s3Client.getObject(
                            GetObjectRequest.builder()
                                    .bucket(bucket)
                                    .key(srcKey.getKey())
                                    .build(),
                            AsyncResponseTransformer.toBlockingInputStream())
                    .join();
        } catch (RuntimeException e) {
            var cause = OcflS3Util.unwrapCompletionEx(e);
            if (wasNotFound(cause)) {
                throw new KeyNotFoundException("Key " + srcKey + " not found in bucket " + bucket, cause);
            }
            throw new OcflS3Exception("Failed to download " + srcKey, cause);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String downloadString(String srcPath) {
        try (var stream = downloadStream(srcPath)) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HeadResult head(String path) {
        var key = keyBuilder.buildFromPath(path);

        try {
            var s3Result = s3Client.headObject(HeadObjectRequest.builder()
                            .bucket(bucket)
                            .key(key.getKey())
                            .build())
                    .join();

            return new HeadResult()
                    .setContentEncoding(s3Result.contentEncoding())
                    .setContentLength(s3Result.contentLength())
                    .setETag(s3Result.eTag())
                    .setLastModified(s3Result.lastModified());
        } catch (RuntimeException e) {
            var cause = OcflS3Util.unwrapCompletionEx(e);
            if (wasNotFound(cause)) {
                throw new KeyNotFoundException("Key " + key + " not found in bucket " + bucket, cause);
            }
            throw new OcflS3Exception("Failed to HEAD " + key, cause);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListResult list(String prefix) {
        var prefixedPrefix = keyBuilder.buildFromPath(prefix);
        return toListResult(ListObjectsV2Request.builder().bucket(bucket).prefix(prefixedPrefix.getKey()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListResult listDirectory(String path) {
        var prefix = keyBuilder.buildFromPath(path).getKey();

        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix = prefix + "/";
        }

        LOG.debug("Listing directory {} in bucket {}", prefix, bucket);

        return toListResult(
                ListObjectsV2Request.builder().bucket(bucket).delimiter("/").prefix(prefix));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean directoryExists(String path) {
        var prefix = keyBuilder.buildFromPath(path).getKey();

        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix = prefix + "/";
        }

        LOG.debug("Checking existence of {} in bucket {}", prefix, bucket);

        try {
            var response = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                            .bucket(bucket)
                            .delimiter("/")
                            .prefix(prefix)
                            .maxKeys(1)
                            .build())
                    .join();

            return response.contents().stream().findAny().isPresent()
                    || response.commonPrefixes().stream().findAny().isPresent();
        } catch (RuntimeException e) {
            throw new OcflS3Exception("Failed to list objects under " + prefix, OcflS3Util.unwrapCompletionEx(e));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
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
    @Override
    public void deleteObjects(Collection<String> objectPaths) {
        if (!objectPaths.isEmpty()) {
            var objectKeys = objectPaths.stream()
                    .filter(Objects::nonNull)
                    .map(keyBuilder::buildFromPath)
                    .collect(Collectors.toList());

            deleteObjectsInternal(objectKeys);
        }
    }

    private void deleteObjectsInternal(Collection<CloudObjectKey> objectKeys) {
        LOG.debug("Deleting objects in bucket {}: {}", bucket, objectKeys);

        if (!objectKeys.isEmpty()) {
            var objectIds = objectKeys.stream()
                    .map(key -> ObjectIdentifier.builder().key(key.getKey()).build())
                    .collect(Collectors.toList());

            try {
                var futures = new ArrayList<CompletableFuture<?>>();

                // Can only delete at most 1,000 objects per request
                for (int i = 0; i < objectIds.size(); i += 999) {
                    var toDelete = objectIds.subList(i, Math.min(objectIds.size(), i + 999));
                    futures.add(s3Client.deleteObjects(DeleteObjectsRequest.builder()
                            .bucket(bucket)
                            .delete(builder -> builder.objects(toDelete))
                            .build()));
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {}))
                        .join();
            } catch (RuntimeException e) {
                throw new OcflS3Exception("Failed to delete objects " + objectIds, OcflS3Util.unwrapCompletionEx(e));
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void safeDeleteObjects(String... objectPaths) {
        safeDeleteObjects(Arrays.asList(objectPaths));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void safeDeleteObjects(Collection<String> objectPaths) {
        try {
            deleteObjects(objectPaths);
        } catch (RuntimeException e) {
            LOG.error("Failed to cleanup objects in bucket {}: {}", bucket, objectPaths, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean bucketExists() {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build())
                    .join();
            return true;
        } catch (RuntimeException e) {
            var cause = OcflS3Util.unwrapCompletionEx(e);
            if (wasNotFound(cause)) {
                return false;
            }
            throw new OcflS3Exception("Failed ot HEAD bucket " + bucket, cause);
        }
    }

    private ListResult toListResult(ListObjectsV2Request.Builder requestBuilder) {
        try {
            var result = s3Client.listObjectsV2(requestBuilder.build()).join();

            var prefixLength = prefixLength(result.prefix());
            var repoPrefixLength = repoPrefix.isBlank() ? 0 : repoPrefix.length() + 1;

            var objects = toObjectListings(result, prefixLength);
            var dirs = toDirectoryListings(result, repoPrefixLength);

            while (Boolean.TRUE.equals(result.isTruncated())) {
                result = s3Client.listObjectsV2(requestBuilder
                                .continuationToken(result.nextContinuationToken())
                                .build())
                        .join();

                objects.addAll(toObjectListings(result, prefixLength));
                dirs.addAll(toDirectoryListings(result, repoPrefixLength));
            }

            return new ListResult().setObjects(objects).setDirectories(dirs);
        } catch (RuntimeException e) {
            throw new OcflS3Exception("Failed to list objects", OcflS3Util.unwrapCompletionEx(e));
        }
    }

    private List<ListResult.ObjectListing> toObjectListings(ListObjectsV2Response result, int prefixLength) {
        return result.contents().stream()
                .map(o -> {
                    var key = o.key();
                    return new ListResult.ObjectListing()
                            .setKey(keyBuilder.buildFromKey(key))
                            .setKeySuffix(key.substring(prefixLength));
                })
                .collect(Collectors.toList());
    }

    private List<ListResult.DirectoryListing> toDirectoryListings(ListObjectsV2Response result, int repoPrefixLength) {
        return result.commonPrefixes().stream()
                .filter(p -> p.prefix() != null)
                .map(p -> {
                    var path = p.prefix();
                    return new ListResult.DirectoryListing().setPath(path.substring(repoPrefixLength));
                })
                .collect(Collectors.toList());
    }

    private int prefixLength(String prefix) {
        var prefixLength = 0;
        if (prefix != null && !prefix.isEmpty()) {
            prefixLength = prefix.length();
            if (!prefix.endsWith("/")) {
                prefixLength += 1;
            }
        }
        return prefixLength;
    }

    /**
     * Returns true if the exception indicates the object/bucket was NOT found in S3.
     *
     * @param e the exception
     * @return true if the object/bucket was NOT found in S3.
     */
    private boolean wasNotFound(Throwable e) {
        if (e instanceof NoSuchKeyException || e instanceof NoSuchBucketException) {
            return true;
        } else if (e instanceof S3Exception) {
            // It seems like the CRT client does not return NoSuchKeyExceptions...
            var s3e = (S3Exception) e;
            return 404 == s3e.statusCode();
        }
        return false;
    }

    public static class Builder {
        private S3AsyncClient s3Client;
        private S3TransferManager transferManager;
        private String bucket;
        private String repoPrefix;

        private BiConsumer<String, PutObjectRequest.Builder> putObjectModifier;

        /**
         * The AWS SDK S3 client. Required.
         * <p>
         * This client should NOT be the same client that's used with the transfer manager, and it should NOT be
         * the CRT client. The reason being that the CRT client only performs well when operating on files greater
         * than 8MB, and is <i>significantly</i> slower when operating on smaller files.
         * <p>
         * This client is NOT closed when the repository is closed, and the user is responsible for closing it when appropriate.
         *
         * @param s3Client s3 client
         * @return builder
         */
        public Builder s3Client(S3AsyncClient s3Client) {
            this.s3Client = Enforce.notNull(s3Client, "s3Client cannot be null");
            return this;
        }

        /**
         * The AWS SDK S3 transfer manager. Required.
         * <p>
         * The transfer manager should be configured per the official AWS documentation, using the CRT client.
         * <p>
         * If you are using a 3rd party S3 implementation, then you will likely additionally need to disable the
         * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">object integrity check</a>
         * as most 3rd party implementations do not support it. This easy to do on the CRT client builder by setting
         * {@code checksumValidationEnabled()} to {@code false}.
         * <p>
         * The transfer manager will NOT be closed when the repository is closed, and the user is
         * responsible for closing it when appropriate.
         *
         * @param transferManager S3 transfer manager
         * @return builder
         */
        public Builder transferManager(S3TransferManager transferManager) {
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

        /**
         * Provides a hook to modify putObject requests before they are executed. It is intended to be used to set
         * object attributes such as tags.
         *
         * <p>The first argument is the object key the request is for, and the second is the request builder to apply
         * changes to.
         *
         * @param putObjectModifier hook for modifying putObject requests
         * @return builder
         */
        public Builder putObjectModifier(BiConsumer<String, PutObjectRequest.Builder> putObjectModifier) {
            this.putObjectModifier = putObjectModifier;
            return this;
        }

        /**
         * Constructs a new {@link OcflS3Client}. {@link #s3Client(S3AsyncClient)} and {@link #bucket(String)} must be set.
         * <p>
         * Remember to call {@link OcflRepository#close()} when you are done with the repository so that the default
         * S3 transfer manager is closed.
         *
         * @return OcflS3Client
         */
        public OcflS3Client build() {
            return new OcflS3Client(s3Client, bucket, repoPrefix, transferManager, putObjectModifier);
        }
    }
}
