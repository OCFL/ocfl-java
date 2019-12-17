package edu.wisc.library.ocfl.aws;

import at.favre.lib.bytes.Bytes;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.storage.cloud.CloudClient;
import edu.wisc.library.ocfl.core.storage.cloud.KeyNotFoundException;
import edu.wisc.library.ocfl.core.storage.cloud.ListResult;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.SafeFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * CloudClient implementation that uses Amazon's S3 synchronous v2 client
 */
public class OcflS3Client implements CloudClient {

    private static final Logger LOG = LoggerFactory.getLogger(OcflS3Client.class);

    private static final int KB = 1024;
    private static final int MB = 1024 * KB;
    private static final long GB = 1024 * MB;
    private static final long TB = 1024 * GB;

    private static final long MAX_FILE_BYTES = 5 * TB;
    private static final int MAX_PART_BYTES = 100 * MB;
    private static final int PART_SIZE_BYTES = 10 * MB;
    private static final int MAX_PARTS = 100;
    private static final int PART_SIZE_INCREMENT = 10;
    private static final int PARTS_INCREMENT = 100;

    // TODO experiment with performance with async client
    private S3Client s3Client;
    private String bucket;

    public OcflS3Client(S3Client s3Client, String bucket) {
        this.s3Client = Enforce.notNull(s3Client, "s3Client cannot be null");
        this.bucket = Enforce.notBlank(bucket, "bucket cannot be blank");
    }

    @Override
    public String bucket() {
        return bucket;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String uploadFile(Path srcPath, String dstPath) {
        return uploadFile(srcPath, dstPath, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String uploadFile(Path srcPath, String dstPath, byte[] md5digest) {
        var fileSize = SafeFiles.size(srcPath);

        if (fileSize >= MAX_FILE_BYTES) {
            throw new IllegalArgumentException(String.format("Cannot store file %s because it exceeds the maximum file size.", srcPath));
        }

        if (fileSize > MAX_PART_BYTES) {
            multipartUpload(srcPath, dstPath, fileSize);
        } else {
            LOG.debug("Uploading {} to bucket {} key {} size {}", srcPath, bucket, dstPath, fileSize);

            if (md5digest == null) {
                md5digest = DigestUtil.computeDigest(DigestAlgorithm.md5, srcPath);
            }

            var md5Base64 = Bytes.from(md5digest).encodeBase64();

            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(dstPath)
                    .contentMD5(md5Base64)
                    .contentLength(fileSize)
                    .build(), srcPath);
        }

        return dstPath;
    }

    private void multipartUpload(Path srcPath, String dstPath, long fileSize) {
        var partSize = determinePartSize(fileSize);

        LOG.debug("Multipart upload of {} to bucket {} key {}. File size: {}; part size: {}", srcPath, bucket, dstPath,
                fileSize, partSize);

        var uploadId = beginMultipartUpload(dstPath);

        var completedParts = new ArrayList<CompletedPart>();

        try {
            try (var channel = FileChannel.open(srcPath, StandardOpenOption.READ)) {
                var buffer = ByteBuffer.allocate(partSize);
                var i = 1;

                while (channel.read(buffer) > 0) {
                    buffer.flip();

                    var digest = DigestUtil.computeDigest(DigestAlgorithm.md5, buffer);

                    var partResponse = s3Client.uploadPart(UploadPartRequest.builder()
                            .bucket(bucket)
                            .key(dstPath)
                            .uploadId(uploadId)
                            .partNumber(i)
                            .contentMD5(Bytes.from(digest).encodeBase64())
                            // TODO entire part is in memory. stream part to file first?
                            .build(), RequestBody.fromByteBuffer(buffer));

                    completedParts.add(CompletedPart.builder()
                            .partNumber(i)
                            .eTag(partResponse.eTag())
                            .build());

                    buffer.clear();
                    i++;
                }
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }

            completeMultipartUpload(uploadId, dstPath, completedParts);
        } catch (RuntimeException e) {
            abortMultipartUpload(uploadId, dstPath);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String uploadBytes(String dstPath, byte[] bytes, String contentType) {
        LOG.debug("Writing string to bucket {} key {}", bucket, dstPath);

        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucket)
                .key(dstPath)
                .contentType(contentType)
                .build(), RequestBody.fromBytes(bytes));

        return dstPath;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String copyObject(String srcPath, String dstPath) {
        LOG.debug("Copying {} to {} in bucket {}", srcPath, dstPath, bucket);

        try {
            s3Client.copyObject(CopyObjectRequest.builder()
                    .bucket(bucket)
                    .key(dstPath)
                    .copySource(keyWithBucketName(srcPath))
                    .build());
        } catch (NoSuchKeyException e) {
            throw new KeyNotFoundException(e);
        } catch (SdkException e) {
            // TODO verify class and message
            if (e.getMessage().contains("copy source is larger than the maximum allowable size")) {
                multipartCopy(srcPath, dstPath);
            } else {
                throw e;
            }
        }

        return dstPath;
    }

    private void multipartCopy(String srcPath, String dstPath) {
        var head = headObject(srcPath);
        var fileSize = head.contentLength();
        var partSize = determinePartSize(fileSize);

        LOG.debug("Multipart copy of {} to {} in bucket {}: File size {}; part size: {}", srcPath, dstPath, bucket,
                fileSize, partSize);

        var uploadId = beginMultipartUpload(dstPath);

        try {
            var completedParts = new ArrayList<CompletedPart>();
            var part = 1;
            var position = 0L;

            while (position < fileSize) {
                var end = Math.min(fileSize - 1, part * partSize - 1);
                var partResponse = s3Client.uploadPartCopy(UploadPartCopyRequest.builder()
                        .bucket(bucket)
                        .key(dstPath)
                        .copySource(keyWithBucketName(srcPath))
                        .partNumber(part)
                        .uploadId(uploadId)
                        .copySourceRange(String.format("bytes=%s-%s", position, end))
                        .build());

                completedParts.add(CompletedPart.builder()
                        .partNumber(part)
                        .eTag(partResponse.copyPartResult().eTag())
                        .build());

                part++;
                position = end + 1;
            }

            completeMultipartUpload(uploadId, dstPath, completedParts);
        } catch (RuntimeException e) {
            abortMultipartUpload(uploadId, dstPath);
            throw e;
        }
    }

    private HeadObjectResponse headObject(String key) {
        return s3Client.headObject(HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Path downloadFile(String srcPath, Path dstPath) {
        LOG.debug("Downloading bucket {} key {} to {}", bucket, srcPath, dstPath);

        try {
            s3Client.getObject(GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(srcPath)
                    .build(), dstPath);
        } catch (NoSuchKeyException e) {
            throw new KeyNotFoundException(e);
        }

        return dstPath;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream downloadStream(String srcPath) {
        LOG.debug("Streaming bucket {} key {}", bucket, srcPath);

        try {
            return s3Client.getObject(GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(srcPath)
                    .build());
        } catch (NoSuchKeyException e) {
            throw new KeyNotFoundException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String downloadString(String srcPath) {
        try (var stream = downloadStream(srcPath)) {
            return new String(stream.readAllBytes());
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListResult list(String prefix) {
        return toListResult(s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListResult listDirectory(String path) {
        var prefix = path;

        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix = prefix + "/";
        }

        LOG.debug("Listing directory {} in bucket {}", prefix, bucket);

        return toListResult(s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .delimiter("/")
                .prefix(prefix)
                .build()));
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

        deleteObjects(keys);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteObjects(Collection<String> objectKeys) {
        LOG.debug("Deleting objects in bucket {}: {}", bucket, objectKeys);

        if (!objectKeys.isEmpty()) {
            var objectIds = objectKeys.stream()
                    .map(key -> ObjectIdentifier.builder().key(key).build())
                    .collect(Collectors.toList());

            s3Client.deleteObjects(DeleteObjectsRequest.builder()
                    .bucket(bucket)
                    .delete(Delete.builder()
                            .objects(objectIds)
                            .build())
                    .build());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void safeDeleteObjects(String... objectKeys) {
        safeDeleteObjects(Arrays.asList(objectKeys));
    }

    /**
     * {@inheritDoc}
     */
    @Override
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
    @Override
    public boolean bucketExists() {
        try {
            s3Client.headBucket(HeadBucketRequest.builder()
                    .bucket(bucket)
                    .build());
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

    private String beginMultipartUpload(String key) {
        return s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .build()).uploadId();
    }

    private void completeMultipartUpload(String uploadId, String key, List<CompletedPart> parts) {
        s3Client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder()
                        .parts(parts)
                        .build())
                .build());
    }

    private void abortMultipartUpload(String uploadId, String key) {
        try {
            s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .build());
        } catch (RuntimeException e) {
            LOG.error("Failed to abort multipart upload. Bucket: {}; Key: {}; Upload Id: {}", bucket, key, uploadId, e);
        }
    }

    private String keyWithBucketName(String key) {
        return SdkHttpUtils.urlEncode(String.format("%s/%s", bucket, key));
    }

    private int determinePartSize(long fileSize) {
        var partSize = PART_SIZE_BYTES;
        var maxParts = MAX_PARTS;

        while (fileSize / partSize > maxParts) {
            partSize += PART_SIZE_INCREMENT;

            if (partSize > MAX_PART_BYTES) {
                maxParts += PARTS_INCREMENT;
                partSize /= 2;
            }
        }

        return partSize;
    }

    private ListResult toListResult(ListObjectsV2Response s3Result) {
        var prefix = s3Result.prefix() == null ? "" : s3Result.prefix();

        var objects = s3Result.contents().stream().map(o -> {
            var key = o.key();
            return new ListResult.ObjectListing()
                    .setKey(key)
                    .setFileName(prefix.isEmpty() ? key : key.substring(prefix.length()));
        }).collect(Collectors.toList());

        var dirs = s3Result.commonPrefixes().stream()
                .filter(p -> p.prefix() != null)
                .map(p -> {
                    var path = p.prefix();
                    return new ListResult.DirectoryListing()
                            .setPath(p.prefix())
                            .setFileName(prefix.isEmpty() ? path : path.substring(prefix.length()));
                })
                .collect(Collectors.toList());

        return new ListResult()
                .setObjects(objects)
                .setDirectories(dirs);
    }

}
