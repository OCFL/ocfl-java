package edu.wisc.library.ocfl.aws;

import at.favre.lib.bytes.Bytes;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;
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

public class S3ClientWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(S3ClientWrapper.class);

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

    public S3ClientWrapper(S3Client s3Client, String bucket) {
        this.s3Client = Enforce.notNull(s3Client, "s3Client cannot be null");
        this.bucket = Enforce.notBlank(bucket, "bucket cannot be blank");
    }

    public String bucket() {
        return bucket;
    }

    /**
     * Uploads a file to the destination, and returns the object key. A md5 digest of the file is calculated prior to
     * initiating the upload, and is used for transmission fixity.
     *
     * @param srcPath src file
     * @param dstPath object key
     * @return object key
     */
    public String uploadFile(Path srcPath, String dstPath) {
        return uploadFile(srcPath, dstPath, null);
    }

    /**
     * Uploads a file to the destination, and returns the object key. If the md5 digest is null, it is calculated prior to
     * upload.
     *
     * @param srcPath src file
     * @param dstPath object key
     * @param md5digest the md5 digest of the file to upload or null
     * @return object key
     */
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
     * Uploads an object with byte content
     *
     * @param dstPath object key
     * @param bytes the object content
     * @return object key
     */
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
     * Copies an object from one location to another within the same bucket.
     *
     * @param srcPath source object key
     * @param dstPath destination object key
     * @return the destination key
     */
    public String copyObject(String srcPath, String dstPath) {
        LOG.debug("Copying {} to {} in bucket {}", srcPath, dstPath, bucket);

        try {
            s3Client.copyObject(CopyObjectRequest.builder()
                    .bucket(bucket)
                    .key(dstPath)
                    .copySource(keyWithBucketName(srcPath))
                    .build());
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

    /**
     * Downloads an object to the local filesystem.
     *
     * @param srcPath object key
     * @param dstPath path to write the file to
     * @return the destination path
     */
    public Path downloadFile(String srcPath, Path dstPath) {
        LOG.debug("Downloading bucket {} key {} to {}", bucket, srcPath, dstPath);

        s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucket)
                .key(srcPath)
                .build(), dstPath);

        return dstPath;
    }

    /**
     * Downloads and object and performs a fixity check as it streams to disk.
     *
     * @param srcPath object key
     * @return stream of object content
     */
    public InputStream downloadStream(String srcPath) {
        LOG.debug("Streaming bucket {} key {}", bucket, srcPath);

        return s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucket)
                .key(srcPath)
                .build());
    }

    /**
     * Downloads an object to a string.
     *
     * @param srcPath object key
     * @return string content of the object
     */
    public String downloadString(String srcPath) {
        try (var stream = downloadStream(srcPath)) {
            return new String(stream.readAllBytes());
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    /**
     * Lists all of the keys under a prefix. No delimiter is used.
     *
     * @param prefix the key prefix
     * @return list response
     */
    public ListObjectsV2Response list(String prefix) {
        return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build());
    }

    /**
     * Lists all of the keys within a virtual directory. Only keys that fall between the specified prefix and the next
     * '/' are returned.
     *
     * @param path the key prefix to list, if it does not end in a '/' one is appended
     * @return list response
     */
    public ListObjectsV2Response listDirectory(String path) {
        var prefix = path;

        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix = prefix + "/";
        }

        LOG.debug("Listing directory {} in bucket {}", prefix, bucket);

        return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .delimiter("/")
                .prefix(prefix)
                .build());
    }

    /**
     * Similar to {@link #listDirectory} except that it only returns a list of the "filenames" of keys found in a
     * virtual directory.
     *
     * @param path the key prefix to list
     * @return list of filenames under the prefix
     */
    public List<String> listObjectsUnderPrefix(String path) {
        return listDirectory(path).contents().stream()
                .map(S3Object::key)
                .map(k -> path.isEmpty() ? k : k.substring(path.length()))
                .collect(Collectors.toList());
    }

    public void deletePath(String path) {
        LOG.debug("Deleting path {} in bucket {}", path, bucket);

        var keys = list(path).contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toList());

        deleteObjects(keys);
    }

    /**
     * Deletes all of the specified objects. If an object does not exist, nothing happens.
     *
     * @param objectKeys keys to delete
     */
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
     * Deletes all of the objects and does not throw an exception on failure
     *
     * @param objectKeys keys to delete
     */
    public void safeDeleteObjects(String... objectKeys) {
        safeDeleteObjects(Arrays.asList(objectKeys));
    }

    /**
     * Deletes all of the objects and does not throw an exception on failure
     *
     * @param objectKeys keys to delete
     */
    public void safeDeleteObjects(Collection<String> objectKeys) {
        try {
            deleteObjects(objectKeys);
        } catch (RuntimeException e) {
            LOG.error("Failed to cleanup objects in bucket {}: {}", bucket, objectKeys, e);
        }
    }

    public HeadBucketResponse headBucket() {
        return s3Client.headBucket(HeadBucketRequest.builder()
                .bucket(bucket)
                .build());
    }

    public HeadObjectResponse headObject(String key) {
        return s3Client.headObject(HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build());
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

}
