package edu.wisc.library.ocfl.ibm;

import at.favre.lib.bytes.Bytes;
import com.ibm.cloud.objectstorage.SdkClientException;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.model.*;
import com.ibm.cloud.objectstorage.services.s3.transfer.TransferManager;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.storage.cloud.KeyNotFoundException;
import edu.wisc.library.ocfl.core.storage.cloud.ListResult;
import edu.wisc.library.ocfl.core.storage.cloud.CloudClient;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.SafeFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
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
    private AmazonS3 s3Client;
    private TransferManager transferManager;
    private String bucket;

    public IbmCosClient(AmazonS3 s3Client, TransferManager transferManager, String bucket) {
        this.s3Client = Enforce.notNull(s3Client, "s3Client cannot be null");
        this.transferManager = Enforce.notNull(transferManager, "transferManager cannot be null");
        this.bucket = Enforce.notBlank(bucket, "bucket cannot be blank");
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
    public String uploadFile(Path srcPath, String dstPath) {
        return uploadFile(srcPath, dstPath, null);
    }

    /**
     * {@inheritDoc}
     */
    public String uploadFile(Path srcPath, String dstPath, byte[] md5digest) {
        var fileSize = SafeFiles.size(srcPath);

        LOG.debug("Uploading {} to bucket {} key {} size {}", srcPath, bucket, dstPath, fileSize);

        if (fileSize >= MAX_FILE_BYTES) {
            throw new IllegalArgumentException(String.format("Cannot store file %s because it exceeds the maximum file size.", srcPath));
        }

        if (md5digest == null) {
            md5digest = DigestUtil.computeDigest(DigestAlgorithm.md5, srcPath);
        }

        var md5Base64 = Bytes.wrap(md5digest).encodeBase64();

        var metadata = new ObjectMetadata();
        metadata.setContentMD5(md5Base64);

        var request = new PutObjectRequest(bucket, encodeKey(dstPath), srcPath.toFile()).withMetadata(metadata);

        if (fileSize > MAX_PART_BYTES) {
            try {
                transferManager.upload(request).waitForUploadResult();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            s3Client.putObject(request);
        }

        return dstPath;
    }

    /**
     * {@inheritDoc}
     */
    public String uploadBytes(String dstPath, byte[] bytes, String contentType) {
        LOG.debug("Writing string to bucket {} key {}", bucket, dstPath);

        var metadata = new ObjectMetadata();
        metadata.setContentType(contentType);
        metadata.setContentLength(bytes.length);

        s3Client.putObject(new PutObjectRequest(bucket, encodeKey(dstPath), new ByteArrayInputStream(bytes), metadata));

        return dstPath;
    }

    /**
     * {@inheritDoc}
     */
    public String copyObject(String srcPath, String dstPath) {
        LOG.debug("Copying {} to {} in bucket {}", srcPath, dstPath, bucket);

        try {
            s3Client.copyObject(bucket, encodeKey(srcPath), bucket, encodeKey(dstPath));
        } catch (AmazonS3Exception e) {
            if (NO_SUCH_KEY.equals(e.getErrorCode())) {
                throw new KeyNotFoundException(e);
            }
            throw e;
        } catch (SdkClientException e) {
            // TODO verify class and message
            if (e.getMessage().contains("copy source is larger than the maximum allowable size")) {
                transferManager.copy(bucket, srcPath, bucket, dstPath);
            } else {
                throw e;
            }
        }

        return dstPath;
    }

    /**
     * {@inheritDoc}
     */
    public Path downloadFile(String srcPath, Path dstPath) {
        LOG.debug("Downloading bucket {} key {} to {}", bucket, srcPath, dstPath);

        try {
            s3Client.getObject(new GetObjectRequest(bucket, encodeKey(srcPath)), dstPath.toFile());
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
        LOG.debug("Streaming bucket {} key {}", bucket, srcPath);

        try {
            return s3Client.getObject(bucket, encodeKey(srcPath)).getObjectContent();
        } catch (AmazonS3Exception e) {
            if (NO_SUCH_KEY.equals(e.getErrorCode())) {
                throw new KeyNotFoundException(e);
            }
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
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
    public ListResult list(String prefix) {
        return toListResult(s3Client.listObjectsV2(bucket, encodeKey(prefix)));
    }

    /**
     * {@inheritDoc}
     */
    public ListResult listDirectory(String path) {
        var prefix = path;

        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix = prefix + "/";
        }

        LOG.debug("Listing directory {} in bucket {}", prefix, bucket);

        return toListResult(s3Client.listObjectsV2(new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(encodeKey(prefix))
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

        deleteObjects(keys);
    }

    /**
     * {@inheritDoc}
     */
    public void deleteObjects(Collection<String> objectKeys) {
        LOG.debug("Deleting objects in bucket {}: {}", bucket, objectKeys);

        if (!objectKeys.isEmpty()) {
            s3Client.deleteObjects(new DeleteObjectsRequest(bucket)
                    .withKeys(encodeKeys(objectKeys).toArray(String[]::new)));
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
            if ("NoSuchBucket".equals(e.getErrorCode())) {
                return false;
            }
            throw e;
        }
    }

    private ListResult toListResult(ListObjectsV2Result s3Result) {
        var prefix = s3Result.getPrefix() == null ? "" : decodeKey(s3Result.getPrefix());

        var objects = s3Result.getObjectSummaries().stream().map(o -> {
            var key = decodeKey(o.getKey());
            return new ListResult.ObjectListing()
                    .setKey(key)
                    .setFileName(prefix.isEmpty() ? key : key.substring(prefix.length()));
        }).collect(Collectors.toList());

        var dirs = s3Result.getCommonPrefixes().stream().map(p -> {
            var path = decodeKey(p);
            return new ListResult.DirectoryListing()
                    .setPath(path)
                    .setFileName(prefix.isEmpty() ? path : path.substring(prefix.length()));
        }).collect(Collectors.toList());

        return new ListResult()
                .setObjects(objects)
                .setDirectories(dirs);
    }

    private Collection<String> encodeKeys(Collection<String> keys) {
        return keys.stream().map(this::encodeKey).collect(Collectors.toList());
    }

    private Collection<String> decodeKeys(Collection<String> encodedKeys) {
        return encodedKeys.stream().map(this::decodeKey).collect(Collectors.toList());
    }

    private String encodeKey(String key) {
        var parts = key.split("/");
        var builder = new StringBuilder();

        for (var i = 0; i < parts.length; i++) {
            builder.append(URLEncoder.encode(parts[i], StandardCharsets.UTF_8));
            if (i < parts.length - 1) {
                builder.append("/");
            }
        }

        if (key.endsWith("/")) {
            builder.append("/");
        }

        return builder.toString();
    }

    private String decodeKey(String encodedKey) {
        return URLDecoder.decode(encodedKey, StandardCharsets.UTF_8);
    }

}
