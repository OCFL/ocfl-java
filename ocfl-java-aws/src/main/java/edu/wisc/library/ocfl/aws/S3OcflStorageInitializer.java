package edu.wisc.library.ocfl.aws;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflVersion;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

public class S3OcflStorageInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(S3OcflStorageInitializer.class);

    private S3Client s3Client;


    // TODO do we need to support creating a repository within a bucket. ie allow for other repos in the same bucket or non-repo content?
    public ObjectIdPathMapper initializeStorage(String bucketName, OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        Enforce.notBlank(bucketName, "bucketName cannot be blank");
        Enforce.notNull(ocflVersion, "ocflVersion cannot be null");

        ensureBucketExists(bucketName);

        if (listDirectory(bucketName, "").contents().isEmpty()) {
            return initNewRepo(bucketName, ocflVersion, layoutConfig);
        } else {
            return validateExistingRepo(bucketName, ocflVersion, layoutConfig);
        }
    }

    private ObjectIdPathMapper initNewRepo(String bucketName, OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        return null;
    }

    private ObjectIdPathMapper validateExistingRepo(String bucketName, OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        Enforce.notNull(layoutConfig, "layoutConfig cannot be null when initializing a new repo");

        LOG.info("Initializing new OCFL repository in the bucket {}", bucketName);

        return null;
    }

    private void ensureBucketExists(String bucketName) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (RuntimeException e) {
            throw new IllegalStateException(String.format("Bucket %s does not exist or is not accessible.", bucketName), e);
        }
    }

    private ListObjectsV2Response listDirectory(String bucketName, String path) {
        return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(path)
                .delimiter("/")
                .build());
    }

}
