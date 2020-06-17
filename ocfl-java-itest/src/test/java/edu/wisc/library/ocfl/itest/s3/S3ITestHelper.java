package edu.wisc.library.ocfl.itest.s3;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.extension.storage.layout.HashedTruncatedNTupleExtension;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static edu.wisc.library.ocfl.itest.ITestHelper.computeDigest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S3ITestHelper {

    private S3Client s3Client;

    public S3ITestHelper(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public void verifyRepo(Path expected, String bucket) {
        assertTrue(bucketExists(bucket), bucket + " should exist");
        assertEquals(expected.getFileName().toString(), bucket);

        var expectedPaths = listAllFiles(expected);
        var actualObjects = listAllObjects(bucket);

        assertThat(actualObjects, containsInAnyOrder(expectedPaths.toArray()));

        expectedPaths.forEach(path -> {
            var expectedFile = expected.resolve(path);
            assertEquals(computeDigest(expectedFile), computeS3Digest(bucket, path),
                    () -> String.format("Comparing %s to object in bucket %s: \n\n%s",
                            expectedFile, bucket, new String(getObjectContent(bucket, path))));
        });
    }

    public void deleteBucket(String bucket) {
        var result = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix("").build());
        var keys = result.contents().stream().map(S3Object::key).map(k -> ObjectIdentifier.builder().key(k).build()).collect(Collectors.toList());
        s3Client.deleteObjects(DeleteObjectsRequest.builder().bucket(bucket).delete(Delete.builder().objects(keys).build()).build());
        s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
    }

    private List<String> listAllFiles(Path root) {
        var paths = new ArrayList<String>();

        try (var walk = Files.walk(root)) {
            walk.filter(p -> {
                var pStr = p.toString();
                return !(pStr.contains(".gitkeep")
                        || pStr.contains("deposit")
                        // TODO remove this once layout an extensions are finalized
                        || pStr.contains("ocfl_layout")
                        || pStr.contains(HashedTruncatedNTupleExtension.EXTENSION_NAME));
            }).filter(p -> Files.isRegularFile(p)).forEach(p -> {
                paths.add(FileUtil.pathToStringStandardSeparator(root.relativize(p)));
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return paths;
    }

    private byte[] getObjectContent(String bucket, String key) {
        try (var result = s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build())) {
            return result.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String computeS3Digest(String bucket, String key) {
        return DigestUtil.computeDigestHex(DigestAlgorithm.md5, getObjectContent(bucket, key));
    }

    public List<String> listAllObjects(String bucket) {
        var result = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix("")
                .build());

        return result.contents().stream().map(S3Object::key).filter(k -> {
            return !(k.contains("ocfl_layout") || k.contains(HashedTruncatedNTupleExtension.EXTENSION_NAME));
        }).collect(Collectors.toList());
    }

    private boolean bucketExists(String bucket) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder()
                    .bucket(bucket)
                    .build());
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

}
