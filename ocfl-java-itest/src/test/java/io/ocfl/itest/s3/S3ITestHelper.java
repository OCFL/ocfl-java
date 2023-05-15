package io.ocfl.itest.s3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.core.util.DigestUtil;
import io.ocfl.core.util.FileUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.ocfl.itest.ITestHelper;
import org.junit.jupiter.api.Assertions;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3ITestHelper {

    private static final String OCFL_SPEC_FILE = "ocfl_1.1.md";

    private S3Client s3Client;

    public S3ITestHelper(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public static S3Client createS3Client(String accessKey, String secretKey) {
        return S3Client.builder()
                .region(Region.US_EAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .httpClientBuilder(ApacheHttpClient.builder())
                .build();
    }

    public void verifyRepo(Path expected, String bucket, String prefix) {
        var expectedPaths = listAllFiles(expected);
        var actualObjects = listAllObjects(bucket, prefix);

        assertThat(actualObjects, containsInAnyOrder(expectedPaths.toArray()));

        expectedPaths.forEach(path -> {
            var expectedFile = expected.resolve(path);
            Assertions.assertEquals(
                    ITestHelper.computeDigest(expectedFile),
                    computeS3Digest(bucket, prefix, path),
                    () -> String.format(
                            "Comparing %s to object in bucket %s: \n\n%s",
                            expectedFile, bucket, new String(getObjectContent(bucket, prefix, path))));
        });
    }

    private List<String> listAllFiles(Path root) {
        var paths = new ArrayList<String>();

        try (var walk = Files.walk(root)) {
            walk.filter(p -> {
                        var pStr = p.toString();
                        return !pStr.contains(".gitkeep")
                                && !pStr.equals(OCFL_SPEC_FILE)
                                && !pStr.equals("ocfl_1.0.txt");
                    })
                    .filter(Files::isRegularFile)
                    .forEach(p -> {
                        paths.add(FileUtil.pathToStringStandardSeparator(root.relativize(p)));
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return paths;
    }

    private byte[] getObjectContent(String bucket, String prefix, String key) {
        try (var result = s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucket)
                .key(prefix + "/" + key)
                .build())) {
            return result.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String computeS3Digest(String bucket, String prefix, String key) {
        return DigestUtil.computeDigestHex(DigestAlgorithm.md5, getObjectContent(bucket, prefix, key));
    }

    public List<String> listAllObjects(String bucket, String prefix) {
        var result = s3Client.listObjectsV2(
                ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build());

        return result.contents().stream()
                .map(S3Object::key)
                .map(k -> k.substring(prefix.length() + 1))
                .filter(entry -> {
                    return !entry.equals(OCFL_SPEC_FILE) && !entry.equals("ocfl_1.0.txt");
                })
                .collect(Collectors.toList());
    }
}
