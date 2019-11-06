package edu.wisc.library.ocfl.aws;

import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapper;
import edu.wisc.library.ocfl.core.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.VersionId;
import edu.wisc.library.ocfl.core.storage.OcflStorage;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.util.NamasteTypeFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;

public class S3OcflStorage implements OcflStorage {

    private static final Logger LOG = LoggerFactory.getLogger(S3OcflStorage.class);

    private S3Client s3Client;
    private String bucketName;
    private Path cache;
    private Path workDir;

    private ObjectIdPathMapper objectIdPathMapper;
    private InventoryMapper inventoryMapper;

    // TODO object keys: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html#object-key-guidelines
    // TODO retries are built into client. sufficient?

    /**
     * {@inheritDoc}
     */
    @Override
    public Inventory loadInventory(String objectId) {
        var objectRootPath = objectRootPathFull(objectId);
        var inventoryPath = inventoryPath(objectRootPath);
        var tempDir = FileUtil.createTempDir(workDir, objectId);
        var localInventoryPath = tempDir.resolve(inventoryPath);

        try {
            downloadFile(inventoryPath, localInventoryPath);
            var inventory = inventoryMapper.read(localInventoryPath);
            var expectedDigest = getDigestFromSidecar(objectRootPath, inventory);
            verifyInventory(expectedDigest, localInventoryPath, inventory.getDigestAlgorithm());
            return inventory;
        } catch (NoSuchKeyException e) {
            // Return null when the inventory file isn't found
            return null;
        } finally {
            FileUtil.safeDeletePath(tempDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNewVersion(Inventory inventory, Path stagingDir) {
        var objectRootPath = objectRootPathFull(inventory.getId());
        var versionPath = objectRootPath.resolve(inventory.getHead().toString());

        try {
            if (isFirstVersion(inventory)) {
                writeObjectNamasteFile(objectRootPath);
            }

            // TODO cleanup previous failures?

            // TODO write a copy to the cache?

            // TODO this is incorrect because you can't rely on contentDirectory being set
            storeContentInS3(inventory, stagingDir, versionPath.resolve(inventory.resolveContentDirectory()));
            storeInventoryInS3(inventory, stagingDir, versionPath);
        } catch (RuntimeException e) {
            rollback(objectRootPath, versionPath, inventory);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, OcflFileRetriever> getObjectStreams(Inventory inventory, VersionId versionId) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reconstructObjectVersion(Inventory inventory, VersionId versionId, Path stagingDir) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FixityCheckInputStream retrieveFile(Inventory inventory, String fileId) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeObject(String objectId) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commitMutableHead(Inventory inventory, Path stagingDir) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeMutableHead(String objectId) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsObject(String objectId) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String objectRootPath(String objectId) {
        return objectIdPathMapper.map(objectId).toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initializeStorage(String ocflVersion) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

    }

    private void storeContentInS3(Inventory inventory, Path sourcePath, Path versionContentPath) {
        // TODO parallelize
        var stagedContent = sourcePath.resolve(inventory.resolveContentDirectory());
        var files = FileUtil.findFiles(sourcePath);

        for (var file : files) {
            var contentRelativePath = stagedContent.relativize(file);
            // TODO verify fixity of all files before uploading to s3?
            var digest = verifyFileFixity(inventory, file, contentRelativePath);
            if (DigestAlgorithm.md5.equals(inventory.getDigestAlgorithm())) {
                digest = DigestUtil.computeDigest(DigestAlgorithm.md5, file);
            }
            var destinationPath = versionContentPath.resolve(contentRelativePath);
            uploadFile(file, destinationPath, digest);
        }
    }

    private void storeInventoryInS3(Inventory inventory, Path sourcePath, Path versionPath) {
        var inventoryPath = inventoryPath(sourcePath);
        var sidecarPath = inventorySidecarPath(sourcePath, inventory.getDigestAlgorithm());

        uploadFile(inventoryPath, inventoryPath(versionPath));
        uploadFile(sidecarPath, inventorySidecarPath(versionPath, inventory.getDigestAlgorithm()));
        // TODO uploaded twice because I'm not sure if it's possible to copy an object before it replicates -- try it
        uploadFile(inventoryPath, inventoryPath(versionPath.getParent()));
        uploadFile(sidecarPath, inventorySidecarPath(versionPath.getParent(), inventory.getDigestAlgorithm()));
    }

    private Path objectRootPathFull(String objectId) {
        return objectIdPathMapper.map(objectId);
    }

    private Path inventoryPath(Path rootPath) {
        return rootPath.resolve(OcflConstants.INVENTORY_FILE);
    }

    private Path inventorySidecarPath(Path rootPath, DigestAlgorithm digestAlgorithm) {
        return rootPath.resolve(OcflConstants.INVENTORY_FILE + "." + digestAlgorithm.getOcflName());
    }

    private String getDigestFromSidecar(Path objectRootPath, Inventory inventory) {
        var inventorySidecarPath = inventorySidecarPath(objectRootPath, inventory.getDigestAlgorithm());
        try {
            return s3Client.getObjectAsBytes(GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(inventorySidecarPath.toString())
                    .build()).asUtf8String();
        } catch (NoSuchKeyException e) {
            throw new IllegalStateException("Missing inventory sidecar: " + inventorySidecarPath, e);
        }
    }

    private void verifyInventory(String expectedDigest, Path inventoryPath, DigestAlgorithm algorithm) {
        var actualDigest = DigestUtil.computeDigest(algorithm, inventoryPath);

        if (!expectedDigest.equalsIgnoreCase(actualDigest)) {
            throw new FixityCheckException(String.format("Invalid inventory file: %s. Expected %s digest: %s; Actual: %s",
                    inventoryPath, algorithm.getOcflName(), expectedDigest, actualDigest));
        }
    }

    private String verifyFileFixity(Inventory inventory, Path file, Path contentRelativePath) {
        var expectedDigest = inventory.getHeadVersion().getFileId(contentRelativePath.toString());
        if (expectedDigest == null) {
            throw new IllegalStateException(String.format("File not found in object %s version %s: %s",
                    inventory.getId(), inventory.getHead(), contentRelativePath));
        }
        var actualDigest = DigestUtil.computeDigest(inventory.getDigestAlgorithm(), file);
        if (!expectedDigest.equalsIgnoreCase(actualDigest)) {
            throw new FixityCheckException(String.format("File %s in object %s failed its %s fixity check. Expected: %s; Actual: %s",
                    file, inventory.getId(), inventory.getDigestAlgorithm().getOcflName(), expectedDigest, actualDigest));
        }
        return actualDigest;
    }

    private boolean isFirstVersion(Inventory inventory) {
        return inventory.getVersions().size() == 1;
    }

    private void writeObjectNamasteFile(Path objectRootPath) {
        var namasteFile = new NamasteTypeFile(OcflConstants.OCFL_OBJECT_VERSION);
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(objectRootPath.resolve(namasteFile.fileName()).toString())
                .build(), RequestBody.fromString(namasteFile.fileContent()));
    }

    private void rollback(Path objectRootPath, Path versionPath, Inventory inventory) {
        try {
            if (isFirstVersion(inventory)) {
                deletePath(objectRootPath.toString());
            } else {
                deletePath(versionPath.toString());
                var previousVersionRoot = objectRootPath.resolve(inventory.getHead().previousVersionId().toString());
                copyInventory(previousVersionRoot, objectRootPath, inventory);
            }
        } catch (RuntimeException e) {
            LOG.error("Failed to rollback changes to object {} cleanly.", inventory.getId(), e);
        }
    }

    private void copyInventory(Path sourcePath, Path destinationPath, Inventory inventory) {
        var digestAlgorithm = inventory.getDigestAlgorithm();

        copyFile(inventoryPath(sourcePath).toString(), inventoryPath(destinationPath).toString());
        copyFile(inventorySidecarPath(sourcePath, digestAlgorithm).toString(),
                inventorySidecarPath(destinationPath, digestAlgorithm).toString());
    }

    private void uploadFile(Path localPath, Path remotePath, String md5digest) {
        // TODO multipart upload for large files
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(remotePath.toString())
                .contentMD5(md5digest)
                .build(), localPath);
    }

    private void uploadFile(Path localPath, Path remotePath) {
        // TODO multipart upload for large files
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(remotePath.toString())
                .build(), localPath);
    }

    private void downloadFile(Path remotePath, Path localPath) {
        s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(remotePath.toString())
                .build(), localPath);
    }

    private void deletePath(String objectPrefix) {
        var responses = s3Client.listObjectsV2Paginator(ListObjectsV2Request.builder()
                .bucket(bucketName).prefix(objectPrefix)
                .build());
        responses.stream().forEach(response -> {
            var objectIds = response.contents().stream().map(object -> {
                return ObjectIdentifier.builder().key(object.key()).build();
            }).collect(Collectors.toList());

            LOG.debug("Deleting objects: " + objectIds);

            s3Client.deleteObjects(DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder().objects(objectIds).build()).build());
        });
    }

    private void copyFile(String sourceKey, String destinationKey) {
        // TODO works for files over 5GB?
        s3Client.copyObject(CopyObjectRequest.builder()
                .copySource(bucketName + "/" + sourceKey)
                .bucket(bucketName)
                .key(destinationKey)
                .build());
    }

}
