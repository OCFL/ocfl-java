package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.OcflObjectReader;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.VersionDetails;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;
import edu.wisc.library.ocfl.core.storage.OcflStorage;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.ResponseMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;

/**
 * Default implementation of OcflObjectReader that is used by DefaultOcflRepository to provide read access to an object.
 */
public class DefaultOcflObjectReader implements OcflObjectReader {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOcflObjectReader.class);

    private OcflStorage storage;

    private Inventory inventory;
    private Version version;
    private VersionId versionId;

    private ResponseMapper responseMapper;
    private VersionDetails versionDetails;

    public DefaultOcflObjectReader(OcflStorage storage, Inventory inventory, VersionId versionId) {
        this.storage = Enforce.notNull(storage, "storage cannot be null");
        this.inventory = Enforce.notNull(inventory, "inventory cannot be null");
        this.versionId = Enforce.notNull(versionId, "versionId cannot be null");

        version = inventory.getVersion(versionId);
        responseMapper = new ResponseMapper();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VersionDetails describeVersion() {
        if (versionDetails == null) {
            var objectRootPath = Paths.get(storage.objectRootPath(inventory.getId()));
            versionDetails = responseMapper.mapVersion(inventory, versionId, version, objectRootPath);
        }
        return versionDetails;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> listFiles() {
        return Collections.unmodifiableCollection(version.listPaths());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectReader getFile(String sourcePath, Path destinationPath, OcflOption... ocflOptions) {
        Enforce.notBlank(sourcePath, "sourcePath cannot be blank");
        Enforce.notNull(destinationPath, "destinationPath cannot be null");

        var stream = getFile(sourcePath);

        FileUtil.createDirectories(destinationPath.getParent());

        try {
            Files.copy(stream, destinationPath, FileUtil.toCopyOptions(ocflOptions));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }

        stream.checkFixity();

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FixityCheckInputStream getFile(String sourcePath) {
        Enforce.notBlank(sourcePath, "sourcePath cannot be blank");
        var fileId = lookupFileId(sourcePath);
        return new FixityCheckInputStream(storage.retrieveFile(inventory, fileId),
                inventory.getDigestAlgorithm().getJavaStandardName(), fileId);
    }

    private String lookupFileId(String sourcePath) {
        var fileId = version.getFileId(sourcePath);

        if (fileId == null) {
            throw new NotFoundException(String.format("File %s does not exist in object %s.", sourcePath, inventory.getId()));
        }

        return fileId;
    }

}
