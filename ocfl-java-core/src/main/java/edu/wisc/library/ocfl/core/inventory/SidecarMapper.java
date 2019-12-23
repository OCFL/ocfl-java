package edu.wisc.library.ocfl.core.inventory;

import edu.wisc.library.ocfl.api.DigestAlgorithmRegistry;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.model.Inventory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SidecarMapper {

    public void writeSidecar(Inventory inventory, String digest, Path dstDirectory) {
        try {
            var sidecarPath = ObjectPaths.inventorySidecarPath(dstDirectory, inventory);
            Files.writeString(sidecarPath, digest + "\t" + OcflConstants.INVENTORY_FILE);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public String readSidecar(Path sidecarPath) {
        try {
            var parts = Files.readString(sidecarPath).split("\\s");
            if (parts.length == 0) {
                throw new CorruptObjectException("Invalid inventory sidecar file: " + sidecarPath);
            }
            return parts[0];
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public DigestAlgorithm getDigestAlgorithmFromSidecar(String inventorySidecarPath) {
        var value = Paths.get(inventorySidecarPath).getFileName().toString().substring(OcflConstants.INVENTORY_FILE.length() + 1);
        var algorithm = DigestAlgorithmRegistry.getAlgorithm(value);

        if (!OcflConstants.ALLOWED_DIGEST_ALGORITHMS.contains(algorithm)) {
            throw new CorruptObjectException(String.format("Inventory sidecar at <%s> specifies digest algorithm %s. Allowed algorithms are: %s",
                    inventorySidecarPath, value, OcflConstants.ALLOWED_DIGEST_ALGORITHMS));
        }

        return algorithm;
    }

}
