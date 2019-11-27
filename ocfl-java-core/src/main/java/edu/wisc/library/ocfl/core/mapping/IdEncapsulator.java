package edu.wisc.library.ocfl.core.mapping;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.encode.Encoder;
import edu.wisc.library.ocfl.core.encode.NoOpEncoder;

/**
 * Uses the entire object id as the encapsulation directory
 */
public class IdEncapsulator implements Encapsulator {

    private Encoder encoder;
    private boolean useEncodedId;

    /**
     * Uses the entire encoded id as the encapsulation directory
     *
     * @return encapsulator
     */
    public static IdEncapsulator useEncodedId() {
        return new IdEncapsulator(new NoOpEncoder(), true);
    }

    /**
     * Encodes the object id again and uses this value as the encapsulation directory
     *
     * @param encoder encoder
     * @return encapsulator
     */
    public static IdEncapsulator useOriginalId(Encoder encoder) {
        return new IdEncapsulator(encoder, false);
    }

    public IdEncapsulator(Encoder encoder, boolean useEncodedId) {
        this.encoder = Enforce.notNull(encoder, "encoder cannot be null");
        this.useEncodedId = useEncodedId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String encapsulate(String objectId, String encodedId) {
        if (useEncodedId) {
            return encodedId;
        }
        return encoder.encode(objectId);
    }

}
