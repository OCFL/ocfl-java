package edu.wisc.library.ocfl.core.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.exception.OcflExtensionException;
import edu.wisc.library.ocfl.core.extension.storage.layout.FlatLayoutExtension;
import edu.wisc.library.ocfl.core.extension.storage.layout.HashedNTupleIdEncapsulationLayoutExtension;
import edu.wisc.library.ocfl.core.extension.storage.layout.HashedNTupleLayoutExtension;
import java.util.Collections;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class ExtensionSupportEvaluatorTest {

    @Test
    public void returnTrueWhenSupportedExt() {
        var evaluator = new ExtensionSupportEvaluator();
        assertTrue(evaluator.checkSupport(FlatLayoutExtension.EXTENSION_NAME));
        assertTrue(evaluator.checkSupport(HashedNTupleLayoutExtension.EXTENSION_NAME));
        assertTrue(evaluator.checkSupport(HashedNTupleIdEncapsulationLayoutExtension.EXTENSION_NAME));
        assertTrue(evaluator.checkSupport(OcflConstants.MUTABLE_HEAD_EXT_NAME));
    }

    @Test
    public void failWhenUnsupportedAndSetToFail() {
        var evaluator = new ExtensionSupportEvaluator();

        assertEquals(
                "Extension init is not currently supported by ocfl-java.",
                assertThrows(OcflExtensionException.class, () -> {
                            evaluator.checkSupport("init");
                        })
                        .getMessage());
    }

    @Test
    public void doNotFailWhenSetToWarn() {
        var evaluator = new ExtensionSupportEvaluator(UnsupportedExtensionBehavior.WARN, Collections.emptySet());
        assertFalse(evaluator.checkSupport("init"));
    }

    @Test
    public void doNotFailWhenSetToFailAndExtSetToIgnore() {
        var evaluator = new ExtensionSupportEvaluator(UnsupportedExtensionBehavior.FAIL, Set.of("init"));
        assertFalse(evaluator.checkSupport("init"));
    }
}
