package edu.wisc.library.ocfl.core.test;

import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.core.DefaultOcflRepository;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.util.ObjectMappers;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

public final class ITestHelper {

    private ITestHelper() {

    }

    public static void fixTime(OcflRepository repository, String timestamp) {
        ((DefaultOcflRepository) repository).setClock(Clock.fixed(Instant.parse(timestamp), ZoneOffset.UTC));
    }

    public static InventoryMapper testInventoryMapper() {
        return new InventoryMapper(prettyPrintMapper());
    }

    public static ObjectMapper prettyPrintMapper() {
        return ObjectMappers.prettyPrintMapper().setDefaultPrettyPrinter(prettyPrinter());
    }

    public static PrettyPrinter prettyPrinter() {
        return new DefaultPrettyPrinter().withObjectIndenter(new DefaultIndenter("  ", "\n"));
    }

}
