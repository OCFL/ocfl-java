package io.ocfl.core.test;

import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ocfl.api.OcflRepository;
import io.ocfl.core.DefaultOcflRepository;
import io.ocfl.core.inventory.InventoryMapper;
import io.ocfl.core.util.ObjectMappers;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

public final class ITestHelper {

    private ITestHelper() {}

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
