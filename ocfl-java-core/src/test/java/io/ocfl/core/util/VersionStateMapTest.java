package io.ocfl.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.ocfl.core.model.Version;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class VersionStateMapTest {

    private final Version version = Version.builder()
            .created(OffsetDateTime.now())
            .addFile("i1", "f1")
            .addFile("i2", "dir/f2")
            .addFile("i2", "dir/f2-copy")
            .build();

    @Test
    public void shouldOnlyMapRequestedPathWhenLookingUpSinglePath() {
        var mapped = new ArrayList<String>();
        var map = new VersionStateMap<>(version, (path, fileId) -> {
            mapped.add(path);
            return path + "=" + fileId;
        });

        assertThat(map.get("dir/f2")).isEqualTo("dir/f2=i2");
        assertThat(map.containsKey("f1")).isTrue();
        assertThat(map.size()).isEqualTo(3);
        assertThat(map.keySet()).containsExactlyInAnyOrder("f1", "dir/f2", "dir/f2-copy");
        assertThat(map.keySet().contains("dir/f2-copy")).isTrue();
        assertThat(mapped).containsExactly("dir/f2");
    }

    @Test
    public void shouldReturnNullWhenPathNotInVersion() {
        var map = new VersionStateMap<>(version, (path, fileId) -> path);

        assertThat(map.get("missing")).isNull();
        assertThat(map.get(1)).isNull();
        assertThat(map.containsKey("missing")).isFalse();
        assertThat(map.containsKey(null)).isFalse();
    }

    @Test
    public void shouldEqualEagerlyMappedState() {
        var map = new VersionStateMap<>(version, (path, fileId) -> path + "=" + fileId);

        var expected = new HashMap<String, String>();
        expected.put("f1", "f1=i1");
        expected.put("dir/f2", "dir/f2=i2");
        expected.put("dir/f2-copy", "dir/f2-copy=i2");

        assertThat(map).isEqualTo(expected);
        assertThat(expected).isEqualTo(map);
        assertThat(map.hashCode()).isEqualTo(expected.hashCode());
        assertThat(map.values()).containsExactlyInAnyOrderElementsOf(expected.values());
    }

    @Test
    public void shouldBeEmptyWhenVersionHasNoFiles() {
        var map = new VersionStateMap<>(
                Version.builder().created(OffsetDateTime.now()).build(), (path, fileId) -> path);

        assertThat(map).isEmpty();
        assertThat(map.entrySet()).isEmpty();
    }

    @Test
    public void shouldRejectModification() {
        Map<String, String> map = new VersionStateMap<>(version, (path, fileId) -> path);

        assertThatThrownBy(() -> map.put("f3", "f3")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> map.remove("f1")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(map::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> map.entrySet().iterator().next().setValue("x"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(List.copyOf(map.keySet())).containsExactlyInAnyOrder("f1", "dir/f2", "dir/f2-copy");
    }
}
