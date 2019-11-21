package edu.wisc.library.ocfl.core.model;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.*;

/**
 * BiDirection map implementation that focuses on OCFL structures. FileIds are digest values and are case insensitive.
 * A single fileId can map to many paths, but a single path can only map to one fileId.
 */
public class PathBiMap {

    private Map<String, Set<String>> fileIdToPaths;
    private Map<String, String> pathToFileId;

    /**
     * Constructs a new PathBiMap from an existing map of fileIds to paths.
     *
     * @param map fileId =&gt; paths map
     * @return PathBiMap
     */
    public static PathBiMap fromFileIdMap(Map<String, Set<String>> map) {
        Enforce.notNull(map, "map cannot be null");

        var biMap = new PathBiMap();

        map.forEach((digest, paths) -> {
           paths.forEach(path -> biMap.put(digest, path));
        });

        return biMap;
    }

    public PathBiMap() {
        fileIdToPaths = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        pathToFileId = new HashMap<>();
    }

    /**
     * Indicates if the specified fileId is in the map
     *
     * @param fileId fileId
     * @return true if it's in the map
     */
    public boolean containsFileId(String fileId) {
        return fileIdToPaths.containsKey(fileId);
    }

    /**
     * Indicates if the specified path is in the map
     *
     * @param path path
     * @return true if it's in the map
     */
    public boolean containsPath(String path) {
        return pathToFileId.containsKey(path);
    }

    /**
     * Adds a new fileId => path mapping
     *
     * @param fileId fileId
     * @param path path
     */
    public void put(String fileId, String path) {
        fileIdToPaths.computeIfAbsent(fileId, k -> new TreeSet<>(Comparator.naturalOrder())).add(path);
        pathToFileId.put(path, fileId);
    }

    /**
     * Returns all of the paths associated to the fileId, or an empty set.
     *
     * @param fileId fileId
     * @return set of paths
     */
    public Set<String> getPaths(String fileId) {
        return fileIdToPaths.getOrDefault(fileId, Collections.EMPTY_SET);
    }

    /**
     * Returns the fileId associated to the path or null
     *
     * @param path path
     * @return fileId or null
     */
    public String getFileId(String path) {
        return pathToFileId.get(path);
    }

    /**
     * Removes the path from the map. If it is the only path associated to a fileId, then the fileId is also removed.
     *
     * @param path the path to remove
     * @return the fileId of the path that was removed or null
     */
    public String removePath(String path) {
        var fileId = pathToFileId.remove(path);

        if (fileId != null) {
            var paths = fileIdToPaths.get(fileId);
            if (paths != null) {
                if (paths.size() == 1 && paths.contains(path)) {
                    fileIdToPaths.remove(fileId);
                } else {
                    paths.remove(path);
                }
            }
        }

        return fileId;
    }

    /**
     * Removes a fileId from the map. All of its paths are also removed.
     *
     * @param fileId the fileId to remove
     * @return the associated paths
     */
    public Set<String> removeFileId(String fileId) {
        var paths = fileIdToPaths.remove(fileId);

        if (paths == null) {
            return Collections.emptySet();
        }

        paths.forEach(pathToFileId::remove);

        return paths;
    }

    /**
     * Returns an immutable view of the PathBiMap, mapping fileIds to paths.
     *
     * @return immutable fileId =&gt; paths map
     */
    public Map<String, Set<String>> getFileIdToPaths() {
        return Collections.unmodifiableMap(fileIdToPaths);
    }

    /**
     * Returns an immutable view of the PathBiMap, mapping paths to fileIds
     *
     * @return immutable path =&gt; fileId map
     */
    public Map<String, String> getPathToFileId() {
        return Collections.unmodifiableMap(pathToFileId);
    }

    @Override
    public String toString() {
        return "PathBiMap{" +
                "fileIdToPaths=" + fileIdToPaths +
                '}';
    }

}
