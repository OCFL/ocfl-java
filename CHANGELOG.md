# Changelog

## [Unreleased] - ReleaseDate

## [2.0.1] - 2024-03-01

### Fixed

- Mutable head revision writes were failing if they only add a file with content that was already present in the object: https://github.com/OCFL/ocfl-java/issues/105

### Changed

- Updated dependencies

## [2.0.0] - 2023-05-17

### Fixed

- Delete old revision markers after creating a new mutable HEAD revision: https://github.com/OCFL/ocfl-java/pull/91
- Use an AtomicBoolean to track if the repository has been closed: https://github.com/OCFL/ocfl-java/pull/87

### Changed

- Renamed the package root from `edu.wisc.ocfl` to `io.ocfl`
- Prefer `Files.find()` to `Files.walk()`: https://github.com/OCFL/ocfl-java/pull/92

## [1.5.0] - 2022-10-09

This release adds support for [OCFL spec version 1.1](https://ocfl.io/1.1/spec/). By default, all newly create repositories will now be OCFL 1.1 repositories. Existing repositories will **not** be upgraded automatically. See [Upgrading OCFL Repositories](https://github.com/OCFL/ocfl-java/blob/main/docs/USAGE.md#upgrading-ocfl-repositories) in the readme for details on how to upgrade existing repositories and objects.

There are no functional differences between 1.0 and 1.1. The 1.1 release mostly clarifies some of the 1.0 language so that it is more explicit.

## [1.4.6] - 2022-02-11

### Fixes

1. Always validate content paths so that files outside of an object can never be accessed https://github.com/UW-Madison-Library/ocfl-java/pull/70

## [1.4.5] - 2022-02-04

### Fixes

- The new `0007-n-tuple-omit-prefix-storage-layout` implementation was missing a setter, causing it to fail to deserialize https://github.com/UW-Madison-Library/ocfl-java/pull/69

## [1.4.4] - 2022-01-28

### Fixed

- Fixed how the max zero-padded version is calculated https://github.com/UW-Madison-Library/ocfl-java/pull/66

## [1.4.3] - 2022-01-25

### Fixed

1. MariaDB startup no longer fails if the DB tables exist and the user does not have permission to create them https://github.com/UW-Madison-Library/ocfl-java/pull/62
2. MariaDB now uses MEDIUMBLOB https://github.com/UW-Madison-Library/ocfl-java/pull/62

### Added

1. [0007-n-tuple-omit-prefix-storage-layout](https://ocfl.github.io/extensions/0007-n-tuple-omit-prefix-storage-layout.html) support https://github.com/UW-Madison-Library/ocfl-java/pull/58 and https://github.com/UW-Madison-Library/ocfl-java/pull/64

### Changed

1. Updated dependencies

## [1.4.2] - 2021-01-05

### Changes

- Optimized repository initialization when using S3 storage https://github.com/UW-Madison-Library/ocfl-java/pull/57

## [1.4.1] - 2021-11-10

### Bug fixes

1. When using S3, the key prefix was calculated incorrectly when listing the repository root in the root of a bucket https://github.com/UW-Madison-Library/ocfl-java/pull/55
2. When using S3, paged list results were not being fully loaded https://github.com/UW-Madison-Library/ocfl-java/pull/55

## [1.4.0] - 2021-11-03

### Bug Fixes

1. Fixed a bug in the omit prefix extension that would result in incorrect paths being generated when a multi-character delimiter is used but not found in the object id https://github.com/UW-Madison-Library/ocfl-java/commit/b24159acc2030d59e35c6a5e0d9daef92d41fba0

### New Features

1. Added support for MariaDB https://github.com/UW-Madison-Library/ocfl-java/pull/53 (thanks @MormonJesus69420)

### Breaking Changes

1. There was a large refactor to the storage layer to make the code more reusable (https://github.com/UW-Madison-Library/ocfl-java/pull/52). This resulted in a small breaking change to how repositories are initialized, and perhaps more extensive breaking changes if you had extended ocfl-java's internals. The following is an example of how you'd now initialize a local filesystem based OCFL repository:

```java
new OcflRepositoryBuilder()
                .storage(storage -> storage.fileSystem(repoDir))
                .workDir(workDir)
                .build();
```

## [1.3.1] - 2021-09-29

### Bug fixes

1. There was a bug introduced in `v1.3.0` where duplicate files either copied or streamed into nested paths would leave empty directories in the version content. For example, writing a/b/file.txt, where file.txt is a duplicate of a file already in the object, would result in the empty directories a/b being left in the version. (https://github.com/UW-Madison-Library/ocfl-java/pull/50)

### Improvements

1. Small validation optimizations (https://github.com/UW-Madison-Library/ocfl-java/pull/49)

## [1.3.0] - 2021-09-28

### Bug fixes

1. When validating an object, if a version directory did not have an inventory file, then the contents of its content directory were not validated (https://github.com/UW-Madison-Library/ocfl-java/pull/48)

### Improvements

1. Added `verifyStaging` configuration option, which controls whether the contents of a newly constructed version are verified immediately prior to moving the version into the object. Default: `true` (https://github.com/UW-Madison-Library/ocfl-java/pull/46)
2. Added `verifyInventoryDigest` configuration option, which controls whether inventory digests are verified on read. Default: `true` (https://github.com/UW-Madison-Library/ocfl-java/pull/46)
3. Removed unneeded file existence checks (https://github.com/UW-Madison-Library/ocfl-java/pull/46)
4. `DefaultOcflObjectUpdater.writeFile()` now streams the file directly the its staging location rather than a temp location first (https://github.com/UW-Madison-Library/ocfl-java/pull/46)
5. Updating objects by copying files now computes the digest of the files while copying into staging and then deletes the file after the copy if it's a dup, rather than computing the digest first and then deciding whether to copy. (https://github.com/UW-Madison-Library/ocfl-java/pull/46)
6. Streamlined the S3 logic, reducing the number of list operations, particularly when reading an inventory. (https://github.com/UW-Madison-Library/ocfl-java/pull/46)

### Breaking changes

1. Removed `checkNewVersionFixity` configuration option. This option was disabled by default and controlled whether or not staged content files had their digests checked immediately prior to moving them into the object. This check was completely removed. (https://github.com/UW-Madison-Library/ocfl-java/pull/46)

## [1.2.3] - 2021-09-08

### Bug fixes

1. There was a bug in the validation logic, causing it to fail to validate objects with many versions: https://github.com/UW-Madison-Library/ocfl-java/pull/45
2. There were a few unbuffered input streams, causing performance problems. I added buffers to all streams to be on the safe side (some of the buffering should be handled by the core lib): https://github.com/UW-Madison-Library/ocfl-java/commit/94a52ed4629fd5ef43a5e8ff6d91237efbbc9eef

## [1.2.2] - 2021-08-31

### New Features

1. Update api models so that they can be serialized with jackson: https://github.com/UW-Madison-Library/ocfl-java/pull/44

### Maintenance

1. Update dependencies: https://github.com/UW-Madison-Library/ocfl-java/commit/e518d9564235a0bdc10b0ebcba99a76fcdaa9b99

## [1.2.1] - 2021-07-26

### New Features

1. Added [0006-flat-omit-prefix-storage-layout](https://ocfl.github.io/extensions/0006-flat-omit-prefix-storage-layout.html) extension implementation: https://github.com/UW-Madison-Library/ocfl-java/pull/43

### Maintenance

1. Update dependencies: https://github.com/UW-Madison-Library/ocfl-java/commit/3ed5e95364c8e9778db7e9f175c033ba79943bce

## [1.2.0] - 2021-06-25

### New Features

1. Custom layout extensions may now provide a spec: https://github.com/UW-Madison-Library/ocfl-java/pull/41
2. The repository may now be configured to create zero-padded version numbers: https://github.com/UW-Madison-Library/ocfl-java/pull/42

## [1.1.1] - 2021-04-30

### New Features

1. Added an unsafe operation for adding files to objects without calculating their digest: https://github.com/UW-Madison-Library/ocfl-java/commit/9b8fbcc0b8696b1b5f17d56d19afe414b881a692

### Breaking Changes

1. Changed how the DB object lock works. If you were using the DB lock, you must drop your existing lock table to use this version of `ocfl-java`: https://github.com/UW-Madison-Library/ocfl-java/commit/857b247abd08307a896dc019ff0f9a36490b2f4f

```sql
DROP TABLE ocfl_object_lock;
```

### Bug Fixes

1. Fixed a couple of validation codes: https://github.com/UW-Madison-Library/ocfl-java/commit/549bd4a81abb66df13eb91c7645e32c060afb9bd

## [1.0.3] - 2021-04-05

## New Features

1. Object validation API: https://github.com/UW-Madison-Library/ocfl-java/commit/1dd8f57c3b5a6e0f34bea84267670ddb935bfeac
2. Ensure inventory ID matches expected ID: https://github.com/UW-Madison-Library/ocfl-java/commit/1832e7d2c6dd48fc88b8030d8fb4a0d65416bd81

## Bug Fixes

1. Do not create version content directories when a version has no content: https://github.com/UW-Madison-Library/ocfl-java/commit/9248c1b2974de9c30ed3508a67d310530291ee35
2. Allow `fixity` block to be null: https://github.com/UW-Madison-Library/ocfl-java/commit/1832e7d2c6dd48fc88b8030d8fb4a0d65416bd81
3. Fix junit-params scope: https://github.com/UW-Madison-Library/ocfl-java/commit/aaee98af0e5296a485f3026ca1deb8572c3dcdc2

## [1.0.2] - 2021-03-17

### New Features

1. The OCFL extensions spec is now written to the storage root (https://github.com/UW-Madison-Library/ocfl-java/commit/ec2d62e7161a05b99feda7ccf736828a27dad295)
2. The inventory digest is now calculated as it's being serialized to disk (https://github.com/UW-Madison-Library/ocfl-java/commit/3a202ba44f5e78061a74d97027df6e7d32069cad)
3. Added new APIs for invalidating the inventory cache, if it's used. These methods will also clear out the object details table, if used. (https://github.com/UW-Madison-Library/ocfl-java/commit/9aa2bb6c5f956943207ab3a99090598fb153d5be)

### Bug Fixes

1. Do not iterate into the `extensions` directory when listing objects (https://github.com/UW-Madison-Library/ocfl-java/commit/c1d3732d3ee8e2744085333a2c1b1a1ff9d175f2)
2. The object details table will now always be populated with an identical copy of the inventory to what's on disk (https://github.com/UW-Madison-Library/ocfl-java/commit/3dc5ea48ae879292832d43eb5f7488642486a0ea)
3. Exporting an object/version from S3 now correctly resolves path prefixes (https://github.com/UW-Madison-Library/ocfl-java/commit/779efd14b10c2be38660c63e841bf1aa35a6b579)

## [1.0.1] - 2021-02-08

1. https://github.com/UW-Madison-Library/ocfl-java/commit/eba5583f9bd1093eddd1b2777765135a9937ed0a: Moved `unsupportedExtensionBehavior` definitions up to the repository builder level from the storage layout builders.

## [1.0.0] - 2021-02-05

### Breaking Changes

1. Changed the mutable HEAD extension name: https://github.com/UW-Madison-Library/ocfl-java/commit/da0017f881c187199cc0f7bb24df71eb3598c5a1. This version of `ocfl-java` will not read any existing objects that contain a mutable HEAD. The name was changed to match the name in the final, approved version of the extension and will not change again.

### New Features

1. Configure the behavior of unsupported extensions: https://github.com/UW-Madison-Library/ocfl-java/commit/779e60eb5bf9e9231a2b61e7114cf6ce293191e1. By default, `ocfl-java` will fail when it encounters an extension that it does not support. This behavior may be configured to simply ignore these extensions, if desired.

### Bug Fixes

1. Allow for multiple spaces in the inventory side car: https://github.com/UW-Madison-Library/ocfl-java/commit/f01434ae09e43bc7bb564b82f42eb23866adb824. Previously, the inventory side car parsing logic only worked if the file contained a single space separator. It now correctly accepts any number of spaces.

## [0.2.0] - 2020-12-28

### Breaking Changes

1. Layout extensions are updated to conform to the latest extension spec: https://github.com/UW-Madison-Library/ocfl-java/commit/244abe022fc1f20f18e74717ec4c72b315f5549f. This means that layout configs are now stored at `extensions/NNNN-extension-name/config.json`, and any old configs **will not** be loaded.

### New Features:

1. Programmatically specified layout configurations are now treated as defaults and will be ignored if the repo has a layout config specified on disk: https://github.com/UW-Madison-Library/ocfl-java/commit/4b13382d9f95211393bc9c9a71903828c658a716
1. The table names of DB tables are now configurable: https://github.com/UW-Madison-Library/ocfl-java/commit/8f65d51889ef63667e7a3d6ebb898f1c6744af21
1. There is a new hook for modifying S3 requests: https://github.com/UW-Madison-Library/ocfl-java/commit/e7d006f96da8bca8aed02028afb44a9b941228e9

### Bug Fixes

1. Fixed a bug where verifying the fixity of a file that was streamed into the object would fail: https://github.com/UW-Madison-Library/ocfl-java/commit/0cfb06e8566a2903944121b21a91b4cba1cce7d4

## [0.1.0] - 2020-11-05

This is a preliminary release of ocfl-java. All major functionality is in place. However, there will be a few breaking configuration changes prior to the v1.0.0 release, specifically in regards to configuring storage layouts.

[Unreleased]: https://github.com/ocfl/ocfl-java/compare/v2.0.1...HEAD
[2.0.1]: https://github.com/ocfl/ocfl-java/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/ocfl/ocfl-java/compare/v1.5.0...v2.0.0
[1.5.0]: https://github.com/ocfl/ocfl-java/compare/v1.4.6...v1.5.0
[1.4.6]: https://github.com/ocfl/ocfl-java/compare/v1.4.5...v1.4.6
[1.4.5]: https://github.com/ocfl/ocfl-java/compare/v1.4.4...v1.4.5
[1.4.4]: https://github.com/ocfl/ocfl-java/compare/v1.4.3...v1.4.4
[1.4.3]: https://github.com/ocfl/ocfl-java/compare/v1.4.2...v1.4.3
[1.4.2]: https://github.com/ocfl/ocfl-java/compare/v1.4.1...v1.4.2
[1.4.1]: https://github.com/ocfl/ocfl-java/compare/v1.4.0...v1.4.1
[1.4.0]: https://github.com/ocfl/ocfl-java/compare/v1.3.1...v1.4.0
[1.3.1]: https://github.com/ocfl/ocfl-java/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/ocfl/ocfl-java/compare/v1.2.2...v1.3.0
[1.2.3]: https://github.com/ocfl/ocfl-java/compare/v1.2.2...v1.2.3
[1.2.2]: https://github.com/ocfl/ocfl-java/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/ocfl/ocfl-java/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/ocfl/ocfl-java/compare/v1.1.1...v1.2.0
[1.1.1]: https://github.com/ocfl/ocfl-java/compare/v1.0.3...v1.1.1
[1.0.3]: https://github.com/ocfl/ocfl-java/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/ocfl/ocfl-java/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/ocfl/ocfl-java/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/ocfl/ocfl-java/compare/v0.2.0...v1.0.0
[0.2.0]: https://github.com/ocfl/ocfl-java/compare/v0.1.0...v0.2.0
[1.0.0]: https://github.com/ocfl/ocfl-java/releases/tag/v0.1.0