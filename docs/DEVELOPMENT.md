# Development

## Requirements

In addition to Java 11 or greater, `ocfl-java` requires [Maven
3](https://maven.apache.org/index.html) for building. You can either
install Maven globally on your system or use the provided `mvnw`
wrapper.

## Common commands

The following example commands are all run in the
project's root directory.

Execute all of the unit tests:

``` shell
./mvnw test
```

Execute all of the unit tests, integration tests, and spotless:

``` shell
./mvnw verify
```

Build the source and install it in your local Maven cache:

``` shell
./mvnw install
```

## Just

Optionally, you may use [just](https://just.systems/) to run build
commands. It is a simple abstraction on top of Maven. The available
commands are:

``` 
Available recipes:
    build               # Builds ocfl-java
    default             # Lists available commands
    format              # Applies the code formatter
    install             # Installs ocfl-java into local M2
    test                # Runs the tests
    test-filter PATTERN # Runs the tests that match the pattern
```

## Formatting

The code is formatted using
[Spotless](https://github.com/diffplug/spotless) and
[palantir-java-format](https://github.com/palantir/palantir-java-format).
The code must be formatted prior to submitting a PR. To do this,
simply run `./mvnw spotless:apply`.

## Testing

`ocfl-java` is extensively tested by integration tests located in the
`ocfl-java-itest` module. These tests run against both storage
implementations. The CI pipeline runs all of the tests against on
Linux and Windows, and tests against a live Amazon S3 bucket.
`ocfl-java's` validator is tested against the [official
fixtures](https://github.com/ocfl/fixtures) in addition to custom
fixtures, as part of the unit tests in the `ocfl-java-core` module.