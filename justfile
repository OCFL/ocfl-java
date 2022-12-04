# Lists available commands
default:
    just --list

# Builds ocfl-java
build:
    ./mvnw -DskipTests clean package

# Installs ocfl-java into local M2
install: build
    ./mvnw -DskipTests clean install

# Runs the tests
test:
    ./mvnw clean test

# Runs the tests that match the pattern
test-filter PATTERN:
    ./mvnw clean test -Dtest={{PATTERN}}

# Applies the code formatter
format:
    ./mvnw spotless:apply
