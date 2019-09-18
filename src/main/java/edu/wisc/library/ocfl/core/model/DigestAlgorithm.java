package edu.wisc.library.ocfl.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public enum DigestAlgorithm {

    md5("md5","md5"),
    sha1("sha1", "sha-1"),
    sha256("sha256", "sha-256"),
    sha512("sha512", "sha-512"),
    blake2b("blake2b", "blake2b-512");

    private final String value;
    private final String javaStandardName;

    private DigestAlgorithm(String value, String javaStandardName) {
        this.value = value;
        this.javaStandardName = javaStandardName;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    public String getJavaStandardName() {
        return javaStandardName;
    }

    public MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance(javaStandardName);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @JsonCreator
    public static DigestAlgorithm fromValue(String value) {
        for (var entry : values()) {
            if (entry.value.equalsIgnoreCase(value)) {
                return entry;
            }
        }

        throw new IllegalArgumentException("Unknown DigestAlgorithm: " + value);
    }

}
