package edu.wisc.library.ocfl.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * OCFL user object.
 */
@JsonPropertyOrder({
        "name",
        "address"
})
public class User {

    private final String name;
    private final String address;

    @JsonCreator
    public User(
            @JsonProperty("name") String name,
            @JsonProperty("address") String address) {
        this.name = name;
        this.address = address;
    }

    /**
     * Name of the person who updated an object.
     */
    @JsonGetter("name")
    public String getName() {
        return name;
    }

    /**
     * Address of the person who updated an object.
     */
    @JsonGetter("address")
    public String getAddress() {
        return address;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", address='" + address + '\'' +
                '}';
    }

}
