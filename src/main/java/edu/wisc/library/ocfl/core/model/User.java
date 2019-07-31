package edu.wisc.library.ocfl.core.model;

/**
 * OCFL user object.
 */
public class User {

    private String name;
    private String address;

    public User() {

    }

    public User(String name, String address) {
        this.name = name;
        this.address = address;
    }

    /**
     * Name of the person who updated an object.
     */
    public String getName() {
        return name;
    }

    public User setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Address of the person who updated an object.
     */
    public String getAddress() {
        return address;
    }

    public User setAddress(String address) {
        this.address = address;
        return this;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", address='" + address + '\'' +
                '}';
    }

}
