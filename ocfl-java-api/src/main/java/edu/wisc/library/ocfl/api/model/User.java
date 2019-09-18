package edu.wisc.library.ocfl.api.model;

/**
 * Details about the user who authored an object version
 */
public class User {

    private String name;
    private String address;

    /**
     * Name of the user
     */
    public String getName() {
        return name;
    }

    public User setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Email address of the user
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
