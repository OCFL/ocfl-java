package edu.wisc.library.ocfl.api;

public class CommitMessage {

    private String user;
    private String address;
    private String message;

    public CommitMessage() {

    }

    public String getUser() {
        return user;
    }

    public CommitMessage setUser(String user) {
        this.user = user;
        return this;
    }

    public String getAddress() {
        return address;
    }

    public CommitMessage setAddress(String address) {
        this.address = address;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public CommitMessage setMessage(String message) {
        this.message = message;
        return this;
    }

    @Override
    public String toString() {
        return "CommitMessage{" +
                "user=" + user +
                ", address='" + address + '\'' +
                ", message='" + message + '\'' +
                '}';
    }

}
