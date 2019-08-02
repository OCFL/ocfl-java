package edu.wisc.library.ocfl.api.model;

public class CommitInfo {

    private User user;
    private String message;

    public User getUser() {
        return user;
    }

    public CommitInfo setUser(User user) {
        this.user = user;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public CommitInfo setMessage(String message) {
        this.message = message;
        return this;
    }

    @Override
    public String toString() {
        return "CommitInfo{" +
                "user=" + user +
                ", message='" + message + '\'' +
                '}';
    }

}
