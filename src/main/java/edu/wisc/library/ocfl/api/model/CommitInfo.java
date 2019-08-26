package edu.wisc.library.ocfl.api.model;

/**
 * Descriptive information about an object version.
 */
public class CommitInfo {

    private User user;
    private String message;

    /**
     * The user who authored the version
     */
    public User getUser() {
        return user;
    }

    public CommitInfo setUser(User user) {
        this.user = user;
        return this;
    }

    /**
     * Description of version changes
     */
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
