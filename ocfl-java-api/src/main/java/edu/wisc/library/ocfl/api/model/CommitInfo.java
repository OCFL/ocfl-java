package edu.wisc.library.ocfl.api.model;

/**
 * Descriptive information about an object version.
 */
public class CommitInfo {

    private User user;
    private String message;

    /**
     * Convenience method for constructing a commit info object.
     *
     * @param message the commit message
     * @param userName the name of the user who committed the change
     * @param userAddress the address of the user who committed the change
     * @return commit info
     */
    public static CommitInfo build(String message, String userName, String userAddress) {
        var info = new CommitInfo().setMessage(message);
        if (userName != null || userAddress != null) {
            info.setUser(new User()
                    .setName(userName)
                    .setAddress(userAddress));
        }
        return info;
    }

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
