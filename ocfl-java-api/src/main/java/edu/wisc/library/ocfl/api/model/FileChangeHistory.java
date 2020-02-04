/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package edu.wisc.library.ocfl.api.model;

import java.util.Iterator;
import java.util.List;

/**
 * Contains the complete change history of a file.
 */
public class FileChangeHistory {

    private String path;
    private List<FileChange> fileChanges;

    /**
     * The logical path of the file
     *
     * @return logical path
     */
    public String getPath() {
        return path;
    }

    public FileChangeHistory setPath(String path) {
        this.path = path;
        return this;
    }

    /**
     * An ordered list, oldest to newest, of every change that occurred to the file
     *
     * @return list of changes
     */
    public List<FileChange> getFileChanges() {
        return fileChanges;
    }

    public FileChangeHistory setFileChanges(List<FileChange> fileChanges) {
        this.fileChanges = fileChanges;
        return this;
    }

    /**
     * The most recent file change to occur
     *
     * @return most recent change
     */
    public FileChange getMostRecent() {
        return fileChanges.get(fileChanges.size() - 1);
    }

    /**
     * The oldest change to occur. This will be the change that introduced the file into the object.
     *
     * @return oldest change
     */
    public FileChange getOldest() {
        return fileChanges.get(0);
    }

    /**
     * Iterator for traversing file changes from newest to oldest.
     *
     * @return reverse change iterator
     */
    public Iterator<FileChange> getReverseChangeIterator() {
        return new Iterator<>() {
            private int index = fileChanges.size() - 1;

            @Override
            public boolean hasNext() {
                return index >= 0;
            }

            @Override
            public FileChange next() {
                return fileChanges.get(index--);
            }
        };
    }

    /**
     * Iterator for traversing file changes from oldest to newest
     *
     * @return forward change iterator
     */
    public Iterator<FileChange> getForwardChangeIterator() {
        return fileChanges.iterator();
    }

    @Override
    public String toString() {
        return "FileChangeHistory{" +
                "path='" + path + '\'' +
                ", fileChanges=" + fileChanges +
                '}';
    }

}
