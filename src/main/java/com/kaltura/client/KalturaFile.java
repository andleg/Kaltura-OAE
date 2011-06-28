/**
 * Copyright 2011 Unicon (R) Licensed under the
 * Educational Community License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.osedu.org/licenses/ECL-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.kaltura.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * This is an abstraction of a file which allows this construct to hold a File OR a Stream
 * 
 * @author Aaron Zeckoski (azeckoski @ vt.edu)
 */
public class KalturaFile {

    private String name;
    private File file;
    private FileInputStream fileInputStream;

    /**
     * Create a KF from a File object
     * @param file the file (must not be null)
     */
    public KalturaFile(File file) {
        if (file == null) {
            throw new IllegalArgumentException("file must be set");
        }
        if (! file.canRead() || ! file.isFile()) {
            throw new IllegalArgumentException("file ("+file.getAbsolutePath()+") is not readable or not a file");
        }
        this.file = file;
        this.name = this.file.getName();
    }

    /**
     * Create a KF from a FileInputStream object
     * @param fileInputStream the file stream (must not be null)
     */
    public KalturaFile(FileInputStream fileInputStream, String name) {
        if (fileInputStream == null) {
            throw new IllegalArgumentException("fileInputStream must be set");
        }
        if (name == null || "".equals(name)) {
            throw new IllegalArgumentException("name must be set");
        }
        this.fileInputStream = fileInputStream;
        this.name = name;
    }

    /**
     * @return the name for the file (is NEVER null)
     */
    public String getName() {
        return name;
    }

    /**
     * @return the File object if one is set (this can be null)
     */
    public File getFile() {
        return file;
    }

    /**
     * @return the input stream for this File (this is NEVER null)
     */
    public FileInputStream getFileInputStream() {
        FileInputStream fis = fileInputStream;
        if (fileInputStream == null && file != null) {
            try {
                fis = new FileInputStream(file);
            } catch (FileNotFoundException e) {
                // should not be possible for this to happen
                throw new IllegalArgumentException("file ("+file.getAbsolutePath()+") is not readable or not a file");
            }
        }
        return fis;
    }

    /**
     * @return the size of this file
     */
    public long getSize() {
        long size = 0;
        if (file != null) {
            size = file.length();
        } else if (fileInputStream != null) {
            try {
                size = fileInputStream.getChannel().size();
            } catch (IOException e) {
                // should not happen
                throw new RuntimeException("Failure trying to read info from inptustream: "+e, e);
            }
        }
        return size;
    }
}
