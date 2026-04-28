/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.util.FileUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A file-system backed implementation of {@link ArchiveStorage}.
 *
 * <p>Each storage key is treated as a relative path under the configured {@code rootPath}. If a
 * requested file does not exist on disk, this implementation attempts to load it from the
 * classloader's {@code web/} resource directory and copies it to disk for future requests.
 */
public class FileArchiveStorage implements ArchiveStorage<File> {

    /** Root directory under which all archive files are stored. */
    private final File rootPath;

    /**
     * Creates a new {@link FileArchiveStorage}.
     *
     * @param rootPath root directory for archive files; must not be {@code null}
     * @throws IOException if the canonical path of {@code rootPath} cannot be resolved
     */
    public FileArchiveStorage(File rootPath) throws IOException {
        this.rootPath = checkNotNull(rootPath).getCanonicalFile();
    }

    @Override
    public boolean exists(String key) {
        return new File(rootPath, key).exists();
    }

    @Nullable
    @Override
    public File get(String key) throws IOException {
        if (!exists(key)) {
            return null;
        }
        return new File(rootPath, key);
    }

    @Override
    public void put(String key, String archiveContent) throws IOException {
        File target = new File(rootPath, key);
        writeTargetFile(target, archiveContent);
    }

    @Override
    public void delete(String key) throws IOException {
        File target = new File(rootPath, key);
        Files.deleteIfExists(target.toPath());
    }

    @Override
    public void deletePrefix(String keyPrefix) throws IOException {
        File directory = new File(rootPath, keyPrefix);
        if (directory.isDirectory()) {
            FileUtils.deleteDirectory(directory);
        }
    }

    @Override
    public List<File> getByPrefix(String keyPrefix) throws IOException {
        File directory = new File(rootPath, keyPrefix);
        File[] files = directory.listFiles();
        if (files != null) {
            return Arrays.asList(files);
        }
        return new ArrayList<>();
    }

    @Override
    public void close() {
        // nothing to close
    }

    void writeTargetFile(File target, String json) throws IOException {
        Path parent = target.getParentFile().toPath();

        try {
            Files.createDirectories(parent);
        } catch (FileAlreadyExistsException ignored) {
            // there may be left-over directories from the previous attempt
        }

        Path targetPath = target.toPath();

        // We overwrite existing files since this may be another attempt
        // at fetching this archive.
        // Existing files may be incomplete/corrupt.
        Files.deleteIfExists(targetPath);

        Files.createFile(target.toPath());
        try (FileWriter fw = new FileWriter(target)) {
            fw.write(json);
            fw.flush();
        }
    }
}
