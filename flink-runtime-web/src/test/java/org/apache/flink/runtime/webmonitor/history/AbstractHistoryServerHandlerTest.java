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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.handler.router.Router;
import org.apache.flink.runtime.webmonitor.testutils.HttpUtils;
import org.apache.flink.runtime.webmonitor.utils.WebFrontendBootstrap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Common HTTP-level tests for {@link AbstractHistoryServerHandler} subclasses. Subclasses only need
 * to provide a concrete handler instance via {@link #createHandler(File)}.
 */
abstract class AbstractHistoryServerHandlerTest {

    /** Creates the concrete handler under test, bound to the given web directory. */
    protected abstract AbstractHistoryServerHandler<?> createHandler(File webDir) throws Exception;

    /**
     * Tests requests against static files served from the {@code web/} resources packaged with the
     * handler module:
     *
     * <ul>
     *   <li>a request for {@code /} is rewritten to {@code /index.html};
     *   <li>{@code /index.html} is loaded via the classloader fallback when not present on disk;
     *   <li>a missing static file (e.g. {@code /hello.html}) results in 404.
     * </ul>
     */
    @Test
    void testRespondWithStaticFile(@TempDir Path tmpDir) throws Exception {
        runWithTestBody(
                tmpDir,
                (webDir, handler, baseUrl) -> {
                    // /index.html is loaded from the classloader (web/index.html) and served
                    Tuple2<Integer, String> index = HttpUtils.getFromHTTP(baseUrl + "/index.html");
                    assertThat(index.f0).isEqualTo(200);
                    assertThat(index.f1).contains("Apache Flink Web Dashboard");

                    // a trailing slash is rewritten to "/index.html"
                    Tuple2<Integer, String> index2 = HttpUtils.getFromHTTP(baseUrl + "/");
                    assertThat(index2).isEqualTo(index);

                    // a static file that is neither on disk nor in the classloader yields 404
                    Tuple2<Integer, String> missing =
                            HttpUtils.getFromHTTP(baseUrl + "/hello.html");
                    assertThat(missing.f0).isEqualTo(404);
                    assertThat(missing.f1).contains("not found");
                });
    }

    /**
     * Tests file-system safety checks performed by {@code responseWithFile}:
     *
     * <ul>
     *   <li>a directory request returns 405;
     *   <li>a path that escapes the {@code webDir} returns 403.
     * </ul>
     */
    @Test
    void testRespondWithFile(@TempDir Path tmpDir) throws Exception {
        runWithTestBody(
                tmpDir,
                (webDir, handler, baseUrl) -> {
                    // requesting a directory rather than a file yields 405
                    Files.createDirectory(webDir.resolve("dir.json"));
                    Tuple2<Integer, String> dirNotFound =
                            HttpUtils.getFromHTTP(baseUrl + "/dir.json");
                    assertThat(dirNotFound.f0).isEqualTo(405);
                    assertThat(dirNotFound.f1).contains("not found");

                    // requesting a file outside of the webDir is rejected with 403
                    Files.createFile(tmpDir.resolve("secret"));
                    Tuple2<Integer, String> outsideWebDir =
                            HttpUtils.getFromHTTP(baseUrl + "/../secret");
                    assertThat(outsideWebDir.f0).isEqualTo(403);
                    assertThat(outsideWebDir.f1).contains("Forbidden");
                });
    }

    /**
     * Tests requests served via the {@link ArchiveStorage} resource.
     *
     * <ul>
     *   <li>an existing entry is returned with its content;
     *   <li>a missing entry results in 404.
     * </ul>
     */
    @Test
    void testRespondWithResource(@TempDir Path tmpDir) throws Exception {
        runWithTestBody(
                tmpDir,
                (webDir, handler, baseUrl) -> {
                    String resourcePath = "/overviews/job1.json";
                    String resourceContent = "{\"job\":\"job1\"}";
                    handler.archiveStorage.put(resourcePath, resourceContent);

                    // request without an extension: handler will append ".json" and serve the
                    // entry from the ArchiveStorage via respondWithResource
                    Tuple2<Integer, String> resource =
                            HttpUtils.getFromHTTP(baseUrl + "/overviews/job1");
                    assertThat(resource.f0).isEqualTo(200);
                    assertThat(resource.f1).isEqualTo(resourceContent);

                    // request a missing resource: archiveStorage returns null and the handler
                    // responds 404
                    Tuple2<Integer, String> missing = HttpUtils.getFromHTTP(baseUrl + "/hello");
                    assertThat(missing.f0).isEqualTo(404);
                    assertThat(missing.f1).contains("not found");
                });
    }

    /**
     * Boots up a {@link WebFrontendBootstrap} backed by the handler under test and hands the
     * resolved {@code webDir}, the handler instance and the server's base URL to the given test
     * body. Takes care of tearing the server down afterwards.
     */
    private void runWithTestBody(Path tmpDir, TestBody testBody) throws Exception {
        final Path webDir = Files.createDirectory(tmpDir.resolve("webDir"));
        final Path uploadDir = Files.createDirectory(tmpDir.resolve("uploadDir"));

        AbstractHistoryServerHandler<?> handler = createHandler(webDir.toFile());
        Router router = new Router().addGet("/:*", handler);
        WebFrontendBootstrap webUI =
                new WebFrontendBootstrap(
                        router,
                        LoggerFactory.getLogger(getClass()),
                        uploadDir.toFile(),
                        null,
                        "localhost",
                        0,
                        new Configuration());

        try {
            testBody.accept(webDir, handler, "http://localhost:" + webUI.getServerPort());
        } finally {
            webUI.shutdown();
        }
    }

    /** Body of a test that runs against a live {@link WebFrontendBootstrap} instance. */
    @FunctionalInterface
    private interface TestBody {
        void accept(Path webDir, AbstractHistoryServerHandler<?> handler, String baseUrl)
                throws Exception;
    }
}
