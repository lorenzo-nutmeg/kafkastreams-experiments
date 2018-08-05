package com.nutmeg.kstreams.wordcount;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Read the book from a resource and send lines to a (in-memory) Stream of String.
 */
public class BookReader {

    public static Stream<String> bookLines(String bookResource) throws IOException, URISyntaxException {
        final Path path = Paths.get(ClassLoader.getSystemResource(bookResource).toURI());
        return Files.lines(path);
    }
}
