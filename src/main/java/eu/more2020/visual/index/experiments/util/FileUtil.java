package eu.more2020.visual.index.experiments.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileUtil {

    private static void buildDirectory(String filePath) throws IOException {
        Path pathDir = new File(filePath).toPath();
        Files.createDirectory(pathDir);
    }

    public static void build(String filePath) throws IOException {
        if (!(new File(filePath).exists())) buildDirectory(filePath);
    }
}
