package eu.more2020.visual.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;

@Configuration
public class InitialDataMigration {

    private final Logger log = LoggerFactory.getLogger(InitialDataMigration.class);
//    private final ApplicationProperties applicationProperties;

    private static void copyFile(File sourceFile, File destinationFile)
        throws IOException {
        try (InputStream in = new FileInputStream(sourceFile);
             OutputStream out = new FileOutputStream(destinationFile)) {
            byte[] buf = new byte[1024];
            int length;
            while ((length = in.read(buf)) > 0) {
                out.write(buf, 0, length);
            }
        }
    }

    public static void copyDirectoryCompatibityMode(File source, File destination) throws IOException {
        if (source.isDirectory()) {
            copyDirectory(source, destination);
        } else {
            copyFile(source, destination);
        }
    }

    private static void copyDirectory(File sourceDirectory, File destinationDirectory) throws IOException {
        if (!destinationDirectory.exists()) {
            destinationDirectory.mkdir();
        }
        for (String f : sourceDirectory.list()) {
            copyDirectoryCompatibityMode(new File(sourceDirectory, f), new File(destinationDirectory, f));
        }
    }

    public InitialDataMigration(ApplicationProperties applicationProperties) throws IOException {
//        this.applicationProperties = applicationProperties;
//        String initialDataPath = getClass().getClassLoader().getResource("initial-data").getPath();
//
//        File workspaceDirectory = new File(applicationProperties.getWorkspacePath());
//        if (!workspaceDirectory.exists()) {
//            workspaceDirectory.mkdir();
//        }
//        log.debug(workspaceDirectory.toPath().toString());
//        for (File file : new File(initialDataPath).listFiles()) {
//            log.debug("Copying metadata file " + file.getName() + " to workspace directory.");
//            try {
//                copyDirectoryCompatibityMode(file, workspaceDirectory.toPath().resolve(file.getName()).toFile());
//            } catch (FileAlreadyExistsException e) {
//                log.debug("Metadata file " + file.getName() + " already exists in workspace directory.");
//            }
//        }
    }
}
