package eu.europeana.harvester.db.filesystem;

import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.MediaFile;
import org.joda.time.DateTime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;

public class FileSystemMediaStorageClientImpl implements MediaStorageClient {

    private final String folderPath;

    public FileSystemMediaStorageClientImpl(final String folderPath) {
        this.folderPath = folderPath;
    }

    private Path pathOfId(String id) {
        return Paths.get(folderPath + "/" + id);
    }

    @Override
    public Boolean checkIfExists(String id) {
        return Files.exists(Paths.get(folderPath + "/" + id));
    }

    @Override
    public MediaFile retrieve(String id, Boolean withContent) throws IOException, NoSuchAlgorithmException {

        final byte[] content = Files.readAllBytes(pathOfId(id));

        return new MediaFile("s", id, Collections.<String>emptyList(), id,
                id + ".com", DateTime.now(), content, 1, "",
                Collections.<String, String>emptyMap(), content.length);
    }

    @Override
    public void createOrModify(MediaFile mediaFile) {
        try {
            if (!Files.exists(pathOfId(mediaFile.getId()))) {
                Files.createFile(pathOfId(mediaFile.getId()));
            } else {
                Files.delete(pathOfId(mediaFile.getId()));
            }
            Files.write(pathOfId(mediaFile.getId()), mediaFile.getContent(), StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete(String id) throws IOException {
        Files.delete(pathOfId(id));
    }
}
