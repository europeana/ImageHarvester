package eu.europeana.uimtester.reportprocessingjobs.logic;

import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.db.swift.SwiftConfiguration;
import eu.europeana.harvester.db.swift.SwiftMediaStorageClientImpl;
import eu.europeana.harvester.domain.*;
import eu.europeana.uimtester.reportprocessingjobs.domain.ProcessingJobWithStatsAndResults;

import javax.print.attribute.standard.Media;
import java.io.IOException;
import java.io.Writer;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

public class ProcessingJobReportWriter {
    private final Writer writer;
    private final boolean printMore;
    private final MediaStorageClient swiftMediaStorage;

    public ProcessingJobReportWriter (final Writer writer, final SwiftConfiguration configuration, boolean printMore) {
       this.writer = writer;
       this.printMore = printMore;

       if (printMore && null != configuration) {
           swiftMediaStorage = new SwiftMediaStorageClientImpl(configuration);
       }
       else {
           swiftMediaStorage = null;
       }
    }

    public String objectsToString(final List<ProcessingJobWithStatsAndResults> inputs) {
        final StringBuilder output = new StringBuilder();

        output.append("processingJobStats = [\n");
        for (final ProcessingJobWithStatsAndResults input : inputs) {
            output.append(objectToString(input).replaceAll("(?m)^", "\t")).append("\n");
        }
        output.append("]\n");
        return output.toString();
    }

    public String objectToString(final ProcessingJobWithStatsAndResults input){
        final StringBuilder output = new StringBuilder();
        output.append("\t {\n");
        output.append("\t\t").append("job.id").append(" = ").append(input.getProcessingJob().getId()).append("\n");
        output.append("\t\t").append("job.state").append(" = ").append(input.getProcessingJob().getState().name()).append("\n");
        output.append("\t\t").append("referenceOwner.executionId").append(" = ").append(input.getProcessingJob()
                                                                                             .getReferenceOwner()
                                                                                             .getExecutionId());
        output.append("\t\t").append("referenceOwner.recordId").append(" = ").append(input.getProcessingJob()
                                                                                          .getReferenceOwner()
                                                                                          .getRecordId());
        output.append("\t\t").append("referenceOwner.collectionId").append(" = ").append(input.getProcessingJob()
                                                                                              .getReferenceOwner()
                                                                                              .getCollectionId());
        output.append("\t\t").append("referenceOwner.providerId").append(" = ").append(input.getProcessingJob().getReferenceOwner().getProviderId());
        output.append("\t\t").append("ipAddress").append(" = ").append(input.getProcessingJob().getIpAddress());
        output.append("\t\t").append("sourceDocumentReference = [\n");
        for (final SourceDocumentReference reference : input.getSourceDocumentReferenceList()) {
            output.append("\t\t\t").append("{\n");
            output.append("\t\t\t\t").append("sourceDocumenReferenceId = " + reference.getId());
            output.append("\t\t\t\t").append("url = ").append(reference.getUrl());
            if (null != reference.getUrlSourceType()) {
                output.append("\t\t\t\t").append("urlSourceType = ").append(reference.getUrlSourceType());
            }
            output.append("\t\t\t").append("}\n");
        }
        output.append("\t\t").append("]\n");





        if (printMore) {
            output.append("\t\t").append("taskStatistics = [\n");
            for (final Map.Entry<String, SourceDocumentProcessingStatistics> entry: input.getSourceDocumentReferenceIdToStatsMap().entrySet()) {
                final SourceDocumentProcessingStatistics stats = entry.getValue();

                if (null == stats) {
                    output.append("\t\t\t {\n");
                    output.append("\t\t\t\t").append(" id = " + entry.getKey()).append("\n");
                    output.append("\t\t\t\t").append(" info = \"statistics is null\"").append("\n");
                    output.append("\t\t\t }\n");
                    continue;
                }

                output.append("\t\t\t {\n");
                output.append("\t\t\t\t").append("taskType = " + stats.getTaskType()).append("\n");
                output.append("\t\t\t\t").append("createdAt = \"" + stats.getCreatedAt()).append("\"\n");
                output.append("\t\t\t\t").append("state = " + stats.getState()).append("\n");
                output.append("\t\t\t\t").append("isActive = " + stats.getActive()).append("\n");
                output.append("\t\t\t\t").append("httpCode = " + stats.getHttpResponseCode()).append("\n");
                output.append("\t\t\t\t").append("httpContentType = " + stats.getHttpResponseContentType()).append("\n");
                output.append("\t\t\t\t").append("httpContentSize = " + stats.getHttpResponseContentSizeInBytes()).append("B\n");
                output.append("\t\t\t\t").append("log = \"" + stats.getLog()).append("\"\n");
                output.append("\t\t\t }\n");
            }
            output.append("\t\t]\n");

            if (null != swiftMediaStorage) {

                output.append("\t\t").append("metaInfoExistenceInSwift = [\n");

                for (final Map.Entry<String, SourceDocumentReferenceMetaInfo> entry : input.getSourceDocumentReferenceIdTometaInfoMap()

                                                                                           .entrySet()) {
                    final SourceDocumentReferenceMetaInfo metaInfo = entry.getValue();
                    output.append("\t\t\t {\n");
                    if (null == metaInfo) {
                        output.append("\t\t\t\t").append(" id = " + entry.getKey()).append("\n");
                        output.append("\t\t\t\t").append(" info = \"metainfo is null\"").append("\n");
                        output.append("\t\t\t }\n");
                        continue;
                    }
                    String url = getMediaFileName(metaInfo);

                    output.append("\t\t\t\t").append("id = " + metaInfo.getId()).append("\n");
                    output.append("\t\t\t\t").append("mediaFileName = " + url).append("\n");
                    output.append("\t\t\t\t").append("exists = " + swiftMediaStorage.checkIfExists(url)).append("\n");
                    output.append("\t\t\t }\n");
                }

                output.append("\t\t ]\n");
            }
        }

        output.append("\t }\n");
        return output.toString();
    }

    private String getMediaFileName (SourceDocumentReferenceMetaInfo metaInfo) {
        if (null == metaInfo) return "";
        if (null == metaInfo.getImageMetaInfo()) return metaInfo.getId();

        final int height = metaInfo.getImageMetaInfo().getHeight();
        String sizeName = "Original";

        if (ThumbnailType.LARGE.getHeight() == height) {
            sizeName = ThumbnailType.LARGE.name();
        }
        else if (ThumbnailType.MEDIUM.getHeight() == height) {
            sizeName = ThumbnailType.MEDIUM.name();
        }

        return metaInfo.getId() + "-" + sizeName;
    }

    public void write(final String content) throws IOException {
        writer.write(content);
        writer.flush();
    }

    public Writer getWriter () {
        return writer;
    }

    public boolean isPrintMore () {
        return printMore;
    }
}
