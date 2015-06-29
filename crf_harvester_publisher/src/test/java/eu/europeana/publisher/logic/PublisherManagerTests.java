package eu.europeana.publisher.logic;

import categories.IntegrationTest;
import com.mongodb.*;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.crf_faketags.extractor.MediaTypeEncoding;
import org.apache.solr.client.solrj.SolrServerException;
import utilities.inverseLogic.CommonPropertyExtractor;
import utilities.inverseLogic.ImagePropertyExtractor;
import utilities.inverseLogic.SoundPropertyExtractor;
import utilities.inverseLogic.VideoPropertyExtractor;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.*;

import static utilities.DButils.*;
import static utilities.ConfigUtils.*;

@Category(IntegrationTest.class)
public class PublisherManagerTests {
    private PublisherConfig publisherConfig;

    @After
    public void tearDown() {
      cleanMongoDatabase(publisherConfig);
      cleanSolrDatabase(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
    }



    private void runPublisher(final PublisherConfig publisherConfig) {
        try {
            final ExecutorService service = Executors.newSingleThreadExecutor();

            final Future<Void> f = service.submit(new Callable<Void>() {
                @Override
                public Void call () throws Exception {
                    new PublisherManager(publisherConfig).start();
                    return null;
                }
            });
            f.get(5, TimeUnit.SECONDS);

        } catch (InterruptedException | ExecutionException e) {
            fail("Failed to execute: " + e.getMessage());
        } catch (TimeoutException e) {
           System.out.println("Future timed out");
        }
    }

    @Test
    public void testPublishAllData() throws  IOException {
        final String pathToData =  "src/test/resources/data-files/validData/";
        publisherConfig = createPublisherConfig("src/test/resources/config-files/validData/publisher.conf");

        loadMongoData(publisherConfig.getSourceMongoConfig(), pathToData + "jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData(publisherConfig.getSourceMongoConfig(), pathToData + "metaInfo.json",
                      "SourceDocumentReferenceMetaInfo");
        loadSOLRData(pathToData + "solrData.json", publisherConfig.getTargetDBConfig().get(0).getSolrUrl());

        runPublisher(publisherConfig);

        final DBCollection sourceMetaInfoDB = publisherConfig.getSourceMongoConfig().connectToDB()
                .getCollection("SourceDocumentReferenceMetaInfo");


        final DBCollection targetMetaInfoDB = publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToDB
                                                                                                                  ()
                                                             .getCollection("WebResourceMetaInfo");


        final BasicDBObject keys = new BasicDBObject();
        keys.put("className", 0);
        keys.put("recordId", 0);

        final DBCursor sourceResults = sourceMetaInfoDB.find(new BasicDBObject(), keys).sort(new BasicDBObject("_id", 1));
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));

        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());

        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        final SolrQuery query = new SolrQuery();
        query.setQuery("is_fulltext:*");
        query.addField("europeana_id");

        try {
            assertEquals(sourceResults.size(), solrServer.query(query).getResults().size());
        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    @Test
    public void testPublishAllDataAfterDate() throws IOException {
        final String pathToData =  "src/test/resources/data-files/filterDataByDate/";
        publisherConfig = createPublisherConfig("src/test/resources/config-files/filterDataByDate" +
                                                        "/publisher.conf");

        loadMongoData(publisherConfig.getSourceMongoConfig(), pathToData + "jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData(publisherConfig.getSourceMongoConfig(), pathToData + "metaInfo.json", "SourceDocumentReferenceMetaInfo");
        loadSOLRData (pathToData + "solrData.json", publisherConfig.getTargetDBConfig().get(0).getSolrUrl());

        runPublisher(publisherConfig);
        final DBCollection sourceMetaInfoDB = publisherConfig.getSourceMongoConfig().connectToDB()
                                                             .getCollection("SourceDocumentReferenceMetaInfo");


        final DBCollection targetMetaInfoDB = publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToDB()
                .getCollection("WebResourceMetaInfo");

        final BasicDBObject findQuery = new BasicDBObject();
        final String[] validIds = new String[]{"902b31943b4e66f5578539bfa60f2b82",
                "75a47a2953516381bd9d4d1220bdcfc3",
                "46787456ff68f404835064723a4d1cd9",
                "cc6103bee8aa27bfa11b851a2377a015",
                "764fff8f4654274f9cff28280d7e0008",
                "2d74ba5b07344f9587b5c67693fcb3a5"
        };

        findQuery.put("_id", new BasicDBObject("$in", Arrays.asList(validIds)));

        final BasicDBObject keys = new BasicDBObject();
        keys.put("className", 0);
        keys.put("recordId", 0);

        final DBCursor sourceResults = sourceMetaInfoDB.find(findQuery, keys).sort(new BasicDBObject("_id", 1));
        System.out.println(sourceResults.getQuery().toString());
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));
        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());

        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        final SolrQuery query = new SolrQuery();
        query.setQuery("is_fulltext:*");
        query.addField("europeana_id");

        try {
            assertEquals(sourceResults.size(), solrServer.query(query).getResults().size());
        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    @Test
    public void testPublishAllDataWithMetaData() throws IOException {
        final String pathToData =  "src/test/resources/data-files/dataWithMissingMetaInfo/";
        publisherConfig = createPublisherConfig("src/test/resources/config-files/dataWithMissingMetaInfo/publisher.conf");


        loadMongoData(publisherConfig.getSourceMongoConfig(), pathToData + "jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData(publisherConfig.getSourceMongoConfig(), pathToData + "metaInfo.json", "SourceDocumentReferenceMetaInfo");
        loadSOLRData (pathToData + "solrData.json", publisherConfig.getTargetDBConfig().get(0).getSolrUrl());

        runPublisher(publisherConfig);
        final DBCollection sourceMetaInfoDB = publisherConfig.getSourceMongoConfig().connectToDB()
                .getCollection("SourceDocumentReferenceMetaInfo");


        final DBCollection targetMetaInfoDB = publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToDB()
                .getCollection("WebResourceMetaInfo");


        final BasicDBObject keys = new BasicDBObject();
        keys.put("className", 0);
        keys.put("recordId", 0);

        final DBCursor sourceResults = sourceMetaInfoDB.find(new BasicDBObject(), keys).sort(new BasicDBObject("_id", 1));
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));

        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());

        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        final SolrQuery query = new SolrQuery();
        query.setQuery("is_fulltext:*");
        query.addField("europeana_id");

        try {
            assertEquals(sourceResults.size(), solrServer.query(query).getResults().size());
        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    @Test
    public void testPublishAllDataWithSolrDocEquivelent() throws IOException {
        final String pathToData =  "src/test/resources/data-files/dataWithMissingSolrDoc/";
        publisherConfig = createPublisherConfig("src/test/resources/config-files/dataWithMissingSolrDoc" +
                                                        "/publisher.conf");

        loadMongoData(publisherConfig.getSourceMongoConfig(), pathToData + "jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData(publisherConfig.getSourceMongoConfig(), pathToData + "metaInfo.json",
                      "SourceDocumentReferenceMetaInfo");
        loadSOLRData(pathToData + "solrData.json", publisherConfig.getTargetDBConfig().get(0).getSolrUrl());

        runPublisher(publisherConfig);
        final DBCollection sourceMetaInfoDB = publisherConfig.getSourceMongoConfig().connectToDB()
                .getCollection("SourceDocumentReferenceMetaInfo");


        final DBCollection targetMetaInfoDB = publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToDB()
                .getCollection("WebResourceMetaInfo");


        final BasicDBObject keys = new BasicDBObject();
        keys.put("className", 0);
        keys.put("recordId", 0);

        final BasicDBObject filterQuery = new BasicDBObject();
        final String[] validIds = new String[]{"902b31943b4e66f5578539bfa60f2b82",
                "cc6103bee8aa27bfa11b851a2377a015",
                "764fff8f4654274f9cff28280d7e0008",
                "2d74ba5b07344f9587b5c67693fcb3a5"
        };
        filterQuery.put("_id", new BasicDBObject("$in", Arrays.asList(validIds)));

        final DBCursor sourceResults = sourceMetaInfoDB.find(filterQuery, keys).sort(new BasicDBObject("_id", 1));
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));

        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());

        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        final SolrQuery query = new SolrQuery();
        query.setQuery("is_fulltext:*");
        query.addField("europeana_id");

        try {
            assertEquals(sourceResults.size(), solrServer.query(query).getResults().size());
        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }


    @Test
    public void testFakeTags() throws IOException {
        final String pathToData =  "src/test/resources/data-files/dataFakeTags/";
        publisherConfig = createPublisherConfig("src/test/resources/config-files/dataFakeTags/publisher.conf");


        loadMongoData(publisherConfig.getSourceMongoConfig(), pathToData + "jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData(publisherConfig.getSourceMongoConfig(), pathToData + "metaInfo.json", "SourceDocumentReferenceMetaInfo");
        loadSOLRData (pathToData + "solrData.json", publisherConfig.getTargetDBConfig().get(0).getSolrUrl());

        runPublisher(publisherConfig);

        final DBCollection sourceMetaInfoDB = publisherConfig.getSourceMongoConfig().connectToDB()
                                                          .getCollection("SourceDocumentReferenceMetaInfo");


        final DBCollection targetMetaInfoDB = publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToDB()
                                                          .getCollection("WebResourceMetaInfo");

        final BasicDBObject keys = new BasicDBObject();
        keys.put("className", 0);
        keys.put("recordId", 0);

        final DBCursor sourceResults = sourceMetaInfoDB.find(new BasicDBObject(), keys).sort(new BasicDBObject("_id", 1));
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));

        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());

        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        final SolrQuery query = new SolrQuery();
        query.setQuery("is_fulltext:*");
        query.addField("europeana_id");
        query.addField("facet_tags");
        query.addField("filter_tags");

        try {
            final QueryResponse solrResults = solrServer.query(query);
            assertEquals(sourceResults.size(), solrResults.getResults().size());

            //go through all solr elements and check to see if the fake tags where generated ok
            for (final SolrDocument document : solrResults.getResults()) {
                final String recordId = (String) document.getFieldValue("europeana_id");
                final Collection<Object> facetTags = document.getFieldValues("facet_tags");

                if (null == facetTags) {
                    fail("Facet tags filed cannot be null for this test!");
                    return;
                }

                for (final Object facetTag : facetTags) {
                    final Map.Entry<String, Object> queryParam = getFacetTagValue((Integer)facetTag);

                    if (null != queryParam) {
                        final BasicDBObject findQuery = new BasicDBObject("recordId", recordId);
                        if (queryParam.getKey().equals("imageMetaInfo.fileSize")) {
                            findQuery.put(queryParam.getKey(), new BasicDBObject("$lt", queryParam.getValue()));
                        }
                        else if (queryParam.getKey().equals("audioMetaInfo.duration")) {
                            findQuery.put(queryParam.getKey(), new BasicDBObject("$lt", queryParam.getValue()));
                        }
                        else if (queryParam.getKey().equals("videoMetaInfo.duration")) {
                            findQuery.put(queryParam.getKey(), new BasicDBObject("$lt", queryParam.getValue()));
                        }
                        else {
                            findQuery.put(queryParam.getKey(), queryParam.getValue());
                        }

                        assertTrue("Unkown facet tag: " + facetTag + " for recordId " + recordId,
                                    sourceMetaInfoDB.find(findQuery).size() == 1
                                  );
                    }
                }
            }
        } catch (Exception e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    private Map.Entry<String, Object> getFacetTagValue(final Integer facetTag) {
        final String mimeTye = CommonPropertyExtractor.getMimeType(facetTag);
        final MediaTypeEncoding mediaType = CommonPropertyExtractor.getType(facetTag);
        final String mediaTypeName = mediaType.name().toLowerCase() + "MetaInfo";

        if (null != mimeTye && !mimeTye.trim().isEmpty()) {
            return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".mimeType", mimeTye);
        }

        switch (CommonPropertyExtractor.getType(facetTag)) {
            case IMAGE:  {
                final String aspectRatio = ImagePropertyExtractor.getAspectRatio(facetTag).trim();
                final Integer size = ImagePropertyExtractor.getSize(facetTag);
                final String color = ImagePropertyExtractor.getColor(facetTag).trim();
                final String colorSpace = ImagePropertyExtractor.getColorSpace(facetTag).trim();

                if (!aspectRatio.isEmpty())
                    return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".orientation", aspectRatio);
                else if (size != Integer.MIN_VALUE)
                    return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".fileSize", size);
                else if (!color.isEmpty())
                    return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".colorPalette", color);
                else if (!colorSpace.isEmpty())
                    return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".colorSpace", colorSpace);
                else return null;
            }

            case VIDEO:
                final Long duration = VideoPropertyExtractor.getDuration(facetTag);
                if (duration != Long.MIN_VALUE) return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".duration", duration);
                else return null;

            case AUDIO:
                final Long durationSound = SoundPropertyExtractor.getDuration(facetTag);
                if (durationSound != Long.MIN_VALUE) return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".duration", durationSound);
                else return null;

            default:
                return null;
        }
    }
}
