package eu.europeana.uimtester.logic;

import com.typesafe.config.Config;
import eu.europeana.uimtester.domain.UIMTestSample;
import eu.europeana.uimtester.domain.UIMTesterFieldNames;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Helpers to load the UIM Tester record samples from a config object.
 */
public class UIMTestSampleLoader {

    public static final List<UIMTestSample> loadSamplesFromConfig(final Config config) {
        final List<UIMTestSample> results = new ArrayList<>();
        for (final Config individualTestSampleConfig : config.getConfigList(UIMTesterFieldNames.FieldNames.RECORD_SAMPLES)) {
            results.add(loadSingleSamplesFromConfig(individualTestSampleConfig));
        }
        return results;
    }

    public static final UIMTestSample loadSingleSamplesFromConfig(final Config config) {
        final String sampleId = config.getString(UIMTesterFieldNames.FieldNames.SAMPLE_ID);
        final String collectionId = config.getString(UIMTesterFieldNames.FieldNames.COLLECTION_ID);
        final String providerId = config.getString(UIMTesterFieldNames.FieldNames.PROVIDER_ID);
        final String recordId = config.getString(UIMTesterFieldNames.FieldNames.RECORD_ID);
        final String executionId = config.getString(UIMTesterFieldNames.FieldNames.EXECUTION_ID);
        final String edmObjectUrl = (config.hasPath(UIMTesterFieldNames.FieldNames.EDM_OBJECT_URL)) ? config.getString(UIMTesterFieldNames.FieldNames.EDM_OBJECT_URL) : null;
        final List<String> edmHasViewUrls = (config.hasPath(UIMTesterFieldNames.FieldNames.EDM_HAS_VIEWS_URLS)) ? config.getStringList(UIMTesterFieldNames.FieldNames.EDM_HAS_VIEWS_URLS) : Collections.EMPTY_LIST;
        final String edmIsShownByUrl = (config.hasPath(UIMTesterFieldNames.FieldNames.EDM_IS_SHOWN_BY_URL)) ? config.getString(UIMTesterFieldNames.FieldNames.EDM_IS_SHOWN_BY_URL) : null;
        final String edmIsShownAtUrl = (config.hasPath(UIMTesterFieldNames.FieldNames.EDM_IS_SHOWN_AT_URL)) ? config.getString(UIMTesterFieldNames.FieldNames.EDM_IS_SHOWN_AT_URL) : null;

        return new UIMTestSample(sampleId, collectionId, providerId, recordId, executionId, edmObjectUrl, edmHasViewUrls, edmIsShownByUrl, edmIsShownAtUrl);
    }


}
