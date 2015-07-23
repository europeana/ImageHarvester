package eu.europeana.jobcreator.logic;

import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentReferenceProcessingProfile;
import eu.europeana.harvester.domain.URLSourceType;
import org.joda.time.DateTime;
import org.joda.time.Period;

public class SourceDocumentReferenceProcessingProfileBuilder {


    public static SourceDocumentReferenceProcessingProfile edmObjectUrl(final String id,
                                                                        final ReferenceOwner owner,
                                                                        final int priority
    ) {
        return new SourceDocumentReferenceProcessingProfile(true,
                owner,
                id,
                URLSourceType.OBJECT,
                DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                priority,
                DateTime.now().plusMonths(12).toDate(),
                Period.weeks(52).toStandardSeconds().getSeconds());
    }

    public static SourceDocumentReferenceProcessingProfile edmHasView(final String id,
                                                                      final ReferenceOwner owner,
                                                                      final int priority
    ) {
        return new SourceDocumentReferenceProcessingProfile(true,
                owner,
                id,
                URLSourceType.HASVIEW,
                DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                priority,
                DateTime.now().plusMonths(12).toDate(),
                Period.weeks(52).toStandardSeconds().getSeconds());

    }


    public static SourceDocumentReferenceProcessingProfile edmIsShownAt(final String id,
                                                                        final ReferenceOwner owner,
                                                                        final int priority
    ) {
        return new SourceDocumentReferenceProcessingProfile(true,
                owner,
                id,
                URLSourceType.ISSHOWNAT,
                DocumentReferenceTaskType.CHECK_LINK,
                priority,
                DateTime.now().plusMonths(12).toDate(),
                Period.weeks(52).toStandardSeconds().getSeconds());

    }

    public static SourceDocumentReferenceProcessingProfile edmIsShownBy(final String id,
                                                                        final ReferenceOwner owner,
                                                                        final int priority
    ) {
        return new SourceDocumentReferenceProcessingProfile(true,
                owner,
                id,
                URLSourceType.ISSHOWNBY,
                DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                priority,
                DateTime.now().plusMonths(12).toDate(),
                Period.weeks(52).toStandardSeconds().getSeconds());
    }
}
