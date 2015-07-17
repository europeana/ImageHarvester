package eu.europeana.jobcreator.logic;

import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentReferenceProcessingProfile;
import eu.europeana.harvester.domain.URLSourceType;
import org.joda.time.DateTime;
import org.joda.time.Days;

import java.util.Date;


/**
 * Created by paul on 16/07/15.
 */
public class SourceDocumentReferenceProcessingProfileBuilder {


    public static SourceDocumentReferenceProcessingProfile edmObjectUrl (final String id,
                                                                         final ReferenceOwner owner,
                                                                         final int priority
                                                                        )
    {
        return new SourceDocumentReferenceProcessingProfile(true,
                                                            owner,
                                                            id,
                                                            URLSourceType.OBJECT,
                                                            DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                                                            priority,
                                                            DateTime.now().plusMonths(12).toDate(),
                                                            Days.ONE.toStandardSeconds().getSeconds() * 365L
                                                          );
    }

    public static SourceDocumentReferenceProcessingProfile edmHasView (final String id,
                                                                       final ReferenceOwner owner,
                                                                       final int priority
                                                                      )
    {
        return new SourceDocumentReferenceProcessingProfile(true,
                                                            owner,
                                                            id,
                                                            URLSourceType.HASVIEW,
                                                            DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                                                            priority,
                                                            DateTime.now().plusMonths(12).toDate(),
                                                            Days.ONE.toStandardSeconds().getSeconds() * 365L
                                                          );

    }


    public static SourceDocumentReferenceProcessingProfile edmIsShownAt (final String id,
                                                                         final ReferenceOwner owner,
                                                                         final int priority
                                                                        )
    {
        return new SourceDocumentReferenceProcessingProfile(true,
                                                            owner,
                                                            id,
                                                            URLSourceType.ISSHOWNAT,
                                                            DocumentReferenceTaskType.CHECK_LINK,
                                                            priority,
                                                            DateTime.now().plusMonths(12).toDate(),
                                                            Days.ONE.toStandardSeconds().getSeconds() * 365L
                                                          );

    }

    public static SourceDocumentReferenceProcessingProfile edmIsShownBy (final String id,
                                                                         final ReferenceOwner owner,
                                                                         final int priority
                                                                        )
    {
         return new SourceDocumentReferenceProcessingProfile(true,
                                                             owner,
                                                             id,
                                                             URLSourceType.ISSHOWNBY,
                                                             DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                                                             priority,
                                                             DateTime.now().plusMonths(12).toDate(),
                                                             Days.ONE.toStandardSeconds().getSeconds() * 365L
                                                            );
    }
}
