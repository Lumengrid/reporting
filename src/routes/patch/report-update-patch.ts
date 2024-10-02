import { NextFunction, Request, Response } from 'express';
import { BaseReportManagerResponse } from '../../models/report-manager';
import { ReportNotFoundException } from '../../reports/exceptions/ReportNotFoundException';
import SessionManager from '../../services/session/session-manager.session';
import { ReportUpdate } from '../../reports/components/ReportUpdate';
import { loggerFactory } from '../../services/logger/logger-factory';
import { ReportId } from '../../reports/value_objects/ReportId';
import { ReportException } from '../../reports/exceptions/ReportException';
import { ReportPatchInput } from '../../reports/interfaces/patch.interface';
import { ReportValidation } from '../../reports/components/ReportValidation';
import { MandatoryFieldNotFoundException } from '../../reports/exceptions/MandatoryFieldNotFoundException';

/**
 * @internal
 * @category Report
 * @summary Patch update report
 * @method PATCH
 *
 * @get id_report [string, optional] ID of the report
 * @url /analytics/v1/reports/{id_report}
 *
 * @parameter platform [string, required] Platform related to the report
 * @parameter loginRequired [boolean, optional] Define if is necessary the login in order to download the report
 * @parameter description [string, optional] Report's description
 * @parameter timezone [string, optional] Report's timezone
 * @parameter title [string, optional] Report's title
 * @parameter visibility [object, optional] Info about the visibility
 *     @item type [integer, optional] Visibility type, 1 = only god admins, 2 = god admins and pu, 3 = god admin and selected pu, if set to 3 at least one of groups, branches or users must be set
 *     @item groups [array, optional] List of group id that can see the report
 *         @item id [integer, required]
 *     @end
 *     @item branches [array, optional] List of branch id that can see the report
 *         @item id [integer, required]
 *         @item descendants [boolean, optional]
 *     @end
 *     @item users [array, optional] List of idst that can see the report
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter planning [object, optional] Info about schedule report
 *     @item active [boolean, optional] Define if the planning is active or not
 *     @item option [object, optional] Schedule option
 *         @item every [integer, optional] Is correlated to "timeFrame", it defines how many times the report must be schedule
 *         @item recipients [array(string), optional] The array of email recipients
 *         @item timeFrame [string, optional] It correlate to "every" and it defines the recurrence: day, week, or month
 *         @item scheduleFrom [string, optional] Schedule start date
 *         @item startHour [string, optional] Schedule start hour
 *     @end
 * @end
 * @parameter fields [array(string), optional] List of fields selected
 * @parameter sortingOptions [object, optional] Object that describe the sorting option
 *     @item orderBy [string, optional] Order by asc or desc
 *     @item selector [string, optional] Type of selector, can be default or custom
 *     @item selectedField [string, optional] field that define the sorting
 * @end
 * @parameter users [object, optional] Object that describe the users filter
 *     @item all [boolean, optional] Is all selected
 *     @item hideExpiredUsers [boolean, optional] hide expired users
 *     @item hideDeactivated [boolean, optional] hide deactivated users
 *     @item isUserAddFields [boolean, optional] is user additional field
 *     @item showOnlyLearners [boolean, optional] Show only learners user
 *     @item groups [array, optional] List of group selected
 *         @item id [integer, required]
 *     @end
 *     @item branches [array, optional] List of branches selected
 *         @item id [integer, required]
 *         @item descendants [boolean, optional]
 *     @end
 *     @item users [array, optional] List of idst selected
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter courses [object, optional] Object that describe the courses filter
 *     @item courseType [integer, optional] The course type to filter, can be 0: All Types | 1: E-Learning | 2: ILT
 *     @item all [boolean, optional] Is all selected
 *     @item categories [array, optional] List of id of categories selected
 *         @item id [integer, required]
 *     @end
 *     @item instructors [array, optional] List of id of instructors selected
 *         @item id [integer, required]
 *     @end
 *     @item courses [array, optional] List of id courses selected
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter surveys [object, optional] Object that describe the survey filter
 *     @item all [boolean, optional] Is all selected
 *     @item surveys [array, optional] The list of id surveys selected
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter learningPlans [object, optional] Object that describe the learningPlans filter
 *     @item all [boolean, optional] Is all selected
 *     @item learningPlans [array, optional] The list of id learningPlans selected
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter badges [object, optional] Object that describe the badges filter
 *     @item all [boolean, optional] Is all selected
 *     @item badges [array, optional] The list of id badges selected
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter assets [object, optional] Object that describe the assets filter
 *     @item all [boolean, optional] Is all selected
 *     @item assets [array, optional] The list of id assets selected
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter conditions [string, optional] How to combine the different filter, can be allConditions or atLeastOneCondition
 * @parameter enrollment [object, optional] Object that describe the enrollment filter
 *     @item completed [boolean, optional] if status is completed
 *     @item inProgress [boolean, optional] if status is in progress
 *     @item notStarted [boolean, optional] if status is not started
 *     @item waitingList [boolean, optional] if status is waiting list
 *     @item suspended [boolean, optional] if status is suspended
 *     @item enrollmentsToConfirm [boolean, optional] if status is enrollments to confirm
 *     @item subscribed [boolean, optional] if status is subscribed
 *     @item overbooking [boolean, optional] if status is overbooking
 *     @item enrollmentTypes [integer, optional] enrollment type (1 = active, 2 = archived, 3 = both)
 * @end
 * @parameter sessionDates [object, optional] Object that describe dates for session filter
 *     @item conditions [string, optional] How to combine the different filter, can be allConditions or atLeastOneCondition
 *     @item startDate [object, optional] Object that describe the date filter for the session start date
 *          @item days [integer, optional] Number of days
 *          @item from [string, optional] Date from
 *          @item to [string, optional] Date to
 *          @item any [boolean, optional] Any date
 *          @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *          @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 *    @end
 *    @item endDate [object, optional] Object that describe the date filter for the session end date
 *          @item days [integer, optional] Number of days
 *          @item from [string, optional] Date from
 *          @item to [string, optional] Date to
 *          @item any [boolean, optional] Any date
 *          @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *          @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 *     @end
 * @end
 * @parameter instructors [object, optional] Object that describe the instructors filter
 *     @item all [boolean, optional] Is all selected
 *     @item instructors [array, optional] The list of instructors selected
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter certification [object, optional] Info certification filter
 *      @item all [boolean, optional] Is all selected
 *      @item expiredCertifications [boolean, optional] Include expired certification
 *      @item activeCertifications [boolean, optional] Include active certifications
 *      @item conditions [string, optional] How to combine the different filter, can be allConditions or atLeastOneCondition
 *      @item archivedCertifications [boolean, optional] include archived certification
 *      @item certifications [array, optional] Ids of the certification selected
 *          @item id [integer, required]
 *      @end
 *      @item certificationDate [object, optional] Object that describe the date filter for the certification date
 *          @item days [integer, optional] Number of days
 *          @item from [string, optional] Date from
 *          @item to [string, optional] Date to
 *          @item any [boolean, optional] Any date
 *          @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *          @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 *     @end
 *     @item certificationExpirationDate [object, optional] Object that describe the date filter for the certification expiration date
 *          @item days [integer, optional] Number of days
 *          @item from [string, optional] Date from
 *          @item to [string, optional] Date to
 *          @item any [boolean, optional] Any date
 *          @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *          @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 *     @end
 * @end
 * @parameter enrollmentDate [object, optional] Object that describe the date filter for the enrollment date
 *      @item days [integer, optional] Number of days
 *      @item from [string, optional] Date from
 *      @item to [string, optional] Date to
 *      @item any [boolean, optional] Any date
 *      @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *      @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 * @end
 * @parameter completionDate [object, optional] Object that describe the date filter for the completion date
 *      @item days [integer, optional] Number of days
 *      @item from [string, optional] Date from
 *      @item to [string, optional] Date to
 *      @item any [boolean, optional] Any date
 *      @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *      @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 * @end
 * @parameter surveyCompletionDate [object, optional] Object that describe the date filter for the survey completion date
 *      @item days [integer, optional] Number of days
 *      @item from [string, optional] Date from
 *      @item to [string, optional] Date to
 *      @item any [boolean, optional] Any date
 *      @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *      @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 * @end
 * @parameter archivingDate [object, optional] Object that describe the date filter for the archiving date
 *      @item days [integer, optional] Number of days
 *      @item from [string, optional] Date from
 *      @item to [string, optional] Date to
 *      @item any [boolean, optional] Any date
 *      @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *      @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 * @end
 * @parameter courseExpirationDate [object, optional] Object that describe the date filter for the course expiration date
 *      @item days [integer, optional] Number of days
 *      @item from [string, optional] Date from
 *      @item to [string, optional] Date to
 *      @item any [boolean, optional] Any date
 *      @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *      @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 * @end
 * @parameter issueDate [object, optional] Object that describe the date filter for the issue date
 *      @item days [integer, optional] Number of days
 *      @item from [string, optional] Date from
 *      @item to [string, optional] Date to
 *      @item any [boolean, optional] Any date
 *      @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *      @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 * @end
 * @parameter creationDateOpts [object, optional] Object that describe the date filter for the creation date options
 *      @item days [integer, optional] Number of days
 *      @item from [string, optional] Date from
 *      @item to [string, optional] Date to
 *      @item any [boolean, optional] Any date
 *      @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *      @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 * @end
 * @parameter expirationDateOpts [object, optional] Object that describe the date filter for the expiration date options
 *      @item days [integer, optional] Number of days
 *      @item from [string, optional] Date from
 *      @item to [string, optional] Date to
 *      @item any [boolean, optional] Any date
 *      @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *      @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 * @end
 * @parameter publishedDate [object, optional] Object that describe the date filter for the published date
 *      @item days [integer, optional] Number of days
 *      @item from [string, optional] Date from
 *      @item to [string, optional] Date to
 *      @item any [boolean, optional] Any date
 *      @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *      @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 * @end
 * @parameter contributionDate [object, optional] Object that describe the date filter for the contribution date
 *      @item days [integer, optional] Number of days
 *      @item from [string, optional] Date from
 *      @item to [string, optional] Date to
 *      @item any [boolean, optional] Any date
 *      @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *      @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 * @end
 * @parameter externalTrainingDate [object, optional] Object that describe the date filter for the external training date
 *      @item days [integer, optional] Number of days
 *      @item from [string, optional] Date from
 *      @item to [string, optional] Date to
 *      @item any [boolean, optional] Any date
 *      @item type [integer, optional] The operator for the comparison, can be relative, absolute, range
 *      @item operator [string, optional] Operator, can be isAfter, isBefore, range, expiringIn
 * @end
 * @parameter externalTrainingStatusFilter [object, optional] Object that describe the external training status filter
 *      @item approved [boolean, optional] Filter for approved external training
 *      @item waiting [boolean, optional] Filter for waiting external training
 *      @item rejected [boolean, optional] Filter for rejected external training
 * @end
 * @parameter publishStatus [object, optional] Object that describe the asset publish status filter
 *      @item published [boolean, optional] Filter for published asset
 *      @item unpublished [boolean, optional] Filter for unpublished asset
 * @end
 * @parameter sessionAttendanceType [object, optional] Object that describe the VILT session attendance type filter
 *     @item blended [boolean, optional] Blended attendance type filter
 *     @item flexible [boolean, optional] Flexible attendance type filter
 *     @item fullOnline [boolean, optional] Full On-Line attendance type filter
 *     @item fullOnsite [boolean, optional] Full On-Site attendance type filter
 * @end
 * @parameter sessions [object, optional] Object that describe the VILT session filter
 *     @item all [boolean, optional] Is all selected
 *     @item sessions [array, optional] The list of VILT sessions selected
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter userAdditionalFieldsFilter [object, optional] Key-value string, number object that describe the user additional fields filter
 *     @item idAdditionalField [number, required] The value of the additional field, idAdditionalField is the id of the additional field
 * @end
 * @parameter loTypes [object, optional] Key-value string, boolean object that describe the lo types
 *    @item loTypeKey [boolean, required] Define if the loTypeKey must be filtered
 * @end
 *
 * @response success [boolean, required] Whether or not the operation was successful
 *
 * @status 1000 Report id is not a valid UUID
 * @status 1001 Platform is not a valid string
 * @status 1002 Report not found
 * @status 1003 Mandatory field "X" not found
 * @status 1004 Invalid field "X"
 * @status 1005 Field "X" not editable
 */
export const patchReportUpdate = async (req: Request, res: Response, next: NextFunction) => {
    const responseOptions: BaseReportManagerResponse = {success: true};
    const sessionManager: SessionManager = res.locals.session;
    const platform = sessionManager.platform.getPlatformBaseUrl();
    const logger = loggerFactory.buildLogger('[ReportPatch]', platform);
    res.type('application/json');
    res.status(200);
    try {
        const platform = req.body.platform;
        if (!platform) {
            throw new MandatoryFieldNotFoundException('platform');
        }
        const reportId = new ReportId(req.params.id_report ?? '', platform);
        const patchReport = new ReportUpdate(
            logger,
            sessionManager.hydra,
            sessionManager.user,
            sessionManager.platform
        );
        await patchReport.execute(reportId, true, req.body as ReportPatchInput);
    } catch (error: any) {
        responseOptions.success = false;
        logger.errorWithException({ message: 'Error during Patch Report Update' }, error);
        if (error instanceof ReportException) {
            res.status(error instanceof ReportNotFoundException ? 404 : 400);
            responseOptions.error = error.message;
            responseOptions.errorCode = error.getCode();
            res.json(responseOptions);
            return;
        }

        res.status(500);
    }

    res.json({success: responseOptions.success});
};
