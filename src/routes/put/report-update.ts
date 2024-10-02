import { NextFunction, Request, Response } from 'express';
import SessionManager from '../../services/session/session-manager.session';
import { ReportUpdateComponent } from './report-update.component';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { BaseReportManagerResponse } from '../../models/report-manager';
import { NotFoundException, DisabledReportTypeException, BadRequestException } from '../../exceptions/';

/**
 * @category Report
 * @summary Update report
 * @method PUT
 *
 * @get id_report [string, required] ID of the report
 * @url /analytics/v1/reports/{id_report}
 *
 * @parameter idReport [string, required] Uuid of the report
 * @parameter loginRequired [boolean, optional] Define if is necessary the login in order to download the report
 * @parameter creationDate [string, optional] Date of creation report
 * @parameter isReportDownloadPermissionLink [boolean, optional] Is report download permission link
 * @parameter isCustomColumnSortingEnabled [boolean, optional] Is custom column sorting enabled
 * @parameter deleted [boolean, required] Is deleted
 * @parameter platform [string, required] Platform related to the report
 * @parameter timezone [string, optional] Timezone of the user at the creation report time
 * @parameter description [string, optional] Report's description
 * @parameter importedFromLegacyId [integer, optional] Related legacy report id
 * @parameter standard [boolean, optional] Is a standard report
 * @parameter author [integer, required] Report's author id
 * @parameter type [string, required] Report's type
 * @parameter title [string, required] Report's title
 * @parameter visibility [object, required] Info about the visibility
 *     @item type [integer, required] Visibility type, 1 = only god admins, 2 = god admins and pu, 3 = god admin and selected pu
 *     @item groups [array, required] List of group id that can see the report
 *         @item id [integer, required]
 *     @end
 *     @item branches [array, required] List of branch id that can see the report
 *         @item id [integer, required]
 *         @item descendants [boolean, optional]
 *     @end
 *     @item users [array, required] List of idst that can see the report
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter planning [object, required] Info about schedule report
 *     @item active [boolean, required] Define if the planning is active or not
 *     @item option [object, required] Schedule option
 *         @item every [int, required] Is correlated to "timeFrame", it defines how many times the report must be schedule (1 day, 1 week)
 *         @item recipients [array(string), required] The array of email recipients
 *         @item timeFrame [string, required] It correlate to "every" and it defines the recurrency: day, week, or month
 *         @item isPaused [boolean, required] Define if the planning is in pause
 *         @item scheduleFrom [string, required] Schedule start date
 *     @end
 * @end
 * @parameter fields [array(string), required] List of fields selected
 * @parameter sortingOptions [object, required] Object that describe the sorting option
 *     @item orderBy [string, required] Order by asc or desc
 *     @item selector [string, required] Type of selector
 *     @item selectedField [string, required] field that define the sorting
 * @end
 * @parameter lastEditBy [object, required] Info about user that last edit the report
 *     @item firstname [string, optional] Name of the user
 *     @item lastname [string, optional] Lastname of the user
 *     @item username [string, optional] Username of the user
 *     @item avatar [string, optional] The avatar url of the user
 *     @item timezone [string, required] Timezone of the user at the creation report time
 * @end
 * @parameter users [object, optional] Object that describe the users filter
 *     @item all [boolean,required] Is all selected
 *     @item hideExpiredUsers [boolean, required] hide expired users
 *     @item hideDeactivated [boolean, required] hide deactivated users
 *     @item isUserAddFields [boolean, required] is user additional field
 *     @item showOnlyLearners [boolean, required] Show only learners user
 *     @item groups [array, required] List of group selected
 *         @item id [integer, required]
 *     @end
 *     @item branches [array, required] List of branches selected
 *         @item id [integer, required]
 *         @item descendants [boolean, optional]
 *     @end
 *     @item users [array, required] List of idst selected
 *         @item id [integer, required]
 *     @end
 *     @item entitiesLimits [object,required] entity limit
 *         @item usersLimit [integer, required] how many user can be selected
 *         @item groupsLimit [integer, required] how many groups can be selected
 *         @item branchesLimit [integer, required] how many branches can be selected
 *     @end
 * @end
 * @parameter courses [object, optional] Object that describe the courses filter
 *     @item courseType [integer, required] The course type to filter. 0: All Types | 1: E-Learning | 2: ILT
 *     @item all [boolean,required] Is all selected
 *     @item categories [array, required] List of id of categories selected
 *         @item id [integer, required]
 *     @end
 *     @item instructors [array, required] List of id of instructors selected
 *         @item id [integer, required]
 *     @end
 *     @item courses [array, required] List of id courses selected
 *         @item id [integer, required]
 *     @end
 *     @item entitiesLimits [object,required] entity limit
 *         @item coursesLimit [integer, required] how many courses can be selected
 *         @item lpLimit [integer, required] how many lp can be selected
 *      @end
 * @end
 * @parameter surveys [object, optional] Object that describe the survey filter
 *     @item all [boolean,required] Is all selected
 *     @item surveys [array, required] The list of id surveys selected
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter learningPlans [object, optional] Object that describe the learningPlans filter
 *     @parameter all [boolean, required] Is all selected
 *     @parameter learningPlans [array, required] The list of id learningPlans selected
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter badges [object, optional] Object that describe the badges filter
 *     @item all [boolean, required] Is all selected
 *     @item badges [array, required] The list of id badges selected
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter assets [object, optional] Object that describe the assets filter
 *     @item all [boolean, required] Is all selected
 *     @item assets [array, required] The list of id assets selected
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter conditions [string, optional] How to combine the different filter
 * @parameter enrollment [object, optional] Object that describe the enrollment filter
 *     @item completed [boolean, required] if status is completed
 *     @item inProgress [boolean, required] if status is in progress
 *     @item notStarted [boolean, required] if status is not started
 *     @item waitingList [boolean, required] if status is waiting list
 *     @item suspended [boolean, required] if status is suspended
 *     @item enrollmentsToConfirm [boolean, required] if status is enrollments to confirm
 *     @item subscribed [boolean, required] if status is subscribed
 *     @item overbooking [boolean, required] if status is overbooking
 *     @item enrollmentTypes [integer, required] enrollment type (1 = active, 2 = archived, 3 = both)
 * @end
 * @parameter sessionDates [object, optional] Object that describe dates for session filter
 *     @item conditions [string, required] How to combine the different filter
 *     @item startDate [object, required] Object that describe the date filter for the session start date
 *          @item days [integer, required] Number of days
 *          @item from [string, required] Date from
 *          @item to [string, required] Date to
 *          @item any [boolean, required] Any date
 *          @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *          @item operator [string, required] Operator
 *    @end
 *    @item endDate [object, required] Object that describe the date filter for the session end date
 *          @item days [integer, required] Number of days
 *          @item from [string, required] Date from
 *          @item to [string, required] Date to
 *          @item any [boolean, required] Any date
 *          @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *          @item operator [string, required] Operator
 *     @end
 * @end
 * @parameter instructors [object, optional] Object that describe the instructors filter
 *     @item all [boolean, required] Is all selected
 *     @item instructors [array, required] The list of instructors selected
 *         @item id [integer, required]
 *     @end
 * @end
 * @parameter certification [object, optional] Info certification filter
 *      @item all [boolean,required] Is all selected
 *      @item expiredCertifications [boolean,required] Include expired certification
 *      @item entitiesLimits [integer,required] How many certification can be selectable
 *      @item activeCertifications [boolean,required] include active certifications
 *      @item conditions [string,required] how to combine the different filter
 *      @item archivedCertifications [boolean,required] include archived certification
 *      @item certifications [array,required] id of the certification selected
 *          @item id [integer, required]
 *      @end
 *      @item certificationDate [object, required] Object that describe the date filter for the certification date
 *          @item days [integer, required] Number of days
 *          @item from [string, required] Date from
 *          @item to [string, required] Date to
 *          @item any [boolean, required] Any date
 *          @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *          @item operator [string, required] Operator
 *     @end
 *     @parameter certificationExpirationDate [object, required] Object that describe the date filter for the certification expiration date
 *          @item days [integer, required] Number of days
 *          @item from [string, required] Date from
 *          @item to [string, required] Date to
 *          @item any [boolean, required] Any date
 *          @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *          @item operator [string, required] Operator
 *     @end
 * @end
 * @parameter enrollmentDate [object, optional] Object that describe the date filter for the enrollment date
 *      @item days [integer, required] Number of days
 *      @item from [string, required] Date from
 *      @item to [string, required] Date to
 *      @item any [boolean, required] Any date
 *      @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *      @item operator [string, required] Operator
 * @end
 * @parameter completionDate [object, optional] Object that describe the date filter for the completion date
 *      @item days [integer, required] Number of days
 *      @item from [string, required] Date from
 *      @item to [string, required] Date to
 *      @item any [boolean, required] Any date
 *      @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *      @item operator [string, required] Operator
 * @end
 * @parameter surveyCompletionDate [object, optional] Object that describe the date filter for the survey completion date
 *      @item days [integer, required] Number of days
 *      @item from [string, required] Date from
 *      @item to [string, required] Date to
 *      @item any [boolean, required] Any date
 *      @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *      @item operator [string, required] Operator
 * @end
 * @parameter archivingDate [object, optional] Object that describe the date filter for the archiving date
 *      @item days [integer, required] Number of days
 *      @item from [string, required] Date from
 *      @item to [string, required] Date to
 *      @item any [boolean, required] Any date
 *      @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *      @item operator [string, required] Operator
 * @end
 * @parameter courseExpirationDate [object, optional] Object that describe the date filter for the course expiration date
 *      @item days [integer, required] Number of days
 *      @item from [string, required] Date from
 *      @item to [string, required] Date to
 *      @item any [boolean, required] Any date
 *      @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *      @item operator [string, required] Operator
 * @end
 * @parameter issueDate [object, optional] Object that describe the date filter for the issue date
 *      @item days [integer, required] Number of days
 *      @item from [string, required] Date from
 *      @item to [string, required] Date to
 *      @item any [boolean, required] Any date
 *      @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *      @item operator [string, required] Operator
 * @end
 * @parameter creationDateOpts [object, optional] Object that describe the date filter for the creation date options
 *      @item days [integer, required] Number of days
 *      @item from [string, required] Date from
 *      @item to [string, required] Date to
 *      @item any [boolean, required] Any date
 *      @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *      @item operator [string, required] Operator
 * @end
 * @parameter expirationDateOpts [object, optional] Object that describe the date filter for the expiration date options
 *      @item days [integer, required] Number of days
 *      @item from [string, required] Date from
 *      @item to [string, required] Date to
 *      @item any [boolean, required] Any date
 *      @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *      @item operator [string, required] Operator
 * @end
 * @parameter publishedDate [object, optional] Object that describe the date filter for the published date
 *      @item days [integer, required] Number of days
 *      @item from [string, required] Date from
 *      @item to [string, required] Date to
 *      @item any [boolean, required] Any date
 *      @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *      @item operator [string, required] Operator
 * @end
 * @parameter contributionDate [object, optional] Object that describe the date filter for the contribution date
 *      @item days [integer, required] Number of days
 *      @item from [string, required] Date from
 *      @item to [string, required] Date to
 *      @item any [boolean, required] Any date
 *      @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *      @item operator [string, required] Operator
 * @end
 * @parameter externalTrainingDate [object, optional] Object that describe the date filter for the external training date
 *      @item days [integer, required] Number of days
 *      @item from [string, required] Date from
 *      @item to [string, required] Date to
 *      @item any [boolean, required] Any date
 *      @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *      @item operator [string, required] Operator
 * @end
 * @parameter externalTrainingStatusFilter [object, optional] Object that describe the external training status filter
 *      @item approved [boolean, required] Filter for approved external training
 *      @item waiting [boolean, required] Filter for waiting external training
 *      @item rejected [boolean, required] Filter for rejected external training
 * @end
 * @parameter publishStatus [object, optional] Object that describe the asset publish status filter
 *      @item published [boolean, required] Filter for published asset
 *      @item unpublished [boolean, required] Filter for unpublished asset
 * @end
 * @parameter sessionAttendanceType [object, optional] Object that describe the VILT session attendance type filter
 *     @item blended [boolean, required] Blended attendance type filter
 *     @item flexible [boolean, required] Flexible attendance type filter
 *     @item fullOnline [boolean, required] Full On-Line attendance type filter
 *     @item fullOnsite [boolean, required] Full On-Site attendance type filter
 * @end
 * @parameter sessions [object, optional] Object that describe the VILT session filter
 *     @item all [boolean, required] Is all selected
 *     @item entitiesLimits [integer, required] The maximum number of element selectable
 *     @item sessions [array, required] The list of VILT sessions selected
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
 * @status 404 Report not found!
 * @status 400 Bad request
 */
export const putReportUpdate = async (req: Request, res: Response, next: NextFunction) => {
    // return PUT response success true or false
    const session: SessionManager = res.locals.session;
    const reportUpdate: ReportUpdateComponent = new ReportUpdateComponent(req, session);
    const responseOptions: BaseReportManagerResponse = { success: true };
    const logger: SessionLoggerService = httpContext.get('logger');


    res.type('application/json');
    res.status(200);

    try {
        await reportUpdate.getReportUpdate();
    } catch (err: any) {
        responseOptions.success = false;
        if (err instanceof NotFoundException || err instanceof DisabledReportTypeException) {
            res.status(404);
            responseOptions.error = 'Report not found!';
            responseOptions.errorCode = err.getCode();
            res.json(responseOptions);
            return;
        } else if (err instanceof BadRequestException) {
            res.status(400);
            logger.errorWithStack(`Bad Request.`, err);
        } else {
            res.status(500);
            logger.errorWithStack(`Internal error while performing a report update.`, err);
        }
    }

    res.json({ success: responseOptions.success });
};
