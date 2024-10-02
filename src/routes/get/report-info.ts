import { NextFunction, Request, Response } from 'express';
import { ReportManagerSwitcher } from '../../models/report-switcher';
import { ReportManagerInfoResponse } from '../../models/report-manager';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { DisabledReportTypeException, NotFoundException } from '../../exceptions/';

/**
 * @category Report
 * @summary Get report info
 * @method GET
 *
 * @get id_report [string, required] ID of the report
 * @url /analytics/v1/reports/{id_report}
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, required] Response's data
 *      @item lastEdit [string, required] Last edit datetime
 *      @item loginRequired [boolean, required] Define if is necessary the login in order to download the report
 *      @item creationDate [string,required] Date of creation report
 *      @item idReport [string, required] Uuid of the report
 *      @item isReportDownloadPermissionLink [boolean, required] Is report download permission link
 *      @item isCustomColumnSortingEnabled [boolean, required] Is custom column sorting enabled
 *      @item deleted [boolean, required] Is deleted
 *      @item platform [string, required] Platform related to the report
 *      @item timezone [string, required] Timezone of the user at the creation report time
 *      @item description [string, required] Report's description
 *      @item standard [boolean, required] Is a standard report
 *      @item author [integer, required] Report's author id
 *      @item typeDescription [string, required] Report's type
 *      @item title [string, required] Report's title
 *      @item conditions [string,required] how to combine the different filter
 *      @item type [string, required] Report's type
 *      @item vILTUpdated [boolean, required] Check if is aligned to vILT
 *      @item lastEditBy [object, required] Info about user that last edit the report
 *          @item firstname [string, required] Name of the user
 *          @item lastname [string, required] Lastname of the user
 *          @item username [string, required] Username of the user
 *          @item avatar [string, required] The avatar url of the user
 *          @item timezone [string, required] User's timezone
 *      @end
 *      @item completionDate [object, required] Object that describe the date filter for the completion date
 *          @item days [integer, required] Number of days
 *          @item from [string, required] Date from
 *          @item to [string, required] Date to
 *          @item any [boolean, required] Any date
 *          @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *          @item operator [string, required] Operator
 *      @end
 *      @item surveyCompletionDate [object, required] Object that describe the date filter for the survey completion date
 *          @item days [integer, required] Number of days
 *          @item from [string, required] Date from
 *          @item to [string, required] Date to
 *          @item any [boolean, required] Any date
 *          @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *          @item operator [string, required] Operator
 *      @end
 *      @item courseExpirationDate [object, required] Object that describe the date filter for the course expiration date
 *          @item days [integer, required] Number of days
 *          @item from [string, required] Date from
 *          @item to [string, required] Date to
 *          @item any [boolean, required] Any date
 *          @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *          @item operator [string, required] Operator
 *      @end
 *      @item enrollmentDate [object, required] Object that describe the date filter for the enrollment date
 *          @item days [integer, required] Number of days
 *          @item from [string, required] Date from
 *          @item to [string, required] Date to
 *          @item any [boolean, required] Any date
 *          @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *          @item operator [string, required] Operator
 *      @end
 *      @item archivingDate [object, required] Object that describe the date filter for the archiving date
 *           @item days [integer, required] Number of days
 *           @item from [string, required] Date from
 *           @item to [string, required] Date to
 *           @item any [boolean, required] Any date
 *           @item type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *           @item operator [string, required] Operator
 *      @end
 *      @item visibility [object, required] Info about the visibility
 *          @item groups [array(integer), required] List of group id that can see the report
 *          @item type [integer, required] Visibility type, 1 = only god admins, 2 = god admins and pu, 3 = god admin and selected pu
 *          @item branches [array(integer), required] List of branch id that can see the report
 *          @item users [array(integer), required] List of idst that can see the report
 *      @end
 *      @item planning [object, required] Info about schedule report
 *          @item active [boolean, required] Define if the planning is active or not
 *          @item option [object, required] Schedule option
 *              @item every [int, required] Is correlated to "timeFrame", it defines how many times the report must be schedule (1 day, 1 week)
 *              @item recipients [array(string), required] The array of email recipients
 *              @item timeFrame [string, required] It correlate to "every" and it defines the recurrency: day, week, or month
 *              @item isPaused [boolean, required] Define if the planning is in pause
 *              @item scheduleFrom [string, required] Schedule start date
 *              @item startHour [string, required] Schedule start hour
 *          @end
 *      @end
 *      @item learningPlans [object, required] Info about schedule report
 *          @item all [boolean,required] Is all selected
 *          @item entitiesLimit [int, required] entity limit
 *          @item learningPlans [array(integer), required] Lp selected
 *      @end
 *      @item fields [array(string), required] List of fields selected
 *      @item sortingOptions [object, required] Object that describe the sorting option
 *          @item orderBy [string, required] Order by asc or desc
 *          @item selector [string, required] Type of selector
 *          @item selectedField [string, required] field that define the sorting
 *      @end
 *      @item courses [object, required] Object that describe the courses filter
 *          @item all [boolean,required] Is all selected
 *          @item categories [array(integer), required] List of id of categories selected
 *          @item instructors [array(integer), required] List of id of instructors selected
 *          @item courses [array(integer), required] List of id courses selected
 *          @item courseType [integer, required] The course type to filter. 0: All Types | 1: E-Learning | 2: ILT
 *          @item entitiesLimits [object,required] entity limit
 *              @item coursesLimit [integer, required] how many courses can be selected
 *              @item lpLimit [integer, required] how many lp can be selected
 *          @end
 *      @end
 *      @item users [object, required] Object that describe the users filter
 *          @item all [boolean,required] Is all selected
 *          @item hideExpiredUsers [boolean, required] hide expired users
 *          @item hideDeactivated [boolean, required] hide deactivated users
 *          @item isUserAddFields [boolean, required] is user additional field
 *          @item groups [array(integer), required] List of group selected
 *          @item branches [array(integer), required] List of branches selected
 *          @item showOnlyLearners [boolean, required] Show only learners user
 *          @item users [array(integer), required] List of idst selected
 *          @item entitiesLimits [object,required] entity limit
 *              @item usersLimit [integer, required] how many user can be selected
 *              @item groupsLimit [integer, required] how many groups can be selected
 *              @item branchesLimit [integer, required] how many branches can be selected
 *          @end
 *      @end
 *      @item enrollment [object, required] Object that describe the enrollment filter
 *          @item completed [boolean, required] if status is completed
 *          @item inProgress [boolean, required] if status is in progress
 *          @item notStarted [boolean, required] if status is not started
 *          @item waitingList [boolean, required] if status is waiting list
 *          @item suspended [boolean, required] if status is suspended
 *          @item enrollmentsToConfirm [boolean, required] if status is enrollments to confirm
 *          @item subscribed [boolean, required] if status is subscribed
 *          @item overbooking [boolean, required] if status is overbooking
 *          @item enrollmentTypes [integer, required] enrollment type (1 = active, 2 = archived, 3 = both)
 *      @end
 *      @item sessions [object, optional] Object that describe the VILT session filter
 *          @item all [boolean,required] Is all selected
 *          @item sessions [array(integer), required] The list of VILT sessions selected
 *          @item entitiesLimits [integer, required] The maximum number of element selectable
 *      @end
 *      @item sessionAttendanceType [object, optional] Object that describe the VILT session attendance type filter
 *          @item blended [boolean, required] Bleended attendance type filter
 *          @item flexible [boolean, required] Flexible attendance type filter
 *          @item fullOnline [boolean, required] Full On-Line attendance type filter
 *          @item fullOnsite [boolean, required] Full On-Site attendance type filter
 *      @end
 *      @item surveys [object, optional] Object that describe the survey filter
 *          @item all [boolean,required] Is all selected
 *          @item surveys [array(integer), required] The list of id surveys selected
 *          @item entitiesLimits [integer, required] The maximum number of element selectable
 *      @end
 * @end
 *
 * @status 404 Report not found!
 */
export const getReportInfo = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;

    try {
        const reportHandler = await ReportManagerSwitcher(session, req.params.id_report);
        res.type('application/json');
        res.status(200);
        res.json(await reportHandler.getInfo());
    } catch (err: any) {
        logger.errorWithStack(`Error while getting report info (idReport: ${req.params.id_report}).`, err);
        const response = new ReportManagerInfoResponse();
        response.success = false;
        if (err.message === 'Report type not found!') {
            res.status(400);
            response.error = 'Invalid report type';
        } else if (err instanceof NotFoundException || err instanceof DisabledReportTypeException) {
            res.status(404);
            response.error = 'Report not found!';
            response.errorCode = err.getCode();
        } else {
            res.status(500);
            response.error = 'Generic error. See the logs for more information';
        }
        res.json(response);
    }
};
