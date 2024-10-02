import { NextFunction, Request, Response } from 'express';
import { BaseReportManagerResponse } from '../../models/report-manager';
import SessionManager from '../../services/session/session-manager.session';
import { ReportManagerSwitcher } from '../../models/report-switcher';
import PlatformManager from '../../services/session/platform-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { ConnectionDataSourceException } from '../../exceptions/connectionDataSourceException';
import { ExtractionFailedException } from '../../exceptions/extractionFailedException';
import { NotFoundException, DisabledReportTypeException } from '../../exceptions/';

/**
 * @category Report
 * @summary Generate a temporary preview
 * @method POST
 * @get id_report [string, required] ID of the report
 * @url /analytics/v1/reports/{id_report}/preview
 *
 * @parameter loginRequired [boolean, required] Define if is necessary the login in order to download the report
 * @parameter creationDate [string,required] Date of creation report
 * @parameter isReportDownloadPermissionLink [boolean, required] Is report download permission link
 * @parameter isCustomColumnSortingEnabled [boolean, required] Is custom column sorting enabled
 * @parameter deleted [boolean, required] Is deleted
 * @parameter platform [string, required] Platform related to the report
 * @parameter timezone [string, required] Timezone of the user at the creation report time
 * @parameter description [string, required] Report's description
 * @parameter importedFromLegacyId [integer, required] Related legacy report id
 * @parameter standard [boolean, required] Is a standard report
 * @parameter author [integer, required] Report's author id
 * @parameter type [string, required] Report's type
 * @parameter title [string, required] Report's title
 * @parameter visibility [object, required] Info about the visibility
 *     @parameter groups [array(integer), required] List of group id that can see the report
 *     @parameter type [integer, required] Visibility type, 1 = only god admins, 2 = god admins and pu, 3 = god admin and selected pu
 *     @parameter branches [array(integer), required] List of branch id that can see the report
 *     @parameter users [array(integer), required] List of idst that can see the report
 * @end
 * @parameter certification [object, required] Info certification filter
 *      @parameter all [boolean,required] Is all selected
 *      @parameter expiredCertifications [boolean,required] Include expired certification
 *      @parameter entitiesLimits [integer,required] How many certification can be selectable
 *      @parameter activeCertifications [boolean,required] include active certifications
 *      @parameter certifications [array(integer),required] id of the certification selected
 *      @parameter conditions [string,required] how to combine the different filter
 *      @parameter archivedCertifications [boolean,required] include archived certification
 *      @parameter certificationDate [object, required] Object that describe the date filter for the certification date
 *         @parameter days [integer, required] Number of days
 *         @parameter from [string, required] Date from
 *         @parameter to [string, required] Date to
 *         @parameter any [boolean, required] Any date
 *         @parameter type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *         @parameter operator [string, required] Operator
 *     @end
 *     @parameter certificationExpirationDate [object, required] Object that describe the date filter for the certification expiration date
 *         @parameter days [integer, required] Number of days
 *         @parameter from [string, required] Date from
 *         @parameter to [string, required] Date to
 *         @parameter any [boolean, required] Any date
 *         @parameter type [integer, required] The operator for the comparison, can be isAfter, isBefore, range
 *         @parameter operator [string, required] Operator
 *     @end
 * @end
 * @parameter planning [object, required] Info about schedule report
 *     @parameter active [boolean, required] Define if the planning is active or not
 *     @parameter option [object, required] Schedule option
 *         @parameter every [int, required] Is correlated to "timeFrame", it defines how many times the report must be schedule (1 day, 1 week, 1 month)
 *         @parameter recipients [array(string), required] The array of email recipients
 *         @parameter timeFrame [string, required] It correlate to "every" and it defines the recurrency: day, week, or month
 *         @parameter isPaused [boolean, required] Define if the planning is in pause
 *         @parameter scheduleFrom [string, required] Schedule start date
 *     @end
 * @end
 * @parameter fields [array(string), required] List of fields selected
 * @parameter sortingOptions [object, required] Object that describe the sorting option
 *     @parameter orderBy [string, required] Order by asc or desc
 *     @parameter selector [string, required] Type of selector
 *     @parameter selectedField [string, required] field that define the sorting
 * @end
 * @parameter users [object, required] Object that describe the users filter
 *     @parameter all [boolean,required] Is all selected
 *     @parameter hideExpiredUsers [boolean, required] hide expired users
 *     @parameter hideDeactivated [boolean, required] hide deactivated users
 *     @parameter isUserAddFields [boolean, required] is user additional field
 *     @parameter groups [array(integer), required] List of group selected
 *     @parameter branches [array(integer), required] List of branches selected
 *     @parameter showOnlyLearners [boolean, required] Show only learners user
 *     @parameter users [array(integer), required] List of idst selected
 *     @parameter entitiesLimits [object,required] entity limit
 *         @parameter usersLimit [integer, required] how many user can be selected
 *         @parameter groupsLimit [integer, required] how many groups can be selected
 *         @parameter branchesLimit [integer, required] how many branches can be selected
 *     @end
 * @end
 * @parameter enrollment [object, required] Object that describe the enrollment filter
 *     @parameter completed [boolean, required] if status is completed
 *     @parameter inProgress [boolean, required] if status is in progress
 *     @parameter notStarted [boolean, required] if status is not started
 *     @parameter waitingList [boolean, required] if status is waiting list
 *     @parameter suspended [boolean, required] if status is suspended
 *     @parameter enrollmentsToConfirm [boolean, required] if status is enrollments to confirm
 *     @parameter subscribed [boolean, required] if status is subscribed
 *     @parameter overbooking [boolean, required] if status is overbooking
 * @end
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, optional]
 *      @item deleted [array(integer), required] array of report ids deleted with success
 *      @item notDeleted [array(integer), required] array of report not deleted
 *      @item deletingErrors [array(integer), required] array of report ids that generate an error during deliting
 * @end
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response error [boolean, optional] Error message
 * @response data [array(object), required] Results of the query
 *
 * @status 404 Report not found!
 */
// if there is an editing without saving, load a temporary report to preview
export const postReportTemporaryPreview = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const platformManager: PlatformManager = session.platform;
    const response = new BaseReportManagerResponse();

    res.type('application/json');
    res.status(200);

    let results: [];

    try {
        const reportHandler = await ReportManagerSwitcher(session, req.params.id_report);
        reportHandler.loadInfo(req.body);

        if (session.platform.isDatalakeV3ToggleActive()) {
            const snowflakeDriver = session.getSnowflake();

            const query = await reportHandler.getQuerySnowflake(platformManager.getPreviewExportLimit(), true, true, false);
            results = await snowflakeDriver.runQuery(query);
        } else {
            const athena = session.getAthena();

            const query = await reportHandler.getQuery(platformManager.getPreviewExportLimit(), true, true);
            const data = await athena.runQuery(query);

            results = athena.getQueryResultsAsArray(data);

            const tableDeletion = new Promise(async (resolve, reject) => {
                await reportHandler.dropTemporaryTables();
            });
        }

        const dataResponse = reportHandler.dataResponse(results);
        res.json(dataResponse);
    } catch (err: any) {
        logger.errorWithStack(`Error while getting preview of report (idReport: ${req.params.id_report}).`, err);
        res.status(500);
        response.success = false;
        response.error = 'Generic error. See the logs for more information';
        if (err instanceof NotFoundException || err instanceof DisabledReportTypeException) {
            res.status(404);
            response.error = 'Report not found!';
            response.errorCode = err.getCode();
        } else if (err instanceof ConnectionDataSourceException || err instanceof ExtractionFailedException) {
            response.error = err.message;
            response.errorCode = err.getCode();
        }
        res.json(response);
    }
};
