import { NextFunction, Request, Response } from 'express';
import { NotFoundException, UnauthorizedException } from '../../exceptions';
import { Report, ReportsResponse } from '../../models/custom-report';
import { Dynamo } from '../../services/dynamo';
import { ReportService } from '../../services/report';
import { ReportsComponent } from './report.component';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';

/**
 * @category Report
 * @summary Get new reports available in the platform
 * @method GET
 * @url /analytics/v1/reports
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [array, required]
 *      @item description [string,required] Description of report
 *      @item creationDate [string,required] Date of creation report
 *      @item createdBy [int,required] userId of the user that created the report
 *      @item type [string, required] Type of the report
 *      @item name [string, required] Name of the report
 *      @item idReport [string, required] Uuid of the report
 *      @item standard [boolean, required] Determine if is a standard report
 *      @item visibility [int, required] Determine the report visibility
 *      @item planning [object, required] Info about schedule report
 *          @item active [boolean, required] Define if the planning is active or not
 *          @item option [object, required] Schedule option
 *              @item every [int, required] Is correlated to "timeFrame", it defines how many times the report must be schedule (1 day, 1 week)
 *              @item recipients [array(string), required] The array of email recipients
 *              @end
 *              @item timeFrame [string, required] It correlate to "every" and it defines the recurrency: day, week, or month
 *              @item isPaused [boolean, required] Define if the planning is in pause
 *              @item scheduleFrom [string, required] Schedule start date
 *              @item startHour [string, required] Schedule start hour
 *              @item timezone [string, required] User or Platform Timezone
 *          @end
 *      @end
 *      @item createdByDescription [object, required] Info about user that created the report
 *          @item firstname [string, required] Name of the user
 *          @item lastname [string, required] Lastname of the user
 *          @item userId [string, required] user id
 *      @end
 *      @end
 * @end
 *
 */
export const getReports = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const hydra = session.getHydra();

    const dynamoService: Dynamo = session.getDynamo();
    const reportService: ReportService = new ReportService(hydra);
    const component: ReportsComponent = new ReportsComponent(dynamoService, reportService);

    const response: ReportsResponse = new ReportsResponse();
    let reports: Report[] = [];

    res.type('application/json');
    res.status(200);

    try {
        reports = await component.getReports(session);
    } catch (err: any) {
        if (err instanceof UnauthorizedException) {
            res.status(401);
        } else if (err instanceof NotFoundException) {
            res.status(404);
        } else {
            res.status(500);
        }
        logger.errorWithStack(`Error while performing get reports call.`, err);
        response.success = false;
        response.error = err;
    }
    response.data = reports;
    res.json(response);
};
