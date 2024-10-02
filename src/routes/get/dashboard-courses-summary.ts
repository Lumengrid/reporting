import { NextFunction, Request, Response } from 'express';
import { NotFoundException, UnauthorizedException } from '../../exceptions';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { DashboardCourses } from '../../dashboards/models/dashboard-courses';
import { CourseSummaryResponse, ErrorResponse } from '../../models/custom-report';

/**
 * @category Dashboard
 * @summary Get course enrollment summary statistics
 * @method GET
 * @notes This endpoint retrieves the summary statistics of courses enrollments based on several parameter. Available for datalake v3.
 * @toggle release/weekly/datalake-v3
 * @toggle toggle/admin/report/courses-dashboard-on-athena
 * @url /analytics/v1/dashboard/courses/summary
 *
 * @parameter timeframe [enum(any, this_year, this_week, this_month, custom), optional] Timeframe type, default: any
 * @parameter startDate [date, optional] Used only when timeframe parameter is set to custom. Start date which should be used for the filtering. The expected format is "2023-01-31"
 * @parameter endDate [date, optional] Used only when timeframe parameter is set to custom. End date which should be used for the filtering. The expected format is "2023-01-31"
 * @parameter hide_deactivated_users [boolean, optional] Returns only active users if set to TRUE. Otherwise, returns both active and inactive users. The default value is FALSE. This parameter is optional.
 * @parameter branch_id [integer, optional] Branch to filter the results by
 * @parameter course_id [integer, optional] Parameter for showing results for a single course
 *
 * @response success [boolean, required] Whether the operation was successful
 * @response data [object, required] Summary for courses - enrollments and enrollment statuses
 *      @item enrolled [integer, optional] The number of course enrollments
 *      @item completed [integer, optional] The number of course enrollments with the status "completed"
 *      @item in_progress [integer, optional] The number of course enrollments with the status "in_progress"
 *      @item not_started [integer, optional] The number of course enrollments that aren't started yet
 * @end
 *
 * @status 1001 Invalid Parameter
 * @status 1002 Invalid Course
 */
export const getDashboardCoursesSummary = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const dashboard: DashboardCourses = new DashboardCourses(session);
    const response: CourseSummaryResponse = new CourseSummaryResponse();
    res.type('application/json');
    try {
        response.data = await dashboard.getCoursesSummary(req);
        res.status(200);
        res.json(response);
    } catch (err: any) {
        if (err instanceof UnauthorizedException) {
            res.status(401);
        } else if (err instanceof NotFoundException) {
            res.status(400);
        } else {
            res.status(500);
        }
        logger.errorWithStack(`Error while performing get dashboard course summary call.`, err);
        response.success = false;
        response.error = new ErrorResponse(err.code, err.message);
        res.json(response);
    }
};




