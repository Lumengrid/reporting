import { NextFunction, Request, Response } from 'express';
import { BranchEnrollmentsResponse, ErrorResponse } from '../../models/custom-report';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { DashboardBranches } from '../../dashboards/models/dashboard-branches';
import { NotFoundException } from '../../exceptions/';

/**
 * @category Dashboard
 * @summary List of enrollments for current branch and children.
 * @method GET
 * @toggle release/weekly/datalake-v3
 * @toggle toggle/admin/report/branches-dashboard-on-athena
 * @url /analytics/v1/dashboard/branches/users/list
 *
 * @parameter branch_id [integer, required] The ID of the branch
 * @parameter hide_deactivated_users [boolean, optional] If equals to true - exclude inactive users, default false
 * @parameter status [array(string), optional] Filter enrollments by status. Can contain subscribed, in_progress or completed
 * @parameter sort_attr [enum(username, fullname, course_name, course_code, course_type, enrollment_date, completion_date, status, score, session_time, credits), optional] Sort by this field
 * @parameter sort_dir [enum(asc, desc), optional] Sorting Direction
 * @parameter page_size [integer, optional] Maximum number of results per page
 * @parameter query_id [string, optional] The ID of the query
 *
 * @response data [object, required] Branch Enrollments statistics
 *      @item items [array, required] Details of single enrollment
 *          @item username [string, required] Username
 *          @item fullname [string, required] Fullname
 *          @item course_name [string, required] Name of the course
 *          @item course_code [string, optional] Code of the course
 *          @item course_type [enum(elearning, classroom, webinar), required] Course type
 *          @item enrollment_date [datetime, required] Course enrollment date
 *          @item completion_date [datetime, optional] Course completion date
 *          @item status [enum(subscribed, in_progress, completed), required] Enrollment status
 *          @item score [float, required] Enrollment score
 *          @item session_time [integer, required] The time spent in the Course in seconds
 *          @item credits [float, required] Course credits
 *      @end
 *      @item has_more_data [bool, required] True if the current page is not the last page
 *      @item current_page [integer, required] Page number of the current page
 *      @item current_page_size [integer, required] Number of items per page
 *      @item total_page_count [integer, optional] Total number of pages returned
 *      @item total_count [integer, optional] Total number of Items
 *      @item query_id [string, optional] The ID of the query
 * @end
 *
 * @status 1001 Invalid Parameter
 * @status 1002 The requested branch does not exist
 * @status 1009 Invalid QueryId
 */
export const getDashboardBranchEnrollments = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const response: BranchEnrollmentsResponse = new BranchEnrollmentsResponse();
    const dashboardBranches: DashboardBranches = new DashboardBranches(session);

    res.type('application/json');
    try {
        response.data = await dashboardBranches.getBranchEnrollments(req);
        res.status(200);
        res.json(response);
    } catch (err: any) {
        if (err instanceof NotFoundException) {
            res.status(404);
            response.error = new ErrorResponse(err.getCode(), err.message);
        } else {
            res.status(500);
            logger.errorWithStack(`Error while performing get dashboard branch user enrollment call.`, err);
            response.error = new ErrorResponse(err.code, 'Error while performing get dashboard branch users call');
        }
        response.success = false;
        res.json(response);
    }
};
