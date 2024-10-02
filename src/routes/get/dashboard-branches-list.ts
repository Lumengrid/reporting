import { NextFunction, Request, Response } from 'express';
import { NotFoundException, UnauthorizedException } from '../../exceptions';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { BranchesListResponse, ErrorResponse } from '../../models/custom-report';
import { DashboardBranches } from '../../dashboards/models/dashboard-branches';

/**
 * @category Dashboard
 * @summary Detailed Information For The Children Of The Current Branch
 * @method GET
 * @toggle release/weekly/datalake-v3
 * @toggle toggle/admin/report/branches-dashboard-on-athena
 * @url /analytics/v1/dashboard/branches/list
 * @notes Get detailed information for the children of the current branch. Shows details of user counts and course enrollment statuses.
 *
 * @parameter branch_id [integer, required] The ID of the branch
 * @parameter hide_deactivated_users [boolean, optional] Exclude inactive users
 * @parameter sort_attr [enum(title, has_children, total_users, enrolled, completed, in_progress, subscribed, overdue), optional] Sort by this field, default - title
 * @parameter sort_dir [enum(asc, desc), optional] Sorting Direction: asc = Ascending, desc = descending, default - asc
 * @parameter page [integer, optional] Page to return, default 1
 * @parameter page_size [integer, optional] Maximum number of results per page. Default - all items
 * @parameter query_id [string, optional] The ID of the query
 *
 * @response data [object, required] Branch statistics
 *      @item branch_name [string, required] The name of the selected branch
 *      @item items [array, required] The performance for each branch
 *          @item id [integer, required] The ID of the branch
 *          @item title [string, required] The name of the branch
 *          @item has_children [boolean, required] True, if the branch has children. False if not.
 *          @item total_users [integer, required] The total users in the branch and sub-branches
 *          @item enrolled [integer, required] The number of course enrollments
 *          @item completed [integer, required] The number of course enrollments with the status "completed"
 *          @item in_progress [integer, required] The number of course enrollments with the status "in_progress"
 *          @item subscribed [integer, required] The number of course enrollments with the status "subscribed"
 *          @item overdue [integer, required] The number of course enrollments that have overdue dates
 *      @end
 *      @item has_more_data [boolean, required] True if the current page is not the last page
 *      @item current_page [integer, required] Page number of the current page
 *      @item current_page_size [integer, required] Number of items per page
 *      @item total_page_count [integer, optional] Total number of pages returned
 *      @item total_count [integer, optional] Total number of Items
 *      @item query_id [string, optional] The ID of the query
 * @end
 *
 * @status 1001 Invalid parameter
 * @status 1002 The requested branch does not exist
 * @status 1009 Invalid QueryId
 */
export const getDashboardBranchesList = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const dashboard: DashboardBranches = new DashboardBranches(session);
    const response: BranchesListResponse = new BranchesListResponse();
    res.type('application/json');
    try {
        response.data = await dashboard.getBranchesList(req);
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
        logger.errorWithStack(`Error while performing get dashboard branches list call.`, err);
        response.success = false;
        response.error = new ErrorResponse(err.code, err.message);
        res.json(response);
    }
};
