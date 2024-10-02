import { NextFunction, Request, Response } from 'express';
import { NotFoundException, UnauthorizedException } from '../../exceptions';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { BranchesSummaryResponse, ErrorResponse } from '../../models/custom-report';
import { DashboardBranches } from '../../dashboards/models/dashboard-branches';

/**
 * @category Dashboard
 * @summary Summary Of The Current Branch
 * @method GET
 * @toggle release/weekly/datalake-v3
 * @toggle toggle/admin/report/branches-dashboard-on-athena
 * @url /analytics/v1/dashboard/branches/summary
 * @notes This endpoint retrieves a summary of current branch showing total users, number of enrollments (and their statuses) and general information about the branch.
 *
 * @parameter branch_id [integer, required] The ID of the branch
 * @parameter hide_deactivated_users [boolean, optional] If set to "TRUE", inactive users will be excluded from the results.
 *
 * @response success [boolean, required] Whether the operation was successful
 * @response data [object, required] The branch statistics
 *      @item id [string, required] The name of the selected branch
 *      @item root [boolean, required] Returns "TRUE" if the branch is the root branch
 *      @item title [string, required] The name of the selected branch
 *      @item code [string, required] The code of the selected branch
 *      @item has_children [boolean, required] Returns "TRUE" if the branch has children
 *      @item total_users [integer, required] The count of total users in the branch and sub-branches
 *      @item enrolled [integer, required] The number of course enrollments
 *      @item completed [integer, required] The number of course enrollments with status "completed"
 *      @item in_progress [integer, required] The number of course enrollments with status "in_progress"
 *      @item subscribed [integer, required] The number of course enrollments with status "subscribed"
 * @end
 *
 * @status 1001 Invalid parameter
 * @status 1002 The requested branch does not exist
 */
export const getDashboardBranchesSummary = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const dashboard: DashboardBranches = new DashboardBranches(session);
    const response: BranchesSummaryResponse = new BranchesSummaryResponse();
    res.type('application/json');
    try {
        response.data = await dashboard.getBranchesSummary(req);
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
        logger.errorWithStack(`Error while performing get dashboard branches summary call. ${err.message}`, err);
        response.success = false;
        response.error = new ErrorResponse(err.code, err.message);
        res.json(response);
    }
};
