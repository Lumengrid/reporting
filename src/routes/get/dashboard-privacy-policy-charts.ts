import { NextFunction, Request, Response } from 'express';
import { NotFoundException, UnauthorizedException } from '../../exceptions';
import { PrivacyChartsResponse } from '../../models/custom-report';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { DashboardPrivacyPolicy } from '../../dashboards/models/dashboard-privacy-policy';

/**
 * @category Dashboard
 * @internal
 * @summary Returns charts data needed for privacy policy
 * @method GET
 * @notes This endpoint retrieves the report data for Charts of privacy policy based on several parameter. Only available for SuperAdmin. Available for datalake v3.
 * @toggle release/weekly/datalake-v3
 * @toggle toggle/admin/report/privacy-policy-dashboard-on-athena
 * @url /analytics/v1/dashboard/privacy_policies/charts
 *
 * @parameter current_version_only [boolean, optional] If set to FALSE, for each username and domain returns the last version of privacy policy the user answered OR the “current“ version if no versions were answered. Otherwise, if set to TRUE, for each username and domain  returns the answer related to the “current” version of privacy policy (accepted/rejected/no answer). The default value is FALSE. This parameter is optional.
 * @parameter hide_deactivated_user [boolean, optional] Returns only active users if set to TRUE. Otherwise, returns both active and inactive users. The default value is FALSE. This parameter is optional.
 * @parameter multidomain_ids [array(integer), optional] An array of multi-domain client IDs. This parameter is optional. PLEASE NOTE: This filter can only be applied if the Extended Enterprise app is activated.
 * @parameter user_ids [array(integer), optional] Filter data by an array of User IDs. This parameter is optional.
 * @parameter branch_id [integer, optional] If a branch ID is supplied here, the results will be filtered by that ID. This parameter is optional.
 * @parameter selection_status [integer, optional] If this parameter is set to "1" then the results will display branches without children. If it is set to "2" then the results will display branches with children. The default value is "1". This parameter is optional.
 * @parameter filters [string, optional] A serialized object containing list of filters and their options. List of filters is:  {*attribute* :{"option": (like, contains, not_equal, ends_with, starts_with, is_empty, not_empty, not_start_with, not_end_with, not_contains), "value": {string}, {...}}. attribute can be: policy_accepted, policy_name, username, version This parameter is optional.
 *
 * @response success [boolean, required] Whether the operation was successful
 * @response data [object, required] Response data
 *     @item donut_chart_data [object, required] Data needed for Donut chart
 *         @item accepted [integer, required] Count of users that have accepted Privacy Policy
 *         @item rejected [integer, required] Count of users that have rejected Privacy Policy
 *         @item no_answer [integer, required] Count of users that haven't answered yet
 *     @end
 *     @item bar_charts_data [object, required] Data needed for Bar charts
 *         @item day [object, required] Count of users for less than 1 day
 *             @item accepted [integer, required] Count of users that have accepted Privacy Policy
 *             @item rejected [integer, required] Count of users that have rejected Privacy Policy
 *             @item no_answer [integer, required] Count of users that haven't answered yet
 *         @end
 *         @item week [object, required] Count of users for less than 1 week
 *             @item accepted [integer, required] Count of users that have accepted Privacy Policy
 *             @item rejected [integer, required] Count of users that have rejected Privacy Policy
 *             @item no_answer [integer, required] Count of users that haven't answered yet
 *         @end
 *         @item month [object, required] Count of users for less than 1 month
 *             @item accepted [integer, required] Count of users that have accepted Privacy Policy
 *             @item rejected [integer, required] Count of users that have rejected Privacy Policy
 *             @item no_answer [integer, required] Count of users that haven't answered yet
 *         @end
 *         @item year [object, required] Count of users for less than 1 year
 *             @item accepted [integer, required] Count of users that have accepted Privacy Policy
 *             @item rejected [integer, required] Count of users that have rejected Privacy Policy
 *             @item no_answer [integer, required] Count of users that haven't answered yet
 *         @end
 *         @item more_than_year [object, required] Count of users for more than 1 year
 *             @item accepted [integer, required] Count of users that have accepted Privacy Policy
 *             @item rejected [integer, required] Count of users that have rejected Privacy Policy
 *             @item no_answer [integer, required] Count of users that haven't answered yet
 *         @end
 *     @end
 * @end
 *
 * @status 400 Bad Request
 */
export const getDashboardPrivacyPolicyCharts = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const response: PrivacyChartsResponse = new PrivacyChartsResponse();
    const dashboardPrivacyPolicy: DashboardPrivacyPolicy = new DashboardPrivacyPolicy(session);

    res.type('application/json');
    try {
        response.data = await dashboardPrivacyPolicy.getCharts(req);
        res.status(200);
        res.json(response);
    } catch (err: any) {
        res.status(500);
        logger.errorWithStack(`Error while performing get dashboard privacy policy charts call.`, err);
        response.success = false;
        response.error = `Error while performing get dashboard privacy policy charts.`;
        res.json(response);
    }
};




