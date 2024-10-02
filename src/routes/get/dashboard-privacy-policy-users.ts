import { NextFunction, Request, Response } from 'express';
import { PrivacyResponse } from '../../models/custom-report';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { DashboardPrivacyPolicy } from '../../dashboards/models/dashboard-privacy-policy';

/**
 * @category Dashboard
 * @summary Report Data For Privacy Policy
 * @method GET
 * @notes This endpoint retrieves the report data for the privacy policy based on several parameter. Only available for SuperAdmin. Available for datalake v3.
 * @toggle release/weekly/datalake-v3
 * @toggle toggle/admin/report/privacy-policy-dashboard-on-athena
 * @url /analytics/v1/dashboard/users/privacy_policies
 *
 * @parameter current_version_only [boolean, optional] If set to FALSE, for each username and domain returns the last version of privacy policy the user answered OR the “current“ version if no versions were answered. Otherwise, if set to TRUE, for each username and domain  returns the answer related to the “current” version of privacy policy (accepted/rejected/no answer). The default value is FALSE. This parameter is optional.
 * @parameter hide_deactivated_user [boolean, optional] Returns only active users if set to TRUE. Otherwise returns both active and inactive users. The default value is FALSE. This parameter is optional.
 * @parameter all_fields [boolean, optional] Returns all available fields if set to TRUE. The default value is FALSE. This parameter is optional.
 * @parameter multidomain_ids [array(integer), optional] An array of multi-domain client IDs. This parameter is optional. PLEASE NOTE: This filter can only be applied if the Extended Enterprise app is activated.
 * @parameter user_ids [array(integer), optional] Filter data by an array of User IDs. This parameter is optional.
 * @parameter branch_id [integer, optional] If a branch ID is supplied here, the results will be filtered by that ID. This parameter is optional.
 * @parameter selection_status [integer, optional] If this parameter is set to "1" then the results will display branches without children. If it is set to "2" then the results will display branches with children. The default value is "1". This parameter is optional.
 * @parameter filters [string, optional] A serialized object containing list of filters and their options. List of filters is:  {*attribute* :{"option": (like, contains, not_equal, ends_with, starts_with, is_empty, not_empty, not_start_with, not_end_with, not_contains), "value": {string}, {...}}. attribute can be: policy_accepted, policy_name, username, version This parameter is optional.
 * @parameter query_id [string, optional] The ID of the query
 *
 * @parameter sort_attr [enum(user_id, username, lastname, firstname, email, last_login, policy_name, policy_accepted, version or domain), optional] Select the fields to sort by. The default value is: version (descending), user_id (descending). This parameter is optional.
 * @parameter sort_dir [enum(asc, desc), optional] Specify the sorting direction. Possible values are: "asc" for ascending or "desc" for descending. The default value is "desc". This parameter is optional.
 * @parameter page [integer, optional] The page number to return. The default value is "1". This parameter is optional.
 * @parameter page_size [integer, optional] The maximum number of results per page. The default value is "10". This parameter is optional.
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, required] The response data
 *      @item items [array, optional] The list of Users
 *          @item user_id [integer, required] The user ID
 *          @item firstname [string, optional] The first name of the user
 *          @item lastname [string, optional] The last name of the user
 *          @item username [string, required] The user name of the user
 *          @item email [string, required] The email address of the user
 *          @item last_login [string, optional] The last login date of the user
 *          @item track_id [string, optional] The tracking ID
 *          @item domain [string, required] The domain
 *          @item version_id [string, required] The version ID
 *          @item version [string, required] The version
 *          @item policy_name [string, required] The privacy policy name
 *          @item policy_accepted [string, required] Shows if the privacy policy is accepted
 *          @item acceptance_date [string, required] Displays the acceptance date of the policy
 *          @item answer_sub_policy_1 [string, optional] The answer of the sub policy
 *          @item answer_sub_policy_2 [string, optional] The answer of the sub policy
 *          @item answer_sub_policy_3 [string, optional] The answer of the sub policy
 *      @end
 *      @item has_more_data [bool, required] True if the current page is not the last page
 *      @item current_page [integer, required] Page number of the current page
 *      @item current_page_size [integer, required] Number of items per page
 *      @item total_page_count [integer, optional] Total number of pages returned
 *      @item total_count [integer, optional] Total number of Items
 *      @item query_id [string, optional] The ID of the query
 * @end
 *
 * @status 400 Bad Request
 */
export const getDashboardPrivacyPolicyUsers = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const response: PrivacyResponse = new PrivacyResponse();
    const dashboardPrivacyPolicy: DashboardPrivacyPolicy = new DashboardPrivacyPolicy(session);

    res.type('application/json');
    try {
        response.data = await dashboardPrivacyPolicy.getUsers(req);
        res.status(200);
        res.json(response);
    } catch (err: any) {
        res.status(500);
        logger.errorWithStack(`Error while performing get dashboard privacy policy users call.`, err);
        response.success = false;
        response.error = `Error while performing get dashboard privacy policy users call.`;
        res.json(response);
    }
};




