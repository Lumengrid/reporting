import { NextFunction, Request, Response } from 'express';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { DashboardExportCsvResponse, ErrorResponse, ExportCsvUrl } from '../../models/custom-report';
import { NotFoundException, UnauthorizedException } from '../../exceptions';
import { DashboardTypes } from '../../dashboards/constants/dashboard-types';

/**
 * @category Dashboard
 * @summary Download Privacy Policies as CSV
 * @method GET
 * @notes This endpoint download Privacy Policies dashboard as csv. Only available for SuperAdmin. Available for datalake v3. Is not feasible to test this API with api-browser. Use postman or similar tools.
 * @toggle release/weekly/datalake-v3
 * @toggle toggle/admin/report/privacy-policy-dashboard-on-athena
 * @get query_execution_id [string, required] Query execution id
 * @url /analytics/v1/dashboard/privacy_policies/{query_execution_id}/exportCsv
 *
 * @response success [boolean, required] Whether the operation was successful
 * @response data [object, required] The response data
 *      @item url [string, required] The Presigned url for the file download
 *      @end
 * @end
 *
 * @status 1009 Invalid query_execution_id
 */
export const getDashboardPrivacyPolicyExportCsv = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const queryExecutionId = req.params.query_execution_id;
    const snowflakeDriver = session.getSnowflake();
    const response: DashboardExportCsvResponse = new DashboardExportCsvResponse();
    res.type('application/json');
    try {
        const url = await snowflakeDriver.getSignedUrlExportCsvFromQueryID(queryExecutionId, DashboardTypes.PRIVACY_POLICIES, 'export_privacy_policies');
        response.data = new ExportCsvUrl();
        response.data.url = url;
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
        logger.errorWithStack(`Error while performing export csv of dashboard privacy policies.`, err);
        response.success = false;
        response.error = new ErrorResponse(err.code, err.message);
        res.json(response);
    }
};



