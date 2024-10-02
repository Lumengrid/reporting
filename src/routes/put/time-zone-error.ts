import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';

import { DataLakeRefreshStatus, ReportsSettingsResponse } from '../../models/base';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';

/**
 * @category Report
 * @summary Put refresh status in error
 * @internal
 * @method PUT
 * @url /analytics/v1/reports/refresh-error
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response message [string, optional] Error message
 *
 * @status 400 Bad Request
 * @status 404 Not found
 */
export const putRefreshInError = async (req: Request, res: Response, next: NextFunction) => {
    const session: SessionManager = res.locals.session;
    const logger: SessionLoggerService = httpContext.get('logger');
    const responseOptions: ReportsSettingsResponse = {success: true};

    res.type('application/json');
    res.status(200);

    try {
        const dynamo = session.getDynamo();
        const datalakeRefreshItem = await dynamo.getLastDataLakeUpdate(session);

        if (datalakeRefreshItem === undefined) {
            res.status(404);
            responseOptions.success = false;
            responseOptions.error = 'Platform not found in refresh_info table';

            res.json(responseOptions);
            return;
        }
        let updatedStatus = false;

        if (datalakeRefreshItem.refreshOnDemandStatus === undefined || datalakeRefreshItem.refreshOnDemandStatus === DataLakeRefreshStatus.RefreshInProgress) {
            updatedStatus = true;
            datalakeRefreshItem.refreshOnDemandStatus = DataLakeRefreshStatus.RefreshError;
        }
        if (datalakeRefreshItem.refreshTimeZoneStatus === undefined || datalakeRefreshItem.refreshTimeZoneStatus === DataLakeRefreshStatus.RefreshInProgress) {
            updatedStatus = true;
            datalakeRefreshItem.refreshTimeZoneStatus = DataLakeRefreshStatus.RefreshError;
        }

        if (updatedStatus === false) {
            res.status(400);
            responseOptions.success = false;
            responseOptions.error = 'No datalake in progress found.';

            res.json(responseOptions);
            return;
        }

        await dynamo.updateDataLakeRefreshItem(datalakeRefreshItem);

    } catch (err: any) {
        logger.errorWithStack(`Internal error while saving reports settings.`, err);
        res.status(500);
        responseOptions.success = false;
    }

    res.json(responseOptions);
};
