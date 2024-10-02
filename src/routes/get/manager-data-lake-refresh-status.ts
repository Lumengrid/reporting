import { NextFunction, Request, Response } from 'express';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { RefreshDataLake } from '../../models/refresh-data-lake';
import { BaseResponse, DataLakeRefreshStatus } from '../../models/base';

/**
 * @category Manager
 * @summary Get datalake status - manager section
 * @method GET
 * @url /analytics/v1/manager/data-lake-status
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, required] Response's data
 *      @item status [string, required] Status' label
 * @end
 */
export const getManagerDataLakeRefreshStatus = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');

    const component = new RefreshDataLake(res.locals.session);
    const response: BaseResponse = {
        success: true,
        data: {},
    };

    try {
        const refreshStatus = await component.getLatestDatalakeStatus(res.locals.session);
        let status = 'online';

        if (!refreshStatus || refreshStatus === DataLakeRefreshStatus.RefreshInProgress) {
            status = 'offline';
        }

        response.data = {
            status: `${status}`,
        };
    } catch (e: any) {
        logger.errorWithStack(`Can not get the refresh status of the data lake.`, e);
        response.success = false;
        response.error = 'Can not get the refresh status of the data lake';

        return res.status(500).json(response);
    }

    res.json(response);
};
