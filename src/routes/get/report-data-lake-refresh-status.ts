import { NextFunction, Request, Response } from 'express';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { RefreshDataLake } from '../../models/refresh-data-lake';
import { BaseResponse } from '../../models/base';

/**
 * @category Report
 * @summary Get refersh status
 * @method GET
 * @url /analytics/v1/reports/data-lake/refresh-status
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, optional]
 *     @item refreshStatus [string, required] Status of the last refresh
 * @end
 */
export const getReportDataLakeRefreshStatus = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');

    const component = new RefreshDataLake(res.locals.session);
    const response: BaseResponse = {
        success: true,
        data: {},
    };

    try {
        response.data = {
            refreshStatus: await component.checkRefreshStatus(res.locals.session),
        };
    } catch (e: any) {
        logger.errorWithStack(`Can not get the refresh status of the data lake.`, e);
        response.success = false;
        response.error = 'Can not get the refresh status of the data lake';

        return res.status(500).json(response);
    }

    res.json(response);
};
