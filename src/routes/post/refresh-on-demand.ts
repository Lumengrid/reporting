import { Request, Response } from 'express';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { RefreshDataLake } from '../../models/refresh-data-lake';
import { BaseResponse } from '../../models/base';

/**
 * @category Report
 * @summary Start refresh on demand
 * @method POST
 * @url /analytics/v1/reports/refresh-on-demand
 *
 * @parameter bjName [string, option] Background job name
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [array, optional] Response's array
 * @end
 */
export const refreshOnDemand = async (req: Request, res: Response) => {
    const logger: SessionLoggerService = httpContext.get('logger');

    const bjName = req.body.bjName;

    const component = new RefreshDataLake(res.locals.session);
    const response: BaseResponse = {
        success: true,
        data: [],
    };

    try {
        await component.startRefreshDataLake(bjName);
    } catch (e: any) {
        logger.errorWithStack(`Can not start the refresh process.`, e);
        response.success = false;
        response.error = 'Can not start the refresh process';

        return res.status(500).json(response);
    }

    res.json(response);

};
