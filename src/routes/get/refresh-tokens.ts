import { NextFunction, Request, Response } from 'express';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { BaseResponse } from '../../models/base';
import { DatalakeRefreshTokens } from '../../models/datalake-refresh-tokens';

/**
 * @category Report
 * @summary Get refresh token
 * @method GET
 * @url /analytics/v1/reports/data-lake/refresh-tokens
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, optional]
 *      @item monthlyRefreshTokens [integer, required] current monthly refresh tokens
 *      @item dailyRefreshTokens [integer, required] current daily refresh tokens
 * @end
 *
 */
export const getRefreshTokens = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const component = new DatalakeRefreshTokens(res.locals.session);

    const response: BaseResponse = {
        success: true,
        data: [],
    };

    try {
        response.data = await component.getCurrentRefreshTokens();
        res.status(200);
    } catch (err: any) {
        logger.errorWithStack(`Internal error on get refresh tokens.`, err);
        res.status(500);
        response.success = false;
    }

    res.json(response);
};
