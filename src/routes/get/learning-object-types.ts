import { NextFunction, Request, Response } from 'express';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import SessionManager from '../../services/session/session-manager.session';
import { BaseResponse } from '../../models/base';

/**
 * @category Report
 * @summary Get learning object types
 * @internal
 * @method GET
 * @url /analytics/v1/reports/learning-object-types
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [array(string), required] Object types available
 */
export const learningObjectTypes = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const response: BaseResponse = {
        success: true,
        data: [],
    };
    try {
        const res = await session.getHydra().getAllLOTypes();
        response.data = res.data;
    } catch (err: any) {
        logger.errorWithStack(`Internal error while retrieving the LO types.`, err);
        res.status(500);
        response.success = false;
    }

    res.json(response);

};
