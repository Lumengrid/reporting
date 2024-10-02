import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';

import { QueryBuilderAdminsResponse } from '../../models/base';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';

/**
 * @category Query Builder
 * @summary Get the username list of the users able to see the query builder v2 section
 * @method GET
 * @url /analytics/v1/query-builder/admins
 *
 * @response data [array(string), required] Username of the users able to see the query builder v2 section
 * @end
 * @response success [boolean, required] Whether or not the operation was successful
 */
export const getQueryBuilderAdmins = async (req: Request, res: Response, next: NextFunction) => {
    // return PUT response success true or false
    const session: SessionManager = res.locals.session;
    const logger: SessionLoggerService = httpContext.get('logger');
    const responseOptions: QueryBuilderAdminsResponse = { success: true };

    res.type('application/json');
    res.status(200);

    try {
        responseOptions.data = session.platform.getQueryBuilderAdmins();
    } catch (err: any) {
        logger.errorWithStack(`Internal error while saving reports settings.`, err);
        res.status(500);
        responseOptions.success = false;
    } finally {
        res.json(responseOptions);
    }
};
