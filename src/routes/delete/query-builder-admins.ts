import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';

import { QueryBuilderAdminsResponse } from '../../models/base';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { SettingsComponent } from '../../shared/components/settings.component';

/**
 * @category Query Builder
 * @summary Remove a user from the user list able to see the query builder section
 * @method DELETE
 * @get id_admin [string, required] ID of the admin that you want to remove
 * @url /analytics/v1/query-builder/admins/{id_admin}
 *
 * @response data [array(string), required] Username of the users able to see the query builder v2 section
 * @end
 * @response success [boolean, required] Whether or not the operation was successful
 */
export const deleteQueryBuilderAdmins = async (req: Request, res: Response, next: NextFunction) => {
    // return PUT response success true or false
    const session: SessionManager = res.locals.session;
    const logger: SessionLoggerService = httpContext.get('logger');
    const responseOptions: QueryBuilderAdminsResponse = { success: true };
    const settings: SettingsComponent = new SettingsComponent(req, session);

    res.type('application/json');
    res.status(200);

    try {
        responseOptions.data = await settings.delQueryBuilderAdmin();
    } catch (err: any) {
        logger.errorWithStack(`Internal error while saving reports settings`, err);
        res.status(500);
        responseOptions.success = false;
        if (err.message === 'Bad hydra request' || err.message === 'Not valid id_admin' || err.message === 'The user wasn\'t a God Admin') {
            responseOptions.error = 'User not found';
            res.status(404);
        }
    } finally {
        res.json(responseOptions);
    }
};
