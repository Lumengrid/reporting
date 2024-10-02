import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { BaseResponse } from '../../models/base';
import { ArchivedAuditTrailArchiveDateComponent } from './archived-audit-trail-log-archive-date-component';

/**
 * @category Archived-audit-trail-log
 * @summary Retrieve the date when the old audit trail logs were archived
 * @method GET
 * @url /analytics/v1/archived-audit-trail-log/archive-date
 * @internal
 *
 * @response data [object, required] Response's data
 *      @item archive_date [string, required] Archive date of the old audit trail log
 * @end
 * @response success [boolean, required] Whether or not the operation was successful
 */
export const archivedAuditTrailArchiveDate = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const responseOptions: BaseResponse = {success: true};

    res.type('application/json');
    try {
        if (req.app.locals.cache.get(session.platform.getPlatformBaseUrl() + '_archive_max_date')) {
            responseOptions.data = { archive_date: req.app.locals.cache.get(session.platform.getPlatformBaseUrl() + '_archive_max_date')};
        } else {
            const archiveDateComponent = new ArchivedAuditTrailArchiveDateComponent(session);
            const archiveMaxDate = await archiveDateComponent.getArchiveDate();
            res.status(200);
            responseOptions.data = {archive_date: archiveMaxDate};
            req.app.locals.cache.set(session.platform.getPlatformBaseUrl() + '_archive_max_date', archiveMaxDate, 0);
        }
    } catch (err: any) {
        logger.errorWithStack('Error on retrieve archived audit trail archive date', err);
        res.status(500);
        responseOptions.success = false;
        responseOptions.error = 'Generic Error. See the log';
    }
    res.json(responseOptions);
};