import { ErrorsCode, GeneralErrorResponse } from '../../models/base';
import { NextFunction, Request, Response } from 'express';
import { MassDeleteResponse } from '../../models/report-manager';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { BaseReportManager } from '../../models/base-report-manager';

/**
 * @category Report
 * @summary Delete reports
 * @method POST
 * @url /analytics/v1/reports/deleted
 *
 * @parameter reports [array(integer), required] Array of report ids to delete
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, optional]
 *      @item deleted [array(integer), required] array of report ids deleted with success
 *      @item notDeleted [array(integer), required] array of report not deleted
 *      @item deletingErrors [array(integer), required] array of report ids that generate an error during deliting
 * @end
 *
 */
export const postReportsDelete = async (req: Request, res: Response, next: NextFunction) => {
    const session: SessionManager = res.locals.session;
    let response: MassDeleteResponse;
    const logger: SessionLoggerService = httpContext.get('logger');


    if (!req.body.reports) {
        res.type('application/json');
        res.status(404);
        res.json(new GeneralErrorResponse('Missing required parameter "reports"', ErrorsCode.WrongParameter));
        return;
    }

    const reports = req.body.reports;

    if (!Array.isArray(reports)) {
        res.type('application/json');
        res.status(404);
        res.json(new GeneralErrorResponse('Wrong parameter "reports"', ErrorsCode.WrongParameter));
        return;
    }

    if (reports.length === 0) {
        res.type('application/json');
        res.status(404);
        res.json(new GeneralErrorResponse('Empty parameter "reports"', ErrorsCode.WrongParameter));
        return;
    }

    try {
        response = await BaseReportManager.deleteReports(reports, session);
    } catch (error: any) {
        logger.errorWithStack('Error on batch report deletion', error);
        res.type('application/json');
        res.status(404);
        res.json(new GeneralErrorResponse('Error during report deletion', ErrorsCode.DatabaseError));
        return;
    }

    res.type('application/json');
    res.status(200);
    res.json(response);
};
