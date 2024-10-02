import { Request, Response } from 'express';
import { ReportManagerSwitcher } from '../../models/report-switcher';
import { ReportManagerDeleteResponse } from '../../models/report-manager';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { NotFoundException, DisabledReportTypeException } from '../../exceptions/';

/**
 * @category Report
 * @summary Delete a report
 * @method DELETE
 * @get id_report [string, required] ID of the report
 * @url /analytics/v1/reports/{id_report}
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response error [string, optional] Error message
 *
 * @status 404 Report not found!
 */
export const deleteReportDelete = async (req: Request, res: Response) => {
    const session: SessionManager = res.locals.session;
    const response = new ReportManagerDeleteResponse();
    const logger: SessionLoggerService = httpContext.get('logger');

    res.status(200);
    res.type('application/json');

    try {
        const reportHandler = await ReportManagerSwitcher(session, req.params.id_report);
        await reportHandler.delete();
    } catch (err: any) {
        logger.errorWithStack(`Error while deleting report (idReport: ${req.params.id_report}). ${err.message}`, err);
        response.success = false;
        if (err.message === 'Report type not found!') {
            res.status(400);
            response.error = 'Invalid report type';
        } else if (err instanceof NotFoundException || err instanceof DisabledReportTypeException) {
            res.status(404);
            response.error = 'Report not found!';
            response.errorCode = err.getCode();
        } else {
            res.status(500);
            response.error = 'Generic error. See the logs for more information';
        }
    } finally {
        res.json(response);
    }
};
