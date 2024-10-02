import { NextFunction, Request, Response } from 'express';
import { ReportManagerSwitcher } from '../../models/report-switcher';
import { ReportManagerAvailablesFieldsResponse } from '../../models/report-manager';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { NotFoundException, DisabledReportTypeException } from '../../exceptions/';

/**
 * @category Report
 * @summary Get available fields
 * @method GET
 * @get id_report [string, required] ID of the report
 * @url /analytics/v1/reports/{id_report}/fields
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, required] Available fields
 *      @item value [array(object), required] category field
 *      @end
 * @end
 *
 * @status 404 Report not found!
 */
export const getReportAvailablesFields = async (req: Request, res: Response, next: NextFunction) => {
    const session: SessionManager = res.locals.session;
    const logger: SessionLoggerService = httpContext.get('logger');
    const response = new ReportManagerAvailablesFieldsResponse();

    try {
        const reportHandler = await ReportManagerSwitcher(session, req.params.id_report);
        response.data = await reportHandler.getAvailablesFields();
        res.type('application/json');
        res.status(200);
        res.json(response);
    } catch (err: any) {
        logger.errorWithStack(`Error while getting available fields (idReport: ${req.params.id_report}).`, err);
        response.success = false;
        if (err instanceof NotFoundException || err instanceof DisabledReportTypeException) {
            res.status(404);
            response.error = 'Report not found!';
            response.errorCode = err.getCode();
        } else {
            res.status(500);
            response.error = 'Generic error. See the logs for more information';
        }

        res.type('application/json');
        res.json(response);
    }
};
