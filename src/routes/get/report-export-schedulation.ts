import { NextFunction, Request, Response } from 'express';
import { ReportManagerExportResponse } from '../../models/report-manager';
import { ReportExportComponent } from './report-export.component';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import SessionManager from '../../services/session/session-manager.session';
import { NotFoundException, DisabledReportTypeException, UnplannedScheduledReportException } from '../../exceptions';

/**
 * @category Report
 * @summary Run export schedulation
 * @method GET
 * @get report_id [string, required] ID of the report
 * @url /analytics/v1/reports/{report_id}/export/schedulation
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, optional]
 *      @item executionId [string,required] This id must be used to see the query results through another call
 * @end
 *
 * @status 404 Report not found!
 */
export const getReportExportSchedulation = async (req: Request, res: Response, next: NextFunction) => {
    const session: SessionManager = res.locals.session;
    const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
    const exportComponent = new ReportExportComponent(req, res.locals.session, req.hostname, subfolder, true);
    let response = new ReportManagerExportResponse();
    const logger: SessionLoggerService = httpContext.get('logger');

    try {
        if (session.platform.isDatalakeV3ToggleActive()) {
            response = await exportComponent.exportReportV3();
        } else {
            response = await exportComponent.exportReport();
        }
    } catch (err: any) {
        logger.errorWithStack(`Internal error while performing a report export.`, err);
        res.status(500);
        response.success = false;
        if (err instanceof UnplannedScheduledReportException) {
            res.status(400);
            response.error = 'The report does not have an active schedule';
        } else if (err instanceof NotFoundException || err instanceof DisabledReportTypeException) {
            res.status(404);
            response.error = 'Report not found!';
            response.errorCode = err.getCode();
        }
    }

    res.json(response);
};
