import { NextFunction, Request, Response } from 'express';
import { ReportManagerExportResponse } from '../../models/report-manager';
import { ReportExportComponent } from './report-export.component';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { NotFoundException, DisabledReportTypeException } from '../../exceptions/';
import { Utils } from '../../reports/utils';
import SessionManager from '../../services/session/session-manager.session';
import { ConnectionDataSourceException } from '../../exceptions/connectionDataSourceException';
import { ExtractionFailedException } from '../../exceptions/extractionFailedException';

/**
 * @category Report
 * @summary Export report as CSV
 * @method GET
 * @get report_id [string, required] ID of the report
 * @url /analytics/v1/reports/{report_id}/export/csv
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, optional]
 *      @item executionId [string,required] This id must be used to see the query results through another call
 * @end
 *
 * @status 404 Report not found!
 */
export const getReportExportCsv = async (req: Request, res: Response, next: NextFunction) => {
    const session: SessionManager = res.locals.session;
    const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
    const generateBackgroundJob: boolean = req.query.generateBackgroundJob ? Utils.stringToBoolean(req.query.generateBackgroundJob.toString()) : true;
    const enableFileCompression: boolean = req.query.enableFileCompression ? Utils.stringToBoolean(req.query.enableFileCompression.toString()) : true;

    const exportComponent = new ReportExportComponent(req, res.locals.session, req.hostname, subfolder, false, 'csv', generateBackgroundJob, enableFileCompression);
    let response = new ReportManagerExportResponse();
    const logger: SessionLoggerService = httpContext.get('logger');

    try {
        if (session.platform.isDatalakeV3ToggleActive()) {
            response = await exportComponent.exportReportV3();
        } else {
            response = await exportComponent.exportReport();
        }
    } catch (err: any) {
        logger.errorWithStack('Error on starting CSV report extraction.', err);
        response.success = false;
        if (err instanceof NotFoundException || err instanceof DisabledReportTypeException) {
            res.status(404);
            response.error = 'Report not found!';
            response.errorCode = err.getCode();
        } else {
            res.status(500);
            if (err instanceof ExtractionFailedException || err instanceof ConnectionDataSourceException) {
                response.error = err.message;
                response.errorCode = err.getCode();
            }
        }
    }

    res.json(response);
};
