import { NextFunction, Request, Response } from 'express';
import { ReportManagerSwitcher } from '../../models/report-switcher';
import { BaseReportManagerResponse } from '../../models/report-manager';
import SessionManager from '../../services/session/session-manager.session';
import PlatformManager from '../../services/session/platform-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { NotFoundException, DisabledReportTypeException } from '../../exceptions/';
import { ConnectionDataSourceException } from '../../exceptions/connectionDataSourceException';
import { ExtractionFailedException } from '../../exceptions/extractionFailedException';

/**
 * @category Report
 * @summary Get report preview
 * @method GET
 * @get id_report [string, required] ID of the report
 * @url /analytics/v1/reports/{id_report}/preview
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [array(object), required] Results of the query
 *
 * @status 404 Report not found!
 */
export const getReportPreview = async (req: Request, res: Response, next: NextFunction) => {
    const session: SessionManager = res.locals.session;
    const platformManager: PlatformManager = session.platform;
    const logger: SessionLoggerService = httpContext.get('logger');


    const response = new BaseReportManagerResponse();
    let results: [];

    try {
        const reportHandler = await ReportManagerSwitcher(session, req.params.id_report);

        try {
            if (session.platform.isDatalakeV3ToggleActive()) {
                const snowflakeDriver = session.getSnowflake();

                const query = await reportHandler.getQuerySnowflake(platformManager.getPreviewExportLimit(), true, true, false);
                results = await snowflakeDriver.runQuery(query);
            } else {
                const athena = session.getAthena();

                const query = await reportHandler.getQuery(platformManager.getPreviewExportLimit(), true, true);
                const data = await athena.runQuery(query);

                results = athena.getQueryResultsAsArray(data);

                const tableDeletion = new Promise(async (resolve, reject) => {
                    await reportHandler.dropTemporaryTables();
                });
            }

            const dataResponse = reportHandler.dataResponse(results);
            res.type('application/json');
            res.status(200);
            res.json(dataResponse);
        } catch (err: any) {
            logger.errorWithStack(`Error while getting preview of report (idReport: ${req.params.id_report}).`, err);
            res.type('application/json');
            res.status(500);
            response.success = false;
            response.error = 'Generic error. See the logs for more information';
            if (err instanceof NotFoundException || err instanceof DisabledReportTypeException) {
                response.error = 'Report not found!';
                response.errorCode = err.getCode();
            } else if (err instanceof ConnectionDataSourceException || err instanceof ExtractionFailedException) {
                response.error = err.message;
                response.errorCode = err.getCode();
            }
            res.json(response);
        }
    } catch (err: any) {
        logger.errorWithStack(`Error while getting preview of report (idReport: ${req.params.id_report}).`, err);
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
        res.json(response);
    }
};
