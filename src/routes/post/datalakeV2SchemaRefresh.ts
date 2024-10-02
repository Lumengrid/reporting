import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { ExtractionComponent } from '../../models/extraction.component';
import { RefreshDataLake } from '../../models/refresh-data-lake';
import { BaseReportManagerResponse } from '../../models/report-manager';

/**
 * @category Report
 * @summary Start datalake refresh v2 on demand
 * @method POST
 * @url /analytics/v1/reports/schema-refresh
 *
 * @parameter bjName [string, optional] Background job name
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response error [string, optional] Error message
 * @response errorCode [integer, optional] Error code
 */
export const datalakeV2SchemaRefresh = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const bjName = req.body.bjName;
    const response = new BaseReportManagerResponse();
    response.success = true;

    try {
        const refreshDataLake = new RefreshDataLake(session);
        const extractionComponent = new ExtractionComponent();
        const refreshInfo = await extractionComponent.getDataLakeLastRefresh(session);
        const sqs = session.getSQS();
        await sqs.runDataLakeV2Refresh(session, refreshInfo, true);
        await refreshDataLake.startRefreshBackgroundJob(bjName);
        res.status(200);
    } catch (e: any) {
        logger.errorWithStack('Error on starting a manual datalake V2 refresh', e);
        response.success = false;
        res.status(500);
    }
    res.json(response);

};
