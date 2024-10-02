import { Request, Response } from 'express';
import { ExtractionComponent } from '../../models/extraction.component';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';

/**
 * @category Report
 * @summary Set datalake refresh status in progress
 * @method POST
 * @internal
 * @notes This api is callable only from a HYDRA container
 * @url /aamon/reports/scheduled/export
 *
 * @parameter platforms [array(string), required] Platforms list
 * @parameter isRefreshOnDemand [boolean, required] Is refresh on demand
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response description [string, optional] Error message
 */
export const runScheduledExtractions = async (req: Request, res: Response) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const platforms = req.body.platforms;
    const isRefreshOnDemand = req.body.isRefreshOnDemand;
    const extractionComponent = new ExtractionComponent();

    try {
        await extractionComponent.performScheduledReportsExport(platforms, isRefreshOnDemand);
        res.status(200);
        res.json({status: true});
    } catch (scheduleError: any) {
        logger.errorWithStack(`Cannot perform the scheduled export.`, scheduleError);
        res.status(500);
        res.json({status: false, description: 'Cannot launch the scheduled export'});
    }

};
