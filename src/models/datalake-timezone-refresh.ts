import { Request, Response } from 'express';
import { SessionLoggerService } from '../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { ExtractionComponent } from './extraction.component';

/**
 * @category Report
 * @summary Start datalake refresh
 * @method POST
 * @internal
 * @url /aamon/reports/time-zone-refresh
 * @notes This api is callable only from a HYDRA container
 *
 * @parameter platforms [array(string), required] Platforms list
 * @parameter isRefreshOnDemand [boolean, required] Is refresh on demand
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response description [string, optional] Error message
 */
export const startDataLakePerTimezone = async (req: Request, res: Response) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const platforms = req.body.platforms;
    const extractionComponent = new ExtractionComponent();

    try {
        await extractionComponent.startDataLakeRefresh(platforms, false);
        res.status(200);
        res.json({status: true});
    } catch (scheduleError: any) {
        logger.errorWithStack(`Cannot update the status of the platforms requested, error was: ${scheduleError.message}`, scheduleError);
        res.status(500);
        res.json({status: false, description: 'Cannot update the status of the platforms'});
    }

};
