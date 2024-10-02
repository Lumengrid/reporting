import { Request, Response } from 'express';
import { SessionLoggerService } from '../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { ExtractionComponent } from './extraction.component';
import HydraEventTriggerService from '../services/hydra-event-trigger.service';
import { HydraEvents } from './base';

/**
 * @category Report
 * @summary Set datalake refresh on error
 * @method POST
 * @internal
 * @notes This api is callable only from a HYDRA container
 * @url /aamon/reports/data-lake/error
 *
 * @parameter platforms [array(string), required] Platforms list
 * @parameter isRefreshOnDemand [boolean, required] Is refresh on demand
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response description [string, optional] Error message
 */
export const dataLakeError = async (req: Request, res: Response) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const platforms: string[] = req.body.platforms;
    const isRefreshOnDemand = req.body.isRefreshOnDemand;
    const extractionComponent = new ExtractionComponent();

    // log the error for Loggly, we need to know when an error is triggered from EMR
    logger.error(`Data Lake refresh${isRefreshOnDemand ? ' after a refresh on demand' : ''} goes to error, platforms affected: ${JSON.stringify(platforms)}`);


    try {
        await extractionComponent.setDataLakeStatusToError(platforms, isRefreshOnDemand);

        if (isRefreshOnDemand) {
            await extractionComponent.restoreRefreshTokensAfterIngestionError(platforms);
        } else {
            for (const platform of platforms) {
                const hydraEventTrigger = new HydraEventTriggerService(platform);
                await hydraEventTrigger.triggerEventOnPlatform(HydraEvents.ROG_ERROR, {});
            }
        }

        res.status(200);
        res.json({status: true});
    } catch (error: any) {
        logger.errorWithStack(`Cannot update the data lake status to error, error was: ${error.toString()}, platforms affected: ${JSON.stringify(platforms)}`, error);
        res.status(500);
        res.json({status: false, description: 'Cannot update the data lake status to error'});
    }

};
