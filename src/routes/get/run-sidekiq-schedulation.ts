import { Request, Response } from 'express';
import httpContext from 'express-http-context';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import { sqsManagerFactory } from '../../services/sqs/sqs-manager-factory';
import Config from '../../config';


/**
 * Internal API called by Sidekiq for the report schedulation
 */
export const runSidekiqSchedulation = async (req: Request, res: Response) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const { id_report, platform } = req.params;

    try {
        const config = new Config();
        const queue = config.getDatalakeV3MessagingQueueUrl();
        logger.debug(`Generating event "NewExtraction" in queue ${queue} for Platform: ${platform} - ReportId: ${id_report}`);
        const response = await sqsManagerFactory.getSqsManager().sendNewExtractionToQueue(id_report, platform);
        logger.debug(`Event "NewExtraction" for Platform: ${platform} - ReportId: ${id_report} generated in queue ${queue} with MessageId "${response.MessageId}"`);
        res.status(200);
        res.json({status: true});
    } catch (err: any) {
        logger.errorWithStack(`Error generating event "NewExtraction" in queue for Platform: ${platform} - ReportId: ${id_report}`, err);
        res.status(500);
        res.json({status: false, description: 'Cannot launch the scheduled export'});
    }
};
