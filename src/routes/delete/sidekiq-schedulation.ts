import { Request, Response } from 'express';
import httpContext from 'express-http-context';

import { NotFoundException, UnauthorizedException } from '../../exceptions';
import { ReportsResponse } from '../../models/custom-report';
import { SidekiqScheduler } from '../../models/sidekiq-scheduler';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { ReportsSettings } from '../../models/base';


export const deleteSidekiqSchedulation = async (req: Request, res: Response) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const response: ReportsResponse = new ReportsResponse();

    res.type('application/json');
    res.status(200);

    let scheduledReports = [];

    try {
        const platform = session.platform.getPlatformBaseUrl();
        const dynamo = session.getDynamo();

        const settings = await dynamo.getSettings() as ReportsSettings;
        if (settings.toggleDatalakeV2) {
            delete settings.toggleDatalakeV2;
        }
        dynamo.createOrEditSettings(settings);

        // Get all reports scheduled
        scheduledReports = await dynamo.getScheduledReportByPlatform([platform]);

        // For each report create a sidekiq job "RemoveTask" and store in redis
        const sidekiqScheduler = new SidekiqScheduler(session.platform.getPlatformBaseUrl());
        for (const report of scheduledReports) {
            await sidekiqScheduler.removeScheduling(report.idReport);
        }

        const scheduledReportIds = scheduledReports.map(report => report.idReport);
        logger.debug(`Sidekiq schedulation removed for the following reports: ${scheduledReportIds}. Total reports affected: ${scheduledReportIds.length}`);

        await dynamo.updateDataLakeRefreshItem({platform});
        logger.debug(`Reset datalake refresh info for platform: ${platform}`);
    } catch (err: any) {
        if (err instanceof UnauthorizedException) {
            res.status(401);
        } else if (err instanceof NotFoundException) {
            res.status(404);
        } else {
            res.status(500);
        }
        logger.errorWithStack(`Error while removing the Sidekiq schedulation: ${err.message}`, err);
        response.success = false;
        response.error = err.message;
    }

    res.json(response);
};
