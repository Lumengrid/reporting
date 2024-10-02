import { Request, Response } from 'express';
import httpContext from 'express-http-context';

import { ReportsResponse } from '../../models/custom-report';
import { ReportManagerInfo } from '../../models/report-manager';
import { SidekiqScheduler } from '../../models/sidekiq-scheduler';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { ReportsSettings } from '../../models/base';

export const postSidekiqSchedulation = async (req: Request, res: Response) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const response: ReportsResponse = new ReportsResponse();
    const isToggleOnEvent = req.body.isToggleOnEvent;

    res.type('application/json');
    res.status(200);

    try {
        const platform = session.platform.getPlatformBaseUrl();
        const dynamo = session.getDynamo();

        // Retrieve all scheduled reports
        const scheduledReports = await dynamo.getScheduledReportByPlatform([platform], true) as ReportManagerInfo[];

        // When switching the TOGGLE_FORCE_DATALAKE_V1 OFF, update all the scheduled reports adding the "startHour" and "timezone" fields
        if (isToggleOnEvent) {
            const settings = await dynamo.getSettings() as ReportsSettings;
            settings.toggleDatalakeV2 = true;
            dynamo.createOrEditSettings(settings);

            const startHour = '06:00';
            const timezone = session.user.getTimezone();

            // Update all reports
            scheduledReports.forEach((report: ReportManagerInfo) => {
                report.planning = {
                    ...report.planning,
                    option: {
                        ...report.planning.option,
                        startHour,
                        timezone
                    }
                };
            });
            await dynamo.updateReportSchedulationToggleOn(scheduledReports);
        }

        // Create a Sidekiq Job for each scheduled reports
        const sidekiqScheduler = new SidekiqScheduler(session.platform.getPlatformBaseUrl());
        for (const report of scheduledReports) {
            const planningOption = {
                startHour: report.planning.option.startHour,
                timezone: report.planning.option.timezone,
                scheduleFrom: report.planning.option.scheduleFrom
            };
            await sidekiqScheduler.activateScheduling(report.idReport, planningOption);
        }
        // Closing redis connection after working with schedulations
        const scheduledReportIds = scheduledReports.map(report => report.idReport);
        logger.debug(`Sidekiq schedulation activated after switching on the TOGGLE_FORCE_DATALAKE_V1 OFF for the following reports: ${scheduledReportIds} - Number of reports affected: ${scheduledReports.length}`);

        await dynamo.updateDataLakeRefreshItem({platform});
        logger.debug(`Reset datalake refresh info for platform: ${platform}`);
    } catch (err: any) {
        logger.errorWithStack(`Error while refreshing the Sidekiq jobs.`, err);
        res.status(500);
        response.success = false;
    }

    res.json(response);
};
