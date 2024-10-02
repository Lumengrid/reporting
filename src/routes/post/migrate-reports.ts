import { NextFunction, Request, Response } from 'express';
import SessionManager from '../../services/session/session-manager.session';
import { MigrateInputPayload, MigrationComponent } from '../../models/migration-component';
import { BaseResponse } from '../../models/base';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';

/**
 * @category Report
 * @summary Migrate legacy report
 * @method POST
 * @url /analytics/v1/reports/migrations
 *
 * @parameter types [array(integer), required] Legacy report types
 * @parameter isMigrationWithOverwrite [boolean, required] Overwrite previous report already migrated
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, optional] Response's object
 * @end
 */
export const migrateReports = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const reportFiltersPayload: MigrateInputPayload = req.body;
    const session: SessionManager = res.locals.session;
    const migrateComponent = new MigrationComponent(session);
    const response: BaseResponse = {
        success: true,
        data: [],
    };
    try {
        response.data = await migrateComponent.migrateReports(reportFiltersPayload);
    } catch (err: any) {
        logger.errorWithStack(`Cannot perform the migration of the reports.`, err);
        res.status(500);
        response.success = false;
    }

    res.json(response);
};
