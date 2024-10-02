import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';

import { ReportsSettingsResponse } from '../../models/base';
import { DatalakeRefreshTokens } from '../../models/datalake-refresh-tokens';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { SettingsComponent } from '../../shared/components/settings.component';

/**
 * @category Report
 * @summary Put aamon settings
 * @internal
 * @method PUT
 * @notes Returns only field that contains a value different from the default value
 * @url /analytics/v1/reports/settings
 *
 * @parameter csvExportLimit [integer, optional] max lines number exportable in csv. Set 0 to set default value
 * @parameter xlxExportLimit [integer, optional] max lines number exportable in xlx. Set 0 to set default value
 * @parameter previewExportLimit [integer, optional] max lines number visible in preview. Set 0 to set default value
 * @parameter entityUsersLimit [integer, optional] max users number selectable in users filter. Set 0 to set default value
 * @parameter entityGroupsLimit [integer, optional] max groups number selectable in users filter. Set 0 to set default value
 * @parameter entityBranchesLimit [integer, optional] max branches number selectable in users filter. Set 0 to set default value
 * @parameter entityCoursesLimit [integer, optional] max courses number selectable in courses filter. Set 0 to set default value
 * @parameter entityLPLimit [integer, optional] max lp number selectable in courses filter. Set 0 to set default value
 * @parameter entityCourseInstructorsLimit [integer, optional] max instructor number selectable in courses filter. Set 0 to set default value
 * @parameter entityClassroomLimit [integer, optional] max classroom number selectable in courses filter. Set 0 to set default value
 * @parameter entityWebinarLimit [integer, optional] max webinar number selectable in courses filter. Set 0 to set default value
 * @parameter entitySessionsLimit [integer, optional] max classroom sessions number selectable in session filter
 * @parameter monthlyRefreshTokens [integer, optional] number of monthly refresh tokens. Set 0 to set default value
 * @parameter dailyRefreshTokens [integer, optional] number of daily refresh tokens
 * @parameter datalakeV2ExpirationTime [integer,optional] Set the datalake expiration time (in seconds)s. Set 0 to set default value
 * @parameter errorCount [integer,optional] Set error count value
 * @parameter extractionTimeLimit [integer, optional] number of minuts the a report have to extract the data before set the status to FAILED
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, required] Settings info. Returns only field that contains a value different from the default value
 *      @item platform [string, required] platform's url
 *      @item csvExportLimit [integer, optional] max lines number exportable in csv
 *      @item xlxExportLimit [integer, optional] max lines number exportable in xlx
 *      @item previewExportLimit [integer, optional] max lines number visible in preview
 *      @item entityUsersLimit [integer, optional] max users number selectable in users filter
 *      @item entityGroupsLimit [integer, optional] max groups number selectable in users filter
 *      @item entityBranchesLimit [integer, optional] max branches number selectable in users filter
 *      @item entityCoursesLimit [integer, optional] max courses number selectable in courses filter
 *      @item entityLPLimit [integer, optional] max lp number selectable in courses filter
 *      @item entityCourseInstructorsLimit [integer, optional] max instructor number selectable in courses filter
 *      @item entityClassroomLimit [integer, optional] max classroom number selectable in courses filter
 *      @item entityWebinarLimit [integer, optional] max webinar number selectable in courses filter
 *      @item entitySessionsLimit [integer, optional] max classroom sessions number selectable in session filter
 *      @item monthlyRefreshTokens [integer, optional] number of monthly refresh tokens
 *      @item dailyRefreshTokens [integer, optional] number of daily refresh tokens
 *      @item datalakeV2ExpirationTime [integer,optional] The datalake expiration time (in seconds)
 *      @item errorCount [integer,optional] The error count value
 *      @item extractionTimeLimit [integer, optional] number of minuts the a report have to extract the data before set the status to FAILED
 * @end
 */
export const putReportSettingsUpdate = async (req: Request, res: Response, next: NextFunction) => {
    // return PUT response success true or false
    const session: SessionManager = res.locals.session;
    const logger: SessionLoggerService = httpContext.get('logger');
    const responseOptions: ReportsSettingsResponse = { success: true };
    const settings: SettingsComponent = new SettingsComponent(req, session);
    const datalakeRefreshTokens: DatalakeRefreshTokens = new DatalakeRefreshTokens(session);

    res.type('application/json');
    res.status(200);

    try {
        responseOptions.data = await settings.updateSettings();
        req.app.locals.cache.del(session.platform.getPlatformBaseUrl());
        req.app.locals.cache.set(session.platform.getPlatformBaseUrl(), responseOptions.data, 60 * 60);

        // reset the number of tokens and last request/reset
        await datalakeRefreshTokens.resetTokens(responseOptions.data);

    } catch (err: any) {
        logger.errorWithStack(`Internal error while saving reports settings. ${err.toString()}`, err);
        res.status(500);
        responseOptions.success = false;
    } finally {
        res.json(responseOptions);
    }
};
