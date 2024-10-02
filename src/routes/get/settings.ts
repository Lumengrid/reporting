import { NextFunction, Request, Response } from 'express';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { ReportsSettings, ReportsSettingsResponse } from '../../models/base';

/**
 * @category Report
 * @summary Get aamon settings
 * @notes Returns only field that contains a value different from the default value
 * @internal
 * @method GET
 * @url /analytics/v1/reports/settings
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, required] Settings info
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
 *      @item extractionTimeLimit [integer, optional] number of minuts the a report have to extract the data before set the status to FAILED
 * @end
 */
export const getReportSettings = async (req: Request, res: Response, next: NextFunction) => {
    // return PUT response success true or false
    const session: SessionManager = res.locals.session;
    const logger: SessionLoggerService = httpContext.get('logger');
    const responseOptions: ReportsSettingsResponse = { success: true };


    res.type('application/json');
    res.status(200);

    try {
        if (req.app.locals.cache.get(session.platform.getPlatformBaseUrl())) {
            responseOptions.data = req.app.locals.cache.get(session.platform.getPlatformBaseUrl());
        } else {
            responseOptions.data = await session.getDynamo().getSettings() as ReportsSettings;
        }

        // if doesn't exist datalakeV2ExpirationTime field in dynamo, we return the default value
        if (!responseOptions.data.datalakeV2ExpirationTime) {
            const hydra = session.getHydra();
            let installationType = '';
            try {
                installationType = (await hydra.getInstallationType()).data.installationType;
            } catch (error: any) {
                logger.errorWithStack(`Error on retrieve installation type for the platform ${session.platform.getPlatformBaseUrl()}.`, error);
                installationType = '';
            }
            responseOptions.data.datalakeV2ExpirationTime = session.platform.getDatalakeV2ExpirationTime(installationType);
        }
    } catch (err: any) {
        logger.errorWithStack(`Internal error while recover reports settings.`, err);
        console.log(err);
        res.status(500);
        responseOptions.success = false;
    } finally {
        res.json(responseOptions);
    }
};
