import { NextFunction, Request, Response } from 'express';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { BaseReportManagerResponse } from '../../models/report-manager';
import { ExtractionComponent } from '../../models/extraction.component';
import { AuthenticationDataSourceException } from '../../exceptions/authenticationDataSourceException';
import { ConnectionDataSourceException } from '../../exceptions/connectionDataSourceException';

/**
 * @category Report
 * @summary Get information about last refresh
 * @method GET
 * @url /analytics/v1/reports/last-refresh-date
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [array, optional] Response's data
 *      @item refreshStatus [string, required] Status of the last refresh
 *      @item refreshDate [string, required] Datetime of the last refresh
 *      @item lastRefreshStartDate [string, optional] Datetime of the last refresh start date (Only available with Datalake v2.5)
 *      @item isRefreshNeeded [boolean, optional] If true a new refresh of the data will be triggered on report extraction otherwise no (Only available with Datalake v2.5)
 *      @item errorCount [number, optional] Number of error occurred during the refresh procedure (Only available with Datalake v2.5)
 * @end
 * @response error [string, optional] Error message (Only available with Datalake v3)
 * @response errorCode [number, optional] Error code (Only available with Datalake v3)
 */
export const getDateLakeRefreshDate = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const extractionComponent = new ExtractionComponent();
    const response = new BaseReportManagerResponse();
    response.success = true;

    try {
        response.data = await extractionComponent.getDataLakeLastRefresh(res.locals.session);
    } catch (err: any) {
        logger.errorWithStack(`Internal error on get last data lake update.`, err);
        res.status(500);
        response.success = false;
        if (err instanceof ConnectionDataSourceException || err instanceof AuthenticationDataSourceException) {
            res.type('application/json');
            response.error = err.message;
            response.errorCode = err.getCode();
        }
    }

    res.json(response);
};
