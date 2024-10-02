import { NextFunction, Request, Response } from 'express';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { CustomReportTypesManager } from '../../query-builder/manager/custom-report-types-manager';
import { NotFoundException } from '../../exceptions/not-found.exception';
import { BadRequestException } from '../../exceptions/bad-request.exception';
import { CustomReportTypesResponse } from '../../query-builder/models/query-builder';


/**
 * @category Query Builder
 * @summary Get the reports associated to the query builder v2
 * @method GET
 * @get id_custom_report_types [string, required] ID of the query builder v2
 * @url /analytics/v1/custom-report-types/{id_custom_report_types}/reports
 * @internal
 *
 * @response data [object, required] Info about the reports id related to this query builder
 *      @item report_data [object, required]
 *          @item idReport [string, required] Uuid of the report
 *          @item title [string, required] Title of the report
 *      @end
 * @end
 * @response success [boolean, required] Whether or not the operation was successful
 */
export const getCustomReportTypesReports = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const responseOptions: CustomReportTypesResponse = {success: true};

    const idCustomReportType = req.params.id_custom_report_types;
    try {
        const reports = await CustomReportTypesManager.getReportsByCustomReportType(session, idCustomReportType);

        responseOptions.data = reports;
        responseOptions.success = true;
    } catch (err: any) {
        responseOptions.success = false;
        responseOptions.errorCode = err.getCode();
        logger.errorWithStack(`Error while getting get reports related to custom report type (idCustomReportType: ${idCustomReportType}).`, err);
        responseOptions.error = 'Generic error. See the logs for more information';
        if (err instanceof NotFoundException) {
            res.status(404);
        } else if (err instanceof BadRequestException) {
            responseOptions.errorMessageSql = err.message;
            responseOptions.error = 'Provide a valid SQL';
            res.status(400);
        } else {
            res.status(500);
        }
    }
    res.json(responseOptions);
};
