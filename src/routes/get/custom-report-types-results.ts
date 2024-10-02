import { NextFunction, Request, Response } from 'express';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { CustomReportTypesManager } from '../../query-builder/manager/custom-report-types-manager';
import { NotFoundException } from '../../exceptions/not-found.exception';
import { BadRequestException } from '../../exceptions/bad-request.exception';
import { CustomReportTypesResponse } from '../../query-builder/models/query-builder';
import { CustomReportType } from '../../query-builder/interfaces/query-builder.interface';

/**
 * @category Query Builder
 * @summary Get the preview of a query builder v2
 * @method GET
 * @get id_custom_report_types [string, required] ID of the query builder v2
 * @get query_execution_id [string, required] ID of the query execution
 * @url /analytics/v1/custom-report-types/{id_custom_report_types}/preview/{query_execution_id}
 *
 * @response data [object, required]
 *      @item queryStatus [string,required] String of the name status
 *      @item result [array, required] Array key-value that contains  the results found
 *      @end
 * @end
 * @response success [boolean, required] Whether or not the operation was successful
 */
export const getCustomReportTypesResults = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const responseOptions: CustomReportTypesResponse = {success: true};
    const dynamo = session.getDynamo();

    const idCustomReportType = req.params.id_custom_report_types;
    const queryExecutionId = req.params.query_execution_id;
    try {
        const customReportTypes: CustomReportType = await dynamo.getCustomReportTypesById(idCustomReportType) as CustomReportType;
        const value = await CustomReportTypesManager.checkQueryExecutionIsValid(session, queryExecutionId, idCustomReportType);

        if (value === null) {
            throw new NotFoundException('QueryExecutionId doesn\'t exist or not is associated with this customReportId');
        }
        const data: any = await CustomReportTypesManager.getSqlResult(session, queryExecutionId);

        responseOptions.data = data;
        responseOptions.success = true;
    } catch (err: any) {
        const errorCode = typeof err.code !== 'undefined' ? err.code : '';
        responseOptions.success = false;
        responseOptions.errorCode = errorCode;
        responseOptions.error = err.message;
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
