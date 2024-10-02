import { NextFunction, Request, Response } from 'express';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { CustomReportTypesManager } from '../../query-builder/manager/custom-report-types-manager';
import { JSON_AREA_ERROR, NotFoundException } from '../../exceptions';
import { BadRequestException } from '../../exceptions/bad-request.exception';
import { CustomReportTypesResponse } from '../../query-builder/models/query-builder';
import { CustomReportType } from '../../query-builder/interfaces/query-builder.interface';

/**
 * @category Query Builder
 * @summary Create query builder v2 preview
 * @method POST
 * @get id_custom_report_types [string, required] ID of the query builder v2
 * @url /analytics/v1/custom-report-types/{id_custom_report_types}/preview
 *
 * @parameter sql [string, required] Sql string
 * @parameter json [string,required] Json string
 *
 * @response data [object, required]
 *      @item QueryExecutionId [string, required] This id must be used to see the query results through another call
 * @end
 * @response success [boolean, required] Whether or not the operation was successful
 */
export const customReportTypesPreview = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const responseOptions: CustomReportTypesResponse = {success: true};
    const dynamo = session.getDynamo();

    const query = CustomReportTypesManager.removeExtrasSemicolon(req.body.sql);
    const json = req.body.json;
    const idCustomReportType = req.params.id_custom_report_types;
    try {
        const customReportTypes: CustomReportType = await dynamo.getCustomReportTypesById(idCustomReportType) as CustomReportType;

        const data: any = await CustomReportTypesManager.getQueryExecutionIdBySql(session, query, json, true);

        await CustomReportTypesManager.saveQueryExecutionIdOnRedis(session, data.QueryExecutionId, idCustomReportType);
        responseOptions.data = {QueryExecutionId : data.QueryExecutionId};
        responseOptions.success = true;
    } catch (err: any) {
        const errorCode = typeof err.code !== 'undefined' ? err.code : '';
        responseOptions.success = false;
        responseOptions.errorCode = errorCode;
        responseOptions.error = err.message;
        if (err instanceof NotFoundException) {
            res.status(404);
        } else if (err instanceof BadRequestException) {
            if (JSON_AREA_ERROR.includes(<number> errorCode)) {
                responseOptions.errorMessageJson = err.message;
                responseOptions.error = 'Provide a valid JSON';
            } else {
                responseOptions.errorMessageSql =  err.message;
                responseOptions.error = 'Provide a valid SQL';
            }
            res.status(400);
        } else {
            res.status(500);
        }
    }
    res.json(responseOptions);

};
