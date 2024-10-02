import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';
import moment from 'moment';

import { BadRequestException } from '../../exceptions/bad-request.exception';
import { NotFoundException } from '../../exceptions/not-found.exception';
import { CustomReportTypeDetail } from '../../query-builder/interfaces/query-builder.interface';
import { CustomReportTypesManager } from '../../query-builder/manager/custom-report-types-manager';
import { CustomReportTypesResponse } from '../../query-builder/models/query-builder';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { QUERY_BUILDER_ACTIVE } from '../../query-builder/models/query-builder-detail';
import { ErrorCode, JSON_AREA_ERROR } from '../../exceptions/error-codes.enum';

/**
 * @category Query Builder
 * @summary Edit query builder v2
 * @method PUT
 * @get id_custom_report_types [string, required] ID of the query builder v2
 * @url /analytics/v1/custom-report-types/{id_custom_report_types}
 *
 * @parameter name [string, required] Name of the query builder
 * @parameter description [string,required] Description of query builder
 * @parameter status [int, required] Determine if is a query builder status is active (=1) or inactive (=0)
 * @parameter sql [string, required] The sql string
 * @parameter json [string, required] The json filter
 *
 * @response success [boolean, required] Whether or not the operation was successful
 *
 * @status 15 Found related reports to the query builder
 * @status 16 Json is not json
 * @status 17 More filter in json area then sql area
 * @status 18 Filter ${filterName} not found in json area
 * @status 19 Missing field in ${filterName}
 * @status 20 Missing type in ${filterName}
 * @status 21 Type not allowed in ${filterName}
 * @status 22 Fill the json area
 * @status 23 Json area filled but sql not contains filter
 * @status 24 Provide a valid SQL
 * @status 25 Missing description in ${filterName}
 * @status 33 Name field is mandatory
 */
export const putCustomReportTypes = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const dynamo = session.getDynamo();
    const hydra = session.getHydra();
    const responseOptions: CustomReportTypesResponse = {success: true};

    res.type('application/json');

    try {
        const idCustomReportType = req.params.id_custom_report_types;

        let sql = req.body.sql;
        const name = req.body.name;
        const status = req.body.status;
        const json = req.body.json;

        const customReportTypeDetail: CustomReportTypeDetail = await dynamo.getCustomReportTypesById(idCustomReportType) as CustomReportTypeDetail;

        if (Object.keys(req.body).length === 0) { // body empty, no field to change
            res.status(200);
            res.json(responseOptions);
            return;
        }

        if (name !== undefined && name === '') {
            throw new BadRequestException('Name field is mandatory', ErrorCode.MISSING_NAME_FIELD);
        }

        if (sql !== undefined) {
            req.body.sql = sql = CustomReportTypesManager.removeExtrasSemicolon(sql);
        }

        const customReportTypeUpdated = CustomReportTypesManager.updateCustomReportTypes(customReportTypeDetail, req.body);
        if ((sql === undefined || sql === '') && customReportTypeDetail.status) {
            if (customReportTypeDetail.sql === '' || customReportTypeDetail.sql === null) {
                throw new BadRequestException('Provide a valid SQL', ErrorCode.WRONG_SQL);
            }
        }

        // if you want to change a SQL or a JSON area of an active custom report types
        // or if you want to set active a query builder
        // we need to check if the pair SQL and JSON is valid
        if (parseInt(status, 10) === QUERY_BUILDER_ACTIVE || ((sql !== undefined || json !== undefined) && customReportTypeDetail.status === QUERY_BUILDER_ACTIVE)) {
            await CustomReportTypesManager.isSqlValid(session, customReportTypeDetail.sql, customReportTypeDetail.json);
        }

        // change status to inactive
        if (status !== undefined && parseInt(status, 10) !== QUERY_BUILDER_ACTIVE) {
            const reports = await CustomReportTypesManager.getReportsByCustomReportType(session, idCustomReportType);

            if (reports.length > 0) {
                responseOptions.data = reports.map(report => report.title);

                throw new BadRequestException('Found related reports to the query builder', ErrorCode.QUERY_BUILDER_RELATED_REPORT);
            }
        }

        customReportTypeUpdated.lastEditBy = session.user.getIdUser();
        customReportTypeUpdated.lastEditByDate = moment(new Date()).format('YYYY-MM-DD HH:mm:ss');

        await dynamo.createOrEditCustomReportTypes(customReportTypeUpdated);

        // Event for audit trail
        const eventPayload = {
            entity_id: customReportTypeUpdated.id,
            entity_name: customReportTypeUpdated.name,
            entity_attributes: {
                description: customReportTypeUpdated.description,
                status: customReportTypeUpdated.status === 0 ? 'inactive' : 'active',
                source: 'query_builder_v2',
            },
            event_name: 'update-custom-query',
        };
        await hydra.generateEventOnEventBus(eventPayload);

        res.status(200);
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
            } else if (errorCode === ErrorCode.WRONG_SQL) {
                responseOptions.errorMessageSql = err.message;
                responseOptions.error = 'Provide a valid SQL';
            }
            res.status(400);
        } else {
            res.status(500);
        }
    }
    res.json(responseOptions);
};
