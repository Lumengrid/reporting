import { NextFunction, Request, Response } from 'express';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { v4 } from 'uuid';
import { CustomReportType } from '../../query-builder/interfaces/query-builder.interface';
import { CustomReportTypesCreationResponse } from '../../query-builder/models/query-builder';
import moment from 'moment';
import { ErrorsCode, GeneralErrorResponse } from '../../models/base';

/**
 * @category Query Builder
 * @summary Create query builder v2
 * @method POST
 * @url /analytics/v1/custom-report-types
 *
 * @parameter name [string, required] Name of the query builder
 * @parameter description [string, optional] Description of query builder
 *
 * @response data [object, required]
 *      @item idCustomReportTypes [string, required] Id of the query builder created
 * @end
 * @response success [boolean, required] Whether or not the operation was successful
 */
export const postCustomReportTypes = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const dynamo = session.getDynamo();
    const hydra = session.getHydra();

    if (!req.body.name) {
        res.type('application/json');
        res.status(400);
        res.json(new GeneralErrorResponse('Missing required parameter "name"', ErrorsCode.WrongParameter));
        return;
    }

    try {
        const name = req.body.name;
        const description = req.body.description;

        const customReportTypes: CustomReportType = {} as CustomReportType;

        const date = new Date();
        const uuid = v4();
        customReportTypes.id = uuid;
        customReportTypes.name = name;
        customReportTypes.description = description;
        customReportTypes.authorId = session.user.getIdUser();
        customReportTypes.lastEditBy = session.user.getIdUser();
        customReportTypes.lastEditByDate = moment(date).format('YYYY-MM-DD HH:mm:ss');
        customReportTypes.creationDate = moment(date).format('YYYY-MM-DD HH:mm:ss');
        customReportTypes.status = 0;
        customReportTypes.platform = session.platform.getPlatformBaseUrl();
        customReportTypes.deleted = false;

        await dynamo.createOrEditCustomReportTypes(customReportTypes);
        const response = new CustomReportTypesCreationResponse(uuid);

        // Event for audit trail
        const eventPayload = {
            entity_id: uuid,
            entity_name: name,
            entity_attributes: {
                description,
                status: 'inactive',
                source: 'query_builder_v2',
            },
            event_name: 'create-custom-query',
        };

        await hydra.generateEventOnEventBus(eventPayload);

        res.type('application/json');
        res.status(200);
        res.json(response);
    } catch (err: any) {
        logger.errorWithStack('Error on create a new query builder', err);
        res.type('application/json');
        res.status(500);
        const response = new CustomReportTypesCreationResponse(undefined);
        response.success = false;
        response.error = err.message;
        res.json(response);
    }
};
