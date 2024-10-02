import { NextFunction, Request, Response } from 'express';
import { NotFoundException, UnauthorizedException } from '../../exceptions';
import { Dynamo } from '../../services/dynamo';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { CustomReportType, CustomReportTypesResponse } from '../../query-builder/interfaces/query-builder.interface';
import { CustomReportTypesComponent } from './custom-report-types.component';

/**
 * @category Query Builder
 * @summary Get query builder v2 available in the platform
 * @method GET
 * @url /analytics/v1/custom-report-types/active
 *
 * @response data [array, required]
 *   @item [object, required]
 *      @item id [string, required] Uuid of the query builder
 *      @item name [string, required] Name of the query builder
 *      @item description [string,required] Description of query builder
 *   @end
 * @end
 * @response success [boolean, required] Whether or not the operation was successful
 */
export const getActiveCustomReportTypes = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const hydra = session.getHydra();

    const dynamoService: Dynamo = session.getDynamo();
    const component: CustomReportTypesComponent = new CustomReportTypesComponent(dynamoService, hydra);

    const response = {} as CustomReportTypesResponse;
    let customReportTypesList: CustomReportType[] = [];
    res.type('application/json');

    try {
        customReportTypesList = await component.getActiveCustomReportTypes() as CustomReportType[];
        response.success = true;
        response.data = customReportTypesList;
        res.status(200);
    } catch (err: any) {
        if (err instanceof UnauthorizedException) {
            res.status(401);
        } else if (err instanceof NotFoundException) {
            res.status(404);
        } else {
            res.status(500);
        }
        logger.errorWithStack(`Error while performing get custom report types active call.`, err);
        response.success = false;
        response.error = 'Generic error. See the logs for more information';
    }

    response.data = customReportTypesList;
    res.json(response);
};
