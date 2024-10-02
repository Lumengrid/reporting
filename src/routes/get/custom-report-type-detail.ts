import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';
import { UserPropsResponse } from '../../services/hydra';

import { NotFoundException, UnauthorizedException } from '../../exceptions';
import {
    CustomReportTypeDetail,
    CustomReportTypeDetailsResponse,
} from '../../query-builder/interfaces/query-builder.interface';
import { QueryBuilderDetail } from '../../query-builder/models/query-builder-detail';
import { Dynamo } from '../../services/dynamo';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';

/**
 * @category Query Builder
 * @summary Get query builder v2 details
 * @method GET
 * @get id [string, required] ID of the query builder v2
 * @url /analytics/v1/custom-report-types/{id}
 *
 * @response data [object, required]
 *      @item id [string, required] Uuid of the query builder
 *      @item name [string, required] Name of the query builder
 *      @item platform [string, required] The platform associated to the query builder id
 *      @item description [string, required] Description of query builder
 *      @item creationDate [string, required] Date of creation query builder
 *      @item authorId [int, required] userId of the user that created the query builder
 *      @item lastEditBy [int, required] userId of the user that last edit the query builder
 *      @item lastEditByDate [string,required] Date of last edit the query builder
 *      @item status [int, required] Determine if is a query builder status is active (=1) or inactive (=0)
 *      @item sql [string, required] The sql string
 *      @item json [string, required] The json filter
 *      @item deleted [boolean, required] Determine if is a query builder has been deleted
 *      @item relatedReports [array, required] Info about the reports id related to this query builder
 *          @item report_data [object, required]
 *              @item idReport [string, required] Uuid of the report
 *              @item title [string, required] Title of the report
 *          @end
 *      @end
 *      @item lastEditByDetails [object, required] Info about user that last edit the query builder
 *          @item firstname [string, required] Name of the user
 *          @item lastname [string, required] Lastname of the user
 *          @item username [string, required] Username of the user
 *          @item avatar [string, required] The avatar url of the user
 *          @item timezone [string, required] The timezone of the user
 *      @end
 * @end
 * @response success [boolean, required] Whether or not the operation was successful
 */
export const getCustomReportTypeDetail = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;

    const hydra = session.getHydra();
    const dynamoService: Dynamo = session.getDynamo();

    const response = {} as CustomReportTypeDetailsResponse;

    res.type('application/json');
    res.status(200);

    try {
        // Get the data from dynamo
        const dynamoMetadata = await dynamoService.getCustomReportTypesById(req.params.id) as AWS.DynamoDB.DocumentClient.AttributeMap;

        // Get the user's detail associated with the id
        let lastEditByUserInfo: UserPropsResponse = {
            data: {
                idUser: dynamoMetadata.lastEditBy,
                firstname: 'Deleted',
                lastname: 'User',
                username: '',
                avatar: ''
            }
        };
        try {
            lastEditByUserInfo = await hydra.getUserProps(dynamoMetadata.lastEditBy);
        } catch (err: any) {
            // Nothing to do here, the SA was deleted from the platform so we will return empty info for the user
        }

        const linkedReports = await dynamoService.getReportsByCustomReportType(req.params.id);

        const customReportTypeDetail = {
            ...dynamoMetadata,
            lastEditByDetails: lastEditByUserInfo.data,
            relatedReports: linkedReports
        } as unknown as CustomReportTypeDetail;

        response.success = true;
        response.data = new QueryBuilderDetail(customReportTypeDetail) as CustomReportTypeDetail;

    } catch (err: any) {
        if (err instanceof UnauthorizedException) {
            res.status(401);
        } else if (err instanceof NotFoundException) {
            res.status(404);
        } else {
            res.status(500);
        }
        logger.errorWithStack(`Error while getting the custom report type details with id ${req.params.id}.`, err);
        response.success = false;
        response.error = 'Generic error. See the logs for more information';
    }

    res.json(response);
};
