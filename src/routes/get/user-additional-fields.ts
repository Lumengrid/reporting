import { NextFunction, Request, Response } from 'express';
import { ReportManagerExportResponse } from '../../models/report-manager';
import { UserAdditionalFieldsComponent } from './user-additional-fields-component';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';

/**
 * @category Report
 * @summary Get user additional fields
 * @internal
 * @method GET
 * @url /analytics/v1/reports/user-additional-fields
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [array, required] Response's data
 *      @item id [integer, required] id of additional field
 *      @item title [string, required] additional field's title
 *      @item sequence [integer, required] sequence's number
 *      @item options [array, required] options
 *          @item value [integer, required] value
 *          @item label [string, required] label
 *      @end
 * @end
 */
export const userAdditionalFields = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const userAdditionalFieldsComponent = new UserAdditionalFieldsComponent(res.locals.session);
    const response = new ReportManagerExportResponse();
    response.success = true;

    try {
        response.data = await userAdditionalFieldsComponent.getUserAdditionalFields();
    } catch (err: any) {
        logger.errorWithStack(`Internal error while performing a report export.`, err);
        res.status(500);
        response.success = false;
    }

    res.json(response);
};
