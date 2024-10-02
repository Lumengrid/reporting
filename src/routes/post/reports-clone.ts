import { ErrorsCode, GeneralErrorResponse } from '../../models/base';
import { NextFunction, Request, Response } from 'express';
import { ReportCreationResponse } from '../../models/custom-report';
import { ReportManagerSwitcher } from '../../models/report-switcher';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';

/**
 * @category Report
 * @summary Clone a report
 * @method POST
 * @get report_id [string, required] ID of the report
 * @url /analytics/v1/reports/{id_report}/clones
 *
 * @parameter name [string, required] Name of the new report
 * @parameter description [string, optional] Description of the new report
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response error [string, optional] Error Message
 * @response errorCode [integer, optional] Error code
 * @response data [object, optional]
 *      @item idReport [integer, required] id of the new report
 * @end
 *
 * @status 404 Report not found!
 */
export const postReportsClone = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    let reportHandler;

    try {
        reportHandler = await ReportManagerSwitcher(session, req.params.id_report);
    } catch (error: any) {
        logger.errorWithStack('Error on create report main class', error);
        res.type('application/json');
        res.status(404);
        res.json(new GeneralErrorResponse('Report not found!', ErrorsCode.ReportNotExist));
        return;
    }

    // The report name was mandatory for the cloned report
    if (!req.body.name) {
        res.type('application/json');
        res.status(404);
        res.json(new GeneralErrorResponse('Report title not set or empty', ErrorsCode.WrongParameter));
        return;
    }
    const title = req.body.name;

    let description = '';
    if (req.body.description) {
        description = req.body.description;
    }

    let uuid: string;
    try {
        uuid = await reportHandler.cloneReport(session, title, description, reportHandler.info.queryBuilderId);
    } catch (error: any) {
        logger.errorWithStack('Error on report cloning process', error);
        res.type('application/json');
        res.status(404);
        res.json(new GeneralErrorResponse('Error during report creation', ErrorsCode.DatabaseError));
        return;
    }

    const response = new ReportCreationResponse(uuid);
    res.type('application/json');
    res.status(200);
    res.json(response);
};
