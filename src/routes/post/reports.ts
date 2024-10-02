import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';
import { ReportCreationResponse } from '../../models/custom-report';
import { NewReportManagerSwitcher } from '../../models/report-switcher';
import { CustomReportType } from '../../query-builder/interfaces/query-builder.interface';
import { QUERY_BUILDER_ACTIVE } from '../../query-builder/models/query-builder-detail';
import { ReportsTypes } from '../../reports/constants/report-types';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { BadRequestException } from '../../exceptions/bad-request.exception';
import { NotFoundException } from '../../exceptions/not-found.exception';

/**
 * @category Report
 * @summary Create report
 * @method POST
 *
 * @url /analytics/v1/reports
 *
 * @parameter type [string, required] Report's type
 * @parameter name [string, required] Report's title
 * @parameter description [string, optional] Report's description
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response error [string, optional] Error message
 * @response data [object, optional] Response's data
 *      @item idReport [string, required] The ID of the new report created
 * @end
 *
 * @status 400 Bad request
 * @status 500 Internal server error
 */
export const postReports = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const hydra = session.getHydra();
    const dynamo = session.getDynamo();

    try {
        if (!req.body.type) {
            throw new BadRequestException('Report type not set or empty');
        }

        if (!Object.values(ReportsTypes).includes(req.body.type)) {
            throw new BadRequestException('Wrong report type');
        }

        const type = req.body.type;

        if (!req.body.name) {
            throw new BadRequestException('Report title not set or empty');
        }

        const title = req.body.name;
        const queryBuilderId = req.body.queryBuilderId;
        let description = '';
        if (req.body.description) {
            description = req.body.description;
        }

        if (type === ReportsTypes.QUERY_BUILDER_DETAIL && !queryBuilderId) {
            throw new BadRequestException('Query builder Id not set or empty');
        }

        // Only ERP and SUPER ADMIN can create a custom report type
        if (type === ReportsTypes.QUERY_BUILDER_DETAIL && !session.user.isGodAdmin()) {
            res.sendStatus(403);
            return;
        }

        if (type === ReportsTypes.QUERY_BUILDER_DETAIL) {
            const customReportTypes: CustomReportType = await dynamo.getCustomReportTypesById(queryBuilderId) as CustomReportType;
            if (customReportTypes.status !== QUERY_BUILDER_ACTIVE) {
                throw new BadRequestException('Query builder selected is not active');
            }
        }

        if (type === ReportsTypes.USERS_WEBINAR && !session.platform.isToggleWebinarsEnableCreation()) {
            throw new NotFoundException('Report not found!');
        }

        const reportManager = NewReportManagerSwitcher(session, type);
        if (reportManager === undefined) {
            throw new BadRequestException('Wrong report type');
        }

        const reportType = type;
        const descriptionReport = description;
        const uuid = await reportManager.saveNewReport(session.platform.getPlatformBaseUrl(), title, description, session.user.getIdUser(), queryBuilderId);

        const payload = {
            entity_id: uuid,
            entity_name: title,
            entity_attributes: {
                type: reportType,
                description: descriptionReport,
                source: 'new_reports',
            },
            event_name: 'create-custom-report',
        };

        const response = new ReportCreationResponse(uuid);
        await hydra.generateEventOnEventBus(payload);

        res.type('application/json');
        res.status(200);
        res.json(response);
    } catch (err: any) {
        logger.errorWithStack(`Error while creating the report.`, err);
        res.type('application/json');
        const response = new ReportCreationResponse(undefined);
        response.error = 'Generic error. See the logs for more information';
        res.status(500);
        if (err instanceof BadRequestException) {
            response.error = err.message;
            res.status(400);
        }
        response.success = false;
        res.json(response);
    }
};
