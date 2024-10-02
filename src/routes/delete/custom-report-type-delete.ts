import { Request, Response } from 'express';
import { ReportManagerDeleteResponse } from '../../models/report-manager';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { NotFoundException } from '../../exceptions/';
import { CustomReportTypesManager } from '../../query-builder/manager/custom-report-types-manager';
import { BaseReportManager } from '../../models/base-report-manager';
import { CustomReportTypeDetail } from '../../query-builder/interfaces/query-builder.interface';

/**
 * @category Query Builder
 * @summary Delete query builder v2
 * @method DELETE
 * @get id [string, required] ID of the query builder v2
 * @url /analytics/v1/custom-report-types/{id}
 *
 * @response success [boolean, required] Whether or not the operation was successful
 */
export const deleteCustomReportType = async (req: Request, res: Response) => {
    const session: SessionManager = res.locals.session;
    const response = new ReportManagerDeleteResponse();
    const logger: SessionLoggerService = httpContext.get('logger');
    const dynamo = session.getDynamo();
    const hydra = session.getHydra();

    try {
        const idCustomReportType = req.params.id;

        const reports = await CustomReportTypesManager.getReportsByCustomReportType(session, idCustomReportType);

        if (reports.length > 0) {
            const idReports = reports.map(report => report.idReport);
            await BaseReportManager.deleteReports(idReports, session);
        }

        // Event for audit trail
        const customReportTypeDetail: CustomReportTypeDetail = await dynamo.getCustomReportTypesById(idCustomReportType) as CustomReportTypeDetail;
        const eventPayload = {
            entity_id: customReportTypeDetail.id,
            entity_name: customReportTypeDetail.name,
            entity_attributes: {
                description: customReportTypeDetail.description,
                status: customReportTypeDetail.status === 0 ? 'inactive' : 'active',
                source: 'query_builder_v2',
            },
            event_name: 'delete-custom-query',
        };
        await hydra.generateEventOnEventBus(eventPayload);

        await dynamo.deleteCustomReportTypeById(idCustomReportType);

        res.status(200);
        res.type('application/json');
    } catch (err: any) {
        logger.errorWithStack(err.toString(), err);
        response.success = false;
        if (err instanceof NotFoundException) {
            res.status(404);
            response.error = 'CustomReportType not found!';
            response.errorCode = err.getCode();
        } else {
            res.status(500);
            response.error = err.message;
        }
    } finally {
        res.json(response);
    }
};
