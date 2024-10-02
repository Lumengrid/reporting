import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';

import { DisabledReportTypeException, ErrorCode, NotFoundException, UnauthorizedException } from '../../exceptions';
import { ErrorsCode, GeneralErrorResponse } from '../../models/base';
import { ExportStatuses, ReportManagerInfo } from '../../models/report-manager';
import { ReportManagerSwitcher } from '../../models/report-switcher';
import { ReportExtractionDetails, ReportExtractionInfo } from '../../services/dynamo';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { getSession, getRequestInfos } from '../permissions';

/**
 * @category Report
 * @summary Get exported report file stream
 * @notes Is not feasible to test this API with api-browser. Use postman or similar tools.
 * @method GET
 * @get id_report [string, required] ID of the report
 * @get id_export [string, required] Query execution id
 * @url /analytics/v1/reports/{id_report}/exports/{id_export}/download
 *
 * @status 404 Report not found!
 */
export const getReportExportDownload = async (req: Request, res: Response, next: NextFunction) => {
    const session: SessionManager = res.locals.session;
    const dynamo = session.getDynamo();
    const hydra = session.getHydra();
    const logger: SessionLoggerService = httpContext.get('logger');

    let extraction: ReportExtractionInfo;

    // check if the file needs to be retrieved with access token
    let reportDetail;
    try {
        reportDetail = await dynamo.getReport(req.params.id_report) as ReportManagerInfo;
    } catch (e: any) {
        logger.errorWithStack(`Error on get report ${req.params.id_report} from dynamo - ${req.hostname}`, e);

        let status: number;
        let errorMessage: GeneralErrorResponse;

        if (e instanceof NotFoundException || e instanceof DisabledReportTypeException) {
            status = 404;
            errorMessage = new GeneralErrorResponse('Report not found!', ErrorsCode.ReportNotExist);
        } else if (e.code === ErrorCode.REPORT_NOT_FOUND) {
            status = 404;
            errorMessage = new GeneralErrorResponse('Report not exists', ErrorCode.REPORT_NOT_FOUND);
        } else {
            status = 500;
            errorMessage = new GeneralErrorResponse('Unexpected Error', ErrorsCode.UnexpectedError);
        }

        res.type('application/json');
        res.status(status);
        res.json({...errorMessage});

        return;
    }

    // check if the report has been deleted
    if (reportDetail.deleted) {
        res.type('application/json');
        res.status(404);
        logger.error(`Error on get report ${req.params.id_report}. Report has been deleted`);
        res.json(new GeneralErrorResponse('Report not found!', ErrorsCode.ReportNotExist));
        return;
    }

    // check if the report needs an authenticated user - safety first
    if (reportDetail.loginRequired !== false) {
        logger.debug(`Authentication required for the report ${req.params.id_report}`);
        try {
            const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
            const reqInfos = getRequestInfos(req);
            await getSession(reqInfos.token, req.hostname, subfolder, req.app.locals.cache, reqInfos.cookie, reqInfos.xCSRFToken);
        } catch (e: any) {
            if (e instanceof NotFoundException) {
                logger.errorWithStack(`Hydra session request not found.`, e);
                res.sendStatus(404);
            } else if (e instanceof UnauthorizedException) {
                logger.errorWithStack(`User not authorized to download the report ${req.params.id_report}.`, e);
                res.sendStatus(401);
            } else {
                logger.errorWithStack(`Error during the authentication process - ${req.hostname}.`, e);
                res.sendStatus(500);
            }

            return;
        }
    }


    // Check if the extraction was present
    try {
        extraction = await dynamo.getReportExtraction(req.params.id_report, req.params.id_export);
    } catch (error: any) {
        logger.errorWithStack('Error on recover report extraction', error);
        res.type('application/json');
        res.status(400);
        res.json(new GeneralErrorResponse('Extraction not found', ErrorsCode.ExtractionNotExist));
        return;
    }

    const info = new ReportExtractionDetails(extraction);

    // Return an error if the extraction wasn't completed
    if (info.status !== ExportStatuses.SUCCEEDED) {
        res.type('application/json');
        res.status(400);
        res.json(new GeneralErrorResponse('Extraction download not ready', ErrorsCode.ExtractionNotComplete));
        return;
    }

    // Return an error also if the extraction was older then 30 days (also the time was checked)
    const extractionDate = new Date('' + info.dateEnd);
    const currentDate = new Date();
    const daysElapsed = (currentDate.getTime() - extractionDate.getTime()) / (1000 * 60 * 60 * 24);
    if (daysElapsed > 30) {
        res.type('application/json');
        res.status(404);
        res.json(new GeneralErrorResponse('Extraction expired', ErrorsCode.ExtractionExpired));
        return;
    }
    let extension = 'zip';
    let contentType = 'application/zip';
    // check if exists the property enableFileCompression for the retro compatibility
    if (extraction.hasOwnProperty('enableFileCompression') && !extraction.enableFileCompression) {
        extension = 'csv';
        contentType = 'text/csv';
    }
    // All fine create a read stream to the S3 file and send it to the user
    try {
        const reportHandler = await ReportManagerSwitcher(session, req.params.id_report, true);
        const stream = await info.getS3DownloadStream(session.getS3(), extension, typeof extraction.snowflakeRequestID !== 'undefined');

        const fileName = `${reportHandler.convertDateObjectToExportDate(extractionDate)}_${reportHandler.getExportReportName()}.${extension}`;
        res.setHeader(
            'Content-disposition',
            `attachment; filename=${Buffer.from(fileName).toString('base64')}`
        );
        res.setHeader('Content-type', `${contentType}`);
        res.setHeader('Access-Control-Expose-Headers', 'Content-disposition');

        let eventName = 'custom-report-logged-user-downloaded';
        if (session.user.getIdUser() === 0) {
            eventName = 'custom-report-not-logged-user-downloaded';
        }
        const eventPayload = {
            entity_id: reportDetail.idReport,
            entity_name: reportDetail.title,
            entity_attributes: {
                type: reportDetail.type,
                description: reportDetail.description,
            },
            event_name: eventName,
        };
        await hydra.generateEventOnEventBus(eventPayload);

        stream.pipe(res, {end: true});
    } catch (error: any) {
        logger.errorWithStack('Error on report download', error);
        res.type('application/json');
        res.status(404);
        if (error instanceof NotFoundException || error instanceof DisabledReportTypeException) {
            res.json(new GeneralErrorResponse('Report not found!', error.getCode()));
        } else {
            res.json(new GeneralErrorResponse('Extraction file not found', ErrorsCode.ExtractionNotExist));
        }
        return;
    }
};
