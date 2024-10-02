import { NextFunction, Request, Response } from 'express';
import { ReportManagerSwitcher } from '../../models/report-switcher';
import { ReportExtractionDetails } from '../../services/dynamo';
import { ExportStatuses, ReportManagerExportDetailsResponse } from '../../models/report-manager';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { ReportExportComponent } from './report-export.component';
import { ExtractionComponent } from '../../models/extraction.component';
import { LastRefreshDate } from '../../reports/interfaces/extraction.interface';
import { DataLakeRefreshStatus } from '../../models/base';
import moment from 'moment';
import { NotFoundException, DisabledReportTypeException } from '../../exceptions/';

/**
 * @category Report
 * @summary Get export procedure status
 * @method GET
 * @get id_report [string, required] ID of the report
 * @get id_export [string, required] Query execution id
 * @url /analytics/v1/reports/{id_report}/exports/{id_export}
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response data [object, optional]
 *      @item idReport [string,required] ID of the report
 *      @item idExtraction [string,required]  Query execution id
 *      @item status [string,required]  String of the name status
 *      @item dateStart [string,required]
 *      @item dateEnd [string,required]
 *      @item type [string,required]
 *      @item idUser [int,required]
 *      @item hostname [string,required]
 *      @item downloadLabel [string,required]
 *      @item downloadUrl [string,required]
 * @end
 *
 * @status 404 Report not found!
 */
export const getReportExportsInfo = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const response = new ReportManagerExportDetailsResponse();
    try {
        const reportHandler = await ReportManagerSwitcher(session, req.params.id_report);
        const dynamo = session.getDynamo();
        const extraction = await dynamo.getReportExtraction(req.params.id_report, req.params.id_export);
        if (extraction) {
            const info = new ReportExtractionDetails(extraction);

            const extractionComponent = new ExtractionComponent();

            if (!session.platform.isDatalakeV3ToggleActive()) {
                let refreshInfo: LastRefreshDate;
                try {
                    refreshInfo = await extractionComponent.getDataLakeLastRefresh(session);
                } catch (e: any) {
                    // Nothing to do for now
                }

                refreshInfo = await extractionComponent.startDataLakeRefreshIfStatusIsError(session, refreshInfo);

                if (session.platform.isDatalakeV2Active() && info.idExtraction === info.queuedExtractionID) {
                    if (refreshInfo.refreshStatus !== DataLakeRefreshStatus.RefreshInProgress) {
                        // Check if the refresh is completed and in case start the query on Athena
                        const exportComponent = new ReportExportComponent(req, session, extraction.hostname, extraction.subfolder, false, extraction.type, false, extraction.enableFileCompression);
                        exportComponent.startQueuedReportExtraction(extraction).catch(error => { throw (error); });
                    } else if (moment().subtract(120, 'minutes').diff(moment(extraction.date_start)) > 0) {
                        extraction.status = ExportStatuses.FAILED;
                        extraction.error_details = `Set to FAILED as query not started after 120 minutes`;
                        await dynamo.createOrEditReportExtraction(extraction);
                        info.status = ExportStatuses.FAILED;
                    }
                }
            }

            if (extraction.scheduled !== true) { // new event driven extraction management sets this flag to TRUE
                let extractionTime = moment(extraction.date_start);
                if (extraction.date_start_from_queue) {
                    extractionTime = moment(extraction.date_start_from_queue);
                } else if (info.idExtraction === info.queuedExtractionID) {
                    extractionTime = moment();
                }
                if (extraction.processLastTime) {
                    const processLastTime = moment(extraction.processLastTime);

                    if (moment().subtract(3, 'minutes').diff(processLastTime) > 0 && info.status !== ExportStatuses.FAILED && info.status !== ExportStatuses.SUCCEEDED) {
                        extraction.status = ExportStatuses.FAILED;
                        extraction.error_details = `Set to FAILED as no update after 3 minutes`;
                        logger.error(`Set to failed the extraction of the report ${extraction.report_id} and extraction ${extraction.extraction_id} for the user ${extraction.id_user}. The old status was ${info.status} - extraction process not responsive`);
                        await dynamo.createOrEditReportExtraction(extraction);
                        info.status = ExportStatuses.FAILED;
                    }
                }

                if (moment().subtract(session.platform.getExtractionTimeLimit(), 'minutes').diff(extractionTime) > 0 && info.status !== ExportStatuses.FAILED && info.status !== ExportStatuses.SUCCEEDED) {
                    extraction.status = ExportStatuses.FAILED;
                    extraction.error_details = `Set to FAILED as no status update after ${session.platform.getExtractionTimeLimit()} minutes`;
                    logger.error(`Set to failed the extraction of the report ${extraction.report_id} and extraction ${extraction.extraction_id} for the user ${extraction.id_user}. The old status was ${info.status} - time limit exceeded`);
                    await dynamo.createOrEditReportExtraction(extraction);
                    info.status = ExportStatuses.FAILED;
                }
            } else {
                const checkTime = extraction.processLastTime ? moment(extraction.processLastTime) : moment(extraction.dateStart);
                const maxTimeAllowed = session.platform.getExtractionTimeLimit();
                if (info.status !== ExportStatuses.FAILED && info.status !== ExportStatuses.SUCCEEDED && moment().subtract(maxTimeAllowed, 'minutes').diff(checkTime) > 0) {
                    extraction.status = ExportStatuses.FAILED;
                    extraction.error_details = `Set to FAILED as no status update after ${maxTimeAllowed} minutes`;
                    logger.error(`Set to failed the extraction of the report ${extraction.report_id} and extraction ${extraction.extraction_id} for the user ${extraction.id_user}. The old status was ${info.status} - time limit exceeded`);
                    await dynamo.createOrEditReportExtraction(extraction);
                    info.status = ExportStatuses.FAILED;
                }
            }
            info.generateDownloadUrl(req.hostname, req.params.id_report, req.params.id_export);
            info.queuedExtractionID = undefined;
            response.data = info;
            res.type('application/json');
            res.status(200);
            res.json(response);
        } else {
            response.error = 'Report export not found';
            response.success = false;
            logger.error(response.error);
            res.type('application/json');
            res.status(500);
            res.json(response);
        }
    } catch (err: any) {
        logger.errorWithStack(`Error while getting report procedure status (idReport: ${req.params.id_report}, exportId: ${req.params.export}).`, err);
        res.type('application/json');
        res.status(500);
        response.success = false;
        response.error = 'Generic error. See the logs for more information';
        if (err instanceof NotFoundException || err instanceof DisabledReportTypeException) {
            res.status(404);
            response.error = 'Report not found!';
        }
        res.json(response);
    }
};
