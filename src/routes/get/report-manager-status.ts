import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { BaseResponse, DataLakeRefreshStatus, ErrorsCode, GeneralErrorResponse } from '../../models/base';
import { GetQueryExecutionOutput } from 'aws-sdk/clients/athena';
import { ExportStatuses } from '../../models/report-manager';
import { ReportExtractionInfo } from '../../services/dynamo';
import moment from 'moment';
import { LastRefreshDate } from '../../reports/interfaces/extraction.interface';
import { ExtractionComponent } from '../../models/extraction.component';
import { ReportExportComponent } from './report-export.component';
import { ReportManagerComponent } from '../../shared/components/report-manager.component';

/**
 * @category Manager
 * @summary Get report manager extraction status
 * @method GET
 * @get report_type_code [string, required] Report type code
 * @get query_execution_id [string, required] Query execution id
 * @url /analytics/v1/manager/report/{report_type_code}/{query_execution_id}
 *
 * @response success [boolean, required] Whether or not the operation was successful
 * @response error [string, optional] Error message
 * @response data [object, required]
 *      @item queryStatus [string,required] String of the name status
 * @end
 */
export const reportManagerStatus = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const reportTypeCode = req.params.report_type_code;
    const queryExecutionId = req.params.query_execution_id;
    const responseOptions: BaseResponse = {success: true};
    const dynamo = session.getDynamo();

    let extraction: ReportExtractionInfo;
    const reportManagerComponent = new ReportManagerComponent(session);

    try {
        extraction = await dynamo.getReportExtraction(reportTypeCode, queryExecutionId);
    } catch (error: any) {
        logger.errorWithStack('Error on recover report extraction', error);
        res.type('application/json');
        res.status(404);
        res.json(new GeneralErrorResponse('Extraction not found', ErrorsCode.ExtractionNotExist));
        return;
    }
    try {
        const extractionComponent = new ExtractionComponent();
        let queryExecutionStatus: string;

        if (session.platform.isDatalakeV3ToggleActive()) {
            const queryStatus = await session.getSnowflake().checkStatusOfQueryV3(extraction.snowflakeRequestID);
            if (queryStatus.IsRunning) {
                extraction.status = ExportStatuses.RUNNING
                extraction.processLastTime = moment(new Date()).format('YYYY-MM-DD HH:mm:ss');
            } else {
                extraction.status = queryStatus.IsError ? ExportStatuses.FAILED : ExportStatuses.SUCCEEDED;
                extraction.date_end = extraction.dateEnd = moment(new Date()).format('YYYY-MM-DD HH:mm:ss');
            }
            await dynamo.createOrEditReportExtraction(extraction);
            queryExecutionStatus = extraction.status;
        } else {
            let refreshInfo: LastRefreshDate;
            const athena = session.getAthena();

            try {
                refreshInfo = await extractionComponent.getDataLakeLastRefresh(session);
            } catch (e: any) {
                // nothing to do for now
            }

            refreshInfo = await extractionComponent.startDataLakeRefreshIfStatusIsError(session, refreshInfo);

            if (session.platform.isDatalakeV2Active() && extraction.extraction_id === extraction.queuedExtractionID && refreshInfo.refreshStatus !== DataLakeRefreshStatus.RefreshInProgress) {
                // Check if the refresh if completed and in case start the query on Athena
                const exportComponent = new ReportExportComponent(req, session, extraction.hostname, extraction.subfolder, false, extraction.type, false, extraction.enableFileCompression);
                const data = await athena.runCSVExport(extraction.query);
                extraction.query = undefined;
                extraction.queuedExtractionID = data.QueryExecutionId;
                await dynamo.createOrEditReportExtraction(extraction);
            } else if (session.platform.isDatalakeV2Active() && extraction.extraction_id === extraction.queuedExtractionID && refreshInfo.refreshStatus === DataLakeRefreshStatus.RefreshInProgress) {
                responseOptions.data = {queryStatus: ExportStatuses.RUNNING};
                res.status(200);
                res.json(responseOptions);
                return;
            }

            const status = await athena.checkQueryStatus(extraction.queuedExtractionID ?? queryExecutionId) as GetQueryExecutionOutput;
            queryExecutionStatus = status && status.QueryExecution && status.QueryExecution.Status ? status.QueryExecution.Status.State : '';

            if (queryExecutionStatus === ExportStatuses.FAILED) {
                extraction.status = ExportStatuses.FAILED;
                extraction.error_details = status.QueryExecution.Status.StateChangeReason;

                reportManagerComponent.dropTemporaryTable(extraction.managerSubordinatesTable);
                extraction.managerSubordinatesTable = undefined;

                await dynamo.createOrEditReportExtraction(extraction);
            } else if (queryExecutionStatus === ExportStatuses.SUCCEEDED) {

                reportManagerComponent.dropTemporaryTable(extraction.managerSubordinatesTable);
                extraction.managerSubordinatesTable = undefined;

                extraction.status = ExportStatuses.SUCCEEDED;
                const dateEnd = new Date();
                extraction.date_end = moment(dateEnd).format('YYYY-MM-DD HH:mm:ss');
                await dynamo.createOrEditReportExtraction(extraction);
            }
        }
        responseOptions.data = {queryStatus: queryExecutionStatus};
        res.status(200);
    } catch (e: any) {
        responseOptions.success = false;
        reportManagerComponent.dropTemporaryTable(extraction.managerSubordinatesTable);
        logger.errorWithStack(`Error while getting the query status (idReport: ${reportTypeCode}, exportId: ${queryExecutionId}).`, e);
        responseOptions.error = 'Generic error. See the logs for more information';

        res.status(500);
    }

    res.json(responseOptions);
};
