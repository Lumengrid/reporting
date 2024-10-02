import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { BaseResponse, DataLakeRefreshStatus, ErrorsCode, GeneralErrorResponse } from '../../models/base';
import { GetQueryExecutionOutput } from 'aws-sdk/clients/athena';
import { ExportStatuses } from '../../models/report-manager';
import { ReportExtractionDetails, ReportExtractionInfo } from '../../services/dynamo';
import moment from 'moment';
import { v4 } from 'uuid';
import { ReportExportComponent } from './report-export.component';
import { ExtractionComponent } from '../../models/extraction.component';
import { LastRefreshDate } from '../../reports/interfaces/extraction.interface';
import { ReportManagerComponent } from '../../shared/components/report-manager.component';
import { S3 } from '../../services/s3';

/**
 * @category Manager
 * @summary Get report manager extraction in XLSX format
 * @method GET
 * @notes Is not feasible to test this API with api-browser. Use postman or similar tools.
 * @get report_type_code [string, required] Report type code
 * @get query_execution_id [string, required] Query execution id
 * @url /analytics/v1/manager/report/{report_type_code}/{query_execution_id}/xls
 *
 */
export const reportManagerXls = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');

    const session: SessionManager = res.locals.session;
    const reportTypeCode = req.params.report_type_code;
    const queryExecutionId = req.params.query_execution_id;
    const responseOptions: BaseResponse = {success: true};

    const dynamo = session.getDynamo();

    let extraction: ReportExtractionInfo;
    try {
        extraction = await dynamo.getReportExtraction(reportTypeCode, queryExecutionId);
    } catch (error: any) {
        logger.errorWithStack('Error on recover report extraction', error);
        res.type('application/json');
        res.status(404);
        res.json(new GeneralErrorResponse('Extraction not found', ErrorsCode.ExtractionNotExist));
        return;
    }

    const info = new ReportExtractionDetails(extraction);
    const reportManagerComponent = new ReportManagerComponent(session);

    if (info.status === ExportStatuses.FAILED) {
        if (!session.platform.isDatalakeV3ToggleActive()) {
            reportManagerComponent.dropTemporaryTable(extraction.managerSubordinatesTable);
        }
        res.type('application/json');
        res.status(400);
        res.json(new GeneralErrorResponse('Extraction download failed', ErrorsCode.ExtractionFailed));

        return;
    }

    if (session.platform.isDatalakeV3ToggleActive()) {
        if (info.getDaysElapsed() > 30) {
            res.type('application/json');
            res.status(404);
            res.json(new GeneralErrorResponse('Extraction download not found', ErrorsCode.ExtractionNotExist));
            return;
        }

        if (session.platform.istoggleManagerReportXLSXPolling() && extraction.status === ExportStatuses.CONVERTING) {
            res.status(400);
            res.json(new GeneralErrorResponse('XLSX Conversion in progress', ErrorsCode.XLSXConversionInProgress));
            return;
        }

        if (info.status !== ExportStatuses.SUCCEEDED) {
            res.type('application/json');
            res.status(400);
            res.json(new GeneralErrorResponse('Extraction download not ready', ErrorsCode.ExtractionNotComplete));

            return;
        } else {
            const s3: S3 = session.getS3();
            if (!await s3.checkIfCsvFileExists(extraction.extraction_id, true)) {
                const snowflake = session.getSnowflake();
                await snowflake.saveCSVFromQueryID(extraction.snowflakeRequestID, extraction.snowflakeRequestSort, extraction.snowflakeRequestSelectedColumns, extraction.extraction_id);
            }
        }
    } else {
        let refreshInfo: LastRefreshDate;
        const extractionComponent = new ExtractionComponent();
        const athena = session.getAthena();

        try {
            refreshInfo = await extractionComponent.getDataLakeLastRefresh(session);
        } catch (e: any) {
            // nothing to do for now
        }

        refreshInfo = await extractionComponent.startDataLakeRefreshIfStatusIsError(session, refreshInfo);

        if (session.platform.isDatalakeV2Active() && extraction.extraction_id === extraction.queuedExtractionID) {
            // Check if the refresh if completed and in case start the query on Athena
            if (refreshInfo.refreshStatus !== DataLakeRefreshStatus.RefreshInProgress) {
                const athena = session.getAthena();
                const exportComponent = new ReportExportComponent(req, session, extraction.hostname, extraction.subfolder, false, extraction.type, false, extraction.enableFileCompression);
                const data = await athena.runCSVExport(extraction.query);
                extraction.query = undefined;
                extraction.queuedExtractionID = data.QueryExecutionId;
                await dynamo.createOrEditReportExtraction(extraction);
            }

            res.type('application/json');
            res.status(400);
            res.json(new GeneralErrorResponse('Extraction download not ready', ErrorsCode.ExtractionNotComplete));

            return;
        }

        // If the extraction status wasn't in succeeded status, we check again the query execution status
        if (info.status !== ExportStatuses.SUCCEEDED && info.status !== ExportStatuses.CONVERTING) {
            const athena = session.getAthena();
            const status = await athena.checkQueryStatus(extraction.queuedExtractionID ?? queryExecutionId) as GetQueryExecutionOutput;
            const queryExecutionStatus = status && status.QueryExecution && status.QueryExecution.Status ? status.QueryExecution.Status.State : '';
            if (queryExecutionStatus !== ExportStatuses.SUCCEEDED) {
                res.type('application/json');
                res.status(400);
                res.json(new GeneralErrorResponse('Extraction download not ready', ErrorsCode.ExtractionNotComplete));

                return;
            } else {
                info.status = extraction.status = ExportStatuses.SUCCEEDED;
                const dateEnd = new Date();
                info.dateEnd = extraction.date_end = moment(dateEnd).format('YYYY-MM-DD HH:mm:ss');

                reportManagerComponent.dropTemporaryTable(extraction.managerSubordinatesTable);
                extraction.managerSubordinatesTable = undefined;

                await dynamo.createOrEditReportExtraction(extraction);
            }
        }

        if (info.getDaysElapsed() > 30) {
            res.type('application/json');
            res.status(404);
            res.json(new GeneralErrorResponse('Extraction download not found', ErrorsCode.ExtractionNotExist));
            return;
        }

        if (session.platform.istoggleManagerReportXLSXPolling() && extraction.status === ExportStatuses.CONVERTING) {
            res.status(400);
            res.json(new GeneralErrorResponse('XLSX Conversion in progress', ErrorsCode.XLSXConversionInProgress));
            return;
        }
    }

    try {
        const s3 = session.getS3();

        // convert the file only if the file doesn't exists yet
        // (the file will be created only at first call)
        if (!await s3.checkIfXlsxFileExists(extraction.queuedExtractionID ?? queryExecutionId.toString(), session.platform.isDatalakeV3ToggleActive())) {
            if (session.platform.istoggleManagerReportXLSXPolling()) {
                const finalResult = new  Promise(async (resolve) => {
                    // Set the export status to Converting
                    info.status = extraction.status = ExportStatuses.CONVERTING;
                    await dynamo.createOrEditReportExtraction(extraction);

                    // Convert to xlsx the CSV file
                    await s3.convertCsvToXlsx(extraction.queuedExtractionID ?? queryExecutionId.toString(), extraction.queuedExtractionID ?? queryExecutionId.toString(), session.platform.isDatalakeV3ToggleActive());

                    // Set the status to Succeeded and close the operation
                    info.status = extraction.status = ExportStatuses.SUCCEEDED;
                    await dynamo.createOrEditReportExtraction(extraction);
                    resolve(true);
                });

                res.status(400);
                res.json(new GeneralErrorResponse('XLSX Conversion in progress', ErrorsCode.XLSXConversionInProgress));
                return;
            } else {
                await s3.convertCsvToXlsx(extraction.queuedExtractionID ?? queryExecutionId.toString(), extraction.queuedExtractionID ?? queryExecutionId.toString(), session.platform.isDatalakeV3ToggleActive());
            }
        }
        const stream = await info.getS3DownloadStream(s3, 'xlsx', session.platform.isDatalakeV3ToggleActive());
        const extractionDate = new Date(info.dateEnd.toString());
        const extractionDateToFormat = moment(extractionDate).format('YYYYMMDD');
        const randomId = v4();
        const fileName = `${extractionDateToFormat}_${randomId}.xlsx`;

        res.setHeader(
            'Content-disposition',
            `attachment; filename=${Buffer.from(fileName).toString('base64')}`
        );
        res.setHeader('Content-type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Access-Control-Expose-Headers', 'Content-disposition');

        stream.pipe(res, {end: true});
    } catch (e: any) {
        responseOptions.success = false;
        logger.errorWithStack(`Error while getting xlsx of the report manager (idReport: ${reportTypeCode}, exportId: ${queryExecutionId}).`, e);
        responseOptions.error = 'Generic error. See the logs for more information';
        res.json(responseOptions);

        res.status(500);
    }
};
