import { Request } from 'express';

import {
    ReportManagerExportResponse,
    ExportStatuses,
    ReportManagerExportResponseData
} from '../../models/report-manager';
import { ReportManagerSwitcher } from '../../models/report-switcher';
import SessionManager from '../../services/session/session-manager.session';
import { DataLakeRefreshStatus } from '../../models/base';
import { ReportExtractionInfo } from '../../services/dynamo';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import AWS from 'aws-sdk';
import { ExtractionComponent } from '../../models/extraction.component';
import { LastRefreshDate } from '../../reports/interfaces/extraction.interface';
import { v4 } from 'uuid';
import { BaseReportManager } from '../../models/base-report-manager';
import { SidekiqScheduler } from '../../models/sidekiq-scheduler';
import moment from 'moment';
import { Utils } from '../../reports/utils';
import { UnplannedScheduledReportException } from '../../exceptions';
import { QueryStatus } from '../../domain/entities/Extraction';

export class ReportExportComponent {
    private logger: SessionLoggerService;

    req: Request;
    session: SessionManager;
    fromSchedule: boolean;
    type: string;
    hostname: string;
    subfolder: string;
    generateBackgroundJob: boolean;
    enableFileCompression: boolean;

    constructor(req: Request, session: SessionManager, hostname: string, subfolder: string, fromSchedule = false, type = 'csv', generateBackgroundJob = true, enableFileCompression = true) {
        this.req = req;
        this.session = session;
        this.fromSchedule = fromSchedule;
        this.type = type;
        this.hostname = hostname;
        this.subfolder = subfolder;
        this.generateBackgroundJob = generateBackgroundJob;
        this.enableFileCompression = enableFileCompression;
        this.logger = httpContext.get('logger');
    }

    public async exportReport(): Promise<ReportManagerExportResponse> {
        const response = new ReportManagerExportResponse();

        const reportHandler = await ReportManagerSwitcher(this.session, this.req.params.id_report);

        if (this.fromSchedule) {
            if (reportHandler.info.planning.option?.hostname) {
                this.hostname = reportHandler.info.planning.option?.hostname;
            }

            if (reportHandler.info.planning.option?.subfolder) {
                this.subfolder = reportHandler.info.planning.option?.subfolder;
            }
        }

        if (reportHandler.info.deleted) {
            if (this.fromSchedule && this.session.platform.isDatalakeV2Active()) {
                    const sidekiqScheduler: SidekiqScheduler = new SidekiqScheduler(this.session.platform.getPlatformBaseUrl());
                    await sidekiqScheduler.removeScheduling(reportHandler.info.idReport);
                    // Closing redis connection after working with schedulations
                    this.logger.debug(`Removed scheduled task for deleted report id: ${reportHandler.info.idReport}, platform: ${this.hostname}`);
            }
            throw new Error('Report not found!');
        }

        if (this.fromSchedule && !reportHandler.info.planning.active) {
            const sidekiqScheduler: SidekiqScheduler = new SidekiqScheduler(this.session.platform.getPlatformBaseUrl());
            await sidekiqScheduler.removeScheduling(reportHandler.info.idReport);
            // Closing redis connection after working with schedulations
            this.logger.debug(`Removed scheduled task for unplanned report id: ${reportHandler.info.idReport}, platform: ${this.hostname}`);

            throw new UnplannedScheduledReportException();
        }

        this.logger.debug(`Run export for report id: ${this.req.params.id_report}, extension of extraction: ${this.type}, platform: ${this.hostname}`);

        const athena = this.session.getAthena();
        const dynamo = this.session.getDynamo();
        const hydra = this.session.getHydra();

        let refreshInfo: LastRefreshDate;
        const extractionComponent = new ExtractionComponent();

        try {
            refreshInfo = await extractionComponent.getDataLakeLastRefresh(this.session);
        } catch (e: any) {
            // nothing to do here for now
        }

        let refreshError = false;

        if (this.session.platform.isDatalakeV2Active() && (refreshInfo.refreshStatus === DataLakeRefreshStatus.RefreshInProgress || refreshInfo.isRefreshNeeded)) {
            response.data = new ReportManagerExportResponseData(v4());
            // In this case we will save the export to run it again when the refresh will end and prepare the background job if required
            if (refreshInfo.refreshStatus !== DataLakeRefreshStatus.RefreshInProgress && refreshInfo.isRefreshNeeded) {
                const sqs = this.session.getSQS();

                try {
                    await sqs.runDataLakeV2Refresh(this.session, refreshInfo);
                }
                catch (error: any) {
                    refreshError = true;
                }
            }

            if (!refreshError) {
                const date = new Date();
                const extraction = new ReportExtractionInfo(this.req.params.id_report, '' + response.data.executionId, ExportStatuses.RUNNING, reportHandler.convertDateObjectToDatetime(date), this.type, this.session.user.getIdUser(), this.enableFileCompression);

                extraction.hostname = this.hostname;
                extraction.subfolder = this.subfolder;
                extraction.queuedExtractionID = response.data.executionId;

                await dynamo.createOrEditReportExtraction(extraction);

                if (this.generateBackgroundJob) {
                    let notifyTo: string | string[] = '';
                    if (this.fromSchedule) {
                        if (reportHandler.info.planning.option) {
                            notifyTo = reportHandler.info.planning.option.recipients;
                        }
                    } else {
                        notifyTo = this.session.user.getEMail();
                    }

                    await hydra.createExtractionBackgroundJob(this.req.params.id_report, '' + response.data.executionId, this.session.user.getIdUser(), notifyTo, reportHandler.info.title as string, extraction.hostname, extraction.subfolder);
                }

                return response;
            }
        }

        let limit: number;
        if (this.type === 'xlsx') {
            limit = this.session.platform.getXlxExportLimit();
        } else {
            limit = this.session.platform.getCsvExportLimit();
        }

        const query = await reportHandler.getQuery(limit, false, true);

        const data = await athena.runCSVExport(query);

        const startTime = moment();

        const date = new Date();
        const extraction = new ReportExtractionInfo(this.req.params.id_report, '' + data.QueryExecutionId, ExportStatuses.RUNNING, reportHandler.convertDateObjectToDatetime(date), this.type, this.session.user.getIdUser(), this.enableFileCompression);

        extraction.hostname = this.hostname;
        extraction.subfolder = this.subfolder;

        await dynamo.createOrEditReportExtraction(extraction);

        if (this.generateBackgroundJob) {
            let notifyTo: string | string[] = '';
            if (this.fromSchedule) {
                if (reportHandler.info.planning.option) {
                    notifyTo = reportHandler.info.planning.option.recipients;
                }
            } else {
                notifyTo = this.session.user.getEMail();
            }

            await hydra.createExtractionBackgroundJob(this.req.params.id_report, '' + data.QueryExecutionId, this.session.user.getIdUser(), notifyTo, reportHandler.info.title as string, extraction.hostname, extraction.subfolder);
        }

        const finalRes = this.reportActionsAfterQueryExecution(extraction, reportHandler, query, startTime);

        response.data = new ReportManagerExportResponseData(data.QueryExecutionId);
        if (refreshError) {
            response.data.refreshError = true;
        }

        return response;
    }

    private async checkStatusOfQueryV3(queryId: string): Promise<QueryStatus> {
        let queryStatus = '';
        let isStillRunning = true;
        let isError = false;

        await global.snowflakePool.use(
            async (conn) => {
                queryStatus = await conn.getQueryStatus(queryId);
                isStillRunning = await conn.isStillRunning(queryStatus);
                isError = await conn.isErrorStatus(queryStatus);
            }
        );

        return {
            QueryId: queryId,
            Status: queryStatus,
            IsRunning: isStillRunning,
            IsError: isError,
            RunningForTooLong: false,
        };
    }

    public async exportReportV3(): Promise<ReportManagerExportResponse> {
        const response = new ReportManagerExportResponse();
        response.data = new ReportManagerExportResponseData(v4());

        const reportHandler = await ReportManagerSwitcher(this.session, this.req.params.id_report);

        if (this.fromSchedule) {
            if (reportHandler.info.planning.option?.hostname) {
                this.hostname = reportHandler.info.planning.option?.hostname;
            }

            if (reportHandler.info.planning.option?.subfolder) {
                this.subfolder = reportHandler.info.planning.option?.subfolder;
            }
        }

        if (reportHandler.info.deleted) {
            if (this.fromSchedule) {
                    const sidekiqScheduler: SidekiqScheduler = new SidekiqScheduler(this.session.platform.getPlatformBaseUrl());
                    await sidekiqScheduler.removeScheduling(reportHandler.info.idReport);
                    // Closing redis connection after working with schedulations
                    this.logger.debug(`Removed scheduled task for deleted report id: ${reportHandler.info.idReport}, platform: ${this.hostname}`);
            }
            throw new Error('Report not found!');
        }

        if (this.fromSchedule && !reportHandler.info.planning.active) {
            const sidekiqScheduler: SidekiqScheduler = new SidekiqScheduler(this.session.platform.getPlatformBaseUrl());
            await sidekiqScheduler.removeScheduling(reportHandler.info.idReport);
            // Closing redis connection after working with schedulations
            this.logger.debug(`Removed scheduled task for unplanned report id: ${reportHandler.info.idReport}, platform: ${this.hostname}`);

            throw new UnplannedScheduledReportException();
        }

        this.logger.debug(`Check connection and query on snowflake for report id: ${this.req.params.id_report}, extension of extraction: ${this.type}, platform: ${this.hostname}`);
        const snowflake = this.session.getSnowflake();
        const dynamo = this.session.getDynamo();
        const hydra = this.session.getHydra();
        this.logger.debug(`Run export for report id: ${this.req.params.id_report}, extension of extraction: ${this.type}, platform: ${this.hostname}`);
        let limit: number;
        if (this.type === 'xlsx') {
            limit = this.session.platform.getXlxExportLimit();
        } else {
            limit = this.session.platform.getCsvExportLimit();
        }

        const query = await reportHandler.getQuerySnowflake(limit, false, true, true);

        const date = new Date();
        const extraction = new ReportExtractionInfo(this.req.params.id_report, '' + response.data.executionId, ExportStatuses.RUNNING, reportHandler.convertDateObjectToDatetime(date), this.type, this.session.user.getIdUser(), this.enableFileCompression);

        extraction.hostname = this.hostname;
        extraction.subfolder = this.subfolder;
        extraction.snowflakeRequestSort = reportHandler.querySorting;
        if (reportHandler.querySelect !== undefined) {
            extraction.convertSnowflakeRequestSelectedColumns(reportHandler.querySelect);
        }
        await dynamo.createOrEditReportExtraction(extraction);

        if (this.generateBackgroundJob) {
            let notifyTo: string | string[] = '';
            if (this.fromSchedule) {
                if (reportHandler.info.planning.option) {
                    notifyTo = reportHandler.info.planning.option.recipients;
                }
            } else {
                notifyTo = this.session.user.getEMail();
            }

            await hydra.createExtractionBackgroundJob(this.req.params.id_report, '' + response.data.executionId, this.session.user.getIdUser(), notifyTo, reportHandler.info.title as string, extraction.hostname, extraction.subfolder);
        }

        extraction.hostname = this.hostname;
        extraction.subfolder = this.subfolder;
        extraction.processLastTime = reportHandler.convertDateObjectToDatetime(new Date());

        await dynamo.createOrEditReportExtraction(extraction);

        let timeout: any;

        // Snowflake run query and save results on CSV file on S3
        const extractionFn = new Promise(async (resolve): Promise<any> => {
            const deadExtraction = setInterval(async () => {
                await dynamo.updateExtractionProcessLastTime(extraction.extraction_id, extraction.report_id);
            }, 60000);

            try {
                extraction.snowflakeRequestID = await snowflake.runQuery(query, false, true, false, true, true, true);
                await snowflake.getLastRefreshDetails()
                extraction.processLastTime = reportHandler.convertDateObjectToDatetime(new Date());
                await dynamo.createOrEditReportExtraction(extraction);
                let queryStatus: QueryStatus;
                const now = new Date().getTime()
                while (true) {
                    queryStatus = await this.checkStatusOfQueryV3(extraction.snowflakeRequestID);
                    if (!queryStatus.IsRunning || queryStatus.IsError || (new Date().getTime() - now) >= (3600 * 1000)) {
                        break;
                    }
                    await Utils.sleep(5000);
                }
                await snowflake.saveCSVFromQueryID(extraction.snowflakeRequestID, extraction.snowflakeRequestSort, extraction.snowflakeRequestSelectedColumns, extraction.extraction_id);
                const s3 = this.session.getS3();

                if (this.type === 'xlsx') {
                    extraction.status = ExportStatuses.CONVERTING;
                    extraction.processLastTime = reportHandler.convertDateObjectToDatetime(new Date());
                    await dynamo.createOrEditReportExtraction(extraction);
                    await s3.convertCsvToXlsx(extraction.extraction_id, reportHandler.getExportReportName(30), true);
                }

                if (this.enableFileCompression) {
                    extraction.status = ExportStatuses.COMPRESSING;
                    extraction.processLastTime = reportHandler.convertDateObjectToDatetime(new Date());
                    await dynamo.createOrEditReportExtraction(extraction);
                    await s3.compressFile(extraction.extraction_id, this.type, reportHandler.getExportReportName(0), true);
                }

                extraction.status = ExportStatuses.SUCCEEDED;
                clearTimeout(timeout);
            } catch (exception: any) {
                this.logger.errorWithStack(`Error on export for the report id: ${extraction.report_id} and platform: ${extraction.hostname}.\n\n${query}`, exception);
                extraction.query = query;
                extraction.status = ExportStatuses.FAILED;
                extraction.error_details = exception.toString();
                clearTimeout(timeout);
            }
            extraction.processLastTime = extraction.date_end = reportHandler.convertDateObjectToDatetime(new Date());
            clearInterval(deadExtraction);
            await dynamo.createOrEditReportExtraction(extraction);
            resolve(extraction);
        });

        const timeoutFn = new Promise(async (resolve) => {
            timeout = setTimeout(() => {
                extraction.query = query;
                extraction.status = ExportStatuses.FAILED;
                extraction.date_end = reportHandler.convertDateObjectToDatetime(new Date());
                extraction.processLastTime = reportHandler.convertDateObjectToDatetime(new Date());
                extraction.error_details = `Set to FAILED as no status update after ${this.session.platform.getExtractionTimeLimit()} minutes`;
                this.logger.error(`Set to failed the extraction of the report ${extraction.report_id} and extraction ${extraction.extraction_id} for the user ${extraction.id_user} - time limit exceeded`);
                resolve(extraction);
            }, this.session.platform.getExtractionTimeLimit() * 60000);
        });
        const finalRes = Promise.race([extractionFn, timeoutFn]);
        await dynamo.createOrEditReportExtraction(extraction);

        return response;
    }

    public async startQueuedReportExtraction(extraction: ReportExtractionInfo) {
        return new Promise(async (reject) => {
            const athena = this.session.getAthena();
            const dynamo = this.session.getDynamo();

            let limit: number;
            if (this.type === 'xlsx') {
                limit = this.session.platform.getXlxExportLimit();
            } else {
                limit = this.session.platform.getCsvExportLimit();
            }

            const reportHandler = await ReportManagerSwitcher(this.session, extraction.report_id);

            const query = await reportHandler.getQuery(limit, false, true);

            const startTime = moment();
            athena.runCSVExport(query).then(async(data) => {
                extraction.queuedExtractionID = data.QueryExecutionId;
                const date = new Date();
                extraction.date_start_from_queue = reportHandler.convertDateObjectToDatetime(date);
                await dynamo.createOrEditReportExtraction(extraction);
                const finalRes = this.reportActionsAfterQueryExecution(extraction, reportHandler, query, startTime);
            }).catch(error => {
                if (error.hasOwnProperty('code')) {
                    if (error.code !== 'ThrottlingException' && error.code !== 'Throttling' && error.code !== 'TooManyRequestsException') {
                        reject(error);
                    }
                } else {
                    reject(error);
                }
            });
        });
    }

    public async reportActionsAfterQueryExecution(extraction: ReportExtractionInfo, reportHandler: BaseReportManager, query: string, startTime: moment.Moment) {
        const athena = this.session.getAthena();
        const dynamo = this.session.getDynamo();

        const queryID = extraction.queuedExtractionID ?? extraction.extraction_id;

        try {
            let loop = true;
            while (loop) {
                if (moment().subtract(this.session.platform.getExtractionTimeLimit(), 'minutes').diff(startTime) > 0) {
                    extraction.status = ExportStatuses.FAILED;
                    extraction.error_details = `Set to FAILED as no status update after ${this.session.platform.getExtractionTimeLimit()} minutes`;
                    this.logger.error(`Set to failed the extraction of the report ${extraction.report_id} and extraction ${extraction.extraction_id} for the user ${extraction.id_user} - time limit exceeded`);
                    await dynamo.createOrEditReportExtraction(extraction);
                    return;
                }
                let status: AWS.Athena.GetQueryExecutionOutput;
                try {
                    status = await athena.checkQueryStatus(queryID);
                } catch (error: any) {
                    if (error.hasOwnProperty('code')) {
                        if (error.code === 'ThrottlingException' || error.code === 'Throttling' || error.code === 'TooManyRequestsException') {
                            await Utils.sleep(3000);
                            continue;
                        } else {
                            throw(error);
                        }
                    } else {
                        throw(error);
                    }
                }
                if (status && status.QueryExecution && status.QueryExecution.Status) {
                    if (status.QueryExecution.Status.State === ExportStatuses.QUEUED || status.QueryExecution.Status.State === ExportStatuses.RUNNING) {
                        await Utils.sleep(3000);
                        continue;
                    } else if (status.QueryExecution.Status.State === ExportStatuses.FAILED) {
                        throw new Error(status.QueryExecution.Status.StateChangeReason);
                    } else {
                        loop = false;
                    }
                }
            }

            const tableDeletion = new Promise(async (resolve, reject) => {
                await reportHandler.dropTemporaryTables();
            });

            const s3 = this.session.getS3();

            if (this.type === 'xlsx') {
                extraction.status = ExportStatuses.CONVERTING;
                await dynamo.createOrEditReportExtraction(extraction);
                await s3.convertCsvToXlsx(queryID, reportHandler.getExportReportName(30));
            }

            if (this.enableFileCompression) {
                extraction.status = ExportStatuses.COMPRESSING;
                await dynamo.createOrEditReportExtraction(extraction);
                await s3.compressFile(queryID, this.type, reportHandler.getExportReportName(0));
            }

            extraction.status = ExportStatuses.SUCCEEDED;
            const dateEnd = new Date();
            extraction.date_end = reportHandler.convertDateObjectToDatetime(dateEnd);
            await dynamo.createOrEditReportExtraction(extraction);
            this.logger.debug(`Export for the report id: ${extraction.report_id} and platform: ${extraction.hostname} succeeded`);

            return;
        } catch (error: any) {
            this.logger.errorWithStack(`Error on export for the report id: ${extraction.report_id} and platform: ${extraction.hostname}.\n\n${query}`, error);
            extraction.status = ExportStatuses.FAILED;
            extraction.error_details = error.message;
            extraction.query = query;
            await dynamo.createOrEditReportExtraction(extraction);
            return;
        }
    }
}
