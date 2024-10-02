import { ExtractionId } from '../value_objects/ExtractionId';
import { ScheduledReportDetails } from './ScheduledReport';
import { ExportStatuses } from '../../models/report-manager';
import { ReportManagerSwitcher } from '../../models/report-switcher';
import SessionManager from '../../services/session/session-manager.session';
import { InvalidSnowflakeQueryIdException } from '../exceptions/InvalidSnowflakeQueryIdException';
import { DBConnection } from '../../services/snowflake/interfaces/snowflake.interface';
import { Pool } from 'generic-pool';
import moment from 'moment';
import { InvalidExtractionStatusException } from '../exceptions/InvalidExtractionStatusException';
import { S3FileSystem } from '../domain_services/S3/S3FileSystem';

export interface ExtractionStatus {
    readonly dateStart: string;
    readonly enableFileCompression: boolean;
    readonly hostname: string;
    readonly subfolder: string | null;
    readonly id_user: number;
    readonly type: string;
    readonly title: string;
    readonly platform: string;
    readonly scheduled: boolean;
    readonly recipients: readonly string[];
    maxQueryAge: number;
    status: ExportStatuses;
    dateEnd: string | null;
    processLastTime: string | null;
    snowflakeRequestID: string | null;
    snowflakeRequestSelectedColumns: string[] | null;
    snowflakeRequestSort: string | null;
    snowflakeExportRequestID: string | null;
    snowflakeStorageIntegration: string | null;
    snowflakeStorageIntegrationBucket: string | null;
    timezone: string | null;
    spreedSheetName: string | null;
    compressedFileName: string | null;
    error_details: string | null;
}

export enum ExportType {
    CSV = 'csv',
    XLSX = 'xlsx'
}

export interface QueryStatus {
    readonly QueryId: string;
    readonly Status: string;
    readonly IsRunning: boolean;
    readonly IsError: boolean;
    RunningForTooLong: boolean; // true when the query has been stopped because it took way too much to run
}

enum Operation {
    PerformQuery,
    CheckQueryStatus,
    ExportCsv,
    CheckExportCsvStatus,
    CheckExportCsvContent,
    ConvertToXlsx,
    Compress
}

export class Extraction {

    public static createNew(
        newId: ExtractionId,
        details: ScheduledReportDetails,
    ): Extraction {
        return new Extraction(
            newId,
            {
                status: ExportStatuses.INITIALIZING,
                dateStart: moment.utc().format('YYYY-MM-DD HH:mm:ss'),
                dateEnd: null,
                maxQueryAge: 3600,
                hostname: details.planning.option.hostname,
                platform: details.platform,
                subfolder: details.planning.option.subfolder ?? '',
                id_user: details.author,
                title: details.title,
                scheduled: true,
                recipients: details.planning.option.recipients ?? [],
                type: ExportType.CSV,
                enableFileCompression: true,
                snowflakeRequestID: null,
                snowflakeRequestSelectedColumns: null,
                snowflakeRequestSort: null,
                snowflakeExportRequestID: null,
                snowflakeStorageIntegration: null,
                snowflakeStorageIntegrationBucket: null,
                processLastTime: null,
                timezone: null,
                spreedSheetName: null,
                compressedFileName: null,
                error_details: null,
            },
            global.snowflakePool,
        );
    }

    public constructor(
        private readonly id: ExtractionId,
        private status: ExtractionStatus,
        private readonly snowflakeConnectionsPool: Pool<DBConnection>,
    ) {
    }

    private async executePoolQuery(query: string): Promise<any> {
        return await this.snowflakeConnectionsPool.use((conn) => conn.runQuery(query, false, true, false));
    }

    private async checkStatusOfQuery(queryId: string): Promise<QueryStatus> {
        let queryStatus = '';
        let isStillRunning = true;
        let isError = false;

        await this.snowflakeConnectionsPool.use(
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

    private isQueryTakingTooLong(): boolean {
        const startDate = moment(this.status.dateStart);
        const limitDate = startDate.add(this.status.maxQueryAge, 'seconds');
        const currentDate = moment().utc();

        return limitDate.isBefore(currentDate);
    }

    private isCSVTakingTooLong(): boolean {
        const startDate = moment(this.status.dateStart);
        const limitDate = startDate.add(this.status.maxQueryAge, 'seconds');
        const currentDate = moment().utc();

        return limitDate.isBefore(currentDate);
    }

    private switchStatus(newStatus: ExportStatuses): void {
        this.status.status = newStatus;
        this.status.processLastTime = moment.utc().format('YYYY-MM-DD HH:mm:ss');
    }

    private verifyExtractionStatus(operation: Operation): void {
        const allowedStatuses = [];

        switch (operation) {
            case Operation.PerformQuery:
                allowedStatuses.push(ExportStatuses.INITIALIZING);
                allowedStatuses.push(ExportStatuses.RUNNING);
                break;

            case Operation.CheckQueryStatus:
                allowedStatuses.push(ExportStatuses.RUNNING);
                allowedStatuses.push(ExportStatuses.QUERY_CHECKED);
                break;

            case Operation.ExportCsv:
                allowedStatuses.push(ExportStatuses.QUERY_COMPLETED);
                break;

            case Operation.CheckExportCsvStatus:
                allowedStatuses.push(ExportStatuses.EXPORT_CSV_STARTED);
                allowedStatuses.push(ExportStatuses.EXPORT_CSV_CHECKED);
                break;

            case Operation.CheckExportCsvContent:
                allowedStatuses.push(ExportStatuses.EXPORT_CSV_COMPLETED);
                break;

            case Operation.ConvertToXlsx:
                allowedStatuses.push(ExportStatuses.EXPORT_CSV_CONTENT_CHECKED);
                break;

            case Operation.Compress:
                allowedStatuses.push(ExportStatuses.EXPORT_CONVERSION_SKIPPED);
                allowedStatuses.push(ExportStatuses.EXPORT_CONVERTED);
                allowedStatuses.push(ExportStatuses.EXPORT_CSV_CONTENT_CHECKED);
                break;
        }

        if (!allowedStatuses.includes(this.status.status)) {
            throw new InvalidExtractionStatusException(
                `The actual status "${this.status.status}" for extraction ${this.id} does not permit to perform operation ${operation}. 
                Permitted statuses are: ${JSON.stringify(allowedStatuses)}`
            );
        }
    }

    private calculateSelectedColumnsForCsv(fields: string[]): string[] {
        let select = [];

        for (const field of fields) {
            const stringField = field.substring(field.search(/AS ".*"$/m) + 4, field.lastIndexOf('"'));
            select.push(stringField);
        }

        return select;
    }

    private calculateSelectStmtForCsv(): string {
        let selectStatement = '';
        if (!this.status.snowflakeRequestSelectedColumns || this.status.snowflakeRequestSelectedColumns.length === 0) {
            return '*';
        }
        let first = true;
        for (const field of this.status.snowflakeRequestSelectedColumns) {
            if (first) {
                first = false;
            } else {
                selectStatement += ',';
            }
            selectStatement += '"' + field + '" AS "' + field.replaceAll('"', '""') + '"';
        }

        return selectStatement;
    }

    /**
     * Marks the extraction as completed, with an optional explanation of the error (if any).
     * @private
     */
    private finalize(success: boolean, errorDetails = ''): void {
        if (success) {
            this.status.dateEnd = moment.utc().format('YYYY-MM-DD HH:mm:ss');
        } else {
            this.status.error_details = errorDetails;
        }

        this.switchStatus(success ? ExportStatuses.SUCCEEDED : ExportStatuses.FAILED);
    }

    public get Id(): ExtractionId {
        return this.id;
    }

    public get Status(): Readonly<ExtractionStatus> {
        return this.status;
    }

    /**
     * Performs the extraction query against Snowflake in a fire-and-forget way and returns the relative query id
     *
     * @return The id of thr query returned by Snowflake
     * @throws InvalidExtractionStatusException
     * @throws InvalidSnowflakeQueryIdException
     */
    public async performQuery(session: SessionManager): Promise<string> {
        this.verifyExtractionStatus(Operation.PerformQuery);

        if ((this.status.snowflakeRequestID ?? '') !== '') {
            return this.status.snowflakeRequestID;
        }
        const reportHandler = await ReportManagerSwitcher(session, this.id.ReportId);
        const snowflake = session.getSnowflake();
        const limit = session.platform.getCsvExportLimit();

        const querySql = await reportHandler.getQuerySnowflake(limit, false, true, true);

        const queryId = await snowflake.runQuery(querySql, false, true, true, false);

        if (typeof queryId !== 'string') {
            throw new InvalidSnowflakeQueryIdException('Snowflake returned an invalid query id');
        }

        const newsStatus: ExtractionStatus = {
            ...this.status,
            snowflakeRequestID: queryId,
            snowflakeRequestSort: reportHandler.querySorting,
            snowflakeStorageIntegrationBucket: session.platform.getSnowflakeStorageIntegrationBucket(),
            snowflakeStorageIntegration: session.platform.getSnowflakeStorageIntegration(),
            timezone: session.user.getTimezone(),
            maxQueryAge: session.platform.getExtractionTimeLimit() * 60
        };

        newsStatus.snowflakeRequestSelectedColumns = reportHandler.querySelect !== undefined ?
            this.calculateSelectedColumnsForCsv(reportHandler.querySelect)
            : null;

        if (this.status.enableFileCompression) {
            newsStatus.compressedFileName = reportHandler.getExportReportName(0);
        }

        if (this.status.type === ExportType.XLSX) {
            newsStatus.spreedSheetName = reportHandler.getExportReportName(30);
        }

        this.status = newsStatus;
        this.switchStatus(ExportStatuses.RUNNING);

        return queryId;
    }

    /**
     * Checks the status of the query on Snowflake. Switches the status of the extraction to QueryCompleted if the query
     * has been completed.
     * Returns the status of the query, along with other details.
     *
     * @return The status of the query
     * @throws InvalidExtractionStatusException
     */
    public async checkSnowflakeQueryStatus(): Promise<Readonly<QueryStatus>> {
        this.verifyExtractionStatus(Operation.CheckQueryStatus);

        const queryStatus = await this.checkStatusOfQuery(this.status.snowflakeRequestID);

        if (queryStatus.IsRunning) {
            if (this.isQueryTakingTooLong()) {
                queryStatus.RunningForTooLong = true;
                this.finalizeWithError('Query did take too long');
            } else {
                this.switchStatus(ExportStatuses.QUERY_CHECKED);
            }
        } else {
            if (queryStatus.IsError) {
                this.finalizeWithError('Error in Query execution');
            } else {
                this.switchStatus(ExportStatuses.QUERY_COMPLETED);
            }
        }

        return queryStatus;
    }

    /**
     * Performs a fire-and-forget COPY TO S3 query against Snowflake and returns the relative query id
     *
     * @return The id of the query returned by Snowflake
     * @throws InvalidExtractionStatusException
     * @throws InvalidSnowflakeQueryIdException
     */
    public async exportToCsv(): Promise<string> {
        this.verifyExtractionStatus(Operation.ExportCsv);
        const selectStmt = this.calculateSelectStmtForCsv();
        const fullPathS3 = `s3://${this.status.snowflakeStorageIntegrationBucket}/snowflake-exports/${this.id.Id}.csv`;
        const tableScanQuery = `(SELECT ${selectStmt} FROM TABLE(RESULT_SCAN('${this.status.snowflakeRequestID}')) ${this.status.snowflakeRequestSort})`;

        const query = `COPY INTO ${fullPathS3} FROM ${tableScanQuery} 
            FILE_FORMAT=(NULL_IF=() TYPE='CSV' COMPRESSION='NONE' SKIP_HEADER=0 FIELD_DELIMITER=',' FIELD_OPTIONALLY_ENCLOSED_BY='"') 
            SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=5368709120 STORAGE_INTEGRATION=${this.status.snowflakeStorageIntegration}`;

        const exportQueryId = await this.executePoolQuery(query);

        if (typeof exportQueryId !== 'string') {
            throw new InvalidSnowflakeQueryIdException('Snowflake Export Query Id  is not valid');
        }

        this.status.snowflakeExportRequestID = exportQueryId;
        this.switchStatus(ExportStatuses.EXPORT_CSV_STARTED);

        return exportQueryId;
    }

    /**
     * Checks the status of the COPY-TO-S3 query on Snowflake. Switches the status of the extraction to ExportCsvCompleted
     * if the COPY query has been completed.
     * Returns the status of the COPY query, along with other details.
     *
     * @return The status of the COPY query
     * @throws InvalidExtractionStatusException
     */
    public async checkCsvExportQueryStatus(): Promise<QueryStatus> {
        this.verifyExtractionStatus(Operation.CheckExportCsvStatus);

        const queryStatus = await this.checkStatusOfQuery(this.status.snowflakeExportRequestID);

        if (queryStatus.IsRunning) {
            if (this.isCSVTakingTooLong()) {
                queryStatus.RunningForTooLong = true;
                this.finalizeWithError('Export CSV query did take too long');
            } else {
                this.switchStatus(ExportStatuses.EXPORT_CSV_CHECKED);
            }
        } else {
            if (queryStatus.IsError) {
                this.finalizeWithError('Error in Export CSV Query execution');
            } else {
                this.switchStatus(ExportStatuses.EXPORT_CSV_COMPLETED);
            }
        }

        return queryStatus;
    }

    /**
     * Ensures that the expected CSV file exists on S3 and that it's not empty, or creates a CSV file with at least
     * the headers.
     *
     * @throws InvalidExtractionStatusException
     */
    public async checkExportedCsvContent(fileSystem: S3FileSystem): Promise<void> {
        this.verifyExtractionStatus(Operation.CheckExportCsvContent);

        const bucketName = this.status.snowflakeStorageIntegrationBucket;
        const fileName = `${this.id.Id}.csv`;

        if (await fileSystem.fileIsEmpty(bucketName, fileName)) {
            let fields = [];
            for (const field of this.status.snowflakeRequestSelectedColumns) {
                fields.push(`"${field}"`);
            }
            await fileSystem.createFile(
                bucketName,
                fileName,
                fields.join(','),
                'application/csv',
            );
        }

        this.switchStatus(ExportStatuses.EXPORT_CSV_CONTENT_CHECKED);
    }

    /**
     * Performs a compression of the file generated by Snowflake on S3 and generates the relative zip file in the same directory
     * as the original CSV file. The original CSV file is eventually deleted from the bucket.
     *
     * @param fileSystem The instance of S3FileSystem to use
     */
    public async compress(fileSystem: S3FileSystem): Promise<void> {
        this.verifyExtractionStatus(Operation.Compress);

        const bucketName = this.status.snowflakeStorageIntegrationBucket;
        const sourceFileName = `${this.id.Id}.${this.status.type}`;
        const targetFileName = `${this.id.Id}.zip`;
        const contentFileName = `${this.status.compressedFileName}.${this.status.type}`;

        await fileSystem.compressFile(
          bucketName,
          '',
          sourceFileName,
          targetFileName,
          contentFileName,
        );

        await fileSystem.deleteFile(
          bucketName,
          sourceFileName,
        );

        this.finalizeWithSuccess();
    }

    /**
     * Marks the extraction as 'completed successfully'
     */
    public finalizeWithSuccess(): void {
        this.finalize(true);
    }

    public finalizeWithError(errorDetails: string): void {
        this.finalize(false, errorDetails);
    }
}
