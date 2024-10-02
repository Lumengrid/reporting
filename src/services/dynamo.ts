import AWS, { AWSError } from 'aws-sdk';
import { ExportStatuses, InformationReport, ReportManagerInfo } from '../models/report-manager';
import { S3 } from './s3';
import stream from 'stream';
import { DataLakeRefreshItem, ExtractionModel } from '../reports/interfaces/extraction.interface';
import { BatchWriteItemOutput } from 'aws-sdk/clients/dynamodb';
import Config from '../config';
import { SessionLoggerService } from './logger/session-logger.service';
import httpContext from 'express-http-context';
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import ItemList = DocumentClient.ItemList;
import ExpressionAttributeNameMap = DocumentClient.ExpressionAttributeNameMap;
import { NotFoundException, ErrorCode, ResourceNotFoundException } from '../exceptions';
import SessionManager from './session/session-manager.session';
import { ReportsTypes } from '../reports/constants/report-types';
import BatchGetItemInput = DocumentClient.BatchGetItemInput;
import { DataLakeRefreshStatus, ErrorsCode, ReportsSettings } from '../models/base';
import moment from 'moment';
import { RefreshTokenItem } from '../reports/interfaces/tokens.interface';
import ScanInput = DocumentClient.ScanInput;
import { QUERY_BUILDER_ACTIVE } from '../query-builder/models/query-builder-detail';
import PlatformManager from './session/platform-manager.session';
import { DatalakeV2Schemas } from '../models/datalake-v2-schemas';
import { Utils } from '../reports/utils';

export interface ReportsListExpressionAttributeValues {
    ':platform': string;
    ':deleted': boolean;
    ':usersclassroomsessions'?: string;
    ':userscertifications'?: string;
    ':certificationsusers'?: string;
    ':usersbadges'?: string;
    ':usersexternaltraining'?: string;
    ':ecommercetransaction'?: string;
    ':share'?: string;
}

export interface DynamoString {
    S: string;
}

export class DynamoList {
    L: DynamoString[];

    constructor() {
        this.L = [];
    }
}

export interface DynamoNumber {
    N: string;
}

export interface DynamoBoolean {
    BOOL: boolean;
}

export class ReportExtractionInfo {
    // Disable linting here, keeping snake case here is useful for compatibility
    // tslint:disable: variable-name
    report_id: string;
    extraction_id: string;
    status: string;
    date_start: string;
    dateStart?: string; // backwards compatibility
    date_end?: string;
    dateEnd?: string;  // backwards compatibility
    type: string;
    id_user: number;
    error_details?: string;
    hostname?: string;
    subfolder?: string;
    query?: string;
    enableFileCompression: boolean;
    queuedExtractionID?: string;
    snowflakeRequestID?: string;
    snowflakeRequestSort?: string;
    snowflakeRequestSelectedColumns?: string[];
    managerSubordinatesTable?: string;
    date_start_from_queue?: string;
    processLastTime?: string;
    scheduled?: boolean;

    // tslint:enable: variable-name
    public constructor(reportId: string, extractionId: string, status: string, date: string, type: string, idUser: number, enableFileCompression = true, dateEnd?: string, queuedExtractionID?: string, managerSubordinatesTable?: string, snowflakeRequestID?: string, snowflakeRequestSort?: string, snowflakeRequestSelectedColumns?: string[]) {
        this.report_id = reportId;
        this.extraction_id = extractionId;
        this.status = status;
        this.date_start = this.dateStart = date;
        this.date_end = this.dateEnd = dateEnd;
        this.type = type;
        this.id_user = idUser;
        this.enableFileCompression = enableFileCompression;
        this.queuedExtractionID = queuedExtractionID;
        this.managerSubordinatesTable = managerSubordinatesTable;
        this.snowflakeRequestID = snowflakeRequestID;
        this.snowflakeRequestSort = snowflakeRequestSort;
        this.snowflakeRequestSelectedColumns = snowflakeRequestSelectedColumns;
    }

    public convertSnowflakeRequestSelectedColumns(selectArray: string[]): void {
        const from: string[] = [];
        let select: string[] = [];
        for (const field of selectArray) {
            const stringField = field.substring(field.search(/AS ".*"$/m) + 4, field.lastIndexOf('"'));
            select.push(stringField);
        }

        this.snowflakeRequestSelectedColumns = select;
    }
}

export class ReportExtractionDetails {
    idReport: string;
    idExtraction: string;
    status: string;
    dateStart: string;
    dateEnd?: string;
    type: string;
    idUser: number;
    downloadUrl?: string;
    downloadLabel?: string;
    hostname?: string;
    subfolder?: string;
    enableFileCompression: boolean;
    queuedExtractionID?: string;

    public constructor(data: ReportExtractionInfo) {
        this.idReport = data.report_id;
        this.idExtraction = data.extraction_id;
        this.status = data.status;
        this.dateStart = data.dateStart ?? data.date_start;
        this.dateEnd = data.dateEnd ?? data.date_end;
        this.type = data.type;
        this.idUser = data.id_user;
        if (data.hostname) {
            this.hostname = data.hostname;
        }
        if (data.subfolder) {
            this.subfolder = data.subfolder;
        }

        this.enableFileCompression = data.enableFileCompression ?? true;

        if (data.queuedExtractionID) {
            this.queuedExtractionID = data.queuedExtractionID;
        }
    }

    public generateDownloadUrl(url: string, idReport: string, idExtraction: string) {
        if (this.status === ExportStatuses.SUCCEEDED) {
            this.downloadLabel = 'Download file';
            // platform url that manage the file download process based on the report preferences

            if (this.hostname) {
                url = this.hostname + (this.subfolder && this.subfolder !== '' ? `/${this.subfolder}` : '');
            }

            this.downloadUrl = `https://${url}/report/download-file?reportId=${idReport}&extractionId=${idExtraction}`;
        }
    }

    public getS3DownloadStream(s3: S3, extension = 'zip', v3 = false): Promise<stream.Readable> {
        return s3.getReportExtractionDownloadStream(this.queuedExtractionID ?? this.idExtraction, extension, v3);
    }

    /**
     * Get number of days elapsed from extraction date
     */
    public getDaysElapsed(): number {
        const extractionDate = new Date(this.dateEnd?.toString() ?? '');
        const currentDate = new Date();
        return (currentDate.getTime() - extractionDate.getTime()) / (1000 * 60 * 60 * 24);
    }
}

export type DynamoReport = AWS.DynamoDB.DocumentClient.AttributeMap;

export class Dynamo {
    protected region: string;
    protected db: string;
    protected customReportTypesDB: string;
    protected exportsDb: string;
    protected platform: string;
    protected dataLakeRefreshInfoTable: string;
    protected refreshTokensTable: string;
    protected settingsTable: string;

    protected connection: AWS.DynamoDB;
    protected document: AWS.DynamoDB.DocumentClient;
    protected config: Config;
    private logger: SessionLoggerService;

    private readonly datalakeV2SchemasTableName: string;
    private readonly mysqlDbName: string;
    private readonly dbHost: string;
    private readonly datalakeV2DBHost: string;

    public constructor(region: string, platform: string, platformOverride: string, platformManager: PlatformManager, logger?: SessionLoggerService) {
        this.config = new Config();

        this.region = region;
        this.platform = platform;
        this.customReportTypesDB = platformManager.getCustomReportTypesTableName();
        this.datalakeV2SchemasTableName = this.config.getDatalakeSchemasTableName();
        this.dbHost = platformManager.getDbHostOverride() !== '' ? platformManager.getDbHostOverride() : platformManager.getDbHost();
        this.mysqlDbName = platformManager.getAthenaSchemaNameOverride() !== '' ?  platformManager.getAthenaSchemaNameOverride() : platformManager.getAthenaSchemaName();
        this.datalakeV2DBHost = platformManager.getDatalakeV2Host();

        if (platformOverride !== '') {
            this.platform = platformOverride;
        }

        const awsCredentials = {
            region: this.region
        };
        AWS.config.update(awsCredentials);
        // AWS.config.logger = console; // Uncomment this to have a full log in console for the AWS sdk

        this.connection = new AWS.DynamoDB();
        this.document = new AWS.DynamoDB.DocumentClient({convertEmptyValues: true});

        // set the dynamo reports table name and dynamo exports table name
        this.db = this.config.getReportsTableName();
        this.exportsDb = this.config.getReportExtractionsTableName();
        this.dataLakeRefreshInfoTable = this.config.getDataLakeRefreshInfoTableName();
        this.refreshTokensTable = this.config.getRefreshOnDemandTokensTableName();
        this.settingsTable = this.config.getReportsSettingsTableName();
        this.logger = logger ?? httpContext.get('logger');
    }

    public async listTable() {
        return new Promise((resolve, reject) => {
            this.connection.listTables((err: AWS.AWSError, data: AWS.DynamoDB.ListTablesOutput) => {
                if (err) {
                    reject(err);
                } else {
                    if (data.TableNames) {
                        resolve(data.TableNames);
                    } else {
                        resolve([]);
                    }
                }
            });
        });
    }

    /**
     * Get the reports for the specific session platform
     */
    public async getReports(session: SessionManager): Promise<ItemList> {
        let filterExpression = 'deleted = :deleted';
        const expressionAttributeValues: ReportsListExpressionAttributeValues = {
            ':platform': this.platform,
            ':deleted': false,
        };

        if (!session.platform.checkPluginCertificationEnabled()) {
            filterExpression += ' AND #t <> :userscertifications AND #t <> :certificationsusers';
            expressionAttributeValues[':userscertifications'] = ReportsTypes.USERS_CERTIFICATIONS;
            expressionAttributeValues[':certificationsusers'] = ReportsTypes.CERTIFICATIONS_USERS;
        }

        if (!session.platform.checkPluginClassroomEnabled()) {
            filterExpression += ' AND #t <> :usersclassroomsessions';
            expressionAttributeValues[':usersclassroomsessions'] = ReportsTypes.USERS_CLASSROOM_SESSIONS;
        }

        if (!session.platform.checkPluginGamificationEnabled()) {
            filterExpression += ' AND #t <> :usersbadges';
            expressionAttributeValues[':usersbadges'] = ReportsTypes.USERS_BADGES;
        }

        if (!session.platform.checkPluginTranscriptEnabled()) {
            filterExpression += ' AND #t <> :usersexternaltraining';
            expressionAttributeValues[':usersexternaltraining'] = ReportsTypes.USERS_EXTERNAL_TRAINING;
        }

        if (!session.platform.checkPluginEcommerceEnabled()) {
            filterExpression += ' AND #t <> :ecommercetransaction';
            expressionAttributeValues[':ecommercetransaction'] = ReportsTypes.ECOMMERCE_TRANSACTION;
        }

        if (!session.platform.checkPluginShareEnabled()) {
            filterExpression += ' AND #t <> :share AND #t <> :usersassets';
            expressionAttributeValues[':share'] = ReportsTypes.ASSETS_STATISTICS;
            expressionAttributeValues[':usersassets'] = ReportsTypes.VIEWER_ASSET_DETAILS;
        }

        const params: DocumentClient.QueryInput = {
            TableName: this.db,
            KeyConditionExpression: 'platform = :platform',
            FilterExpression: filterExpression,
            ExpressionAttributeValues: expressionAttributeValues,
            IndexName: 'platform-idReport-index',
            ProjectionExpression: 'idReport, title, platform, author, creationDate, description, #t, standard, visibility, planning',
            ExpressionAttributeNames: {
                '#t': 'type',
            },
        };

        const items = await this.query(params);

        return items;
    }

    public async getReport(idReport: string): Promise<AWS.DynamoDB.DocumentClient.AttributeMap> {
        try {
            await this.updateVILTReport(idReport);
        } catch (error: any) {
            if (error.code !== 'ConditionalCheckFailedException') {
                throw(error);
            }
        }

        const params = {
            TableName: this.db,
            KeyConditionExpression: 'platform = :platform AND idReport = :idReport',
            ExpressionAttributeValues: {
                ':platform': this.platform,
                ':idReport': idReport
            }
        };

        const items = await this.query(params);

        if (!items || items.length === 0) {
            throw new NotFoundException('Report not found!', ErrorsCode.ReportNotExist);
        }

        return items[0];
    }

    public async updateVILTReport(idReport: string) {
        return new Promise((resolve, reject) => {
            const params = {
                TableName: this.db,
                Key: {
                    idReport: {
                        S: idReport
                    },
                    platform: {
                        S: this.platform
                    }
                },
                UpdateExpression: 'SET #u.showOnlyLearners = :f, vILTUpdated = :t',
                ExpressionAttributeValues: {
                    ':t': {
                        BOOL: true
                    },
                    ':f': {
                        BOOL: false
                    },
                    ':uc': {
                        S: ReportsTypes.USERS_COURSES
                    },
                    ':ulo': {
                        S: ReportsTypes.USERS_LEARNINGOBJECTS
                    },
                    ':uet': {
                        S: ReportsTypes.USERS_ENROLLMENT_TIME
                    },
                    ':gc': {
                        S: ReportsTypes.GROUPS_COURSES
                    },
                    ':ucs': {
                        S: ReportsTypes.USERS_CLASSROOM_SESSIONS
                    },
                    ':uws': {
                        S: ReportsTypes.USERS_WEBINAR
                    }
                },
                ExpressionAttributeNames: {
                    '#u': 'users',
                    '#t': 'type',
                },
                ConditionExpression: `#t IN (:uc, :ulo, :uet, :gc, :ucs, :uws) AND vILTUpdated <> :t`
            };
            this.connection.updateItem(params, (err: AWS.AWSError): void => {
                if (err) {
                    reject(err);
                } else {
                    resolve(undefined);
                }
            });
        });
    }

    /**
     * Get all the reports of a specific author
     * @param idUser {number} The report author
     */
    public async getUserIdReports(idUser: number): Promise<string[]> {
        const idReports: string[] = [];

        const params: DocumentClient.QueryInput = {
            TableName: this.db,
            KeyConditionExpression: 'platform = :platform',
            FilterExpression: 'author = :author AND deleted <> :deleted',
            ExpressionAttributeValues: {
                ':platform': this.platform,
                ':author': idUser,
                ':deleted': true
            },
            IndexName: 'platform-idReport-index',
            ProjectionExpression: 'idReport',
        };
        const items = await this.query(params);
        for (const report of items) {
            idReports.push(report.idReport);
        }

        return idReports;
    }

    /**
     * Get all the report ids of the platform
     */
    public async getAllIdReports(): Promise<string[]> {
        const reports: string[] = [];

        const params: DocumentClient.QueryInput = {
            TableName: this.db,
            KeyConditionExpression: 'platform = :platform',
            FilterExpression: 'deleted <> :deleted',
            ExpressionAttributeValues: {
                ':platform': this.platform,
                ':deleted': true
            },
            IndexName: 'platform-idReport-index',
            ProjectionExpression: 'idReport',
        };

        const items = await this.query(params);
        for (const report of items) {
            reports.push(report.idReport);
        }

        return reports;
    }

    public async createOrEditReport(item: any) {

        return new Promise((resolve, reject) => {
            const params = {
                TableName: this.db,
                Item: item
            };
            this.document.put(params, (err: AWS.AWSError): void => {
                if (err) {
                    this.logger.errorWithStack('Error on report createion / update', err);
                    reject(err);
                } else {
                    resolve(undefined);
                }
            });
        });
    }

    public async batchWriteReports(items: ReportManagerInfo[]) {
        await this.batchWrite(this.db, items);
    }

    public async batchWriteDataLakeUpdates(items: DataLakeRefreshItem[]) {
        await this.batchWrite(this.dataLakeRefreshInfoTable, items);
    }

    public async batchWriteRestoreRefreshTokens(items: any[]) {
        await this.batchWrite(this.refreshTokensTable, items);
    }

    public async batchWrite(db: string, items: any[]) {
        const maxItemsPerRequest = 25;
        const params = {
            RequestItems: {} as { [key: string]: any[] },
        };
        params.RequestItems[db] = [];

        for (const item of items) {
            const writeReq = {
                PutRequest: {
                    Item: item
                }
            };
            params.RequestItems[db].push(writeReq as never);
            // do we have to call the batchWrite?
            if (params.RequestItems[db].length % maxItemsPerRequest === 0) {
                this.logger.debug(`Perform batch write of ${params.RequestItems[db].length} items`);
                await this.performBatchWrite(params);
                params.RequestItems[db] = [];
                this.logger.debug(`Performed`);
            }
        }

        // the remaining items or number of items less than maxItemsPerRequest
        if (params.RequestItems[db].length !== 0) {
            this.logger.debug(`Perform batch write of ${params.RequestItems[db].length} items`);
            await this.performBatchWrite(params);
            this.logger.debug(`Performed`);
        }

    }

    private async performBatchWrite(params: any) {
        return new Promise((resolve, reject) => {
            this.document.batchWrite(params, (err: AWSError, data: BatchWriteItemOutput) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(data);
                }
            });
        });
    }

    public async deleteReport(idReport: string) {
        return new Promise((resolve, reject) => {
            const params = {
                TableName: this.db,
                Key: {
                    idReport: {
                        S: idReport
                    },
                    platform: {
                        S: this.platform
                    }
                },
                UpdateExpression: 'SET deleted = :d',
                ExpressionAttributeValues: {
                    ':d': {
                        BOOL: true
                    }
                }
            };
            this.connection.updateItem(params, (err: AWS.AWSError): void => {
                if (err) {
                    reject(err);
                } else {
                    resolve(undefined);
                }
            });
        });
    }

    public async undeleteReport(idReport: string, platform: string) {
        return new Promise((resolve, reject) => {
            const params = {
                TableName: this.db,
                Key: {
                    idReport: {
                        S: idReport
                    },
                    platform: {
                        S: platform
                    }
                },
                UpdateExpression: 'SET deleted = :d',
                ExpressionAttributeValues: {
                    ':d': {
                        BOOL: false
                    }
                }
            };
            this.connection.updateItem(params, (err: AWS.AWSError): void => {
                if (err) {
                    reject(err);
                } else {
                    resolve(undefined);
                }
            });
        });
    }

    public async getReportExtraction(idReport: string, extractionId: string): Promise<ReportExtractionInfo> {
        return new Promise((resolve, reject) => {
            const params = {
                TableName: this.exportsDb,
                KeyConditionExpression: 'report_id = :report_id AND extraction_id = :extraction_id',
                ExpressionAttributeValues: {
                    ':report_id': idReport,
                    ':extraction_id': extractionId
                }
            };
            this.document.query(params, (err: AWS.AWSError, data: AWS.DynamoDB.DocumentClient.QueryOutput) => {
                if (err) {
                    reject(err);
                } else {
                    if (data.Items && data.Items.length > 0) {
                        resolve(data.Items[0] as ReportExtractionInfo);
                    } else {
                        reject('Report extraction not found');
                    }
                }
            });
        });
    }

    public createOrEditReportExtraction(item: ReportExtractionInfo) {
        return new Promise((resolve, reject) => {
            const params = {
                TableName: this.exportsDb,
                Item: item
            };
            this.document.put(params, (err: AWS.AWSError): void => {
                if (err) {
                    reject(err);
                } else {
                    resolve(undefined);
                }
            });
        });
    }

    /**
     * Retrieve the reports for the specified platforms that have the planning active
     * @param platforms List of platforms
     */
    public async getScheduledReportByPlatform(platforms: string[], returnAllFields = false): Promise<ExtractionModel[] | ReportManagerInfo[]>  {

        let items: (ExtractionModel | ReportManagerInfo)[] = [];

        const chunksOfPlatforms: { [key: string]: any }[] = [];

        platforms.forEach((value, index) => {
            const chunkIndex = Math.floor(index / 99);
            if (chunksOfPlatforms[chunkIndex] === undefined) {
                chunksOfPlatforms[chunkIndex] = {};
            }
            chunksOfPlatforms[chunkIndex][`:platform${index}`] = value;
        });

        for (const chunk of chunksOfPlatforms) {
            const params = {
                TableName: this.db,
                FilterExpression: `platform IN (${Object.keys(chunk).toString()})
                                    AND deleted <> :deleted
                                    AND planning.active = :active`,
                ExpressionAttributeValues: {
                    ':deleted': true,
                    ':active': true,
                    ...chunk,
                },
                ProjectionExpression: returnAllFields ? undefined : 'idReport, platform, planning, author'
            };

            const res = await this.scan(params);
            items = items.concat(res as (ExtractionModel | ReportManagerInfo)[]);
        }

        return items;
    }

    /**
     * Get last update of the data of the platform inside the data lake
     */
    public async getLastDataLakeUpdate(session: SessionManager): Promise<DataLakeRefreshItem | undefined> {
        const params = {
            TableName: this.dataLakeRefreshInfoTable,
            KeyConditionExpression: 'platform = :platform',
            ExpressionAttributeValues: {
                ':platform': this.platform
            }
        };

        const items = await this.query(params);
        if (items && items.length > 0) {
            const dataLakeRefreshItem = items[0] as DataLakeRefreshItem;
            // if the datalake v2 is active and the error count is != 0 but is related to a date different from today
            // or we don't have info about the last refesh start date
            if ((session.platform.isDatalakeV2Active() && dataLakeRefreshItem.lastRefreshStartDate &&
                dataLakeRefreshItem.errorCount !== 0 &&
                moment(dataLakeRefreshItem.lastRefreshStartDate).format('YYYY-MM-DD') !== moment.utc().format('YYYY-MM-DD')) || !dataLakeRefreshItem.lastRefreshStartDate) {
                dataLakeRefreshItem.errorCount = 0;
                await this.restartDataLakeErrorCount();
            }
            if (session.platform.isDatalakeV2Active()) {
                if (dataLakeRefreshItem.refreshTimezoneLastDateUpdateV2 === undefined) {
                    dataLakeRefreshItem.refreshTimezoneLastDateUpdate = undefined;
                    dataLakeRefreshItem.refreshOnDemandLastDateUpdate = undefined;
                } else {
                    dataLakeRefreshItem.refreshTimezoneLastDateUpdate = dataLakeRefreshItem.refreshTimezoneLastDateUpdateV2;
                    dataLakeRefreshItem.refreshOnDemandLastDateUpdate = dataLakeRefreshItem.refreshTimezoneLastDateUpdateV2;
                    dataLakeRefreshItem.refreshOnDemandStatus = dataLakeRefreshItem.refreshTimeZoneStatus;
                }
                dataLakeRefreshItem.refreshOnDemandStatus = dataLakeRefreshItem.refreshTimeZoneStatus;
            }

            return dataLakeRefreshItem;
        } else {
            return undefined;
        }
    }

    public async updateDataLakeRefreshOnDemandStatus(refreshStatus: DataLakeRefreshStatus): Promise<void> {

        const now = moment.utc().format('YYYY-MM-DD HH:mm:ss');

        await this.document.update({
            TableName: this.dataLakeRefreshInfoTable,
            Key: {platform: this.platform},
            UpdateExpression: 'SET #a = :x, #b = :y',
            ExpressionAttributeNames: {'#a': 'refreshOnDemandStatus', '#b': 'refreshOnDemandLastDateUpdate'},
            ExpressionAttributeValues: {':x': refreshStatus, ':y': now}
        }).promise();

    }

    public async updateDataLakeNightlyRefreshStatus(refreshStatus: DataLakeRefreshStatus): Promise<void> {

        const now = moment.utc().format('YYYY-MM-DD HH:mm:ss');

        let updateExpression = 'SET #a = :x, #b = :y';
        let expressionAttributeNames: ExpressionAttributeNameMap = {
            '#a': 'refreshTimeZoneStatus',
            '#b': 'refreshTimezoneLastDateUpdate'
        };

        if (refreshStatus === DataLakeRefreshStatus.RefreshInProgress) {
            updateExpression = 'SET #a = :x, #b = :y, #c = :y, #d = :w';
            expressionAttributeNames = {
                '#a': 'refreshTimeZoneStatus',
                '#b': 'refreshTimezoneLastDateUpdate',
                '#c': 'lastRefreshStartDate',
                '#d': 'stepFunctionExecutionId'
            };
        }

        await this.document.update({
            TableName: this.dataLakeRefreshInfoTable,
            Key: {platform: this.platform},
            UpdateExpression: updateExpression,
            ExpressionAttributeNames: expressionAttributeNames,
            ExpressionAttributeValues: {':x': refreshStatus, ':y': now, ':z': DataLakeRefreshStatus.RefreshInProgress, ':w': ''},
            ConditionExpression: '#a <> :z'
        }).promise();

    }

    public async forceUpdateDataLakeNightlyRefreshStatus(refreshStatus: DataLakeRefreshStatus): Promise<void> {

        const now = moment.utc().format('YYYY-MM-DD HH:mm:ss');

        let updateExpression = 'SET #a = :x, #b = :y';
        let expressionAttributeNames: ExpressionAttributeNameMap = {
            '#a': 'refreshTimeZoneStatus',
            '#b': 'refreshTimezoneLastDateUpdate'
        };
        if (refreshStatus === DataLakeRefreshStatus.RefreshInProgress) {
            updateExpression = 'SET #a = :x, #b = :y, #c = :y';
            expressionAttributeNames = {
                '#a': 'refreshTimeZoneStatus',
                '#b': 'refreshTimezoneLastDateUpdate',
                '#c': 'lastRefreshStartDate'
            };

        }
        await this.document.update({
            TableName: this.dataLakeRefreshInfoTable,
            Key: {platform: this.platform},
            UpdateExpression: updateExpression,
            ExpressionAttributeNames: expressionAttributeNames,
            ExpressionAttributeValues: {':x': refreshStatus, ':y': now}
        }).promise();

    }

    public async restartDataLakeErrorCount(): Promise<void> {
        await this.document.update({
            TableName: this.dataLakeRefreshInfoTable,
            Key: {platform: this.platform},
            UpdateExpression: 'SET #a = :x',
            ExpressionAttributeNames: {'#a': 'errorCount'},
            ExpressionAttributeValues: {':x': 0 }
        }).promise();
    }

    public async updateDataLakeRefreshItem(refreshItem: DataLakeRefreshItem): Promise<void> {
        await this.document.put({
            TableName: this.dataLakeRefreshInfoTable,
            Item: refreshItem
        }).promise();
    }

    /**
     * Get the details of the data lake refresh for each platform requested
     * @param platforms {string[]} list of platforms
     */
    async getDataLakeRefreshDetails(platforms: string[]): Promise<DataLakeRefreshItem[]> {

        const keys = platforms.map(platformName => {
            return {platform: platformName};
        });
        const response = await this.batchGetItem(this.dataLakeRefreshInfoTable, keys);
        return response as DataLakeRefreshItem[];
    }

    public async updateStepFunctionExecutionId(stepFunctionExecutionId: string): Promise<void> {
        await this.document.update({
            TableName: this.dataLakeRefreshInfoTable,
            Key: {platform: this.platform},
            UpdateExpression: 'SET #a = :x',
            ExpressionAttributeNames: {'#a': 'stepFunctionExecutionId'},
            ExpressionAttributeValues: {':x': stepFunctionExecutionId}
        }).promise();
    }

    public async getDatalakeV3LastRefreshTime(): Promise<string> {
        const params = {
            TableName: this.dataLakeRefreshInfoTable,
            KeyConditionExpression: 'platform = :platform',
            ExpressionAttributeValues: {
                ':platform': this.platform
            }
        };

        const items = await this.query(params);
        if (items && items.length > 0) {
            const dataLakeRefreshItem = items[0] as DataLakeRefreshItem;
            return dataLakeRefreshItem.lastRefreshStartDateV3 ?? '';
        } else {
            // We have to create and empty string in DB to be sure to have it next time or when we will update it
            await this.batchWriteDataLakeUpdates([{
                platform: this.platform
            }]);

            return '';
        }
    }

    public async updateDatalakeV3LastRefreshTime(lastRefreshStartDateV3: string): Promise<void> {
        await this.document.update({
            TableName: this.dataLakeRefreshInfoTable,
            Key: {platform: this.platform},
            UpdateExpression: 'SET #a = :x',
            ExpressionAttributeNames: {'#a': 'lastRefreshStartDateV3'},
            ExpressionAttributeValues: {':x': lastRefreshStartDateV3}
        }).promise();
    }

    /**
     * Get the details of the data lake refresh for each platform requested
     * @param platforms {string[]} list of platforms
     */
    async getPlatformsSettings(platforms: string[]): Promise<ReportsSettings[]> {
        // TODO: Remove logs here
        const keys = platforms.map(platformName => {
            return {platform: platformName};
        });
        this.logger.debug('Dynamo Table: ' + this.settingsTable);
        const items =  await this.batchGetItem(this.settingsTable, keys);
        // concat the keys with the results found in dynamodb without duplicate. If there is a record in the dynamodb we use it
        this.logger.debug('Dynamo Results: ' + JSON.stringify(items));
        const response = items.concat(keys.filter(key => items.every(itemDynamo => itemDynamo.platform !== key.platform)));
        this.logger.debug('Dynamo Response: ' + JSON.stringify(response));
        return response as ReportsSettings[];
    }

    /**
     * Get the status of the refesh tokens
     */
    public async getRefreshTokensStatus(): Promise<RefreshTokenItem | undefined> {
        const params = {
            TableName: this.refreshTokensTable,
            KeyConditionExpression: 'platform = :platform',
            ExpressionAttributeValues: {
                ':platform': this.platform
            }
        };
        const items = await this.query(params);
        if (items && items.length > 0) {
            return items[0] as RefreshTokenItem;
        } else {
            return undefined;
        }
    }

    public async updateRefreshTokens(refreshTokens: RefreshTokenItem): Promise<void> {

        await this.document.update({
            TableName: this.refreshTokensTable,
            Key: {platform: this.platform},
            UpdateExpression: 'SET #mon = :a, #day = :b,  #res = :c, #req = :d',
            ExpressionAttributeNames: {
                '#mon': 'currentMonthlyTokens',
                '#day': 'currentDailyTokens',
                '#res': 'lastReset',
                '#req': 'lastRequest'
            },
            ExpressionAttributeValues: {
                ':a': refreshTokens.currentMonthlyTokens,
                ':b': refreshTokens.currentDailyTokens,
                ':c': refreshTokens.lastReset,
                ':d': refreshTokens.lastRequest
            }
        }).promise();
    }

    /**
     * Get the refresh token items for each platform requested
     * @param platforms {string[]} list of platforms
     */
    async getRefreshTokenItems(platforms: string[]): Promise<RefreshTokenItem[]> {

        const keys = platforms.map(platformName => {
            return {platform: platformName};
        });
        const response = await this.batchGetItem(this.refreshTokensTable, keys);
        return response as RefreshTokenItem[];
    }

    /**
     * Get all the legacy report ids. If a report has been already migrated it stores the old report id in importedFromLegacyId field
     * @returns {Promise<number[]>} Return an array with old report's ids
     */
    public async getAlreadyMigratedReportIds(): Promise<number[]> {
        const legacyReportMigratedId: number[] = [];

        const params: DocumentClient.QueryInput = {
            TableName: this.db,
            KeyConditionExpression: 'platform = :platform',
            FilterExpression: 'attribute_exists(importedFromLegacyId) AND deleted <> :deleted',
            ExpressionAttributeValues: {
                ':platform': this.platform,
                ':deleted': true
            },
            IndexName: 'platform-idReport-index',
            ProjectionExpression: 'importedFromLegacyId',
        };

        const items = await this.query(params);

        // Store the ids in array
        for (const reportLegacyId of items) {
            legacyReportMigratedId.push(parseInt(reportLegacyId.importedFromLegacyId, 10));
        }

        return legacyReportMigratedId;
    }

    public async batchGetItem(db: string, keys: { [key: string]: string }[]): Promise<ItemList> {
        const maxItemsPerRequest = 100;
        let results: ItemList = [];
        const params: BatchGetItemInput = {
            RequestItems: {
                [db]: {
                    Keys: [] as { [key: string]: string }[],
                }
            }
        };

        let response;

        this.logger.debug(`Requested a batchGetItem of ${keys.length} items`);

        for (const partitionKey of keys) {
            params.RequestItems[db].Keys.push(partitionKey);
            // do we have to call the batchGetItem?
            if (params.RequestItems[db].Keys.length % maxItemsPerRequest === 0) {
                this.logger.debug(`Perform batchGetItem of ${params.RequestItems[db].Keys.length} items`);
                response = await this.document.batchGet(params).promise();
                if (response?.Responses && response.Responses[db]) {
                    results = results.concat(response.Responses[db]);
                }
                params.RequestItems[db].Keys = [];
                this.logger.debug(`Performed`);
            }
        }

        if (params.RequestItems[db].Keys.length !== 0) {
            this.logger.debug(`Perform the last batchGetItem of ${params.RequestItems[db].Keys.length} items`);
            response = await this.document.batchGet(params).promise();
            if (response?.Responses && response.Responses[db]) {
                results = results.concat(response.Responses[db]);
            }
            this.logger.debug(`Performed`);
        }

        this.logger.debug(`Retrieved a total of ${results.length} items`);

        return results;
    }

    /**
     * Wrap the document scan of dynamo handling the pagination
     * @param params {DocumentClient.ScanInput} The input param for the document scan
     */
    private async scan(params: DocumentClient.ScanInput): Promise<ItemList> {
        let items: ItemList = [];
        let data;
        do {
            if (data && data.LastEvaluatedKey) {
                params = {
                    ...params,
                    ExclusiveStartKey: data.LastEvaluatedKey,
                };
            }
            data = await this.internalScan(params);
            items = items.concat(data.Items ? data.Items : []);
        } while (data.LastEvaluatedKey);

        return items;
    }

    /**
     * Execute the document scan of dynamo
     * @param params {DocumentClient.ScanInput} The input param for the document scan
     */
    private async internalScan(params: DocumentClient.ScanInput): Promise<DocumentClient.ScanOutput> {
        return new Promise((resolve, reject) => {
            this.document.scan(params, (err: AWS.AWSError, data: DocumentClient.ScanOutput) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(data);
                }
            });
        });
    }

    /**
     * Wrap the document query of dynamo handling the pagination
     * @param params {DocumentClient.QueryInput} The input param for the document query
     */
    private async query(params: DocumentClient.QueryInput): Promise<ItemList> {
        let items: ItemList = [];
        let data;
        do {
            if (data && data.LastEvaluatedKey) {
                params = {
                    ...params,
                    ExclusiveStartKey: data.LastEvaluatedKey,
                };
            }
            data = await this.internalQuery(params);
            items = items.concat(data.Items ? data.Items : []);
        } while (data.LastEvaluatedKey);

        return items;
    }

    /**
     * Execute the document query of dynamo
     * @param params {DocumentClient.QueryInput} The input param for the document query
     */
    private async internalQuery(params: DocumentClient.QueryInput): Promise<DocumentClient.QueryOutput> {
        return new Promise((resolve, reject) => {
            this.document.query(params, (err: AWS.AWSError, data: DocumentClient.QueryOutput) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(data);
                }
            });
        });
    }

    /**
     * Get the reports settings for the platform, if no settings are found an empty setting object will be returned
     */
    public async getSettings(): Promise<AWS.DynamoDB.DocumentClient.AttributeMap> {
        const params: DocumentClient.QueryInput = {
            TableName: this.settingsTable,
            KeyConditionExpression: 'platform = :platform',
            ExpressionAttributeValues: {
                ':platform': this.platform
            }
        };

        const items = await this.query(params);

        if (!items || items.length === 0) {
            return {
                platform: this.platform
            };
        }

        return items[0];
    }

    /**
     * Create or replace reports settings for the platform
     */
    public async createOrEditSettings(item: any) {
        return new Promise((resolve, reject) => {
            const params = {
                TableName: this.settingsTable,
                Item: item
            };
            this.document.put(params, (err: AWS.AWSError): void => {
                if (err) {
                    reject(err);
                } else {
                    resolve(undefined);
                }
            });
        });
    }

    /**
     * Update the timezone and startTime for the schedulation with
     */
    public async updateReportSchedulationToggleOn(scheduledReports: ReportManagerInfo[]) {
        await this.batchWrite(this.db, scheduledReports);
    }

    public async getDatalakeV2LastCompleteTime(): Promise<string> {
        let items;
        try {
            const params: DocumentClient.QueryInput = {
                TableName: this.datalakeV2SchemasTableName,
                KeyConditionExpression: '#s = :schema_name',
                FilterExpression: 'db_host = :db_host',
                ExpressionAttributeNames: {'#s': 'schema'},
                ExpressionAttributeValues: {
                    ':schema_name': this.mysqlDbName,
                    ':db_host': this.datalakeV2DBHost,
                },
                IndexName: 'schema-index',
                ProjectionExpression: 'complete_time',
            };

            items = await this.query(params);
        } catch (err: any) {
            if (err && err.code === 'ResourceNotFoundException') {
                this.logger.errorWithStack('Error on recover last refresh conplete time for V2', err);
                throw new ResourceNotFoundException(err.message, 500);
            }

            throw err;
        }
        if (!items || items.length === 0) {
            throw new NotFoundException(`Schema name: '${this.mysqlDbName}' not found in ${this.datalakeV2SchemasTableName}. Db host: ${this.dbHost}`, ErrorCode.REPORT_NOT_FOUND);
        }
        const datalakeV2Schemas = items[0] as DatalakeV2Schemas;
        return moment(datalakeV2Schemas.complete_time, 'YYYY/MM/DD HH:mm').format('YYYY-MM-DD HH:mm:ss');
    }



    /************************
     ***  QUERY BUIDLER  ***
     ************************/

    /**
     * Get the custom report types for the specific session platform
     */
    public async getCustomReportTypes(): Promise<ItemList> {
        try {
            const params: DocumentClient.QueryInput = {
                TableName: this.customReportTypesDB,
                KeyConditionExpression: 'platform = :platform',
                FilterExpression: 'deleted <> :deleted',
                ExpressionAttributeValues: {
                    ':platform': this.platform,
                    ':deleted': true,
                },
                IndexName: 'platform-id-index',
            };

            const items = await this.query(params);

            return items;
        } catch (err: any) {
            if (err && err.code === 'ResourceNotFoundException') {
                this.logger.errorWithStack('Error on recover custom report types', err);
                throw new ResourceNotFoundException(err.message, 500);
            }

            throw err;
        }
    }

    /**
     * Get the active custom report types for the specific session platform
     */
    public async getActiveCustomReportTypes(): Promise<ItemList> {
        try {
            const params: ScanInput = {
                TableName: this.customReportTypesDB,
                FilterExpression: 'platform = :platform AND #s = :custom_report_status AND deleted <> :deleted',
                ExpressionAttributeNames: {'#s': 'status'},
                ExpressionAttributeValues: {
                    ':platform': this.platform,
                    ':custom_report_status': QUERY_BUILDER_ACTIVE,
                    ':deleted': true,
                }
            };

            return await this.scan(params);
        } catch (err: any) {
            if (err && err.code === 'ResourceNotFoundException') {
                throw new ResourceNotFoundException(err.message, 500);
            }

            throw err;
        }
    }

    public async createOrEditCustomReportTypes(item: any) {
        try {
            const params = {
                TableName: this.customReportTypesDB,
                Item: item
            };

            await this.document.put(params).promise();
        } catch (err: any) {
            if (err && err.code === 'ResourceNotFoundException') {
                throw new ResourceNotFoundException(err.message, 500);
            }

            throw err;
        }
    }

    /**
     * Get Custom Report Types not deleted by id
     * @param idCustomReportType
     * @throws
     */
    public async getCustomReportTypesById(idCustomReportType: string) {
        try {
            const params = {
                TableName: this.customReportTypesDB,
                KeyConditionExpression: 'platform = :platform AND id = :idCustomReportType',
                FilterExpression: 'deleted <> :deleted',
                ExpressionAttributeValues: {
                    ':platform': this.platform,
                    ':idCustomReportType': idCustomReportType,
                    ':deleted': true,
                }
            };

            const items = await this.query(params);

            if (!items || items.length === 0) {
                throw new NotFoundException('CustomReportTypes not found!', ErrorCode.REPORT_NOT_FOUND);
            }

            return items[0];
        } catch (err: any) {
            if (err && err.code === 'ResourceNotFoundException') {
                throw new ResourceNotFoundException(err.message, 500);
            }

            throw err;
        }
    }

    /**
     * This is not real delete but is a soft delete
     * @param idCustomReportType
     */
    public async deleteCustomReportTypeById(idCustomReportType: string) {
        try {
            const params = {
                TableName: this.customReportTypesDB,
                Key: {
                    id: {
                        S: idCustomReportType
                    },
                    platform: {
                        S: this.platform
                    }
                },
                UpdateExpression: 'SET deleted = :d',
                ExpressionAttributeValues: {
                    ':d': {
                        BOOL: true
                    }
                }
            };

            return await this.connection.updateItem(params).promise();
        } catch (err: any) {
            if (err && err.code === 'ResourceNotFoundException') {
                throw new ResourceNotFoundException(err.message, 500);
            }

            throw err;
        }
    }

    public async getReportsByCustomReportType(idCustomReportType: string): Promise<InformationReport[]> {
        const params: DocumentClient.QueryInput = {
            TableName: this.db,
            KeyConditionExpression: 'platform = :platform',
            FilterExpression: 'deleted <> :deleted AND queryBuilderId = :queryBuilderId',
            ExpressionAttributeValues: {
                ':platform': this.platform,
                ':queryBuilderId': idCustomReportType,
                ':deleted': true,
            },
            IndexName: 'platform-idReport-index',
            ProjectionExpression: 'idReport, title',
        };

        return await this.query(params) as unknown as InformationReport[];
    }

    public async updateExtractionProcessLastTime(extraction_id: string, report_id: string) {
        let count = 0;
        const last_time = moment(new Date()).format('YYYY-MM-DD HH:mm:ss');
        while (count <= 5) {
            try {
                await this.document.update({
                    TableName: this.exportsDb,
                    Key: {extraction_id, report_id},
                    UpdateExpression: 'SET #a = :x',
                    ExpressionAttributeNames: {'#a': 'processLastTime'},
                    ExpressionAttributeValues: {':x': last_time}
                }).promise();
            } catch(error: any) {
                if (error.hasOwnProperty('code') && (String(error.code).toUpperCase().indexOf('THROTTLING') !== -1 || String(error.code) == 'TooManyRequestsException')) {
                    await Utils.sleep(3000);
                    count++;
                    continue;
                } else {
                    throw(error);
                }
            }

            return;
        }
    }
}
