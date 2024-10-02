import * as snowflake from 'snowflake-sdk';
import { SessionLoggerService } from '../logger/session-logger.service';
import { ErrorsCode } from '../../models/base';
import SessionManager from '../session/session-manager.session';
import { v4 } from 'uuid';
import { NotFoundException } from '../../exceptions';
import { ExtractionFailedException } from '../../exceptions/extractionFailedException';
import moment from 'moment-timezone';
import { ExportTranslation } from '../../dashboards/interfaces/dashboard.interface';
import { PoolWrapper } from './poolWrapper';
import { DBConnection, RefreshDetails } from './interfaces/snowflake.interface';
import { Utils } from '../../reports/utils';
import { redisFactory } from '../redis/RedisFactory';
import { QueryStatus } from '../../domain/entities/Extraction';

export type LogLevel = 'ERROR' | 'WARN' | 'INFO' | 'DEBUG' | 'TRACE';

export class Snowflake {
    private REDIS_DB = 9;
    private DEFAULT_PAGE_SIZE = 10;

    public constructor(private readonly session: SessionManager, private readonly logger: SessionLoggerService) {
        if (session.platform.getSnowflakeLogLevel() !== '') {
            // @ts-ignore
            snowflake.configure({ocspFailOpen: false, logLevel: session.platform.getSnowflakeLogLevel() as LogLevel});
        } else {
            snowflake.configure({ocspFailOpen: false});
        }
    }

    private logError(message: string, exception?: any): void {
        message = `** SNOWFLAKE ** ${message}`;
        if (exception === undefined) {
            this.logger.error(message);
        } else {
            this.logger.errorWithStack(message, exception);
        }
    }

    private getRdsInstance(): string {
        let rdsInstance = this.session.platform.getMainDbHost().toUpperCase().split('.')[0].replaceAll('-', '_');
        if (this.session.platform.getSnowflakeDbHost() !== '') {
            rdsInstance = this.session.platform.getSnowflakeDbHost().toUpperCase().split('.')[0].replaceAll('-', '_');
        }
        return rdsInstance;
    }

    public async getLastRefreshDetails(): Promise<RefreshDetails> {
        const result = {
            status: '',
            lastRefreshStart: ''
        };
        const rdsInstance = this.getRdsInstance();
        if (this.session.platform.getSnowflakeLockTable() === '' || rdsInstance === '') {
            this.logError('LOCK STATUS ERROR - LOCKTABLE OR RDSINSTANCE EMPTY');
            return result;
        }
        const query = `SELECT LAST_REFRESH_START, REFRESH_STATUS
                       FROM ${this.session.platform.getSnowflakeLockTable()}
                       WHERE RDS_INSTANCE = '${rdsInstance}'`;
        const data = await this.runQuery(query, true, false, false);
        const { LAST_REFRESH_START, REFRESH_STATUS } = data[0];
        result.status =  REFRESH_STATUS;
        result.lastRefreshStart = moment(LAST_REFRESH_START).format('YYYY-MM-DD HH:mm:ss');

        return result;
    }

    private async waitUnlock(connection: DBConnection): Promise<boolean> {
        const rdsInstance = this.getRdsInstance();
        let status = false;
        if (this.session.platform.getSnowflakeLockTable() === '' || rdsInstance === '') {
            this.logError('LOCK STATUS ERROR - LOCKTABLE OR RDSINSTANCE EMPTY');
            return false;
        }
        const query = `SELECT SWAP_STATUS FROM ${this.session.platform.getSnowflakeLockTable()} WHERE RDS_INSTANCE = '${rdsInstance}'`;
        for (let attempt = 0; attempt < 60; attempt ++) {
            const data = await connection.runQuery(query, true, false, false);
            if (!data || !data[0]) {
                break;
            }
            const { SWAP_STATUS } = data[0];
            if (SWAP_STATUS && SWAP_STATUS === 'running') {
                this.logError('LOCK STATUS ERROR - DATABASE LOCKED FOR UPDATE');
                await Utils.sleep(5000);
            } else {
                status = true;
                break;
            }
        }

        return status;
    }

    private getSchema(): string {
        let mySchema = this.session.platform.getMysqlDbName().toUpperCase();
        if (this.session.platform.getSnowflakeSchema() !== '') {
            mySchema = this.session.platform.getSnowflakeSchema().toUpperCase();
        }

        return mySchema;
    }

    public async runQuery(sqlQuery: string, waitForResults = true, streamResult = false, retFullErrorMessage = false, waitUnlock = true, extraSchemaCheck = true, forceReturnQueryId = false): Promise<any> {
        const schema = this.getSchema();
        const wrapper = new PoolWrapper(global.snowflakePool, this.logger, this.session.platform.getSnowflakeDatabase(), this.session.platform.getSnowflakeDefaultSchema(), schema);

        if (waitUnlock && !await this.waitUnlock(wrapper)) {
            this.logError('QUERY ERROR - TIMEOUT CHECK LOCK TABLE STATUS');
            throw new ExtractionFailedException('Timeout check lock table status', ErrorsCode.ExtractionFailed);
        }

        if (extraSchemaCheck) {
            sqlQuery = `SELECT * FROM (${sqlQuery}) WHERE CURRENT_SCHEMA() ILIKE '${schema}'`;
        }

        try {
            return await wrapper.runQuery(sqlQuery, waitForResults, streamResult, forceReturnQueryId);
        } catch (err: any) {
            const errorMessage = err.hasOwnProperty('message') ? ` message: "${err.message}"` : '';
            const errorCode = err.hasOwnProperty('code') ? ` code: "${err.message}"` : '';
            this.logError(`Error executing query${errorMessage}${errorCode} ${sqlQuery}`, err);
            if (!retFullErrorMessage) {
                err.message = 'Error executing the extraction query';
            }
            throw err;
        }
    }

    private buildExportQueryWithTranslations(queryId: string, sort: string, translations: string): string {
        const data: ExportTranslation[] = JSON.parse(translations);
        const innerQueryParts = [];
        const outerQueryParts = [];
        data.forEach((col) => {
            const {column, translation, valuesOverride} = col;
            const exportColumnName = translation && translation !== '' ? translation : column;
            const valuesName = valuesOverride && valuesOverride !== '' ? valuesOverride : column;

            outerQueryParts.push(`${column} AS ${exportColumnName}`);
            innerQueryParts.push(`${valuesName} AS ${column}`);
        });

        return `(SELECT ${outerQueryParts.join(', ')} FROM (SELECT ${innerQueryParts.join(', ')} FROM TABLE(RESULT_SCAN('${queryId}')) ${sort}))`;
    }

    public saveCSVFromQueryID(queryId: string, sort: string, select: string[], fileName: string, translations?: string): Promise<any> {
        let selectStatement = '';
        if (!select) {
            selectStatement = '*';
        } else {
            let first = true;
            for (const field of select) {
                if (first) {
                    first = false;
                } else {
                    selectStatement += ',';
                }
                selectStatement += '"' + field + '" AS "' + field.replaceAll('"', '""') + '"';
            }
        }
        return new Promise(async (resolve: any, reject: any) => {
            try {
                let tableScanQuery: string;
                let header = 'TRUE';
                if (translations && translations !== '') {
                    tableScanQuery = this.buildExportQueryWithTranslations(queryId, sort, translations);
                } else {
                    tableScanQuery = `(SELECT ${selectStatement} FROM TABLE(RESULT_SCAN('${queryId}')) ${sort})`;
                    const rows = await this.getResultCountFromQueryID(queryId); // Check the number of rows
                    if (rows === 0) {
                        if (!select) {
                            select = [];
                            const result = await this.runQuery(`DESCRIBE RESULT('${queryId}')`, true, false, false, false);
                            for (const row of result) {
                                select.push(row['name']);
                            }
                        }

                        let first = true;
                        for (const field of select) {
                            if (first) {
                                first = false;
                                selectStatement = '';
                            } else {
                                selectStatement += ',';
                            }
                            selectStatement += `'${field.replaceAll('""', '"').replaceAll("'", "''")}'`;
                        }

                        tableScanQuery = '(SELECT ' + selectStatement + ')';
                        header = 'FALSE';
                    }
                }
                const query = `COPY INTO s3://${this.session.platform.getSnowflakeStorageIntegrationBucket()}/snowflake-exports/${fileName}.csv FROM ${tableScanQuery} FILE_FORMAT=(NULL_IF=() TYPE='CSV' COMPRESSION='NONE' SKIP_HEADER=0 FIELD_DELIMITER=',' FIELD_OPTIONALLY_ENCLOSED_BY='"') SINGLE=TRUE MAX_FILE_SIZE=5368709120 HEADER=${header} STORAGE_INTEGRATION = ${this.session.platform.getSnowflakeStorageIntegration()}`;
                const result = await this.runQuery(query, true, true, false, false, false);
                resolve(result);
            } catch (exception: any) {
                reject(exception);
            }
        });
    }

    private async populateRedisValuesForQueryId(queryId: string, totalCount: number, sqlOrderBy?: string, type?: string, translations?: string) {
        const redis = redisFactory.getRedis();
        const userId = this.session.user.getIdUser();
        const domain = this.session.platform.getPlatformBaseUrl();
        const redisSuffix = domain + ':snowflake:';

        const data = {
            userId,
            totalCount,
            type: type ?? '',
            orderBy: sqlOrderBy ?? '',
            translations: translations ?? '',
            language: this.session.user.getLangCode(),
        };

        await redis.writeValue(redisSuffix + queryId, JSON.stringify(data), this.REDIS_DB);
    }

    private async getRedisValuesFromQueryId(queryId: string, type?: string, orderByOverride?: string, translations?: string) {
        const redis = redisFactory.getRedis();
        const userId = this.session.user.getIdUser();
        const domain = this.session.platform.getPlatformBaseUrl();
        const redisSuffix = domain + ':snowflake:';
        const redisKeyExist = redis.exists(redisSuffix + queryId, this.REDIS_DB);
        const data = redisKeyExist ? JSON.parse(await redis.getValue(redisSuffix + queryId, this.REDIS_DB)) : '';

        if (!redisKeyExist || !data || data.userId !== userId || data.type !== (type ?? '') || data.totalCount <= 0) {
            throw new NotFoundException('Invalid QueryId', ErrorsCode.QueryExecutionIdNotFound);
        }

        if (orderByOverride && orderByOverride !== '' && data.orderBy !== orderByOverride) {
            data.orderBy = orderByOverride;
            await redis.writeValue(redisSuffix + queryId, JSON.stringify(data), this.REDIS_DB);
        }

        if (translations && translations !== '' && data.translations !== translations && data.language !== this.session.user.getLangCode()) {
            data.language = this.session.user.getLangCode();
            data.translations = translations;
            await redis.writeValue(redisSuffix + queryId, JSON.stringify(data), this.REDIS_DB);
        }

        return data;
    }

    public async getSignedUrlExportCsvFromQueryID(queryId: string, type: string, fileName?: string): Promise<string> {
        if (!queryId || queryId === '') {
            throw new NotFoundException('Invalid QueryId', ErrorsCode.QueryExecutionIdNotFound);
        }
        const redisValues = await this.getRedisValuesFromQueryId(queryId, type);
        const orderBy = redisValues.orderBy ?? '';
        const translations = redisValues.translations ?? '';
        if (!fileName || fileName === '') {
            fileName = queryId + '_' + v4();
        }
        fileName += '_' + moment().tz(this.session.user.getTimezone()).format('YYYY-MM-DD_HH-mm-ss');
        await this.saveCSVFromQueryID(queryId, orderBy, [], fileName, translations);
        const s3 = this.session.getS3();
        return await s3.getExtractionDownloadUrl(fileName, 'csv', true, 300);
    }

    public getResultFromQueryIdPaginate(queryId: string, page: number, pageSize: number, sort?: string): Promise<any> {
        return new Promise(async (resolve: any, reject: any) => {
            const offSet = (page - 1) * pageSize;
            const query = `SELECT *
                           FROM TABLE(RESULT_SCAN('${queryId}')) ${sort} LIMIT ${pageSize}
                           OFFSET ${offSet}`;
            try {
                const result = await this.runQuery(query);
                resolve(result);
            } catch (exception: any) {
                reject(exception);
            }
        });
    }

    private getCountResultQueryID(queryId: string): Promise<number> {
        return new Promise(async (resolve: any, reject: any) => {
            const query = `SELECT COUNT(*) FROM TABLE(RESULT_SCAN('${queryId}'))`;
            try {
                const result = await this.runQuery(query);
                resolve(result[0]['COUNT(*)']);
            } catch (exception: any) {
                reject(exception);
            }
        });
    }

    public async checkStatusOfQueryV3(queryId: string): Promise<QueryStatus> {
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

    public async paginate(sqlQuery?: string, sqlOrderBy?: string, queryId?: string, page?: number, pageSize: number = this.DEFAULT_PAGE_SIZE, type?: string, translations?: string): Promise<any> {
        let totalCount = 0;
        if (queryId === undefined) {
            queryId = await this.runQuery(sqlQuery, false, true);
            const now = new Date().getTime()
            let queryStatus: QueryStatus;
            while (true) {
                queryStatus = await this.checkStatusOfQueryV3(queryId);
                if (queryStatus.IsError || (new Date().getTime() - now) >= (60 * 30 * 1000)) {
                    this.logError('QUERY ERROR - PAGINATE STATUS');
                    queryId = '';
                    break;
                }
                if (!queryStatus.IsRunning) {
                    break;
                }
                await Utils.sleep(1000);
            }
            if (queryId === '') {
                throw new NotFoundException('Invalid QueryId', ErrorsCode.QueryExecutionIdNotFound);
            }
            totalCount = await this.getCountResultQueryID(queryId);
            await this.populateRedisValuesForQueryId(queryId, totalCount, sqlOrderBy, type, translations);
        } else {
            const redisValues = await this.getRedisValuesFromQueryId(queryId, type, sqlOrderBy, translations);
            totalCount = redisValues.totalCount ?? 0;
        }
        if (pageSize <= 0) pageSize = this.DEFAULT_PAGE_SIZE;
        const totalPageCount = Math.trunc(totalCount / pageSize) + ((totalCount % pageSize) !== 0 ? 1 : 0);
        page = page === undefined ? 1 : Math.max(1, Math.min(totalPageCount, page));
        const result = await this.getResultFromQueryIdPaginate(queryId, page, pageSize, sqlOrderBy);
        return {
            query_id: queryId,
            current_page: page,
            current_page_size: pageSize,
            has_more_data: totalCount > page,
            total_count: totalCount,
            total_page_count: totalPageCount,
            items: result,
        };
    }

    public getResultFromQueryID(queryId: string): Promise<any> {
        return new Promise(async (resolve: any, reject: any) => {
            const query = `SELECT * FROM TABLE(RESULT_SCAN('${queryId}'))`;
            try {
                const result = await this.runQuery(query);
                resolve(result);
            } catch (exception: any) {
                reject(exception);
            }
        });
    }

    private getResultCountFromQueryID(queryId: string): Promise<number> {
        return new Promise(async (resolve: any, reject: any) => {
            const query = `SELECT COUNT(1) AS "rows" FROM TABLE(RESULT_SCAN('${queryId}'))`;
            try {
                const result = await this.runQuery(query);
                resolve(result[0].rows);
            } catch (exception: any) {
                reject(exception);
            }
        });
    }

}
