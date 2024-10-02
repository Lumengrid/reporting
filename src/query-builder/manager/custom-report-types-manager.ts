import { CustomReportTypeDetail, JsonFilter } from '../interfaces/query-builder.interface';
import { BadRequestException } from '../../exceptions/bad-request.exception';
import AthenaExpress from 'athena-express';
import SessionManager from '../../services/session/session-manager.session';
import { ExportStatuses, InformationReport } from '../../models/report-manager';
import { GetQueryExecutionOutput } from 'aws-sdk/clients/athena';
import { LIMIT_QUERY_QUERY_BUILDER_V2 } from '../../shared/constants';
import { ErrorCode } from '../../exceptions/error-codes.enum';
import {
    FILTER_TYPE_ACCEPTED,
    QUERY_BUILDER_FILTER_TYPE_DATE,
    QUERY_BUILDER_FILTER_TYPE_TEXT
} from '../models/query-builder';
import { redisFactory } from '../../services/redis/RedisFactory';

/**
 * Custom Report Types Manager
 */
export class CustomReportTypesManager {
    /**
     * Check if sql is valid
     *
     * @param sessionManager
     * @param sql
     * @param json
     */
    static async isSqlValid(sessionManager: SessionManager, sql?: string, json?: string): Promise<object> {
        const isDatalakeV3ToggleActive = sessionManager.platform.isDatalakeV3ToggleActive();
        const sqlString = this.getRunnableQuery(isDatalakeV3ToggleActive, sql, json);

        // for a faster query
        const limitedQuery = 'select * from (' + sqlString + ') limit 1';
        try {
            if (isDatalakeV3ToggleActive) {
                const snowflake = sessionManager.getSnowflake();
                return await snowflake.runQuery(limitedQuery);
            }
            const athenaExpress = this.getAthenaConnection(sessionManager, {skipResult: true});

            return await athenaExpress.query(limitedQuery);
        } catch (error: any) {
            throw new BadRequestException(error.message, ErrorCode.WRONG_SQL);
        }
    }

    /**
     * If all fields and areas are filled correctly, generate a SQL where placeholders are filled
     * @param isDatalakeV3ToggleActive
     * @param json
     * @param matches
     * @param matchesLength
     * @param sqlString
     * @private
     * @visible for testing
     */
    private static getSqlWithFilterFilled(isDatalakeV3ToggleActive: boolean, json: string, matches: RegExpMatchArray | null, matchesLength: number, sqlString: string): string {
        const jsonArray: object = JSON.parse(json);

        this.checkNumberFilter(jsonArray, matchesLength);

        const arrayFilter = matches?.map(match => (match.replace(/{|}/g, ''))) as [] ?? [];

        this.checkAllFilterAreFilled(arrayFilter, jsonArray);

        this.checkStructureJson(jsonArray);


        return this.substitutePlaceholdersFilter(isDatalakeV3ToggleActive, arrayFilter, jsonArray, sqlString);
    }

    /**
     * Preliminary check of json area
     * @param matchesLength
     * @param json
     * @private
     */
    private static validateJsonArea(matchesLength: number, json: string) {
        try {
            if (typeof json !== 'undefined' && json !== '' && json !== null) {
                JSON.parse(json);
            }
        } catch (e: any) {
            throw new BadRequestException('Json is not json', ErrorCode.WRONG_JSON);
        }

        if (matchesLength > 0 && (typeof json === 'undefined' || json === '' || json === null)) {
            throw new BadRequestException('Fill the json area', ErrorCode.JSON_AREA_EMPTY);
        }

        if (matchesLength === 0 && (typeof json !== 'undefined' && json !== '' && json !== null)) {
            throw new BadRequestException('Json area filled but sql not contains filter', ErrorCode.JSON_AREA_FILLED);
        }
    }

    /**
     * Check if the jsonArea doesn't contains more filter then sql area
     * @param jsonArray
     * @param matchesLength
     * @private
     */
    private static checkNumberFilter(jsonArray: any, matchesLength: number) {
        const filterCount = Object.keys(jsonArray).length;
        if (filterCount > matchesLength) {
            throw new BadRequestException('More filter in json area then sql area', ErrorCode.MORE_FILTER_IN_JSON);
        }
    }

    /**
     * Check if each filter in sql area exists in json area
     * @param arrayFilter
     * @param jsonArray
     * @private
     */
    private static checkAllFilterAreFilled(arrayFilter: [], jsonArray: any): void {
        arrayFilter.forEach(filterName => {
            if (!Object.keys(jsonArray).includes(filterName)) {
                throw new BadRequestException(`Filter ${filterName} not found in json area`, ErrorCode.FILTER_NOT_FOUND_IN_JSON);
            }
        });
    }

    /**
     * Check if each element of json contains the require fields
     * @param jsonArray
     * @private
     */
    private static checkStructureJson(jsonArray: any): void {
        Object.keys(jsonArray).forEach(filterName => {
            const filter = jsonArray[filterName] as JsonFilter;

            if (typeof filter.field === 'undefined') {
                throw new BadRequestException(`Missing field in ${filterName}`, ErrorCode.MISSING_FIELD_IN_JSON_FILTER);
            }

            if (typeof filter.type === 'undefined') {
                throw new BadRequestException(`Missing type in ${filterName}`, ErrorCode.MISSING_TYPE_IN_JSON_FILTER);
            }

            if (!FILTER_TYPE_ACCEPTED.includes(filter.type)) {
                throw new BadRequestException(`Type not allowed in ${filterName}`, ErrorCode.WRONG_TYPE_IN_JSON_FILTER);
            }

            if ((filter.type === QUERY_BUILDER_FILTER_TYPE_DATE || filter.type === QUERY_BUILDER_FILTER_TYPE_TEXT) && typeof filter.description === 'undefined') {
                throw new BadRequestException(`Missing description in ${filterName}`, ErrorCode.MISSING_DESCRIPTION_IN_JSON_FILTER);
            }
        });
    }

    /**
     * Substitute {filter} placeholder in sql string with filter.field value
     * @param isDatalakeV3ToggleActive
     * @param arrayFilter
     * @param jsonArray
     * @param sqlString
     * @private
     */
    private static substitutePlaceholdersFilter(isDatalakeV3ToggleActive: boolean, arrayFilter: [], jsonArray: any, sqlString: string): string {
        arrayFilter.forEach(filterName => {
            const filter = jsonArray[filterName] as JsonFilter;
            const field = isDatalakeV3ToggleActive ? this.convertToDatalakeV3(filter.field) : filter.field;
            sqlString = sqlString.replace(`{${filterName}}`, `(${field} is not null or ${field} is null)`);
        });

        return sqlString;
    }

    private static convertToDatalakeV3 (value: string): string {
        // If it ends with ) as a function do not convert
        if (value.endsWith(')')) {
            return value;
        }
        let table = '';
        if (value.indexOf('.')) {
            table = value.substring(0, value.indexOf('.') + 1);
            value = value.substring(value.indexOf('.') + 1);
        }
        value = value.toLowerCase();
        if (value.startsWith('"') && value.endsWith('"')) {
            return table + value;
        }
        return table + '"' + value + '"';
    }

    private static preliminaryCheck(sql?: string): boolean {
        if (typeof sql === 'undefined' || sql === '') {
            return false;
        }
        let sqlSanitized = sql.replace(/\n/g, ' ');
        sqlSanitized = sqlSanitized.replace(/\s+/g, ' ');
        // check if there are a select * or is a empty string
        // We are assuming that there are no occurrences of the expression ", *" other than in select clause
        // to match select *
        let validationRegex = /(select\s+\*)|(,\s*\*\s*,?)|(^\s+$)/i;
        let match = sqlSanitized.match(validationRegex);

        if (match) {
            return false;
        }

        validationRegex = new RegExp('(\b(alter table)\b)|(\b(create database)\b)|(\b(create table)\b)|(\b(create view)\b)|(\b(drop database)\b)|(\b(drop table)\b)|(\b(drop view)\b)|(\b(msck repair table)\b)|(\b(show columns)\b)|(\b(show create table)\b)|(\b(show create view)\b)|(\b(show databases)\b)|(\b(show partitions)\b)|(\b(show tables)\b)|(\b(show tblproperties)\b)|(\b(show views)\b)|(\b(inser into)\b)', 'i');
        match = sqlSanitized.match(validationRegex);

        return !match;
    }

    /**
     * Return a result of query
     *
     * @param sessionManager
     * @param sql
     * @param json
     * @param limitQuery
     */
    static async getQueryExecutionIdBySql(sessionManager: SessionManager, sql: string, json?: string, limitQuery?: boolean): Promise<object> {
        const isDatalakeV3ToggleActive = sessionManager.platform.isDatalakeV3ToggleActive();
        const sqlString = this.getRunnableQuery(isDatalakeV3ToggleActive, sql, json);
        if (limitQuery) {
            sql = 'select * from (' + sqlString + ') limit ' + LIMIT_QUERY_QUERY_BUILDER_V2;
        }
        if (isDatalakeV3ToggleActive) {
            const snowflake = sessionManager.getSnowflake();
            let lastQueryId = '';
            try {
                lastQueryId = await snowflake.runQuery(sql, false, true, true);
            } catch (error: any) {
                throw new BadRequestException(error.message);
            }
            return {QueryExecutionId: lastQueryId};
        }
        const athenaExpress = this.getAthenaConnection(sessionManager);
        try {
            return await athenaExpress.query(sql);
        } catch (error: any) {
            throw new BadRequestException(error.message);
        }
    }

    /**
     * Return a result of query
     *
     * @param sessionManager
     * @param queryExecutionId
     */
    static async getSqlResult(sessionManager: SessionManager, queryExecutionId: string): Promise<object> {
        let results: any;
        if (sessionManager.platform.isDatalakeV3ToggleActive()) {
            try {
                const snowflake = sessionManager.getSnowflake();
                const queryStatus = await snowflake.checkStatusOfQueryV3(queryExecutionId);
                if (queryStatus.IsError) {
                    throw new Error('Error executing query');
                }
                if (queryStatus.IsRunning) {
                    return { queryStatus: ExportStatuses.RUNNING, result: [] };
                }
                results = await snowflake.getResultFromQueryID(queryExecutionId);
            } catch (error: any) {
                throw new BadRequestException(error.message);
            }

            return {queryStatus: ExportStatuses.SUCCEEDED, result: results};
        }
        const athena = sessionManager.getAthena();
        const status = await athena.checkQueryStatus(queryExecutionId) as GetQueryExecutionOutput;
        const queryExecutionStatus = status && status.QueryExecution && status.QueryExecution.Status ? status.QueryExecution.Status.State : '';

        try {
            if (queryExecutionStatus === ExportStatuses.QUEUED || queryExecutionStatus === ExportStatuses.RUNNING) {
                return {queryStatus: queryExecutionStatus, result: []};
            } else if (queryExecutionStatus === ExportStatuses.FAILED) {
                const errorMessage = status && status.QueryExecution && status.QueryExecution.Status && status.QueryExecution.Status.StateChangeReason ? status.QueryExecution.Status.StateChangeReason : '';
                throw new Error(errorMessage);
            }

            results = await athena.getQueryResult(queryExecutionId);
            const resultArray = athena.getQueryResultsAsArray(results);

            return {queryStatus: queryExecutionStatus, result: resultArray};
        } catch (error: any) {
            throw new BadRequestException(error.message);
        }
    }

    /**
     * Update only field present in body
     *
     * @param customReportTypeDetail
     * @param body
     */
    static updateCustomReportTypes(customReportTypeDetail: CustomReportTypeDetail, body: any): CustomReportTypeDetail {
        if (body.name !== undefined) {
            customReportTypeDetail.name = body.name;
        }

        if (body.description !== undefined) {
            customReportTypeDetail.description = body.description;
        }

        if (body.status !== undefined) {
            customReportTypeDetail.status = parseInt(body.status, 10);
        }

        if (body.sql !== undefined) {
            customReportTypeDetail.sql = body.sql;
        }

        if (body.json !== undefined) {
            customReportTypeDetail.json = body.json;
        }

        return customReportTypeDetail;
    }

    /**
     * Save key created by combination to customReportId + queryExecutionId in redis
     * @param sessionManager
     * @param queryExecutionId
     * @param customReportTypeId
     */
    static async saveQueryExecutionIdOnRedis(sessionManager: SessionManager, queryExecutionId: string, customReportTypeId: string) {
        const redis = redisFactory.getRedis();
        const key = customReportTypeId + ' - ' + queryExecutionId;
        return redis.writeValue(key, '', 4);
    }

    /**
     * Check if query execution id is associated to customReportTypeId and it is valid yet
     * @param sessionManager
     * @param queryExecutionId
     * @param customReportTypeId
     */
    static async checkQueryExecutionIsValid(sessionManager: SessionManager, queryExecutionId: string, customReportTypeId: string): Promise<string | null> {
        const redis = redisFactory.getRedis();
        const key = customReportTypeId + ' - ' + queryExecutionId;
        return redis.getValue(key, 4);
    }

    static async getReportsByCustomReportType(sessionManager: SessionManager, customReportTypeId: string): Promise<InformationReport[]> {
        const dynamo = sessionManager.getDynamo();
        await dynamo.getCustomReportTypesById(customReportTypeId);
        return dynamo.getReportsByCustomReportType(customReportTypeId);
    }

    /**
     * Fill the query with the json filters (if are present)
     * @param isDatalakeV3ToggleActive
     * @param sql
     * @param json
     * @param validate
     * @private
     */
    public static getRunnableQuery(isDatalakeV3ToggleActive: boolean, sql?: string, json?: string, validate = true): string {
        if (validate) {
            if (!this.preliminaryCheck(sql)) {
                throw new BadRequestException('Syntax not valid', ErrorCode.WRONG_SQL);
            }
        }

        let sqlString = sql ?? '';

        const re = new RegExp('{(.*?)}', 'gm');

        const matches = sqlString.match(re);
        const matchesLength = matches?.length ?? 0;
        if (validate) {
            this.validateJsonArea(matchesLength, <string> json);
        }


        if (json) {
            sqlString = this.getSqlWithFilterFilled(isDatalakeV3ToggleActive, json, matches, matchesLength, sqlString);
        }

        return sqlString;
    }
    /**
     * Override the athena default configuration
     *
     * @param sessionManager
     * @param additionalConfig
     * @private
     */
    private static getAthenaConnection(sessionManager: SessionManager, additionalConfig?: object): AthenaExpress {
        const athena = sessionManager.getAthena();
        const athenaExpress = athena.connection;
        athenaExpress.config = {
            ...athenaExpress.config,
            ...{waitForResults: false, getStats: true},
            ...additionalConfig
        };
        return athenaExpress;
    }

    /**
     * Remove extra semicolon and spaces
     * Example:
     * select "idst;" from (select idst as "idst;" from core_user where userid like 'name;surname');
     * Return:
     * select "idst;" from (select idst as "idst;" from core_user where userid like 'name;surname')
     * @param sql
     */
    public static removeExtrasSemicolon(sql: string): string {
        if (!sql) {
            return '';
        }
        if (sql.trim().endsWith(';')) {
            return sql.trim().slice(0, -1);
        }

        return sql.trim();
    }
}
