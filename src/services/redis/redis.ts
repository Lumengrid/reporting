import PlatformManager, { PlatformSettingsRedis } from '../session/platform-manager.session';
import { RedisMissingMandatoryKeyException, RedisConnectionException } from './redis.exceptions';
import { SessionLoggerService } from '../logger/session-logger.service';
import { AamonRedisClient } from './AamonRedisClient';
import { TTL_QUERY_EXECUTION_ID } from '../../shared/constants';
import { AbstractCache } from '../cache/AbstractCache';

type KVObject = {[key: string]: string};

export enum RedisMatchPattern {
    AAMON = 'AAMON_*',
    AMAZON = 'AMAZON_AWS_*',
    LOGGLY = 'LOGGLY_*',
    SNOWFLAKE = 'SNOWFLAKE_*',
    SYSLOG = 'SYSLOG_*',
}

export type LogglyParamsType = {
    LOGGLY_INPUT_KEY: string;
    LOGGLY_LOG_HEADERS: string;
    LOGGLY_TAG: string;
    LOGGLY_ENABLED: string;
    LOGGLY_LEVELS: string;
};

export type SyslogParamsType = {
    SYSLOG_ENABLED: string;
    SYSLOG_REMOTE_SERVER: string;
    SYSLOG_FACILITY: string;
    SYSLOG_REMOTE_PORT: string;
    SYSLOG_REMOTE_TIMEOUT: string;
    SYSLOG_LEVELS: string;
    SYSLOG_IDENTITY: string;
};

class Configuration {
    public constructor(
      private readonly config: KVObject,
      private logger: SessionLoggerService,
    ) {
    }

    public getParam(paramName: string): string | undefined {
        return this.config[paramName];
    }

    public getMandatoryParam(paramName: string): string {
        const paramValue = this.getParam(paramName);

        if (paramValue === undefined) {
            this.logger.error(`Redis global parameter not found! Parameter was: ${paramName}`);
            throw new RedisMissingMandatoryKeyException(paramName);
        }

        return paramValue;
    }
}

export class Redis {
    private logger: SessionLoggerService;
    private commonHash = 'platform_main_config:common';

    public static NewInstance(
      redisClient: AamonRedisClient,
      cache: AbstractCache,
    ): Redis {
        return new Redis(redisClient, cache);
    }

    private constructor(
      private readonly redisClient: AamonRedisClient,
      private readonly cache: AbstractCache,
    ) {
    }

    public async writeValue(key: string, value: string, db: number): Promise<void> {
        return this.redisClient.sendCommand('SET', [key, value, 'EX', `${TTL_QUERY_EXECUTION_ID}`], db);
    }

    public async getValue(key: string, db: number): Promise<string | null> {
        return this.redisClient.sendCommand('GET', [key], db);
    }

    public async hget(hash: string, key: string, db: number): Promise<string> {
        return this.redisClient.sendCommand(`HGET`, [hash, key], db);
    }

    private async hscanDB(hashMapName: string, matchPattern: string, db: number): Promise<string[]> {
        const result = await this.redisClient.sendCommand(
          'HSCAN',
          [hashMapName, `0`, 'MATCH', matchPattern, 'COUNT', `1000`],
          db
        );

        return result[1];
    }

    /**
     * Loads all the items in platform_main_config:common (uses internal in-memory cache)
     * @private
     */
    private async loadGeneralParamsFromCache(): Promise<KVObject> {
        const cacheKey = this.commonHash;

        let values: KVObject = this.cache.get(cacheKey) as KVObject | undefined;

        if (!values) {

            const [
                redisAamonParamScanResult,
                redisAmazonParamScanResult,
                redisSnowflakeParamScanResult,
            ] = await Promise.all([
                this.scanPlatformParams(RedisMatchPattern.AAMON),
                this.scanPlatformParams(RedisMatchPattern.AMAZON),
                this.scanPlatformParams(RedisMatchPattern.SNOWFLAKE),
            ]);

            values = this.convertFlatRedisArrayToObject([
              ...redisAamonParamScanResult,
              ...redisAmazonParamScanResult,
              ...redisSnowflakeParamScanResult,
            ]);

            this.cache.set(cacheKey, values, 60 * 5);
        }

        return values;
    }

    /**
     * Loads platform-specific items in platform_main_config:<platform> (uses internal in-memory cache)
     * @private
     */
    private async loadPlatformOverriddenParamsFromCache(platform: string): Promise<KVObject> {
        const cacheKey = `platform_main_config:${platform}`;

        let values = this.cache.get(cacheKey) as KVObject | undefined;

        if (!values) {

            const [
                redisAamonParamScanResultPlatform,
                redisAmazonParamScanResultPlatform,
                redisSnowflakeParamScanResultPlatform
            ] = await Promise.all([
                this.scanPlatformParams(RedisMatchPattern.AAMON, platform),
                this.scanPlatformParams(RedisMatchPattern.AMAZON, platform),
                this.scanPlatformParams(RedisMatchPattern.SNOWFLAKE, platform)
            ]);

            values = this.convertFlatRedisArrayToObject([
                ...redisAamonParamScanResultPlatform,
                ...redisAmazonParamScanResultPlatform,
                ...redisSnowflakeParamScanResultPlatform,
            ]);

            this.cache.set(cacheKey, values, 60 * 5);
        }

        return values;
    }

    /**
     * Loads platform-specific params from DB10 (makes use of internal in-memory cache)
     * @param platform
     * @private
     */
    private async loadPlatformParamsFromCache(platform: string): Promise<KVObject> {
        const cacheKey = `db10:${platform}`;

        let values = this.cache.get(cacheKey) as KVObject | undefined;

        if (!values) {
            const redisClientParamScanResult: string[] = await this.scanRedisPlatformParams(platform);
            values = this.convertFlatRedisArrayToObject(redisClientParamScanResult);
            this.cache.set(cacheKey, values, 60 * 5);
        }

        return values;
    }

    /**
     * Check if hash key exists in redis db
     * @param key
     * @param db
     */
    public async exists(key: string, db: number): Promise<boolean> {
        return Boolean(await this.redisClient.sendCommand('EXISTS', [key], db));
    }

    /**
     * Check if hash key exists in redis db
     * @param hash
     * @param key
     * @param db
     */
    public async hashKeyExists(hash: string, key: string, db: number): Promise<boolean> {
        return Boolean(await this.redisClient.sendCommand('HEXISTS', [hash, key], db));
    }

    /**
     * Check the validity of a domain against redis settings DB
     * @param domain Hostname to check
     * @returns True if the domain was found in redis config, False otherwise
     */
    public async checkValidDomain(domain: string): Promise<boolean> {
        if (domain.startsWith('www.')) {
            domain = domain.slice(4);
        }

        return this.exists(domain, 10);
    }

    public async getConfigs(platform: string): Promise<PlatformSettingsRedis> {
        // Scan and save global Aamon settings
        const commonParams = await this.getRedisCommonParams(platform);

        // Scan and save client specific Aamon settings
        const platformParams = await this.getRedisPlatformParams(platform);

        const queryBuilderAdmins = JSON.parse(platformParams.queryBuilderAdmins);
        const queryBuilderAdminsV3 = JSON.parse(platformParams.queryBuilderAdminsV3);

        // Build js object
        return {
            // Global keys
            dynamoDbRegion: commonParams.dynamoDbRegion,
            athenaRegion: commonParams.athenaRegion,
            athenaS3Path: commonParams.athenaS3Path,
            athenaS3ExportPath: commonParams.athenaS3ExportPath,
            s3Region: commonParams.s3Region,
            s3Bucket: commonParams.s3Bucket,
            schedulationPrivateKey: commonParams.schedulationPrivateKey,
            customReportTypesTableName: commonParams.customReportTypesTableName,
            athenaSchemaNameOverride: platformParams.athenaSchemaNameOverride,
            dbHostOverride: platformParams.dbHostOverride,
            ignoreOrderByClause: platformParams.ignoreOrderByClause === '1',
            dynamoDbPlatform: platformParams.dynamoDbPlatform,
            originalDomain: platformParams.originalDomain,
            queryBuilderAdmins,
            queryBuilderAdminsV3,
            aamonDatalakeNightlyRefreshTimeout: platformParams.aamonDatalakeNightlyRefreshTimeout,
            datalakeV2DataBucket: commonParams.datalakeV2DataBucket,
            datalakeV2Host: platformParams.datalakeV2Host,
            platformRegion: commonParams.platformRegion,
            mysqlDbName: platformParams.mysqlDbName,
            dbHost: platformParams.hostReplicaDb,
            mainDbHost: platformParams.mainDbHost,
            snowflakeLocator: commonParams.snowflakeLocator,
            snowflakeUsername: commonParams.snowflakeUsername,
            snowflakePassword: commonParams.snowflakePassword,
            snowflakeDatabase: commonParams.snowflakeDatabase,
            snowflakeWarehouse: commonParams.snowflakeWarehouse,
            snowflakeRole: commonParams.snowflakeRole,
            snowflakeStorageIntegration: commonParams.snowflakeStorageIntegration,
            snowflakeStorageIntegrationBucket: commonParams.snowflakeStorageIntegrationBucket,
            snowflakeSchema: platformParams.snowflakeSchema,
            snowflakeDbHost: platformParams.snowflakeDbHost,
            snowflakeLockTable: commonParams.snowflakeLockTable,
            snowflakeLogLevel: platformParams.snowflakeLogLevel,
            snowflakePoolMin: commonParams.snowflakePoolMin,
            snowflakePoolMax: commonParams.snowflakePoolMax,
            snowflakeClientSessionKeepAliveEnabled: commonParams.snowflakeClientSessionKeepAliveEnabled,
            snowflakeClientSessionKeepAliveFrequency: commonParams.snowflakeClientSessionKeepAliveFrequency,
            snowflakeAcquireTimeout: commonParams.snowflakeAcquireTimeout,
            snowflakeDefaultSchema: commonParams.snowflakeDefaultSchema
        };
    }

    public async getRedisCommonParams(platform?: string): Promise<{[key: string]: string}> {
        const generalParams = await this.loadGeneralParamsFromCache();
        let platformParams: KVObject = {};

        if (platform) {
            platformParams = await this.loadPlatformOverriddenParamsFromCache(platform);
        }

        const config = new Configuration({
              ...generalParams,
              ...platformParams,
          },
          this.logger,
        );

        return {
            dynamoDbRegion: config.getParam('AAMON_DYNAMO_REGION') ?? config.getMandatoryParam('AMAZON_AWS_REGION'),
            athenaRegion: config.getParam('AAMON_ATHENA_REGION') ?? config.getMandatoryParam('AMAZON_AWS_REGION'),
            athenaS3Path: config.getMandatoryParam('AAMON_ATHENA_S3_PATH'),
            athenaS3ExportPath: config.getMandatoryParam('AAMON_ATHENA_S3_EXPORT_PATH'),
            s3Region: config.getParam('AAMON_S3_REGION') ?? config.getMandatoryParam('AMAZON_AWS_REGION'),
            s3Bucket: config.getParam('AAMON_S3_BUCKET'),
            schedulationPrivateKey: config.getMandatoryParam('AAMON_SCHEDULATION_PRIVATE_KEY'),
            customReportTypesTableName: config.getParam('AAMON_QUERY_BUILDER_TABLE') ?? 'custom_report_types',
            platformRegion: config.getMandatoryParam('AMAZON_AWS_REGION'),
            datalakeV2DataBucket: config.getParam('AAMON_DATALAKE_DATA_BUKET') ?? 'datalake-aamon-data-bi-eu-west-1',
            snowflakeLocator: config.getParam('SNOWFLAKE_LOCATOR') ?? '',
            snowflakeUsername: config.getParam('SNOWFLAKE_USERNAME') ?? '',
            snowflakePassword: config.getParam('SNOWFLAKE_PASSWORD') ?? '',
            snowflakeDatabase: config.getParam('SNOWFLAKE_DATABASE') ?? '',
            snowflakeWarehouse: config.getParam('SNOWFLAKE_WAREHOUSE') ?? '',
            snowflakeRole: config.getParam('SNOWFLAKE_ROLE') ?? '',
            snowflakeStorageIntegration: config.getParam('SNOWFLAKE_STORAGE_INTEGRATION') ?? '',
            snowflakeStorageIntegrationBucket: config.getParam('SNOWFLAKE_STORAGE_INTEGRATION_BUCKET') ?? '',
            snowflakeLockTable: config.getParam('SNOWFLAKE_LOCK_TABLE') ?? '',
            snowflakePoolMin: config.getParam('SNOWFLAKE_POOL_MIN') ?? '',
            snowflakePoolMax: config.getParam('SNOWFLAKE_POOL_MAX') ?? '',
            snowflakeClientSessionKeepAliveEnabled: config.getParam('SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE_ENABLED') ?? '',
            snowflakeClientSessionKeepAliveFrequency: config.getParam('SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE_FREQUENCY') ?? '',
            snowflakeAcquireTimeout: config.getParam('SNOWFLAKE_ACQUIRE_TIMEOUT') ?? '',
            snowflakeTimeout: config.getParam('SNOWFLAKE_TIMEOUT') ?? '',
            snowflakeDefaultSchema: config.getParam('SNOWFLAKE_DEFAULT_SCHEMA') ?? '',
            snowflakeLogLevel: config.getParam('SNOWFLAKE_LOG_LEVEL') ?? '',
        };
    }

    public async getRedisPlatformParams(platform: string): Promise<KVObject> {
        const values = await this.loadPlatformParamsFromCache(platform);
        const config = new Configuration(values, this.logger);

        return {
            // Client specific keys (lower-case)
            athenaSchemaNameOverride: config.getParam('aamon_athena_schema_name_override') ?? '',
            dbHostOverride: config.getParam('aamon_db_host_override') || '',
            mysqlDbName: config.getParam('db_name') || '',
            ignoreOrderByClause: config.getParam('aamon_ignore_order_by_clause') || '',
            dynamoDbPlatform: config.getParam('aamon_dynamo_db_platform') || '',
            originalDomain: config.getParam('original_domain') || '',
            queryBuilderAdmins: config.getParam('query_builder_admins_v2') || '[]',
            queryBuilderAdminsV3: config.getParam('query_builder_admins_v3') || '[]',
            aamonDatalakeNightlyRefreshTimeout: config.getParam('aamon_datalake_nightly_refresh_timeout') || '450', // 7 hours 30 minutes
            hostReplicaDb: config.getParam('report_host') || config.getParam('db_host'),
            mainDbHost: config.getParam('db_host'),
            datalakeV2Host: config.getParam('datalake_host') || '',
            snowflakeSchema: config.getParam('snowflake_schema') || '',
            snowflakeDbHost: config.getParam('snowflake_db_host') || '',
        };
    }

    // get the loggly keys from redis
    public async getLogglyParams(): Promise<LogglyParamsType> {
        const response = await this.hscanDB('platform_main_config:common', RedisMatchPattern.LOGGLY, 0);
        return this.convertFlatRedisArrayToObject(response) as LogglyParamsType;
    }

    // get the syslog keys from redis
    public async getSyslogParams(): Promise<SyslogParamsType> {
        const response = await this.hscanDB('platform_main_config:common', RedisMatchPattern.SYSLOG, 0);
        return this.convertFlatRedisArrayToObject(response) as SyslogParamsType;
    }

    // get the API_GATEWAY key from redis
    public async getAPIGatewayParam(platform: string): Promise<string> {
        const platformHash = `platform_main_config:${platform}`;
        const key = 'API_GATEWAY';

        if (await this.exists(platformHash, 0)) {
            const apiGateway = await this.hget(platformHash, key, 0);

            if (apiGateway) {
                return apiGateway;
            }
        }

        return this.hget(this.commonHash, key, 0);
    }

    public async getAPIGatewayPortParam(platform: string): Promise<number> {
        const platformHash = `platform_main_config:${platform}`;
        const key = 'API_GATEWAY_PORT';

        if (await this.exists(platformHash, 0)) {
            const port = await this.hget(platformHash, key, 0);

            if (port) {
                return parseInt(port);
            }
        }

        return parseInt(await this.hget(this.commonHash, key, 0));
    }

    public async getAPIGatewaySSLVerify(platform: string): Promise<number> {
        const platformHash = `platform_main_config:${platform}`;
        const key = 'API_GATEWAY_SSL_VERIFY';

        if (await this.exists(platformHash, 0)) {
            const sslVerify = await this.hget(platformHash, key, 0);

            if (sslVerify) {
                return parseInt(sslVerify);
            }
        }

        return parseInt(await this.hget(this.commonHash, key, 0));
    }

    private convertFlatRedisArrayToObject(flatRedisArray: readonly string[]): KVObject {
        const convertedObject: KVObject = {};

        for (let i = 0; i < flatRedisArray.length; i += 2) {
            convertedObject[flatRedisArray[i]] = flatRedisArray[i + 1];
        }

        return convertedObject;
    }

    /**
     * Scans a redis database for client specific settings
     * TODO: implement a proper iterator for the hscan method
     */
    private async scanRedisPlatformParams(hashMapName: string): Promise<string[]> {
        try {
            return await this.hscanDB(hashMapName, '*', 10);
        } catch (error: any) {
            this.logger.errorWithStack('Error while reading platform config from Redis! ', error);
            throw new RedisConnectionException();
        }
    }

    /**
     * Scans a redis database for global specific settings
     * If the platform param is specified, then platform-specific values are loaded from redis
     */
    private async scanPlatformParams(matchPattern: string, platform?: string): Promise<string[]> {
        const hashmapName = platform
            ? `platform_main_config:${platform}`
            : `platform_main_config:common`;

        try {
            return await this.hscanDB(hashmapName, matchPattern, 0);
        } catch (error: any) {
            this.logger.errorWithStack('Error while reading common config from Redis! ', error);
            throw new RedisConnectionException();
        }
    }

    public async saveQueryBuilderAdmins(platform: PlatformManager, admins: string[]) {
        const field = platform.isDatalakeV3ToggleActive()
          ? 'query_builder_admins_v3'
          : 'query_builder_admins_v2';

        return this.redisClient.sendCommand(
          'HSET',
          [
            platform.getPlatformBaseUrl(),
              field,
              JSON.stringify(admins),
            ],
          10
        );
    }

    /**
     * Check first in the DB 0 and then in DB 10
     * @param platform
     */
    public async getEnableInternalApi(platform: string): Promise<boolean> {
        const ENABLE_INTERNAL_API = 'ENABLE_INTERNAL_API';

        return await this.hget('platform_main_config:common', ENABLE_INTERNAL_API, 0) === '1'
            || await this.hget(platform, ENABLE_INTERNAL_API.toLowerCase(), 10) === '1';
    }

    /**
     * Save the combination 'category + userId + queryExecutionId' on db 4 of redis
     * @param userId user that ran the query
     * @param queryExecutionId queryExecutionId returned by athena
     * @param categoryType optional resource context
     */
    public async saveQueryExecutionIdOnRedis(userId: number, queryExecutionId: string, categoryType = ''): Promise<void> {
        const key = Redis.buildKey(userId, queryExecutionId, categoryType);
        await this.writeValue(key, '', 4);
    }

    /**
     * Check if exists the combination 'category + userId + queryExecutionId' on db 4 of redis
     * @param userId
     * @param queryExecutionId
     * @param categoryType
     */
    public async existsQueryExecutionIdOnRedis(userId: number, queryExecutionId: string, categoryType = ''): Promise<boolean> {
        const key = Redis.buildKey(userId, queryExecutionId, categoryType);
        return this.exists(key, 4);
    }

    /**
     * @param platform Base url of the platform
     * @param route API end-point to check
     * @returns TRUE if the end-point is whitelisted, FALSE otherwise
     */
    public async checkWhitelistedAPI(platform: string, route: string, method: string): Promise<boolean> {
        const hash = platform + ':hydra_api_whitelist:analytics';
        route = route.replace(/.*\/v1\//g, '');
        return this.hashKeyExists(hash, method + '@' + route, 8);
    }

    public async getLegacyAuditTrailLogsDBName(): Promise<string> {
        return await this.hget('platform_main_config:common', 'LEGACY_AUDIT_TRAIL_AWS_ATHENA_DB_NAME', 0);
    }

    public async getLegacyAuditTrailLogsTableName(): Promise<string> {
        return await this.hget('platform_main_config:common', 'LEGACY_AUDIT_TRAIL_AWS_ATHENA_TABLE_NAME', 0);
    }

    private static buildKey(userId: number, queryExecutionId: string, categoryType = '') {
        if (categoryType !== '') {
            categoryType = categoryType + ':';
        }

        return categoryType + userId.toString() + ':' + queryExecutionId;
    }
}
