import {
    SessionResponse,
    SessionResponseDataPlugins,
    SessionResponseDataPlatformConfigs,
    SessionResponseDataToggles
} from '../hydra';
import { ExportLimit, EntitiesLimits } from '../../models/report-manager';
import { InvalidSessionConfigsException } from '../../exceptions/invalidSessionConfigsException';
import { InvalidSessionPluginsException } from '../../exceptions/invalidSessionPluginsException';
import { InvalidSessionPlatformBaseUrlException } from '../../exceptions/invalidSessionPlatformBaseUrlException';
import { InvalidSessionDataException } from '../../exceptions/invalidSessionDataException';
import { InvalidSessionPlatformException } from '../../exceptions/invalidSessionPlatformException';
import { ErrorCode } from '../../exceptions/error-codes.enum';
import { ReportsSettings } from '../../models/base';
import { PlatformRefreshTokens } from '../../reports/interfaces/tokens.interface';
import { MONTHLY_REFRESH_TOKENS, DAILY_REFRESH_TOKENS } from '../../shared/constants';
import { SessionLoggerService } from '../logger/session-logger.service';
import { Exception } from '../../exceptions/exception';

export interface PlatformSettings {
    // Dynamo settings
    dynamoDbRegion: string;
    dynamoDbPlatform: string;
    customReportTypesTableName: string;
    // Athena settings
    athenaRegion: string;
    athenaS3Path: string;
    athenaS3ExportPath: string;
    athenaSchemaNameOverride: string;
    dbHostOverride: string;
    ignoreOrderByClause: boolean;
    mysqlDbName: string;
    dbHost: string;
    mainDbHost: string;
    // S3 settings
    s3Region: string;
    s3Bucket: string;
    schedulationPrivateKey: string;
    // Main platform
    originalDomain: string;
    queryBuilderAdmins: string[];
    queryBuilderAdminsV3: string[];

    // Parametric settings
    // Query settings
    csvExportLimit: number;
    xlxExportLimit: number;
    previewExportLimit: number;
    // Entities settings
    entityUsersLimit: number;
    entityGroupsLimit: number;
    entityBranchesLimit: number;
    entityCoursesLimit: number;
    entityLPLimit: number;
    entityCourseInstructorsLimit: number;
    entityBadgesLimit: number;
    entityCertificationsLimit: number;
    entityClassroomLimit: number;
    entityWebinarLimit: number;
    entityAssetsLimit: number;
    entityChannelsLimit: number;
    entitySessionsLimit: number;
    entitySurveysLimit: number;
    // Token settings
    monthlyRefreshTokens: number;
    dailyRefreshTokens: number;
    // Timeout limit for Night Refresh
    aamonDatalakeNightlyRefreshTimeout: string;
    // Datalake V2 data expiration
    datalakeV2ExpirationTime: number;
    // Report extraction time limit
    extractionTimeLimit: number;
    datalakeV2DataBucket: string;
    datalakeV2Host: string;
    platformRegion: string;
    // Snowflake settings
    snowflakeLocator: string;
    snowflakeUsername: string;
    snowflakePassword: string;
    snowflakeDatabase: string;
    snowflakeWarehouse: string;
    snowflakeRole: string;
    snowflakeStorageIntegration: string;
    snowflakeStorageIntegrationBucket: string;
    snowflakeSchema: string;
    snowflakeDbHost: string;
    snowflakeTimeout: number;
    snowflakeLockTable: string;
    snowflakeLogLevel: string;
    snowflakePoolMin: string;
    snowflakePoolMax: string;
    snowflakeClientSessionKeepAliveEnabled: string;
    snowflakeClientSessionKeepAliveFrequency: string;
    snowflakeAcquireTimeout: string;
    snowflakeDefaultSchema: string;
}

export interface PlatformSettingsRedis {
    // Dynamo settings
    dynamoDbRegion: string;
    dynamoDbPlatform: string;
    customReportTypesTableName: string;
    // Athena settings
    athenaRegion: string;
    athenaS3Path: string;
    athenaS3ExportPath: string;
    athenaSchemaNameOverride: string;
    dbHostOverride: string;
    ignoreOrderByClause: boolean;
    mysqlDbName: string;
    dbHost: string;
    mainDbHost: string;
    // S3 settings
    s3Region: string;
    s3Bucket: string;
    schedulationPrivateKey: string;
    // Main platform
    originalDomain: string;
    queryBuilderAdmins: string[];
    queryBuilderAdminsV3: string[];
    aamonDatalakeNightlyRefreshTimeout: string;
    datalakeV2DataBucket: string;
    datalakeV2Host: string;
    platformRegion: string;
    // Snowflake settings
    snowflakeLocator: string;
    snowflakeUsername: string;
    snowflakePassword: string;
    snowflakeDatabase: string;
    snowflakeWarehouse: string;
    snowflakeRole: string;
    snowflakeStorageIntegration: string;
    snowflakeStorageIntegrationBucket: string;
    snowflakeSchema: string;
    snowflakeDbHost: string;
    snowflakeLockTable: string;
    snowflakeLogLevel: string;
    snowflakePoolMin: string;
    snowflakePoolMax: string;
    snowflakeClientSessionKeepAliveEnabled: string;
    snowflakeClientSessionKeepAliveFrequency: string;
    snowflakeAcquireTimeout: string;
    snowflakeDefaultSchema: string;
}

export enum SnowflakeDefaults {
    TIMEOUT = 1800000,
    POOL_MIN = 1,
    POOL_MAX = 160,
    CLIENT_SESSION_KEEP_ALIVE_ENABLED = 0,
    CLIENT_SESSION_KEEP_ALIVE_FREQUENCY = 3600,
    ACQUIRE_TIMEOUT = 180000,
    DEFAULT_SCHEMA = 'PUBLIC'
}

export default class PlatformManager {
    private SAAS_HOUR_DATALAKE_V2_EXPIRATION_TIME = 24; // in HOURS
    private STAGING_HOUR_DATALAKE_V2_EXPIRATION_TIME = 8; // in HOURS
    private ECS_HOUR_DATALAKE_V2_EXPIRATION_TIME = 4; // in HOURS
    private DATALAKE_V2_EXPIRATION_TIME = 24; // in HOURS
    private platformBaseUrl: string;
    private defaultLanguage: string;
    private defaultLanguageCode: string;
    private settings: PlatformSettings;
    private configs: SessionResponseDataPlatformConfigs;
    private plugins: SessionResponseDataPlugins;
    private toggles: SessionResponseDataToggles;
    private logger: SessionLoggerService;

    public constructor(session?: SessionResponse) {
        this.platformBaseUrl = '';
        this.defaultLanguage = '';
        this.defaultLanguageCode = '';
        this.settings = {
            dynamoDbRegion: '',
            dynamoDbPlatform: '',
            customReportTypesTableName: '',
            athenaRegion: '',
            athenaS3Path: '',
            athenaS3ExportPath: '',
            athenaSchemaNameOverride: '',
            dbHostOverride: '',
            ignoreOrderByClause: false,
            s3Region: '',
            s3Bucket: '',
            schedulationPrivateKey: '',
            originalDomain: '',
            queryBuilderAdmins: [],
            queryBuilderAdminsV3: [],
            csvExportLimit: 2000000,
            xlxExportLimit: 1000000,
            previewExportLimit: 100,
            entityUsersLimit: 100,
            entityGroupsLimit: 50,
            entityBranchesLimit: 50,
            entityCoursesLimit: 50,
            entityLPLimit: 100,
            entityCourseInstructorsLimit: 50,
            entityBadgesLimit: 100,
            entityClassroomLimit: 100,
            entityWebinarLimit: 100,
            entitySessionsLimit: 100,
            entityCertificationsLimit: 100,
            entityAssetsLimit: 100,
            entityChannelsLimit: 100,
            entitySurveysLimit: 50,
            monthlyRefreshTokens: MONTHLY_REFRESH_TOKENS,
            dailyRefreshTokens: DAILY_REFRESH_TOKENS,
            aamonDatalakeNightlyRefreshTimeout: '',
            datalakeV2ExpirationTime: 0,
            extractionTimeLimit: 0,
            datalakeV2DataBucket: '',
            datalakeV2Host: '',
            platformRegion: '',
            mysqlDbName: '',
            dbHost: '',
            mainDbHost: '',
            snowflakeLocator: '',
            snowflakeUsername: '',
            snowflakePassword: '',
            snowflakeDatabase: '',
            snowflakeWarehouse: '',
            snowflakeRole: '',
            snowflakeStorageIntegration: '',
            snowflakeStorageIntegrationBucket: '',
            snowflakeSchema: '',
            snowflakeDbHost: '',
            snowflakeTimeout: 0,
            snowflakeLockTable: '',
            snowflakeLogLevel: '',
            snowflakePoolMin: SnowflakeDefaults.POOL_MIN.toString(),
            snowflakePoolMax: SnowflakeDefaults.POOL_MAX.toString(),
            snowflakeClientSessionKeepAliveEnabled: SnowflakeDefaults.CLIENT_SESSION_KEEP_ALIVE_ENABLED.toString(),
            snowflakeClientSessionKeepAliveFrequency: SnowflakeDefaults.CLIENT_SESSION_KEEP_ALIVE_FREQUENCY.toString(),
            snowflakeAcquireTimeout: SnowflakeDefaults.ACQUIRE_TIMEOUT.toString(),
            snowflakeDefaultSchema: SnowflakeDefaults.DEFAULT_SCHEMA.toString(),
        };

        this.configs = {
            showFirstNameFirst: false,
            defaultPlatformTimezone: '',
            reportDownloadPermissionLink: false,
            isUserAddFieldsFiltersForManager: false,
            isLearningplansAssignmentTypeActive: false,
            isCoursesAssignmentTypeActive: false,
        };

        this.plugins = {
            certification: false,
            classroom: false,
            ecommerce: false,
            esignature: false,
            gamification: false,
            transcript: false,
            contentPartners: false,
            share: false,
            flow: false,
            flowMsTeams: false,
            multiDomain: false,
        };

        this.toggles = {
            toggleAdminReport: false,
            toggleNewContribute: false,
            toggleMyTeamUserAddFilter: false,
            toggleWebinarsEnableCreation: false,
            toggleForceDatalakeV1: true,
            toggleAudittrailLegacyArchive: false,
            toggleDatalakeV2ManualRefresh: false,
            toggleManagerReportXLSXPolling: false,
            toggleMultipleEnrollmentCompletions: false,
            toggleDatalakeV3: false,
            togglePrivacyPolicyDashboardOnAthena: false,
            toggleCoursesDashboardOnAthena: false,
            toggleBranchesDashboardOnAthena: false,
            toggleHydraMinimalVersion: false,
            toggleUsersLearningPlansReportEnhancement: false,
            toggleLearningPlansStatisticsReport: false,
            toggleNewLearningPlanManagement: false,
        };

        if (session) {
            this.validateSessionResponse(session);
            this.platformBaseUrl = session.data.platform.platformBaseUrl;
            this.defaultLanguage = session.data.platform.defaultLanguage;
            this.defaultLanguageCode = session.data.platform.defaultLanguageCode;
            this.configs = session.data.platform.configs;
            this.plugins = session.data.platform.plugins;
            this.toggles = session.data.platform.toggles ? session.data.platform.toggles : this.toggles;
        }
    }

    private hasSameKeys(obj1: Object, obj2: Object): boolean {
        const originalStrings = Object.keys(obj1);
        const currentStrings = Object.keys(obj2);

        if (originalStrings.length !== currentStrings.length) return false;

        return originalStrings.every((el: string) => {
            // @ts-ignore
            return typeof obj1[el] === typeof obj2[el];
        });
    }

    public isNewContributeToggleActive() {
        return this.toggles.toggleNewContribute;
    }
    public isDatalakeV2Active() {
        return !this.toggles.toggleForceDatalakeV1;
    }

    public isDatalakeV3ToggleActive() {
        return this.toggles.toggleDatalakeV3 && !this.toggles.toggleForceDatalakeV1;
    }

    public isPrivacyPolicyDashboardOnAthenaActive() {
        return this.toggles.togglePrivacyPolicyDashboardOnAthena;
    }

    public isCoursesDashboardOnAthenaActive() {
        return this.toggles.toggleCoursesDashboardOnAthena;
    }

    public isBranchesDashboardOnAthenaActive() {
        return this.toggles.toggleBranchesDashboardOnAthena;
    }

    public isDatalakeV2ManualRefreshToggleActive() {
        return this.toggles.toggleDatalakeV2ManualRefresh;
    }

    public isMyTeamUserAddFilterToggleActive() {
        return this.toggles.toggleMyTeamUserAddFilter;
    }

    public isAudittrailLegacyArchiveToggleActive() {
        return this.toggles.toggleAudittrailLegacyArchive;
    }

    public isHydraMinimalVersionToggleActive() {
        return this.toggles.toggleHydraMinimalVersion;
    }

    public isToggleWebinarsEnableCreation() {
        return this.toggles.toggleWebinarsEnableCreation;
    }

    public istoggleManagerReportXLSXPolling(): boolean {
        return this.toggles.toggleManagerReportXLSXPolling;
    }

    public isToggleMultipleEnrollmentCompletions() {
        return this.toggles.toggleMultipleEnrollmentCompletions;
    }

    public isToggleNewLearningPlanManagement(): boolean {
        return this.toggles.toggleNewLearningPlanManagement;
    }

    public isToggleUsersLearningPlansReportEnhancement(): boolean {
        return this.toggles.toggleUsersLearningPlansReportEnhancement;
    }

    public isToggleNewLearningPlanManagementAndReportEnhancement(): boolean {
        return this.toggles.toggleUsersLearningPlansReportEnhancement && this.toggles.toggleNewLearningPlanManagement;
    }

    public isToggleLearningPlansStatisticsReport(): boolean {
        return this.toggles.toggleLearningPlansStatisticsReport;
    }

    public isSessionResponseDataPlugins(plugins: SessionResponseDataPlugins): plugins is SessionResponseDataPlugins {
        return this.hasSameKeys(this.plugins, plugins);
    }

    public isSessionResponseDataPlatformConfigs(configs: SessionResponseDataPlatformConfigs): configs is SessionResponseDataPlatformConfigs {
        return this.hasSameKeys(this.configs, configs);
    }

    private validateSessionResponse(session: SessionResponse): void {
        if (!session.data) {
            throw new InvalidSessionDataException('Data not found in session', ErrorCode.SESSION_DATA_NOT_FOUND);
        }
        if (!session.data.platform) {
            throw new InvalidSessionPlatformException('Platform not found in data', ErrorCode.SESSION_DATA_PLATFORM_NOT_FOUND);
        }
        if (!session.data.platform.platformBaseUrl) {
            throw new InvalidSessionPlatformBaseUrlException('Base url not found or invalid in platform', ErrorCode.SESSION_DATA_PLATFORM_BASEURL_FOUND_OR_INVALID);
        }
        if (!session.data.platform.configs || !this.isSessionResponseDataPlatformConfigs(session.data.platform.configs)) {
            throw new InvalidSessionConfigsException('Configs not found or invalid in platform', ErrorCode.SESSION_DATA_PLATFORM_CONFIGS_NOT_FOUND_OR_INVALID);
        }
        if (!session.data.platform.plugins || !this.isSessionResponseDataPlugins(session.data.platform.plugins)) {
            throw new InvalidSessionPluginsException('Plugins not found or invalid in platform', ErrorCode.SESSION_DATA_PLATFORM_PLUGINS_NOT_FOUND_OR_INVALID);
        }
    }

    public getQueryBuilderAdmins(): string[] {
        return this.isDatalakeV3ToggleActive() ? this.settings.queryBuilderAdminsV3 : this.settings.queryBuilderAdmins;
    }

    public getPlatformBaseUrl(): string {
        return this.platformBaseUrl;
    }

    public getPlatformBaseUrlPath(): string {
        return this.platformBaseUrl.replace(/(\.|-)/g, '_');
    }

    public getDefaultLanguage(): string {
        return this.defaultLanguage;
    }

    public getDefaultLanguageCode(): string {
        return this.defaultLanguageCode;
    }

    public getAthenaSchemaName(): string {
        if (this.isDatalakeV2Active() === true) {
            const mysqlDbName = this.getMysqlDbName();
            if (mysqlDbName === '') {
                throw new Exception('Redis client parameter not found or is empty! Parameter was: db_name');
            }
            return mysqlDbName;
        }
        return this.getPlatformBaseUrlPath();
    }

    public loadSettings(settings: PlatformSettingsRedis) {
        this.settings = Object.assign(this.settings, settings);
        if (this.settings.originalDomain !== '') {
            this.platformBaseUrl = this.settings.originalDomain;
        }
    }

    public loadDynamoSettings(settings: ReportsSettings) {
        delete settings.platform;
        this.settings = Object.assign(this.settings, settings);
    }

    public getDynamoDbRegion(): string {
        return this.settings.dynamoDbRegion;
    }

    public getDynamoDbPlatform(): string {
        return this.settings.dynamoDbPlatform;
    }

    public getCustomReportTypesTableName(): string {
        return this.settings.customReportTypesTableName;
    }

    public getAthenaRegion(): string {
        return this.settings.athenaRegion;
    }

    public getAthenaS3Path(): string {
        return this.settings.athenaS3Path;
    }

    public getAthenaS3ExportPath(): string {
        return this.settings.athenaS3ExportPath;
    }

    public getAthenaSchemaNameOverride(): string {
        return this.settings.athenaSchemaNameOverride;
    }

    public getDbHostOverride(): string {
        return this.settings.dbHostOverride;
    }

    public getIgnoreOrderByClause(): boolean {
        return this.settings.ignoreOrderByClause;
    }

    public getS3Region(): string {
        return this.settings.s3Region;
    }

    public getS3Bucket(): string {
        return this.settings.s3Bucket;
    }

    public getAamonDatalakeNightlyRefreshTimeout(): string {
        return this.settings.aamonDatalakeNightlyRefreshTimeout;
    }

    public getSnowflakeDatabase(): string {
        return this.settings.snowflakeDatabase;
    }

    public getSnowflakeStorageIntegration(): string {
        return this.settings.snowflakeStorageIntegration;
    }

    public getSnowflakeStorageIntegrationBucket(): string {
        return this.settings.snowflakeStorageIntegrationBucket;
    }

    public getSnowflakeSchema(): string {
        return this.settings.snowflakeSchema;
    }

    public getSnowflakeDefaultSchema(): string {
        if (this.settings.snowflakeDefaultSchema === '') {
            return SnowflakeDefaults.DEFAULT_SCHEMA;
        }
        return this.settings.snowflakeDefaultSchema;
    }

    public getSnowflakeDbHost(): string {
        return this.settings.snowflakeDbHost;
    }

    public getMainDbHost(): string {
        return this.settings.mainDbHost;
    }

    public getSnowflakeLockTable(): string {
        return this.settings.snowflakeLockTable;
    }

    public getSnowflakeLogLevel(): string {
        return this.settings.snowflakeLogLevel;
    }

    public getDatalakeV2ExpirationTime(installationType = ''): number {
        const defaultExpirationTime = this.settings.datalakeV2ExpirationTime > 0 ? this.settings.datalakeV2ExpirationTime : this.DATALAKE_V2_EXPIRATION_TIME * 60 * 60;
        if (installationType === '') {
            return defaultExpirationTime;
        }

        if (this.settings.datalakeV2ExpirationTime > 0) {
            return defaultExpirationTime;
        }

        switch (installationType.toLowerCase()) {
            // saas:
            case 'trial':
            case 'smb':
            case 'demo':
            case 'internal':
            case 'sales_pre_release':
            case 'mhr_peoplefirst_predisposition':
            case 'mhr_itrent_predisposition':
            case 'mhr_peoplefirst_internal':
            case 'mhr_itrent_internal':
                return this.SAAS_HOUR_DATALAKE_V2_EXPIRATION_TIME * 60 * 60; // hours * minutes * seconds
            // staging:
            case 'staging':
            case 'mhr_peoplefirst_staging':
            case 'mhr_itrent_staging':
                return this.STAGING_HOUR_DATALAKE_V2_EXPIRATION_TIME * 60 * 60; // hours * minutes * seconds
            // ecs:
            case 'ecs':
            case 'large_enterprise':
            case 'mhr_itrent_production':
                return this.ECS_HOUR_DATALAKE_V2_EXPIRATION_TIME * 60 * 60; // hours * minutes * seconds
            default:
                return defaultExpirationTime;
        }
    }

    /**
     * Return the time limit of a report generation in minutes (default at 30 minutes)
     * @returns the number of minutes that a report had to finish the elaboration
     */
    public getExtractionTimeLimit(): number {
        return this.settings.extractionTimeLimit > 0 ? this.settings.extractionTimeLimit : 60;
    }

    public getDatalakeV2DataBucket(): string {
        return this.settings.datalakeV2DataBucket;
    }

    public getDatalakeV2Host(): string {
        return this.settings.datalakeV2Host;
    }

    public getPlatformRegion(): string {
        return this.settings.platformRegion;
    }

    public getShowFirstNameFirst(): boolean {
        return this.configs.showFirstNameFirst;
    }

    public getDefaultPlatformTimezone(): string {
        return this.configs.defaultPlatformTimezone;
    }

    public getReportDownloadPermissionLink(): boolean {
        return this.configs.reportDownloadPermissionLink;
    }

    public checkUserAddFieldsFiltersForManager(): boolean {
        return this.configs.isUserAddFieldsFiltersForManager;
    }

    public isLearningplansAssignmentTypeActive(): boolean {
        return this.configs.isLearningplansAssignmentTypeActive;
    }

    public isCoursesAssignmentTypeActive(): boolean {
        return this.configs.isCoursesAssignmentTypeActive;
    }

    public getMysqlDbName(): string {
        return this.settings.mysqlDbName;
    }

    public getDbHost(): string {
        const dbHost = this.settings.dbHost;
        if (PlatformManager.isJsonString(dbHost)) {
            const jsonDbHost = JSON.parse(dbHost);
            if (!jsonDbHost.hasOwnProperty('db_host')) {
                throw new Exception('Redis client parameter not found or is empty! Parameter was: db_host in report_host key');
            }
            return jsonDbHost.db_host;
        }
        return dbHost;
    }

    public checkPluginCertificationEnabled(): boolean {
        return this.plugins.certification;
    }

    public checkPluginClassroomEnabled(): boolean {
        return this.plugins.classroom;
    }

    public checkPluginEcommerceEnabled(): boolean {
        return this.plugins.ecommerce;
    }

    public checkPluginESignatureEnabled(): boolean {
        return this.plugins.esignature;
    }

    public checkPluginGamificationEnabled(): boolean {
        return this.plugins.gamification;
    }

    public checkPluginTranscriptEnabled(): boolean {
        return this.plugins.transcript;
    }

    public checkPlugincontentPartnersEnabled(): boolean {
        return this.plugins.contentPartners;
    }

    public checkPluginShareEnabled(): boolean {
        return this.plugins.share;
    }


    public checkPluginFlowEnabled(): boolean {
        return this.plugins.flow;
    }

    public checkPluginFlowMsTeamsEnabled(): boolean {
        return this.plugins.flowMsTeams;
    }

    public checkPluginMultiDomainEnabled(): boolean {
        return this.plugins.multiDomain;
    }

    /**
     * Get the csv export limit based on the platform config. Fallback to csvExportInfrastructureLimit if not specified.
     */
    public getCsvExportLimit(): number {
        const csvExportInfrastructureLimit = ExportLimit.CSV;
        let limit = +this.settings.csvExportLimit;
        if (isNaN(limit) || limit <= 0) {
            limit = csvExportInfrastructureLimit;
        }

        return limit;
    }

    /**
     * Get the xlx export limit based on the platform config. Fallback to xlxExportInfrastructureLimit if not specified.
     */
    public getXlxExportLimit(): number {
        const xlxExportInfrastructureLimit = ExportLimit.XLX;
        let limit = +this.settings.xlxExportLimit;
        if (isNaN(limit) || limit > xlxExportInfrastructureLimit || limit <= 0) {
            limit = xlxExportInfrastructureLimit;
        }

        return limit;
    }

    /**
     * Get the preview export limit based on the platform config. Fallback to previewExportInfrastructureLimit if not specified.
     */
    public getPreviewExportLimit(): number {
        const previewExportInfrastructureLimit = ExportLimit.PREVIEW;
        let limit = +this.settings.previewExportLimit;
        if (isNaN(limit) || limit > previewExportInfrastructureLimit || limit <= 0) {
            limit = previewExportInfrastructureLimit;
        }

        return limit;
    }

    /**
     * Get the entities limits based on the platform config
     */
    public getEntitiesLimits(): EntitiesLimits {
        const entitiesLimits = {
            users: {
                usersLimit: this.settings.entityUsersLimit,
                groupsLimit: this.settings.entityGroupsLimit,
                branchesLimit: this.settings.entityBranchesLimit
            },
            courses: {
                coursesLimit: this.settings.entityCoursesLimit,
                lpLimit: this.settings.entityLPLimit,
            },
            classrooms: {
                classroomLimit: this.settings.entityClassroomLimit,
                lpLimit: this.settings.entityLPLimit,
                courseInstructorsLimit: this.settings.entityCourseInstructorsLimit,
                sessionLimit: this.settings.entitySessionsLimit,
            },
            webinars: {
                webinarLimit: this.settings.entityWebinarLimit,
                lpLimit: this.settings.entityLPLimit,
                courseInstructorsLimit: this.settings.entityCourseInstructorsLimit,
            },
            lpLimit: this.settings.entityLPLimit,
            certificationsLimit: this.settings.entityCertificationsLimit,
            badgesLimit: this.settings.entityBadgesLimit,
            surveysLimit: this.settings.entitySurveysLimit,
            assets: {
                assetsLimit: this.settings.entityAssetsLimit,
                channelsLimit: this.settings.entityChannelsLimit
            }
        };

        return entitiesLimits;
    }

    /**
     * Get the refresh tokens based on the platform config
     */
    public getRefreshTokens(): PlatformRefreshTokens {
        return {
            platformMonthlyTokens: this.settings.monthlyRefreshTokens,
            platformDailyTokens: this.settings.dailyRefreshTokens
        };
    }

    /**
     * Check if input string is a valid JSON
     * @param str
     * @private
     */
    private static isJsonString(str) {
        try {
            JSON.parse(str);
        } catch (e: any) {
            return false;
        }
        return true;
    }
}
