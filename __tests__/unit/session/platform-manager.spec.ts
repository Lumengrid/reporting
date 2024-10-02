import { SessionResponse, SessionResponseDataPlugins, SessionResponseDataPlatformConfigs } from '../../../src/services/hydra';
import PlatformManager, { PlatformSettings, SnowflakeDefaults } from '../../../src/services/session/platform-manager.session';
import { ExportLimit, EntitiesLimits } from '../../../src/models/report-manager';
import { InvalidSessionDataException } from '../../../src/exceptions/invalidSessionDataException';
import { InvalidSessionPlatformException } from '../../../src/exceptions/invalidSessionPlatformException';
import { InvalidSessionConfigsException } from '../../../src/exceptions/invalidSessionConfigsException';
import { InvalidSessionPlatformBaseUrlException } from '../../../src/exceptions/invalidSessionPlatformBaseUrlException';
import { InvalidSessionPluginsException } from '../../../src/exceptions/invalidSessionPluginsException';
import { configureMockExpressHttpContext, MockExpressHttpContext } from '../../utils';

describe('Platform Manager', () => {
    const configs: SessionResponseDataPlatformConfigs = {
        showFirstNameFirst: true,
        defaultPlatformTimezone: '',
        reportDownloadPermissionLink: true,
        isUserAddFieldsFiltersForManager: false,
        isLearningplansAssignmentTypeActive: false,
        isCoursesAssignmentTypeActive: false,
    };

    const plugins: SessionResponseDataPlugins = {
        certification: true,
        classroom: true,
        esignature: true,
        ecommerce: true,
        gamification: true,
        transcript: true,
        contentPartners: true,
        share: true,
        flow: false,
        flowMsTeams: true,
        multiDomain: true,
    };

    const response: SessionResponse = {
        data: {
            platform: {
                platformBaseUrl: 'test',
                defaultLanguage: 'english',
                defaultLanguageCode: 'en',
                configs,
                plugins,
                toggles: {
                    toggleAdminReport: true,
                    toggleNewContribute: true,
                    toggleMyTeamUserAddFilter: false,
                    toggleWebinarsEnableCreation: true,
                    toggleForceDatalakeV1: false,
                    toggleAudittrailLegacyArchive: true,
                    toggleDatalakeV2ManualRefresh: true,
                    toggleManagerReportXLSXPolling: true,
                    toggleMultipleEnrollmentCompletions: true,
                    toggleDatalakeV3: true,
                    togglePrivacyPolicyDashboardOnAthena: false,
                    toggleCoursesDashboardOnAthena: false,
                    toggleBranchesDashboardOnAthena: false,
                    toggleHydraMinimalVersion: false,
                    toggleUsersLearningPlansReportEnhancement: false,
                    toggleLearningPlansStatisticsReport: false,
                    toggleNewLearningPlanManagement: false,
                }
            }
        }
    };

    const settings: PlatformSettings = {
        dynamoDbRegion: '1',
        dynamoDbPlatform: '2',
        athenaRegion: '3',
        athenaS3Path: '4',
        athenaS3ExportPath: '5',
        athenaSchemaNameOverride: '6',
        ignoreOrderByClause: false,
        s3Region: '7',
        s3Bucket: '8',
        csvExportLimit: 9,
        previewExportLimit: 10,
        xlxExportLimit: 11,
        schedulationPrivateKey: '12',
        entityUsersLimit: 13,
        entityGroupsLimit: 14,
        entityBranchesLimit: 15,
        entityCoursesLimit: 16,
        entityLPLimit: 17,
        entityCourseInstructorsLimit: 12,
        entityCertificationsLimit: 22,
        entityBadgesLimit: 56,
        entityClassroomLimit: 123,
        entityWebinarLimit: 111,
        entitySessionsLimit: 54,
        entityAssetsLimit: 12,
        entityChannelsLimit: 12,
        entitySurveysLimit: 55,
        originalDomain: '',
        monthlyRefreshTokens: 18,
        dailyRefreshTokens: 19,
        customReportTypesTableName: '20',
        queryBuilderAdmins: [],
        queryBuilderAdminsV3: [],
        aamonDatalakeNightlyRefreshTimeout: '1',
        datalakeV2ExpirationTime: 3600,
        extractionTimeLimit: 20,
        datalakeV2DataBucket: '',
        datalakeV2Host: '',
        platformRegion: '21',
        mysqlDbName: '22',
        dbHost: '23',
        mainDbHost: 'mainDbHost',
        dbHostOverride: '24',
        snowflakeLocator: '25',
        snowflakeUsername: '26',
        snowflakePassword: '27',
        snowflakeDatabase: '28',
        snowflakeWarehouse: '29',
        snowflakeRole: '30',
        snowflakeStorageIntegration: '31',
        snowflakeStorageIntegrationBucket: '32',
        snowflakeSchema: '33',
        snowflakeDbHost: '',
        snowflakeTimeout: 120000,
        snowflakeLockTable: '',
        snowflakeLogLevel: '',
        snowflakePoolMin: SnowflakeDefaults.POOL_MIN.toString(),
        snowflakePoolMax: SnowflakeDefaults.POOL_MAX.toString(),
        snowflakeClientSessionKeepAliveEnabled: SnowflakeDefaults.CLIENT_SESSION_KEEP_ALIVE_ENABLED.toString(),
        snowflakeClientSessionKeepAliveFrequency: SnowflakeDefaults.CLIENT_SESSION_KEEP_ALIVE_FREQUENCY.toString(),
        snowflakeAcquireTimeout: SnowflakeDefaults.ACQUIRE_TIMEOUT.toString(),
        snowflakeDefaultSchema: SnowflakeDefaults.DEFAULT_SCHEMA.toString(),
    };

    let mockExpressHttpContext: MockExpressHttpContext;
    beforeEach(() => {
        mockExpressHttpContext = configureMockExpressHttpContext();
    });

    afterEach(() => {
        mockExpressHttpContext.afterEachRestoreAllMocks();
    });


    it('Should import plugins', () => {
        const platformManager: PlatformManager = new PlatformManager(response);
        expect(platformManager.checkPluginCertificationEnabled()).toEqual(response.data.platform.plugins.certification);
        expect(platformManager.checkPluginClassroomEnabled()).toEqual(response.data.platform.plugins.classroom);
        expect(platformManager.checkPluginESignatureEnabled()).toEqual(response.data.platform.plugins.esignature);
        expect(platformManager.checkPluginGamificationEnabled()).toEqual(response.data.platform.plugins.gamification);
        expect(platformManager.checkPluginTranscriptEnabled()).toEqual(response.data.platform.plugins.transcript);
        expect(platformManager.checkPluginFlowEnabled()).toEqual(response.data.platform.plugins.flow);
        expect(platformManager.checkPluginFlowMsTeamsEnabled()).toEqual(response.data.platform.plugins.flowMsTeams);
        expect(platformManager.checkPluginMultiDomainEnabled()).toEqual(response.data.platform.plugins.multiDomain);
    });

    it('Should import configs', () => {
        const platformManager: PlatformManager = new PlatformManager(response);
        expect(platformManager.getShowFirstNameFirst()).toEqual(response.data.platform.configs.showFirstNameFirst);
    });

    it('Should load settings', () => {
        const platformManager: PlatformManager = new PlatformManager(response);
        platformManager.loadSettings(settings);
        expect(platformManager.getPlatformBaseUrl()).toEqual(response.data.platform.platformBaseUrl);
        let baseFolder = '';
        if (platformManager.isDatalakeV2Active()) {
            baseFolder = platformManager.getMysqlDbName();
        } else {
            baseFolder = response.data.platform.platformBaseUrl.replace(/(\.|-)/g, '_');
        }
        expect(platformManager.getAthenaSchemaName()).toEqual(baseFolder);
    });

    it('Should load settings', () => {
        const platformManager: PlatformManager = new PlatformManager(response);
        platformManager.loadSettings(settings);
        expect(platformManager.getDynamoDbRegion()).toEqual(settings.dynamoDbRegion);
        expect(platformManager.getDynamoDbPlatform()).toEqual(settings.dynamoDbPlatform);
        expect(platformManager.getAthenaRegion()).toEqual(settings.athenaRegion);
        expect(platformManager.getAthenaS3Path()).toEqual(settings.athenaS3Path);
        expect(platformManager.getAthenaS3ExportPath()).toEqual(settings.athenaS3ExportPath);
        expect(platformManager.getAthenaSchemaNameOverride()).toEqual(settings.athenaSchemaNameOverride);
        expect(platformManager.getDbHostOverride()).toEqual(settings.dbHostOverride);
        expect(platformManager.getS3Region()).toEqual(settings.s3Region);
        expect(platformManager.getS3Bucket()).toEqual(settings.s3Bucket);
        expect(platformManager.getAamonDatalakeNightlyRefreshTimeout()).toEqual(settings.aamonDatalakeNightlyRefreshTimeout);
        expect(platformManager.getDatalakeV2ExpirationTime()).toEqual(settings.datalakeV2ExpirationTime);
        expect(platformManager.getMysqlDbName()).toEqual(settings.mysqlDbName);
    });

    it('Should see the db host expected', () => {
        const platformManager: PlatformManager = new PlatformManager(response);
        settings.dbHost = 'http://fakeurl.com';
        platformManager.loadSettings(settings);

        expect(platformManager.getDbHost()).toEqual('http://fakeurl.com');

        settings.dbHost = '{"db_host": "https://fake-url-2.com"}';
        platformManager.loadSettings(settings);

        expect(platformManager.getDbHost()).toEqual('https://fake-url-2.com');
    });

    it('Should take correct limits', () => {
        const platformManager: PlatformManager = new PlatformManager(response);

        platformManager.loadSettings(settings);
        expect(platformManager.getCsvExportLimit()).toEqual(9);
        expect(platformManager.getXlxExportLimit()).toEqual(11);
        expect(platformManager.getPreviewExportLimit()).toEqual(10);
        const entitiesLimits: EntitiesLimits = platformManager.getEntitiesLimits();
        expect(entitiesLimits.users.branchesLimit).toEqual(15);
        expect(entitiesLimits.users.groupsLimit).toEqual(14);
        expect(entitiesLimits.users.usersLimit).toEqual(13);

        const cloneSettings = {...settings};
        cloneSettings.csvExportLimit = 999999999999999999;
        cloneSettings.previewExportLimit = 999999999999999999;
        cloneSettings.xlxExportLimit = 999999999999999999;
        platformManager.loadSettings(cloneSettings);
        expect(platformManager.getCsvExportLimit()).toEqual(cloneSettings.csvExportLimit);
        expect(platformManager.getXlxExportLimit()).toEqual(ExportLimit.XLX);
        expect(platformManager.getPreviewExportLimit()).toEqual(ExportLimit.PREVIEW);
    });

    it('Should throw an error about SessionResponse', () => {
        let brokenResponse: SessionResponse;

        brokenResponse = {} as SessionResponse;
        expect(() => {
            // tslint:disable-next-line:no-unused-expression
            new PlatformManager(brokenResponse);
        }).toThrow(InvalidSessionDataException);

        brokenResponse = {data: {}} as SessionResponse;
        expect(() => {
            // tslint:disable-next-line:no-unused-expression
            new PlatformManager(brokenResponse);
        }).toThrow(InvalidSessionPlatformException);

        brokenResponse = {data: {platform: {}}} as SessionResponse;
        expect(() => {
            // tslint:disable-next-line:no-unused-expression
            new PlatformManager(brokenResponse);
        }).toThrow(InvalidSessionPlatformBaseUrlException);

        brokenResponse = {data: {platform: {platformBaseUrl: 'test2'}}} as SessionResponse;
        expect(() => {
            // tslint:disable-next-line:no-unused-expression
            new PlatformManager(brokenResponse);
        }).toThrow(InvalidSessionConfigsException);

        brokenResponse = {data: {platform: {platformBaseUrl: 'test3', configs}}} as SessionResponse;
        expect(() => {
            // tslint:disable-next-line:no-unused-expression
            new PlatformManager(brokenResponse);
        }).toThrow(InvalidSessionPluginsException);
    });

    it('Should see the datalakeV2ExpirationTime value as expected', () => {
        const platformManager: PlatformManager = new PlatformManager(response);
        settings.datalakeV2ExpirationTime = 0;
        platformManager.loadSettings(settings);

        expect(platformManager.getDatalakeV2ExpirationTime('test')).toEqual(24 * 60 * 60);
        expect(platformManager.getDatalakeV2ExpirationTime('ECS')).toEqual(4 * 60 * 60);
        expect(platformManager.getDatalakeV2ExpirationTime('staging')).toEqual(8 * 60 * 60);
        expect(platformManager.getDatalakeV2ExpirationTime('smb')).toEqual(24 * 60 * 60);

        settings.datalakeV2ExpirationTime = 12;
        platformManager.loadSettings(settings);
        expect(platformManager.getDatalakeV2ExpirationTime('test')).toEqual(12);
        expect(platformManager.getDatalakeV2ExpirationTime('staging')).toEqual(12);
        expect(platformManager.getDatalakeV2ExpirationTime('smb')).toEqual(12);
    });
});
