import Hydra, { SessionResponse } from '../../../src/services/hydra';
import SessionManager from '../../../src/services/session/session-manager.session';
import { FunctionalTestUtils } from '../utils';
import { AttributeMap } from 'aws-sdk/clients/dynamodb';
import { ReportManagerSwitcher } from '../../../src/models/report-switcher';
import CacheService from '../../../src/services/cache/cache';
import { Dynamo } from '../../../src/services/dynamo';
import { ReportService } from '../../../src/services/report';
import { puSession1 } from './puSession1';
import { puSession2 } from './puSession2';
import { puSession3 } from './puSession3';
import { ReportsComponent } from '../../../src/routes/get/report.component';
import { HTTPService } from '../../../src/services/http/HTTPService';
import { HTTPFactory } from '../../../src/services/http/HTTPFactory';
import { configureMockExpressHttpContext, MockExpressHttpContext } from '../../utils';
import { v4 as uuidv4 } from 'uuid';
import { redisFactory } from '../../../src/services/redis/RedisFactory';

jest.setTimeout(90000);

describe('Reports Component', () => {
  let commonKeys: any;
  let items: AttributeMap[];
  let mockExpressHttpContext: MockExpressHttpContext;
  const mock = jest.fn();
  const testReportIds: string[] = [];
  const fakeToken =  uuidv4().replace(/-/g, '');
  const PLATFORM_URL = 'hydra.docebosaas.com';

  afterAll(async () => {
      redisFactory.drainPools();
  });

  const generateAdminSessionResponse = (host: string): SessionResponse  => {
    return {
      data: {
        user: {
          idUser: 13888,
          username: "staff.support",
          eMail: '',
          level: 'super_admin',
          erpAdmin: true,
          lang: 'english',
          langCode: 'en',
          permissions: {
            viewReport: true,
            updateReport: true,
            manager: false,
            viewEcommerceTransaction: true,
          },
          groups: [1],
          branches: [],
          branchesWithParents: [],
          timezone: 'Europe/Rome'
        },
        platform: {
          platformBaseUrl: host,
          defaultLanguage: 'english',
          defaultLanguageCode: 'en',
          configs: {
            showFirstNameFirst: false,
            defaultPlatformTimezone: '',
            reportDownloadPermissionLink: false,
            isUserAddFieldsFiltersForManager: false,
            isLearningplansAssignmentTypeActive: false,
            isCoursesAssignmentTypeActive: false
          },
          plugins: {
            certification: true,
            classroom: true,
            esignature: true,
            ecommerce: true,
            gamification: true,
            transcript: true,
            contentPartners: true,
            share: true,
            flow: true,
            flowMsTeams: true,
            multiDomain: true,
          },
          toggles: {
            toggleAdminReport: false,
            toggleNewContribute: false,
            toggleMyTeamUserAddFilter: false,
            toggleWebinarsEnableCreation: false,
            toggleForceDatalakeV1: false,
            toggleAudittrailLegacyArchive: false,
            toggleManagerReportXLSXPolling: false,
            toggleDatalakeV2ManualRefresh: false,
            toggleMultipleEnrollmentCompletions: false,
            toggleDatalakeV3: true,
            togglePrivacyPolicyDashboardOnAthena: false,
            toggleCoursesDashboardOnAthena: false,
            toggleBranchesDashboardOnAthena: false,
            toggleHydraMinimalVersion: false,
            toggleLearningPlansStatisticsReport: false,
            toggleUsersLearningPlansReportEnhancement: false,
            toggleNewLearningPlanManagement: false
          }
        },
      },
    };
  }

  beforeAll(async () => {
    FunctionalTestUtils.init();
    FunctionalTestUtils.loadTestEnv();
    if (!process.env.REPORTS_TABLE) throw new Error('No test table');

    items = await FunctionalTestUtils.loadFixtureFromFile(process.env.REPORTS_TABLE, '__tests__/functional/reports/fixture.json');
    items.forEach(item => {
      if (item.idReport.S) testReportIds.push(item.idReport.S);
    });

    commonKeys = await redisFactory.getRedis().getRedisCommonParams();

  });

  beforeEach(() => {
    mockExpressHttpContext = configureMockExpressHttpContext();

  });

  afterEach(() => {
    if(mockExpressHttpContext){
      mockExpressHttpContext.afterEachRestoreAllMocks();
    }
  });

  it('Should be able to get reports filtered by permissions', async () => {
    const fakeHttpService = new mock() as HTTPService;
    fakeHttpService.call = async (options: any) => {
      const baseUrl = options?.baseURL ?? "default_base_url";
      const calledHost = baseUrl.startsWith('https') ?
        baseUrl.substring(8) :
        baseUrl.substring(7);

      if (options.url === '/report/v1/report/session') {
        return {
          data: generateAdminSessionResponse(calledHost)
        };
      } else if (options.url === '/report/v1/report/users_details') {
        return {
          data: {}
        };
      } else {
        return {
          data: {}
        };
      }
    }

    HTTPFactory.setHTTPService(fakeHttpService);

    const hydraSuperAdmin: Hydra = new Hydra(PLATFORM_URL, `Bearer ${fakeToken}`, '');
    hydraSuperAdmin.session = jest.fn(async (): Promise<SessionResponse> => generateAdminSessionResponse(PLATFORM_URL));
    const sessionManagerSuperAdmin = await SessionManager.init(hydraSuperAdmin, new CacheService(0));
    const dynamo = new Dynamo(commonKeys.dynamoDbRegion, PLATFORM_URL, '', sessionManagerSuperAdmin.platform);
    const reportServiceSuperAdmin: ReportService = new ReportService(hydraSuperAdmin);

    const hydraPowerUser: Hydra = new Hydra(PLATFORM_URL, `Bearer ${fakeToken}`, '');
    hydraPowerUser.session = jest.fn(async (): Promise<SessionResponse> => puSession1);
    const sessionManagerPowerUser = await SessionManager.init(hydraPowerUser, new CacheService(0));
    const reportServicePowerUser: ReportService = new ReportService(hydraPowerUser);

    const hydraPowerUser2: Hydra = new Hydra(PLATFORM_URL, `Bearer ${fakeToken}`, '');
    hydraPowerUser2.session = jest.fn(async (): Promise<SessionResponse> => puSession2);
    const sessionManagerPowerUser2 = await SessionManager.init(hydraPowerUser2, new CacheService(0));
    const reportServicePowerUser2: ReportService = new ReportService(hydraPowerUser2);

    const hydraPowerUser3: Hydra = new Hydra(PLATFORM_URL, `Bearer ${fakeToken}`, '');
    hydraPowerUser3.session = jest.fn(async (): Promise<SessionResponse> => puSession3);
    const sessionManagerPowerUser3 = await SessionManager.init(hydraPowerUser3, new CacheService(0));
    const reportServicePowerUser3: ReportService = new ReportService(hydraPowerUser3);


    const component1 = new ReportsComponent(dynamo, reportServicePowerUser);
    const reports1 = await component1.getReports(sessionManagerPowerUser);

    expect(reports1.length).toEqual(5);
    expect(reports1
        .map(item => item.idReport.toString())
        .every(reportId => {
          return testReportIds.indexOf(reportId) !== -1;
        })).toEqual(true);

    const component2 = new ReportsComponent(dynamo, reportServicePowerUser2);
    const reports2 = await component2.getReports(sessionManagerPowerUser2);
    const reports2Ids = reports2.map(item => item.idReport.toString());
    expect(reports2Ids.length).toEqual(2);
    expect(reports2Ids.indexOf(testReportIds[0]) === -1).toEqual(true);
    expect(reports2Ids.indexOf(testReportIds[1]) !== -1).toEqual(true);
    expect(reports2Ids.indexOf(testReportIds[2]) === -1).toEqual(true);
    expect(reports2Ids.indexOf(testReportIds[3]) === -1).toEqual(true);
    expect(reports2Ids.indexOf(testReportIds[4]) !== -1).toEqual(true);


    const component3 = new ReportsComponent(dynamo, reportServicePowerUser3);
    const reports3 = await component3.getReports(sessionManagerPowerUser3);
    const reports3Ids = reports3.map(item => item.idReport.toString());

    expect(reports3Ids.length).toEqual(1);
    expect(reports3Ids.indexOf(testReportIds[0]) !== -1).toEqual(true);
    expect(reports3Ids.indexOf(testReportIds[1]) === -1).toEqual(true);
    expect(reports3Ids.indexOf(testReportIds[2]) === -1).toEqual(true);
    expect(reports3Ids.indexOf(testReportIds[3]) === -1).toEqual(true);
    expect(reports3Ids.indexOf(testReportIds[4]) === -1).toEqual(true);

    const component4 = new ReportsComponent(dynamo, reportServiceSuperAdmin);
    const reports4 = await component4.getReports(sessionManagerSuperAdmin);

    expect(reports4.length).toEqual(5);
    expect(reports4
        .map(item => item.idReport.toString())
        .every(reportId => {
          return testReportIds.indexOf(reportId) !== -1;
        })).toEqual(true);

  });

  it('Should be able to get all reports without the deleted one', async () => {
    if (!process.env.REPORTS_TABLE) throw new Error('No test table');

    const fakeHttpService = new mock() as HTTPService;

    fakeHttpService.call = async (options: any) => {
      const baseUrl = options?.baseURL ?? "default_base_url";
      const calledHost = baseUrl.startsWith('https') ?
        baseUrl.substring(8) :
        baseUrl.substring(7);

      if (options.url === '/report/v1/report/session') {
        return {
          data: generateAdminSessionResponse(calledHost),
        };
      } else if (options.url === '/report/v1/report/generate-event-on-eventbus') {
        return {
          data: {}
        }
      } else {
        {
          return {
            data: {}
          }
        }
      }
    }

    HTTPFactory.setHTTPService(fakeHttpService);

    const hydraSuperAdmin: Hydra = new Hydra(PLATFORM_URL, `Bearer ${fakeToken}`, '');
    const sessionManagerSuperAdmin = await SessionManager.init(hydraSuperAdmin, new CacheService(0));

    for (const testReportId of testReportIds) {
      const reportHandler = await ReportManagerSwitcher(sessionManagerSuperAdmin, testReportId);
      await reportHandler.delete();
    }

    const allReports = await FunctionalTestUtils.getAllReportsFromTable(process.env.REPORTS_TABLE);
    expect(allReports.filter(item => !item.deleted.BOOL).length).toEqual(0);

  });
});

