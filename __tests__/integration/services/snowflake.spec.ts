import { TestApp } from '../TestApp';
import { configure } from 'snowflake-sdk';
import { HTTPFactory } from '../../../src/services/http/HTTPFactory';
import { HTTPService } from '../../../src/services/http/HTTPService';
import { redisFactory } from '../../../src/services/redis/RedisFactory';
import { SessionResponse } from '../../../src/services/hydra';
import { UserLevels } from '../../../src/services/session/user-manager.session';

jest.setTimeout(50000);
export type LogLevel = 'ERROR' | 'WARN' | 'INFO' | 'DEBUG' | 'TRACE';
// @ts-ignore
configure({ logLevel: 'ERROR' as LogLevel });

function GenerateSessionResponseForHost(host: string): SessionResponse {
  return {
    data: {
      platform: {
        platformBaseUrl: host,
        defaultLanguage: 'english',
        defaultLanguageCode: 'en',
        configs: {
          showFirstNameFirst: true,
          defaultPlatformTimezone: '',
          reportDownloadPermissionLink: false,
          isUserAddFieldsFiltersForManager: false,
          isCoursesAssignmentTypeActive: false,
          isLearningplansAssignmentTypeActive: false
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
      user: {
        idUser: 1030,
        username: "staff.support",
        eMail: '',
        level: UserLevels.GOD_ADMIN,
        erpAdmin: false,
        lang: 'english',
        langCode: 'en',
        permissions: {
          viewReport: true,
          updateReport: true,
          manager: true,
          viewEcommerceTransaction: true,
        },
        groups: [
          1
        ],
        branches: [],
        branchesWithParents: [],
        timezone: 'Europe/Rome'
      }
    },
  };
}

describe('SnowflakeTest', () => {
  const domainCustomer1 = 'customer1.docebosaas.com';
  let expectedResponseCustomer1: string;
  const domainCustomer2 = 'customer2.docebosaas.com';
  let expectedResponseCustomer2: string;
  const mock = jest.fn();
  let app: TestApp;

  beforeAll(async () => {
    app = await TestApp.createApp(false);
  });

  afterAll(async () => {
    await redisFactory.drainPools();
    await app.stop();
  });

  it('Can get the list of reports for a given host', async () => {
    const host = domainCustomer1;
    const bearerToken = await app.generateBearerTokenForUser(host, 13019);

    const fakeHttpService = new mock() as HTTPService;

    fakeHttpService.call = async (options: any) => {
      if (options.url === '/report/v1/report/session') {
        return {
          data: GenerateSessionResponseForHost(host),
        };
      } else if (options.url === '/report/v1/report/users_details') {
        return {
          data: {
            data: {
              13019: { userid: 'staff.support', firstname: 'Staff', lastname: 'Support' },
            },
          }
        };
      }

      return {data:{}};
    };

    HTTPFactory.setHTTPService(fakeHttpService);

    const response = await app.doGET(
      '/analytics/v1/reports',
      undefined,
      {
        host,
        authorization: `Bearer ${bearerToken}`,
      }
    );

    expect(response.data).toEqual({
      success: true,
      data: [],
    });
  });

  it.each([
    [domainCustomer1],
    [domainCustomer2],
  ])('Prepare response data for customer %s and check not empty response is returned', async (host) => {
    const fakeHttpService = new mock() as HTTPService;

    fakeHttpService.call = async (options: any) => {
      const calledHost = options.baseURL.startsWith('https') ?
        options.baseURL.substring(8) :
        options.baseURL.substring(7);

      if (options.url === '/report/v1/report/session') {
        return {
          data: GenerateSessionResponseForHost(calledHost),
        };
      }

      return {data:{}};
    };

    HTTPFactory.setHTTPService(fakeHttpService);

    const token = await app.generateBearerTokenForUser(host, 13019);

    const response = await app.doGET(
      `/analytics/v1/whoami`,
      undefined,
      {
        host,
        authorization: `Bearer ${token}`,
      }
    );

    expect(response.data.domain).toBeDefined();

    if (host == domainCustomer1) {
      expectedResponseCustomer1 = response.data.domain;
    } else {
      expectedResponseCustomer2 = response.data.domain;
    }
  });

  it('Selects the data from the right Snowflake schema depending on the customer (with parallelism)', async () => {
    const fakeHttpService = new mock() as HTTPService;

    fakeHttpService.call = async (options: any) => {
      const calledHost = options.baseURL.startsWith('https') ?
        options.baseURL.substring(8) :
        options.baseURL.substring(7);

      if (options.url === '/report/v1/report/session') {
        return {
          data: GenerateSessionResponseForHost(calledHost),
        };
      }

      return {data:{}};
    };

    HTTPFactory.setHTTPService(fakeHttpService);

    const callsData = [
      { customer: domainCustomer1, expectedResponse: expectedResponseCustomer1 },
      { customer: domainCustomer2, expectedResponse: expectedResponseCustomer2 },
      { customer: domainCustomer1, expectedResponse: expectedResponseCustomer1 },
      { customer: domainCustomer1, expectedResponse: expectedResponseCustomer1 },
      { customer: domainCustomer1, expectedResponse: expectedResponseCustomer1 },
      { customer: domainCustomer2, expectedResponse: expectedResponseCustomer2 },
      { customer: domainCustomer2, expectedResponse: expectedResponseCustomer2 },
      { customer: domainCustomer1, expectedResponse: expectedResponseCustomer1 },
      { customer: domainCustomer1, expectedResponse: expectedResponseCustomer1 },
      { customer: domainCustomer2, expectedResponse: expectedResponseCustomer2 },
    ];

    const calls = callsData.map(async (data) => {
      const host = data.customer;
      const token = await app.generateBearerTokenForUser(host, 13019);

      const response = await app.doGET(
        `/analytics/v1/whoami`,
        undefined,
        {
          host,
          authorization: `Bearer ${token}`,
        }
      );

      return response.data.domain;
    });

    const resultData = await Promise.all(calls);
    const expectedData = callsData.map((data) => data.expectedResponse);

    expect(resultData).toEqual(expectedData);
  });
});
