import Axios from 'axios';

import { E2ETestUtils } from './utils';
import { Report } from '../../src/models/custom-report';
import Hydra, { SessionResponse } from '../../src/services/hydra';
import { UserLevels } from '../../src/services/session/user-manager.session';

jest.setTimeout(90000);

describe('Reports Service', () => {
  const mock = jest.fn();
  const hydraService = new mock() as Hydra;
  hydraService.getGroups = async (userIds) => {
    return {
      data: {
        13689: 'Marketing'
      }
    };
  };
  hydraService.getUsers = async (userIds) => {
    return {
      data: {
        12345: {
          firstname: 'luca',
          lastname: 'skywalker',
          userid: '12345'
        },
        12312: {
          firstname: 'luca 2',
          lastname: 'skywalker 2',
          userid: '12312'
        },
        22345: {
          firstname: 'luca 3',
          lastname: 'skywalker 3',
          userid: '22345'
        }
      }
    };
  };
  hydraService.session = async (): Promise<SessionResponse> => {
    return {
      data: {
        user: {
          idUser: 1039,
          username: "staff.support",
          eMail: '',
          level: UserLevels.POWER_USER,
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
            1,
            2,
            3,
            50224,
            50245,
            50324,
            50325
          ],
          branches: [
            2990,
            2989
          ],
          branchesWithParents: [
            2989,
            2992,
            2990,
            2989
          ],
          timezone: 'Europe/Rome'
        },
        platform: {
          platformBaseUrl: 'hydra.docebosaas.com',
          defaultLanguage: 'en',
          defaultLanguageCode: 'en',
          configs: {
            showFirstNameFirst: false,
            defaultPlatformTimezone: '',
            reportDownloadPermissionLink: false,
            isUserAddFieldsFiltersForManager: false,
            isLearningplansAssignmentTypeActive: false,
            isCoursesAssignmentTypeActive: false,
          },
          plugins: {
            certification: true,
            classroom: true,
            esignature: false,
            gamification: true,
            transcript: true,
            ecommerce: true,
            contentPartners: true,
            share: true,
            flow: true,
            flowMsTeams: true,
            multiDomain: true,
          },
          toggles: {
              toggleAdminReport: true,
              toggleNewContribute: true,
              toggleMyTeamUserAddFilter: true,
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
              toggleNewLearningPlanManagement: false,
              toggleUsersLearningPlansReportEnhancement: false,
              toggleLearningPlansStatisticsReport: false,
          }
        }
      }
    };
  };

  let adminToken: string;

  beforeAll(async () => {
    E2ETestUtils.loadTestEnv();
    adminToken = await E2ETestUtils.loginAsGodAdmin();
  });

  it('Should be able, as admin, to get all created reports', async () => {
    const testReportIds: string[] = [
      await E2ETestUtils.createReport(
        {
          type: 'Users - Courses',
          name: 'Functional test report',
      description: 'This report is created automatically by a functional test! You should never see this!'
    },
    {
      visibility: {
        groups: [{id: 1}],
        type: 3,
        branches: [],
        users: []
          }
        },
        adminToken
      ),
      await E2ETestUtils.createReport(
        {
          type: 'Users - Courses',
          name: 'Functional test report',
        description: 'This report is created automatically by a functional test! You should never see this!'
      },
      {
        visibility: {
          groups: [],
          type: 3,
          branches: [],
          users: [{id: 1039}]
          }
        },
        adminToken
      ),
      await E2ETestUtils.createReport(
        {
          type: 'Users - Courses',
          name: 'Functional test report',
          description: 'This report is created automatically by a functional test! You should never see this!'
        },
        {
          visibility: {
            groups: [],
            type: 3,
            branches: [{
              id: 2990,
              descendants: true
            }],
            users: []
          }
        },
        adminToken
      ),
      // await E2ETestUtils.createReport(
      //   {
      //     type: 'Users - Courses',
      //     name: 'Functional test report',
      //     description: 'This report is created automatically by a functional test! You should never see this!'
      //   },
      //   {
      //     visibility: {
      //       groups: [],
      //       type: 3,
      //       branches: [{
      //         id: 2990,
      //         descendants: false
      //       }],
      //       users: []
      //     }
      //   },
      //   adminToken
      // )
    ];

    const reports: Report[] = (await Axios.get(
      'https://hydra.docebosaas.com/analytics/v1/reports',
      {headers: {Authorization: `Bearer ${adminToken}`}}
    )).data.data;

    expect(reports).toBeDefined();
    const ids: string[] = reports.map(item => item.idReport.toString());
    expect(testReportIds.every(reportId => {
      return ids.indexOf(reportId) !== -1;
    })).toEqual(true);

    testReportIds.forEach(item => {
      E2ETestUtils.deleteReport(item, adminToken);
    });

  });

  it('Should be able fetch the customer report list', async () => {
    const reports: Report[] = (await Axios.get(
      'https://hydra.docebosaas.com/analytics/v1/reports',
      {headers: {Authorization: `Bearer ${adminToken}`}}
    )).data.data;

    expect(reports).toBeDefined();
    expect(reports.length).toBeGreaterThan(0);

    // Check that a report data is consistent
    const report: Report = reports[0];

    expect(report.idReport).toBeDefined();
    expect(report.name).toBeDefined();

  });

});
