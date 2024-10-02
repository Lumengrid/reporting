import { UserLevels } from '../../../src/services/session/user-manager.session';
import { SessionResponse } from '../../../src/services/hydra';

export const puSession1: SessionResponse = {
  data: {
    user: {
      idUser: 1039,
      username: "pu1",
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
      defaultLanguage: 'english',
      defaultLanguageCode: 'en',
      configs: {
        showFirstNameFirst: false,
        defaultPlatformTimezone: '',
        reportDownloadPermissionLink: true,
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
        toggleDatalakeV2ManualRefresh: false,
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
