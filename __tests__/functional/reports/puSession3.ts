import { UserLevels } from '../../../src/services/session/user-manager.session';
import { SessionResponse } from '../../../src/services/hydra';

export const puSession3: SessionResponse = {
  data: {
    user: {
      idUser: 1030,
      username: "pu3",
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
        1
      ],
      branches: [],
      branchesWithParents: [],
      timezone: 'Europe/Rome'
    },
    platform: {
      platformBaseUrl: 'hydra.docebosaas.com',
      defaultLanguage: 'english',
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
        toggleManagerReportXLSXPolling: true,
        toggleDatalakeV2ManualRefresh: false,
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
