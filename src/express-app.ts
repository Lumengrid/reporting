import './tracer';
import express from 'express';
import Config from './config';
import {
  checkArchivedAuditTrailPermission,
  checkCustomReportTypePermission,
  checkDataLakeRefreshStatus,
  checkDataLakeRefreshStatusMandatory,
  checkDatalakeV2ManualRefreshToggleActivation,
  checkDatalakeV2ToggleActivation,
  checkPrivacyPolicyOnSnowflake,
  checkERPAdminUser,
  checkExistDataFresher,
  checkQueryBuilderPermission,
  checkReportOwnership,
  checkReportsManagerPermission,
  checkRODToggleActivation,
  checkUpdateReportPermission,
  checkViewReportPermission,
  loadAnonymousSession,
  superAdminPermission,
  checkCoursesOnSnowflake,
  checkBranchesOnSnowflake,
  checkAPIWhitelist
} from './routes/permissions';

// GET routes functions
import { getReports } from './routes/get/reports';
import { getReportExportCsv } from './routes/get/report-export-csv';
import { getReportExportXlsx } from './routes/get/report-export-xlsx';
import { getReportExportsInfo } from './routes/get/report-exports-info';
import { getExportPollingEcho } from './routes/get/export-polling-echo';
import { getReportExportDownload } from './routes/get/report-export-download';
import { getReportInfo } from './routes/get/report-info';
import { getReportPreview } from './routes/get/report-preview';
import { getReportAvailablesFields } from './routes/get/report-availables-fields';
import { getReportExportSchedulation } from './routes/get/report-export-schedulation';
import { userAdditionalFields } from './routes/get/user-additional-fields';
import { getRefreshTokens } from './routes/get/refresh-tokens';
import { getReportSettings } from './routes/get/settings';
import { getReportExportResult } from './routes/get/report-export-results';
import { runSidekiqSchedulation } from './routes/get/run-sidekiq-schedulation';

import { getCustomReportTypesResults } from './routes/get/custom-report-types-results';
// POST routes functions
import { postReports } from './routes/post/reports';
import { postReportsClone } from './routes/post/reports-clone';
import { postReportsDelete } from './routes/post/reports-delete';
import { postReportTemporaryPreview } from './routes/post/report-temporary-preview';
import { runScheduledExtractions } from './routes/post/run-scheduled-extractions';
import { migrateReports } from './routes/post/migrate-reports';
import { postSidekiqSchedulation } from './routes/post/sidekiq-schedulation';
// PUT routes functions
import { putReportUpdate } from './routes/put/report-update';
import { putReportSettingsUpdate } from './routes/put/settings';
// DELETE routes functions
import { deleteReportDelete } from './routes/delete/report-delete';
import { deleteSidekiqSchedulation } from './routes/delete/sidekiq-schedulation';

// QUERY BUILDER
// GET routes functions
import { getCustomReportTypes } from './routes/get/custom-report-types';
import { getCustomReportTypeDetail } from './routes/get/custom-report-type-detail';
import { getActiveCustomReportTypes } from './routes/get/custom-report-types-active';
import { getCustomReportTypesReports } from './routes/get/custom-report-types-reports';
import { getQueryBuilderAdmins } from './routes/get/query-builder-admins';
// POST routes functions
import { postCustomReportTypes } from './routes/post/custom-report-types';
import { customReportTypesPreview } from './routes/post/custom-report-types-preview';
// PUT routes functions
import { putCustomReportTypes } from './routes/put/custom-report-types';
import { putQueryBuilderAdmins } from './routes/put/query-builder-admins';
// DELETE routes functions
import { deleteCustomReportType } from './routes/delete/custom-report-type-delete';
import { deleteQueryBuilderAdmins } from './routes/delete/query-builder-admins';

// REPORT-MANAGER
// POST routes functions
import { reportManager } from './routes/post/report-manager';
// GET routes functions
import { reportManagerStatus } from './routes/get/report-manager-status';
import { reportManagerCsv } from './routes/get/report-manager-csv';
import { reportManagerXls } from './routes/get/report-manager-xls';
import { reportManagerResults } from './routes/get/report-manager-results';
import { getManagerDataLakeRefreshStatus } from './routes/get/manager-data-lake-refresh-status';
import { getDateLakeRefreshDate } from './routes/get/date-lake-refresh-date';
import { learningObjectTypes } from './routes/get/learning-object-types';
import { startDataLakePerTimezone } from './models/datalake-timezone-refresh';
import { refreshOnDemand } from './routes/post/refresh-on-demand';
import { getReportDataLakeRefreshStatus } from './routes/get/report-data-lake-refresh-status';
import { dataLakeError } from './models/dataLakeError';
import { BaseResponse } from './models/base';

import { getSwagger } from './routes/get/swagger';
import {
  validateBranches,
  validateCoursesChartsSummary,
  validateCoursesEnrollments,
  validateEnrollmentStatus,
  validatePagination,
  validatePrivacyDashboard,
  validateReport,
  validateSetting
} from './routes/validator';
import { postArchivedAuditTrail } from './routes/post/archived-audit-trail';
import { archivedAuditTrailStatus } from './routes/get/archived-audit-trail-status';
import { archivedAuditTrailResults } from './routes/get/archived-audit-trail-results';
import { archivedAuditTrailCsv } from './routes/get/archived-audit-trail-csv';
import { archivedAuditTrailArchiveDate } from './routes/get/archived-audit-trail-log-archive-date';
import { getDateLakeV2LastCompleteTime } from './routes/get/date-lake-v2-refresh-information';
import { datalakeV2SchemaRefresh } from './routes/post/datalakeV2SchemaRefresh';
import { putRefreshInError } from './routes/put/time-zone-error';
import { postConvertQb } from './routes/post/convert-qb';
import { getDashboardPrivacyPolicyUsers } from './routes/get/dashboard-privacy-policy-users';
import { getDashboardPrivacyPolicyCharts } from './routes/get/dashboard-privacy-policy-charts';
import { getDashboardCoursesEnrollments } from './routes/get/dashboard-courses-enrollments';
import { getDashboardCoursesCharts } from './routes/get/dashboard-courses-charts';
import { getDashboardCoursesSummary } from './routes/get/dashboard-courses-summary';
import { getDashboardBranchesSummary } from './routes/get/dashboard-branches-summary';
import { getDashboardBranchesList } from './routes/get/dashboard-branches-list';
import { getDashboardBranchEnrollments } from './routes/get/dashboard-branch-enrollments';
import { getDashboardCoursesExportCsv } from './routes/get/dashboard-courses-export-csv';
import { getDashboardPrivacyPolicyExportCsv } from './routes/get/dashboard-privacy-policy-export-csv';
import { getDashboardBranchesExportCsv } from './routes/get/dashboard-branches-export-csv';
import { getDashboardUsersExportCsv } from './routes/get/dashboard-users-export-csv';
import { whoami } from './routes/get/whoami';
import { patchReportUpdate } from './routes/patch/report-update-patch';

export async function DeclareExpressRoutes(app: express.Application, config: Config): Promise<void> {
  const urlPrefix = config.urlPrefix;
  const internalUrlPrefix = config.internalUrlPrefix;

// set the routes of the express app
  app.get(`/:subfolder?${urlPrefix}/whoami`, checkAPIWhitelist, checkViewReportPermission, whoami);

// Swagger
  app.get(`/:subfolder?${urlPrefix}/swagger`, checkAPIWhitelist, loadAnonymousSession, getSwagger);
// GET routes
// Anonymous routes
  app.get(`/:subfolder?${urlPrefix}/reports/:id_report/exports/:id_export/download`, checkAPIWhitelist, loadAnonymousSession, getReportExportDownload);
// Logged in route
  app.get(`/:subfolder?${urlPrefix}/reports/settings`, checkAPIWhitelist, checkERPAdminUser, getReportSettings);
  app.get(`/:subfolder?${urlPrefix}/reports/learning-object-types`, checkAPIWhitelist, checkViewReportPermission, learningObjectTypes);
  app.get(`/:subfolder?${urlPrefix}/reports/user-additional-fields`, checkAPIWhitelist, checkViewReportPermission, userAdditionalFields);
  app.get(`/:subfolder?${urlPrefix}/reports/last-refresh-date`, checkAPIWhitelist, checkViewReportPermission, getDateLakeRefreshDate);
  app.get(`/:subfolder?${urlPrefix}/data-lake/last-complete-time`, checkAPIWhitelist, checkViewReportPermission, getDateLakeV2LastCompleteTime);
  app.get(`/:subfolder?${urlPrefix}/reports`, checkAPIWhitelist, checkViewReportPermission, getReports);
  app.get(`/:subfolder?${urlPrefix}/reports/:id_report`, checkAPIWhitelist, checkViewReportPermission, getReportInfo);
  app.get(`/:subfolder?${urlPrefix}/reports/:id_report/preview`, checkAPIWhitelist, checkViewReportPermission, checkDataLakeRefreshStatusMandatory, getReportPreview);
  app.get(`/:subfolder?${urlPrefix}/reports/:id_report/fields`, checkAPIWhitelist, checkViewReportPermission, getReportAvailablesFields);
  app.get(`/:subfolder?${urlPrefix}/reports/:id_report/export/csv`, checkAPIWhitelist, checkViewReportPermission, checkDataLakeRefreshStatus, getReportExportCsv);
  app.get(`/:subfolder?${urlPrefix}/reports/:id_report/export/xlsx`, checkAPIWhitelist, checkViewReportPermission, checkDataLakeRefreshStatus, getReportExportXlsx);
  app.get(`/:subfolder?${urlPrefix}/reports/:id_report/exports/:id_export`, checkAPIWhitelist, checkViewReportPermission, getReportExportsInfo);
  app.get(`/:subfolder?${urlPrefix}/exports/polling/echo`, checkAPIWhitelist, checkViewReportPermission, getExportPollingEcho);
  app.get(`/:subfolder?${urlPrefix}/reports/:id_report/export/schedulation`, checkAPIWhitelist, checkViewReportPermission, checkReportOwnership, getReportExportSchedulation);
  app.get(`/:subfolder?${urlPrefix}/reports/data-lake/refresh-status`, checkAPIWhitelist, checkUpdateReportPermission, getReportDataLakeRefreshStatus);
  app.get(`/:subfolder?${urlPrefix}/reports/data-lake/refresh-tokens`, checkAPIWhitelist, checkUpdateReportPermission, superAdminPermission, checkRODToggleActivation, getRefreshTokens);
  app.get(`/:subfolder?${urlPrefix}/reports/:id_report/exports/:id_export/results`, checkAPIWhitelist, checkViewReportPermission, getReportExportResult);
  app.get(`${internalUrlPrefix}/reports/:id_report/sidekiq-schedulation/:platform`, runSidekiqSchedulation);


// POST routes
// Logged in route
  app.post(`/:subfolder?${urlPrefix}/reports`, checkAPIWhitelist, checkUpdateReportPermission, postReports);
  app.post(`/:subfolder?${urlPrefix}/reports/schema-refresh`, checkAPIWhitelist, checkDatalakeV2ToggleActivation, checkDatalakeV2ManualRefreshToggleActivation, checkUpdateReportPermission, checkDataLakeRefreshStatusMandatory, checkExistDataFresher, datalakeV2SchemaRefresh);
  app.post(`/:subfolder?${urlPrefix}/reports/:id_report/clones`, checkAPIWhitelist, checkUpdateReportPermission, postReportsClone);
  app.post(`/:subfolder?${urlPrefix}/reports/deleted`, checkAPIWhitelist, checkUpdateReportPermission, postReportsDelete);
  app.post(`/:subfolder?${urlPrefix}/reports/:id_report/preview`, checkAPIWhitelist, checkUpdateReportPermission, checkDataLakeRefreshStatusMandatory, postReportTemporaryPreview);
  app.post(`${internalUrlPrefix}/reports/scheduled/export`, runScheduledExtractions);
  app.post(`${internalUrlPrefix}/reports/time-zone-refresh`, startDataLakePerTimezone);
  app.post(`${internalUrlPrefix}/reports/data-lake/error`, dataLakeError);
  app.post(`/:subfolder?${urlPrefix}/reports/migrations`, checkAPIWhitelist, checkUpdateReportPermission, migrateReports);
  app.post(`/:subfolder?${urlPrefix}/reports/refresh-on-demand`, checkAPIWhitelist, checkUpdateReportPermission, superAdminPermission, checkRODToggleActivation, checkDataLakeRefreshStatusMandatory, refreshOnDemand);
  app.post(`/:subfolder?${urlPrefix}/reports/sidekiq-schedulation`, checkAPIWhitelist, checkERPAdminUser, postSidekiqSchedulation);
  app.post(`/:subfolder?${urlPrefix}/convert-qb`, checkAPIWhitelist, checkUpdateReportPermission, postConvertQb);

// PUT routes
// Logged in route
  app.put(`/:subfolder?${urlPrefix}/reports/settings`, checkAPIWhitelist, checkERPAdminUser, validateSetting, putReportSettingsUpdate);
  app.put(`/:subfolder?${urlPrefix}/reports/refresh-error`, checkAPIWhitelist, checkERPAdminUser, putRefreshInError);
  app.put(`/:subfolder?${urlPrefix}/reports/:id_report`, checkAPIWhitelist, checkUpdateReportPermission, checkReportOwnership, validateReport, putReportUpdate);

// PATCH routes
// Logged in route
  app.patch(`/:subfolder?${urlPrefix}/reports/:id_report`, checkAPIWhitelist, checkUpdateReportPermission, checkReportOwnership, patchReportUpdate);

// DELETE routes
// Logged in route
  app.delete(`/:subfolder?${urlPrefix}/reports/sidekiq-schedulation`, checkAPIWhitelist, checkERPAdminUser, deleteSidekiqSchedulation);
  app.delete(`/:subfolder?${urlPrefix}/reports/:id_report`, checkAPIWhitelist, checkUpdateReportPermission, checkReportOwnership, deleteReportDelete);

// QUERY BUILDER
// GET
  app.get(`/:subfolder?${urlPrefix}/custom-report-types`, checkAPIWhitelist, checkQueryBuilderPermission, getCustomReportTypes);
  app.get(`/:subfolder?${urlPrefix}/custom-report-types/active`, checkAPIWhitelist, checkCustomReportTypePermission, getActiveCustomReportTypes); //  checkUpdateReportPermission
  app.get(`/:subfolder?${urlPrefix}/custom-report-types/:id`, checkAPIWhitelist, checkQueryBuilderPermission, getCustomReportTypeDetail);
  app.get(`/:subfolder?${urlPrefix}/custom-report-types/:id_custom_report_types/preview/:query_execution_id`, checkAPIWhitelist, checkQueryBuilderPermission, getCustomReportTypesResults);
  app.get(`/:subfolder?${urlPrefix}/custom-report-types/:id_custom_report_types/reports`, checkAPIWhitelist, checkQueryBuilderPermission, getCustomReportTypesReports);
  app.get(`/:subfolder?${urlPrefix}/query-builder/admins`, checkAPIWhitelist, checkERPAdminUser, getQueryBuilderAdmins);
// POST
  app.post(`/:subfolder?${urlPrefix}/custom-report-types`, checkAPIWhitelist, checkQueryBuilderPermission, postCustomReportTypes);
  app.post(`/:subfolder?${urlPrefix}/custom-report-types/:id_custom_report_types/preview`, checkAPIWhitelist, checkQueryBuilderPermission, customReportTypesPreview);
// PUT
  app.put(`/:subfolder?${urlPrefix}/custom-report-types/:id_custom_report_types`, checkAPIWhitelist, checkQueryBuilderPermission, putCustomReportTypes);
  app.put(`/:subfolder?${urlPrefix}/query-builder/admins/:id_admin`, checkAPIWhitelist, checkERPAdminUser, putQueryBuilderAdmins);
// DELETE
  app.delete(`/:subfolder?${urlPrefix}/custom-report-types/:id`, checkAPIWhitelist, checkQueryBuilderPermission, deleteCustomReportType);
  app.delete(`/:subfolder?${urlPrefix}/query-builder/admins/:id_admin`, checkAPIWhitelist, checkERPAdminUser, deleteQueryBuilderAdmins);

// REPORT MANAGER

// POST
  app.post(`/:subfolder?${urlPrefix}/manager/report/:report_type_code`, checkAPIWhitelist, checkReportsManagerPermission, checkDataLakeRefreshStatus, reportManager);

// GET
  app.get(`/:subfolder?${urlPrefix}/manager/report/:report_type_code/:query_execution_id`, checkAPIWhitelist, checkReportsManagerPermission, reportManagerStatus);
  app.get(`/:subfolder?${urlPrefix}/manager/report/:report_type_code/:query_execution_id/csv`, checkAPIWhitelist, checkReportsManagerPermission, reportManagerCsv);
  app.get(`/:subfolder?${urlPrefix}/manager/report/:report_type_code/:query_execution_id/xls`, checkAPIWhitelist, checkReportsManagerPermission, reportManagerXls);
  app.get(`/:subfolder?${urlPrefix}/manager/report/:report_type_code/:query_execution_id/results`, checkAPIWhitelist, checkReportsManagerPermission, reportManagerResults);

  app.get(`/:subfolder?${urlPrefix}/manager/data-lake-status`, checkAPIWhitelist, checkReportsManagerPermission, getManagerDataLakeRefreshStatus);


// OLD AUDIT TRAIL

// POST
  app.post(`/:subfolder?${urlPrefix}/archived-audit-trail-log`, checkAPIWhitelist, checkArchivedAuditTrailPermission, postArchivedAuditTrail);

// GET
  app.get(`/:subfolder?${urlPrefix}/archived-audit-trail-log/archive-date`, checkAPIWhitelist, checkArchivedAuditTrailPermission, archivedAuditTrailArchiveDate);
  app.get(`/:subfolder?${urlPrefix}/archived-audit-trail-log/:query_execution_id`, checkAPIWhitelist, checkArchivedAuditTrailPermission, archivedAuditTrailStatus);
  app.get(`/:subfolder?${urlPrefix}/archived-audit-trail-log/:query_execution_id/results`, checkAPIWhitelist, checkArchivedAuditTrailPermission, archivedAuditTrailResults);
  app.get(`/:subfolder?${urlPrefix}/archived-audit-trail-log/:query_execution_id/csv`, checkAPIWhitelist, checkArchivedAuditTrailPermission, archivedAuditTrailCsv);


// DASHBOARD

// GET PRIVACY POLICIES
  app.get(`/:subfolder?${urlPrefix}/dashboard/users/privacy_policies`, checkAPIWhitelist, checkViewReportPermission, checkPrivacyPolicyOnSnowflake, validatePrivacyDashboard, validatePagination, getDashboardPrivacyPolicyUsers);
  app.get(`/:subfolder?${urlPrefix}/dashboard/privacy_policies/charts`, checkAPIWhitelist, checkViewReportPermission, checkPrivacyPolicyOnSnowflake, validatePrivacyDashboard, getDashboardPrivacyPolicyCharts);
  app.get(`/:subfolder?${urlPrefix}/dashboard/privacy_policies/:query_execution_id/exportCsv`, checkAPIWhitelist, checkViewReportPermission, checkPrivacyPolicyOnSnowflake, getDashboardPrivacyPolicyExportCsv);

// GET COURSES
  app.get(`/:subfolder?${urlPrefix}/dashboard/courses/list`, checkAPIWhitelist, checkViewReportPermission, checkCoursesOnSnowflake, validateCoursesEnrollments, validatePagination, getDashboardCoursesEnrollments);
  app.get(`/:subfolder?${urlPrefix}/dashboard/courses/charts`, checkAPIWhitelist, checkViewReportPermission, checkCoursesOnSnowflake, validateCoursesChartsSummary, getDashboardCoursesCharts);
  app.get(`/:subfolder?${urlPrefix}/dashboard/courses/summary`, checkAPIWhitelist, checkViewReportPermission, checkCoursesOnSnowflake, validateCoursesChartsSummary, getDashboardCoursesSummary);
  app.get(`/:subfolder?${urlPrefix}/dashboard/courses/:query_execution_id/exportCsv`, checkAPIWhitelist, checkViewReportPermission, checkCoursesOnSnowflake, getDashboardCoursesExportCsv);

// GET BRANCHES
  app.get(`/:subfolder?${urlPrefix}/dashboard/branches/summary`, checkAPIWhitelist, checkViewReportPermission, checkBranchesOnSnowflake, validateBranches, getDashboardBranchesSummary);
  app.get(`/:subfolder?${urlPrefix}/dashboard/branches/list`, checkAPIWhitelist, checkViewReportPermission, checkBranchesOnSnowflake, validateBranches, validatePagination, getDashboardBranchesList);
  app.get(`/:subfolder?${urlPrefix}/dashboard/branches/users/list`, checkAPIWhitelist, checkViewReportPermission, checkBranchesOnSnowflake, validateBranches, validateEnrollmentStatus, validatePagination, getDashboardBranchEnrollments);
  app.get(`/:subfolder?${urlPrefix}/dashboard/branches/:query_execution_id/exportCsv`, checkAPIWhitelist, checkViewReportPermission, checkBranchesOnSnowflake, getDashboardBranchesExportCsv);
  app.get(`/:subfolder?${urlPrefix}/dashboard/branches/users/:query_execution_id/exportCsv`, checkAPIWhitelist, checkViewReportPermission, checkBranchesOnSnowflake, getDashboardUsersExportCsv);

// catch 404 and forward to error handler
  app.use((req, res) => {
    res.status(404);
    const response: BaseResponse = {success: false, error: 'Page not found!'};
    res.json(response);
  });
}
