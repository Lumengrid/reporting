import {
    AdditionalFieldsTypes,
    CourseTypeFilter,
    CourseTypes,
    CourseuserLevels,
    EnrollmentStatuses,
    UserLevelsGroups
} from './base';
import {
    FieldsList,
    FieldTranslation,
    ReportAvailablesFields,
    ReportManagerLearningPlansFilter,
    TablesList,
    TablesListAliases,
    ReportManagerInfo,
    ReportManagerInfoCoursesFilter,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility
} from './report-manager';
import { v4 } from 'uuid';
import {
    DateOptions,
    DateOptionsValueDescriptor,
    EnrollmentTypes,
    SortingOptions,
    VisibilityTypes
} from './custom-report';
import SessionManager from '../services/session/session-manager.session';
import { UserLevels } from '../services/session/user-manager.session';
import { LegacyOrderByParsed, LegacyReport, VisibilityRule } from './migration-component';
import { SessionLoggerService } from '../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { ReportsTypes } from '../reports/constants/report-types';
import { Utils } from '../reports/utils';
import { CourseExtraFieldsResponse, CourseuserExtraFieldsResponse } from '../services/hydra';
import { BaseReportManager } from './base-report-manager';

export class ReportManagerData {
    // Disable linting here, keeping snake case here is useful for compatibility
    // tslint:disable: variable-name
    user_userid?: string;
    user_firstname?: string;
    user_lastname?: string;
    user_email?: string;
    user_valid?: boolean;
    user_suspend_date?: string;
    user_register_date?: string;
    course_code?: string;
    course_name?: string;
    course_category_code?: string;
    course_category?: string;
    course_status?: number;
    course_credits?: number;
    course_type?: string;
    course_date_begin?: string;
    course_date_end?: string;
    course_expired?: boolean;
    course_creation_date?: string;
    courseuser_level?: number;
    courseuser_date_inscr?: string;
    courseuser_date_first_access?: string;
    courseuser_date_last_access?: string;
    courseuser_date_complete?: string;
    courseuser_status?: number;
    courseuser_date_begin_validity?: string;
    courseuser_date_expire_validity?: string;
    courseuser_score_given?: string;
    courseuser_initial_score_given?: string;
    courseuser_assignment_type?: string;
    stats_user_course_completion_percentage?: string;
    stats_total_time_in_course?: number;
    stats_total_sessions_in_course?: number;
    stats_number_of_actions?: number;
    // tslint:enable: variable-name
}

export class UsersCoursesManager extends BaseReportManager {
    reportType = ReportsTypes.USERS_COURSES;
    allFields = {
        user: [
            FieldsList.USER_USERID,
            FieldsList.USER_ID,
            FieldsList.USER_FIRSTNAME,
            FieldsList.USER_LASTNAME,
            FieldsList.USER_FULLNAME,
            FieldsList.USER_EMAIL,
            FieldsList.USER_EMAIL_VALIDATION_STATUS,
            FieldsList.USER_LEVEL,
            FieldsList.USER_DEACTIVATED,
            FieldsList.USER_EXPIRATION,
            FieldsList.USER_SUSPEND_DATE,
            FieldsList.USER_REGISTER_DATE,
            FieldsList.USER_LAST_ACCESS_DATE,
            FieldsList.USER_BRANCH_NAME,
            FieldsList.USER_BRANCH_PATH,
            FieldsList.USER_BRANCHES_CODES,
            FieldsList.USER_DIRECT_MANAGER,
        ],
        course: [
            FieldsList.COURSE_NAME,
            FieldsList.COURSE_ID,
            FieldsList.COURSE_UNIQUE_ID,
            FieldsList.COURSE_CODE,
            FieldsList.COURSE_CATEGORY_CODE,
            FieldsList.COURSE_CATEGORY_NAME,
            FieldsList.COURSE_STATUS,
            FieldsList.COURSE_CREDITS,
            FieldsList.COURSE_DURATION,
            FieldsList.COURSE_TYPE,
            FieldsList.COURSE_DATE_BEGIN,
            FieldsList.COURSE_DATE_END,
            FieldsList.COURSE_EXPIRED,
            FieldsList.COURSE_CREATION_DATE,
            FieldsList.COURSE_LANGUAGE,
            FieldsList.COURSE_SKILLS,
        ],
        courseuser: [
            FieldsList.COURSEUSER_LEVEL,
            FieldsList.COURSEUSER_DATE_INSCR,
            FieldsList.COURSEUSER_DATE_FIRST_ACCESS,
            FieldsList.COURSEUSER_DATE_LAST_ACCESS,
            FieldsList.COURSEUSER_DATE_COMPLETE,
            FieldsList.COURSEUSER_STATUS,
            FieldsList.COURSEUSER_DATE_BEGIN_VALIDITY,
            FieldsList.COURSEUSER_DATE_EXPIRE_VALIDITY,
            FieldsList.COURSEUSER_SCORE_GIVEN,
            FieldsList.COURSEUSER_INITIAL_SCORE_GIVEN
            // FieldsList.COURSEUSER_ENROLLMENT_CODE, ***** TO DO ******
            // FieldsList.COURSEUSER_ENROLLMENT_CODESET ***** TO DO ******
        ],
        usageStatistics: [
            FieldsList.STATS_USER_COURSE_COMPLETION_PERCENTAGE,
            FieldsList.STATS_TOTAL_TIME_IN_COURSE,
            FieldsList.STATS_TOTAL_SESSIONS_IN_COURSE,
            FieldsList.STATS_NUMBER_OF_ACTIONS,
            FieldsList.STATS_SESSION_TIME,
        ],
        mobileAppStatistics: [
            FieldsList.STATS_COURSE_ACCESS_FROM_MOBILE,
            FieldsList.STATS_PERCENTAGE_OF_COURSE_FROM_MOBILE,
            FieldsList.STATS_TIME_SPENT_FROM_MOBILE,
        ],
        flowStatistics: [
            FieldsList.STATS_USER_FLOW_YES_NO,
            FieldsList.STATS_USER_COURSE_FLOW_PERCENTAGE,
            FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW,
        ],
        flowMsTeamsStatistics: [
            FieldsList.STATS_USER_FLOW_MS_TEAMS_YES_NO,
            FieldsList.STATS_USER_COURSE_FLOW_MS_TEAMS_PERCENTAGE,
            FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW_MS_TEAMS,
        ],
    };

    mandatoryFields = {
        user : [
            FieldsList.USER_USERID
        ],
        course: [
            FieldsList.COURSE_NAME
        ],
    };

    logger: SessionLoggerService;

    public constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap | undefined) {
        super(session, reportDetails);
        this.logger = httpContext.get('logger');

        if (this.session.platform.checkPluginESignatureEnabled()) {
            this.allFields.course.push(FieldsList.COURSE_E_SIGNATURE);
            this.allFields.courseuser.push(FieldsList.COURSE_E_SIGNATURE_HASH);
        }

        if (!this.session.platform.checkPluginFlowEnabled()) {
           this.allFields.flowStatistics = [];
        }

        if (!this.session.platform.checkPluginFlowMsTeamsEnabled()) {
            this.allFields.flowMsTeamsStatistics = [];
        }

        if (session.platform.isToggleMultipleEnrollmentCompletions()) {
            this.allFields.courseuser.push(FieldsList.ENROLLMENT_ARCHIVED);
            this.allFields.courseuser.push(FieldsList.ENROLLMENT_ARCHIVING_DATE);
        }

        if (this.session.platform.isCoursesAssignmentTypeActive()) {
            this.allFields.courseuser.push(FieldsList.COURSEUSER_ASSIGNMENT_TYPE);
        }

    }

    public async getQuery(limit = 0, isPreview: boolean, checkPuVisibility = true): Promise<string> {
        const translations = await this.loadTranslations();
        const hydra = this.session.getHydra();
        const athena = this.session.getAthena();

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;
        let fullUsers = '';
        const archivedWhere = [];

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilter(checkPuVisibility);
        }

        const fullCourses = await this.calculateCourseFilter(false, false, checkPuVisibility);

        const select = [];
        const archivedSelect = [];
        const archivedFrom = [];
        const from = [];
        let table = `SELECT * FROM ${TablesList.LEARNING_COURSEUSER_AGGREGATE} WHERE TRUE`;
        archivedFrom.push(`${TablesList.ARCHIVED_ENROLLMENT_COURSE} AS ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}`);

        if (fullUsers !== '') {
            table += ` AND idUser IN (${fullUsers})`;
            archivedWhere.push(` AND user_id IN (${fullUsers})`);
        }

        if (fullCourses !== '') {
            table += ` AND idCourse IN (${fullCourses})`;
            archivedWhere.push(` AND course_id IN (${fullCourses})`);
        }

        // Show only learners
        if (this.info.users && this.info.users.showOnlyLearners) {
            table += ` AND level = ${CourseuserLevels.Student}`;
        }

        // manage the date options - enrollmentDate and completionDate
        table += this.composeDateOptionsFilter();
        archivedWhere.push(this.composeDateOptionsWithArchivedEnrollmentFilter('enrollment_enrolled_at', 'enrollment_completed_at', 'created_at'));

        from.push(`(${table}) AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}`);


        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `valid ${this.getCheckIsValidFieldClause()}` : 'true';

        if (hideExpiredUsers) {
            table += ` AND (expiration IS NULL OR expiration > NOW())`;
        }
        from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);


        // JOIN LEARNING COURSE
        table = `SELECT * FROM ${TablesList.LEARNING_COURSE} WHERE TRUE`;

        if (fullCourses !== '') {
            table += ` AND idCourse IN (${fullCourses})`;
        }

        if (this.info.courses && this.info.courses.courseType !== CourseTypeFilter.ALL) {
            if (this.info.courses.courseType === CourseTypeFilter.E_LEARNING) {
                table += ` AND course_type = '${CourseTypes.Elearning}'`;
                archivedWhere.push(` AND json_extract_scalar(course_info, '$.type') = '${CourseTypes.Elearning}'`);
            }

            if (this.info.courses.courseType === CourseTypeFilter.ILT) {
                table += ` AND course_type = '${CourseTypes.Classroom}'`;
                archivedWhere.push(` AND json_extract_scalar(course_info, '$.type') = '${CourseTypes.Classroom}'`);
            }
        }

        if (this.info.courseExpirationDate) {
            table += this.buildDateFilter('date_end', this.info.courseExpirationDate, 'AND', true);
            archivedWhere.push(this.buildDateFilter('json_extract_scalar(course_info, \'$.end_at\')', this.info.courseExpirationDate, 'AND', true));
        }

        from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);

        let where = [
            `AND ${TablesListAliases.CORE_USER}.userid <> '/Anonymous'`
        ];

        // Variables to check if the specified table was already joined in the query
        let joinCourseCategories = false;
        let joinCoreUserFieldValue = false;
        let joinLearningCourseFieldValue = false;
        let joinLearningCourseuserSign = false;

        // Variables to check if the specified materialized tables was already joined in the query
        let joinCoreUserLevels = false;
        let joinLearningCommontrackCompleted = false;
        let joinLearningOrganizationCount = false;
        let joinLearningTracksessionAggregate = false;
        let joinCoreLangLanguageFieldValue = false;

        let joinCoreUserBranches = false;
        let joinCourseSessionTimeAggregate = false;
        let joinSkillManagersValue = false;
        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        let translationValue = this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');

        let courseExtraFields = {data: { items: []} } as CourseExtraFieldsResponse;
        let courseuserExtraFields = {data: [] } as CourseuserExtraFieldsResponse;

        // load the translations of additional fields only if are selected
        if (this.info.fields.find(item => item.includes('course_extrafield_'))) {
            courseExtraFields = await this.session.getHydra().getCourseExtraFields();
            translationValue = this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course', translationValue);
        }

        if (this.info.fields.find(item => item.includes('courseuser_extrafield_'))) {
            courseuserExtraFields = await this.session.getHydra().getCourseuserExtraFields();
            this.updateExtraFieldsDuplicated(courseuserExtraFields.data, translations, 'course-user', translationValue);
        }

        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            const tmp = await this.getAdditionalFieldsFilters(userExtraFields);
            if (tmp.length) {
                if (!joinCoreUserFieldValue) {
                    joinCoreUserFieldValue = true;
                    from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                }
                where = where.concat(tmp);
            }
        }

        // Course status filter
        if (this.info.enrollment && (this.info.enrollment.completed || this.info.enrollment.inProgress || this.info.enrollment.waitingList || this.info.enrollment.enrollmentsToConfirm || this.info.enrollment.notStarted || this.info.enrollment.suspended || this.info.enrollment.overbooking)) {
            // if all enrollment has set we don't put the filter!
            const allValuesAreTrue = Object.keys(this.info.enrollment).every((k) => this.info.enrollment[k]);

            if (!allValuesAreTrue) {
                const statuses: number[] = [];
                let tmp = '';

                if (this.info.enrollment.notStarted) {
                    statuses.push(EnrollmentStatuses.Subscribed);
                }
                if (this.info.enrollment.inProgress) {
                    statuses.push(EnrollmentStatuses.InProgress);
                }
                if (this.info.enrollment.completed) {
                    statuses.push(EnrollmentStatuses.Completed);
                }
                if (this.info.enrollment.suspended) {
                    statuses.push(EnrollmentStatuses.Suspend);
                }
                if (this.info.enrollment.overbooking) {
                    statuses.push(EnrollmentStatuses.Overbooking);
                }
                let statusesQuery = '';
                const waitingFallbackQuery = `OR (${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.waiting ${this.getCheckIsValidFieldClause()} AND ${TablesListAliases.LEARNING_COURSE}.course_type = 'elearning')`;
                const waitingListQuery = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = -2 ${waitingFallbackQuery}`;
                const enrollmentsToConfirmQuery = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = -1`;

                if (statuses.length > 0) {
                    statusesQuery = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status IN (${statuses.join(',')})`;
                }

                if (statuses.length > 0 && this.info.enrollment.waitingList && this.info.enrollment.enrollmentsToConfirm) {
                    tmp = `AND ( (${statusesQuery}) OR (${waitingListQuery}) OR (${enrollmentsToConfirmQuery}))`;
                } else if (statuses.length > 0 && this.info.enrollment.enrollmentsToConfirm) {
                    tmp = `AND ( (${statusesQuery}) OR (${enrollmentsToConfirmQuery}) )`;
                } else if (statuses.length > 0 && this.info.enrollment.waitingList) {
                    tmp = `AND ( (${statusesQuery}) OR (${waitingListQuery}) )`;
                } else if (statuses.length > 0) {
                    tmp = `AND (${statusesQuery})`;
                } else if (this.info.enrollment.waitingList && !this.info.enrollment.enrollmentsToConfirm) {
                    tmp = `AND (${waitingListQuery})`;
                } else if (!this.info.enrollment.waitingList && this.info.enrollment.enrollmentsToConfirm) {
                    tmp = `AND (${enrollmentsToConfirmQuery})`;
                } else if (this.info.enrollment.waitingList && this.info.enrollment.enrollmentsToConfirm) {
                    tmp = `AND ( (${waitingListQuery}) OR (${enrollmentsToConfirmQuery}) )`;
                }

                if (tmp !== '') {
                    where = where.concat(tmp);
                    tmp = tmp.replace(waitingFallbackQuery, '');
                    archivedWhere.push(tmp.replaceAll(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status`, 'enrollment_status'));
                }
            }
        }

        if (this.info.fields) {
            for (const field of this.info.fields) {
                switch (field) {
                    // User fields
                    case FieldsList.USER_ID:
                        select.push(`${TablesListAliases.CORE_USER}.idst AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_ID])}`);
                        archivedSelect.push(`user_id AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_ID])}`);
                        break;
                    case FieldsList.USER_USERID:
                        select.push(`SUBSTR(${TablesListAliases.CORE_USER}.userid, 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_USERID])}`);
                        archivedSelect.push(`SUBSTR(json_extract_scalar(user_info, '$.username'), 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_USERID])}`);
                        break;
                    case FieldsList.USER_FIRSTNAME:
                        select.push(`${TablesListAliases.CORE_USER}.firstname AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FIRSTNAME])}`);
                        archivedSelect.push(`json_extract_scalar(user_info, '$.firstname') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FIRSTNAME])}`);
                        break;
                    case FieldsList.USER_LASTNAME:
                        select.push(`${TablesListAliases.CORE_USER}.lastname AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LASTNAME])}`);
                        archivedSelect.push(`json_extract_scalar(user_info, '$.lastname') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LASTNAME])}`);
                        break;
                    case FieldsList.USER_FULLNAME:
                        if (this.session.platform.getShowFirstNameFirst()) {
                            select.push(`CONCAT(${TablesListAliases.CORE_USER}.firstname, ' ', ${TablesListAliases.CORE_USER}.lastname) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                            archivedSelect.push(`CONCAT(json_extract_scalar(user_info, '$.firstname'), ' ', json_extract_scalar(user_info, '$.lastname')) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                        } else {
                            select.push(`CONCAT(${TablesListAliases.CORE_USER}.lastname, ' ', ${TablesListAliases.CORE_USER}.firstname) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                            archivedSelect.push(`CONCAT(json_extract_scalar(user_info, '$.lastname'), ' ', json_extract_scalar(user_info, '$.firstname')) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                        }
                        break;
                    case FieldsList.USER_EMAIL:
                        select.push(`${TablesListAliases.CORE_USER}.email AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL])}`);
                        archivedSelect.push(`json_extract_scalar(user_info, '$.email') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL])}`);
                        break;
                    case FieldsList.USER_EMAIL_VALIDATION_STATUS:
                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.CORE_USER}.email_status = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL_VALIDATION_STATUS])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL_VALIDATION_STATUS])}`);
                        break;
                    case FieldsList.USER_LEVEL:
                        if (!joinCoreUserLevels) {
                            joinCoreUserLevels = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_USER_LEVELS} AS ${TablesListAliases.CORE_USER_LEVELS} ON ${TablesListAliases.CORE_USER_LEVELS}.idUser = ${TablesListAliases.CORE_USER}.idst`);
                        }

                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.CORE_USER_LEVELS}.level = ${athena.renderStringInQueryCase(UserLevelsGroups.GodAdmin)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_GODADMIN])}
                                WHEN ${TablesListAliases.CORE_USER_LEVELS}.level = ${athena.renderStringInQueryCase(UserLevelsGroups.PowerUser)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_POWERUSER])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_USER])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LEVEL])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LEVEL])}`);
                        break;
                    case FieldsList.USER_DEACTIVATED:
                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.CORE_USER}.valid ${this.getCheckIsValidFieldClause()} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_DEACTIVATED])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_DEACTIVATED])}`);
                        break;
                    case FieldsList.USER_EXPIRATION:
                        select.push(`${TablesListAliases.CORE_USER}.expiration AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EXPIRATION])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EXPIRATION])}`);
                        break;
                    case FieldsList.USER_SUSPEND_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.CORE_USER}.suspend_date AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_SUSPEND_DATE])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_SUSPEND_DATE])}`);
                        break;
                    case FieldsList.USER_REGISTER_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.CORE_USER}.register_date AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_REGISTER_DATE])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_REGISTER_DATE])}`);
                        break;
                    case FieldsList.USER_LAST_ACCESS_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.CORE_USER}.lastenter AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LAST_ACCESS_DATE])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LAST_ACCESS_DATE])}`);
                        break;
                    case FieldsList.USER_BRANCH_NAME:
                        const subQuery = `(
                        SELECT DISTINCT ${TablesListAliases.CORE_GROUP_MEMBERS}.idstMember, 
                               ${TablesListAliases.CORE_ORG_CHART_TREE}.idst_oc, 
                               ${TablesListAliases.CORE_ORG_CHART_TREE}.lev, 
                               ${TablesListAliases.CORE_ORG_CHART_TREE}.idOrg
                        FROM ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS}
                        JOIN ${TablesList.CORE_GROUP} AS ${TablesListAliases.CORE_GROUP} 
                                ON ${TablesListAliases.CORE_GROUP_MEMBERS}.idst = ${TablesListAliases.CORE_GROUP}.idst 
                                    AND ${TablesListAliases.CORE_GROUP}.groupid LIKE '/oc_%' 
                                    AND ${TablesListAliases.CORE_GROUP}.groupid NOT IN ('/oc_0','/ocd_0')
                        JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE} ON ${TablesListAliases.CORE_GROUP_MEMBERS}.idst = coct.idst_oc)`;
                        const userBranchName =
                            `SELECT DISTINCT ${TablesListAliases.CORE_ORG_CHART_TREE}.idstMember, 
                            IF(${TablesListAliases.CORE_ORG_CHART}1.translation IS NOT NULL AND ${TablesListAliases.CORE_ORG_CHART}1.translation != '', ${TablesListAliases.CORE_ORG_CHART}1.translation, 
                                IF(${TablesListAliases.CORE_ORG_CHART}2.translation IS NOT NULL AND ${TablesListAliases.CORE_ORG_CHART}2.translation != '', ${TablesListAliases.CORE_ORG_CHART}2.translation,
                                    IF(${TablesListAliases.CORE_ORG_CHART}3.translation IS NOT NULL AND ${TablesListAliases.CORE_ORG_CHART}3.translation != '', ${TablesListAliases.CORE_ORG_CHART}3.translation, NULL))) 
                                AS ${FieldsList.USER_BRANCH_NAME}
                        FROM ${subQuery} AS ${TablesListAliases.CORE_ORG_CHART_TREE}
                        JOIN (SELECT idstMember, MAX(lev) AS lev FROM ${subQuery} GROUP BY idstMember) AS ${TablesListAliases.CORE_ORG_CHART_TREE}max 
                            ON ${TablesListAliases.CORE_ORG_CHART_TREE}.idstMember = ${TablesListAliases.CORE_ORG_CHART_TREE}max.idstMember 
                                AND ${TablesListAliases.CORE_ORG_CHART_TREE}.lev = ${TablesListAliases.CORE_ORG_CHART_TREE}max.lev
                        LEFT JOIN ${TablesList.CORE_ORG_CHART} AS ${TablesListAliases.CORE_ORG_CHART}1 ON ${TablesListAliases.CORE_ORG_CHART}1.id_dir = ${TablesListAliases.CORE_ORG_CHART_TREE}.idOrg
                                    AND ${TablesListAliases.CORE_ORG_CHART}1.lang_code = '${this.session.user.getLang()}' 
                        LEFT JOIN ${TablesList.CORE_ORG_CHART} AS ${TablesListAliases.CORE_ORG_CHART}2 ON ${TablesListAliases.CORE_ORG_CHART}2.id_dir = ${TablesListAliases.CORE_ORG_CHART_TREE}.idOrg
                                    AND ${TablesListAliases.CORE_ORG_CHART}2.lang_code = '${this.session.platform.getDefaultLanguage()}' 
                        LEFT JOIN ${TablesList.CORE_ORG_CHART} AS ${TablesListAliases.CORE_ORG_CHART}3 ON ${TablesListAliases.CORE_ORG_CHART}3.id_dir = ${TablesListAliases.CORE_ORG_CHART_TREE}.idOrg
                                    AND ${TablesListAliases.CORE_ORG_CHART}3.lang_code = 'english'`;
                        from.push(`LEFT JOIN (${userBranchName}) AS ${TablesListAliases.CORE_USER_BRANCHES_NAMES} ON ${TablesListAliases.CORE_USER_BRANCHES_NAMES}.idstMember = ${TablesListAliases.CORE_USER}.idst`);
                        select.push(`${TablesListAliases.CORE_USER_BRANCHES_NAMES}.${FieldsList.USER_BRANCH_NAME} AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_NAME])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_NAME])}`);
                        break;
                    case FieldsList.USER_BRANCH_PATH:
                        if (!joinCoreUserBranches) {
                            joinCoreUserBranches = true;
                            let userBranchesTable = '';
                            if (this.session.user.getLevel() === UserLevels.POWER_USER && checkPuVisibility === true) {
                                userBranchesTable = await this.createPuUserBranchesTable();
                            } else {
                                userBranchesTable = this.createUserBranchesTable();
                            }

                            from.push(`LEFT JOIN ${userBranchesTable} AS ${TablesListAliases.CORE_USER_BRANCHES} ON ${TablesListAliases.CORE_USER_BRANCHES}.idst = ${TablesListAliases.CORE_USER}.idst`);
                        }
                        select.push(`${TablesListAliases.CORE_USER_BRANCHES}.branches AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_PATH])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_PATH])}`);
                        break;
                    case FieldsList.USER_BRANCHES_CODES:
                        if (!joinCoreUserBranches) {
                            joinCoreUserBranches = true;
                            let userBranchesTable = '';
                            if (this.session.user.getLevel() === UserLevels.POWER_USER && checkPuVisibility === true) {
                                userBranchesTable = await this.createPuUserBranchesTable();
                            } else {
                                userBranchesTable = this.createUserBranchesTable();
                            }

                            from.push(`LEFT JOIN ${userBranchesTable} AS ${TablesListAliases.CORE_USER_BRANCHES} ON ${TablesListAliases.CORE_USER_BRANCHES}.idst = ${TablesListAliases.CORE_USER}.idst`);
                        }
                        select.push(`${TablesListAliases.CORE_USER_BRANCHES}.codes AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_BRANCHES_CODES])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_BRANCHES_CODES])}`);
                        break;
                    case FieldsList.USER_DIRECT_MANAGER:
                        if (!joinSkillManagersValue) {
                            joinSkillManagersValue = true;
                            from.push(`LEFT JOIN ${TablesList.SKILL_MANAGERS} AS ${TablesListAliases.SKILL_MANAGERS} ON ${TablesListAliases.SKILL_MANAGERS}.idEmployee = ${TablesListAliases.CORE_USER}.idst AND ${TablesListAliases.SKILL_MANAGERS}.type = 1`);
                            from.push(`LEFT JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}s ON ${TablesListAliases.CORE_USER}s.idst = ${TablesListAliases.SKILL_MANAGERS}.idManager`);
                        }
                        let directManagerFullName = '';
                        if (this.session.platform.getShowFirstNameFirst()) {
                            directManagerFullName = `CONCAT(${TablesListAliases.CORE_USER}s.firstname, ' ', ${TablesListAliases.CORE_USER}s.lastname)`;
                        } else {
                            directManagerFullName = `CONCAT(${TablesListAliases.CORE_USER}s.lastname, ' ', ${TablesListAliases.CORE_USER}s.firstname)`;
                        }
                        select.push(`IF(${directManagerFullName} = ' ', SUBSTR(${TablesListAliases.CORE_USER}s.userid, 2), ${directManagerFullName}) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_DIRECT_MANAGER])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_DIRECT_MANAGER])}`);
                        break;

                    // Course fields
                    case FieldsList.COURSE_ID:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.idCourse AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_ID])}`);
                        archivedSelect.push(`CAST(json_extract_scalar(course_info, '$.id') AS INT) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_ID])}`);
                        break;
                    case FieldsList.COURSE_UNIQUE_ID:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.uidCourse AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_UNIQUE_ID])}`);
                        archivedSelect.push(`json_extract_scalar(course_info, '$.uid') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_UNIQUE_ID])}`);
                        break;
                    case FieldsList.COURSE_CODE:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.code AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CODE])}`);
                        archivedSelect.push(`json_extract_scalar(course_info, '$.code') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CODE])}`);
                        break;
                    case FieldsList.COURSE_NAME:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.name AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_NAME])}`);
                        archivedSelect.push(`json_extract_scalar(course_info, '$.name') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_NAME])}`);
                        break;
                    case FieldsList.COURSE_CATEGORY_CODE:
                        if (!joinCourseCategories) {
                            joinCourseCategories = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_CATEGORY} AS ${TablesListAliases.LEARNING_CATEGORY} ON ${TablesListAliases.LEARNING_CATEGORY}.idCategory = ${TablesListAliases.LEARNING_COURSE}.idCategory AND ${TablesListAliases.LEARNING_CATEGORY}.lang_code = '${this.session.user.getLang()}'`);
                        }
                        select.push(`${TablesListAliases.LEARNING_CATEGORY}.code AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_CODE])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_CODE])}`);
                        break;
                    case FieldsList.COURSE_CATEGORY_NAME:
                        if (!joinCourseCategories) {
                            joinCourseCategories = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_CATEGORY} AS ${TablesListAliases.LEARNING_CATEGORY} ON ${TablesListAliases.LEARNING_CATEGORY}.idCategory = ${TablesListAliases.LEARNING_COURSE}.idCategory AND ${TablesListAliases.LEARNING_CATEGORY}.lang_code = '${this.session.user.getLang()}'`);
                        }
                        select.push(`${TablesListAliases.LEARNING_CATEGORY}.translation AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_NAME])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_NAME])}`);
                        break;
                    case FieldsList.COURSE_STATUS:
                        const courseStatus = (field: string) => `CASE
                                WHEN ${field} = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_STATUS_PREPARATION])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_STATUS_EFFECTIVE])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_STATUS])}`;
                        select.push(courseStatus(TablesListAliases.LEARNING_COURSE + `.status`));
                        archivedSelect.push(courseStatus('CAST(json_extract_scalar(course_info, \'$.status\') AS int)'));
                        break;
                    case FieldsList.COURSE_CREDITS:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.credits AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CREDITS])}`);
                        archivedSelect.push(`ROUND(CAST(json_extract_scalar(course_info, '$.credits') AS DOUBLE), 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CREDITS])}`);
                        break;
                    case FieldsList.COURSE_DURATION:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.mediumTime AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DURATION])}`);
                        archivedSelect.push(`CAST(json_extract_scalar(course_info, '$.duration') as INT) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DURATION])}`);
                        break;
                    case FieldsList.COURSE_TYPE:
                        const courseType = (field: string) => `
                            CASE
                                WHEN ${field} = ${athena.renderStringInQueryCase(CourseTypes.Elearning)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_ELEARNING])}
                                WHEN ${field} = ${athena.renderStringInQueryCase(CourseTypes.Classroom)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_CLASSROOM])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_WEBINAR])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_TYPE])}`;
                        select.push(courseType(TablesListAliases.LEARNING_COURSE + `.course_type`));
                        archivedSelect.push(courseType('json_extract_scalar(course_info, \'$.type\')'));
                        break;
                    case FieldsList.COURSE_DATE_BEGIN:
                        select.push(`${this.mapDateDefaultValueWithDLV2(`${TablesListAliases.LEARNING_COURSE}.date_begin`)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_BEGIN])}`);
                        archivedSelect.push(`DATE_PARSE(json_extract_scalar(course_info, '$.start_at'), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_BEGIN])}`);
                        break;
                    case FieldsList.COURSE_DATE_END:
                        select.push(`${this.mapDateDefaultValueWithDLV2(`${TablesListAliases.LEARNING_COURSE}.date_end`)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_END])}`);
                        archivedSelect.push(`DATE_PARSE(json_extract_scalar(course_info, '$.end_at'), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_END])}`);
                        break;
                    case FieldsList.COURSE_EXPIRED:
                        const courseExpired = (field: string) => `
                            CASE
                                WHEN ${field} < CURRENT_DATE THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_EXPIRED])}`;
                        const dateEndColumnDLV2Fix = `(${this.mapDateDefaultValueWithDLV2(`${TablesListAliases.LEARNING_COURSE}.date_end`)})`;
                        select.push(courseExpired(dateEndColumnDLV2Fix));
                        archivedSelect.push(courseExpired('DATE_PARSE(json_extract_scalar(course_info, \'$.end_at\'), \'%Y-%m-%d\')'));
                        break;
                    case FieldsList.COURSE_CREATION_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSE}.create_date AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CREATION_DATE])}`);
                        archivedSelect.push(`DATE_FORMAT(DATE_PARSE(json_extract_scalar(course_info, '$.created_at'),
                        '%Y-%m-%d %H:%i:%s') AT TIME ZONE '${this.info.timezone}',
                        '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CREATION_DATE])}`);
                        break;
                    case FieldsList.COURSE_E_SIGNATURE:
                        if (this.session.platform.checkPluginESignatureEnabled()) {
                            select.push(`
                                CASE
                                    WHEN ${TablesListAliases.LEARNING_COURSE}.has_esignature_enabled = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                    ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                END AS ${athena.renderStringInQuerySelect(FieldsList.COURSE_E_SIGNATURE)}`);
                            archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(FieldsList.COURSE_E_SIGNATURE)}`);
                        }
                        break;
                    case FieldsList.COURSE_LANGUAGE:
                        if (!joinCoreLangLanguageFieldValue) {
                            joinCoreLangLanguageFieldValue = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_LANG_LANGUAGE} AS ${TablesListAliases.CORE_LANG_LANGUAGE} ON ${TablesListAliases.LEARNING_COURSE}.lang_code = ${TablesListAliases.CORE_LANG_LANGUAGE}.lang_code`);

                        }
                        select.push(`${TablesListAliases.CORE_LANG_LANGUAGE}.lang_description AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_LANGUAGE])}`);
                        archivedSelect.push(`json_extract_scalar(course_info, '$.language') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_LANGUAGE])}`);
                        break;
                    case FieldsList.COURSE_SKILLS:
                        from.push(`LEFT JOIN ${TablesList.SKILLS_WITH} AS ${TablesListAliases.SKILLS_WITH} ON ${TablesListAliases.SKILLS_WITH}.idCourse = ${TablesListAliases.LEARNING_COURSE}.idCourse`);
                        archivedFrom.push(`LEFT JOIN ${TablesList.SKILLS_WITH} AS ${TablesListAliases.SKILLS_WITH} ON ${TablesListAliases.SKILLS_WITH}.idCourse = ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_id`);
                        select.push(`${TablesListAliases.SKILLS_WITH}.skillsInCourse AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_SKILLS])}`);
                        archivedSelect.push(`${TablesListAliases.SKILLS_WITH}.skillsInCourse AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_SKILLS])}`);
                        break;
                    // Courseuser fields
                    case FieldsList.COURSEUSER_LEVEL:
                        const courseUserLevel = (field: string) => `
                            CASE
                                WHEN ${field} = ${CourseuserLevels.Teacher} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TEACHER])}
                                WHEN ${field} = ${CourseuserLevels.Tutor} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TUTOR])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_STUDENT])} END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_LEVEL])}`;
                        select.push(courseUserLevel(TablesListAliases.LEARNING_COURSEUSER_AGGREGATE + '.level'));
                        archivedSelect.push(courseUserLevel('enrollment_level'));
                        break;
                    case FieldsList.COURSEUSER_DATE_INSCR:
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_inscr AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_INSCR])}`);
                        archivedSelect.push(`DATE_FORMAT(enrollment_enrolled_at AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_INSCR])}`);
                        break;
                    case FieldsList.COURSEUSER_DATE_FIRST_ACCESS:
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_first_access AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_FIRST_ACCESS])}`);
                        archivedSelect.push(`DATE_FORMAT(enrollment_access_first AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_FIRST_ACCESS])}`);
                        break;
                    case FieldsList.COURSEUSER_DATE_LAST_ACCESS:
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_last_access AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_LAST_ACCESS])}`);
                        archivedSelect.push(`DATE_FORMAT(enrollment_access_last AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_LAST_ACCESS])}`);
                        break;
                    case FieldsList.COURSEUSER_DATE_COMPLETE:
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_complete AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_COMPLETE])}`);
                        archivedSelect.push(`DATE_FORMAT(enrollment_completed_at AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_COMPLETE])}`);
                        break;
                    case FieldsList.COURSEUSER_STATUS:
                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = -1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM])}
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = -2 OR (${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.waiting ${this.getCheckIsValidFieldClause()} AND ${TablesListAliases.LEARNING_COURSE}.course_type = 'elearning') THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = 3 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                                WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = 4 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING])}
                                ELSE CAST (${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status as varchar)
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_STATUS])}`);
                        archivedSelect.push(`
                            CASE
                                WHEN enrollment_status = -1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM])}
                                WHEN enrollment_status = -2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                                WHEN enrollment_status = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                                WHEN enrollment_status = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                                WHEN enrollment_status = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                                WHEN enrollment_status = 3 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                                WHEN enrollment_status = 4 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING])}
                                ELSE CAST (enrollment_status as varchar)
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_STATUS])}`);

                        break;
                    case FieldsList.COURSEUSER_DATE_BEGIN_VALIDITY:
                        const dateBeginValidityColumn = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_begin_validity`;
                        const dateBeginValidityQuery = `${this.mapTimestampDefaultValueWithDLV2(dateBeginValidityColumn, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_BEGIN_VALIDITY])}`;
                        select.push(dateBeginValidityQuery);
                        archivedSelect.push(`DATE_FORMAT(enrollment_validity_start AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_BEGIN_VALIDITY])}`);
                        break;
                    case FieldsList.COURSEUSER_DATE_EXPIRE_VALIDITY:
                        const dateExpireValidityColumn = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_expire_validity`;
                        const dateExpireValidityQuery = `${this.mapTimestampDefaultValueWithDLV2(dateExpireValidityColumn, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_EXPIRE_VALIDITY])}`;
                        select.push(dateExpireValidityQuery);
                        archivedSelect.push(`DATE_FORMAT(enrollment_validity_end AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_EXPIRE_VALIDITY])}`);
                        break;
                    case FieldsList.COURSEUSER_SCORE_GIVEN:
                        select.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.score_given AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_SCORE_GIVEN])}`);
                        archivedSelect.push(`enrollment_score AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_SCORE_GIVEN])}`);
                        break;
                    case FieldsList.COURSEUSER_INITIAL_SCORE_GIVEN:
                        select.push(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.initial_score_given AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_INITIAL_SCORE_GIVEN])}`);
                        archivedSelect.push(`enrollment_score_initial AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_INITIAL_SCORE_GIVEN])}`);
                        break;
                    case FieldsList.COURSE_E_SIGNATURE_HASH:
                        if (this.session.platform.checkPluginESignatureEnabled()) {
                            if (!joinLearningCourseuserSign) {
                                joinLearningCourseuserSign = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEUSER_SIGN} AS ${TablesListAliases.LEARNING_COURSEUSER_SIGN} ON ${TablesListAliases.LEARNING_COURSEUSER_SIGN}.course_id = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse AND ${TablesListAliases.LEARNING_COURSEUSER_SIGN}.user_id = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                            }
                            select.push(`${TablesListAliases.LEARNING_COURSEUSER_SIGN}.signature AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_E_SIGNATURE_HASH])}`);
                            archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_E_SIGNATURE_HASH])}`);
                        }
                        break;
                    case FieldsList.COURSEUSER_ASSIGNMENT_TYPE:
                        if (this.session.platform.isCoursesAssignmentTypeActive()) {
                            select.push(this.getCourseAssignmentTypeSelectField(false, translations));
                            archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_ASSIGNMENT_TYPE])}`);
                        }
                        break;
                    case FieldsList.ENROLLMENT_ARCHIVING_DATE:
                        if (this.session.platform.isToggleMultipleEnrollmentCompletions()) {
                            select.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVING_DATE])}`);
                            archivedSelect.push(`DATE_FORMAT(created_at AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVING_DATE])}`);
                        }
                        break;
                    case FieldsList.ENROLLMENT_ARCHIVED:
                        if (this.session.platform.isToggleMultipleEnrollmentCompletions()) {
                            select.push(`${athena.renderStringInQueryCase(translations[FieldTranslation.NO])} AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVED])}`);
                            archivedSelect.push(`${athena.renderStringInQueryCase(translations[FieldTranslation.YES])} AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVED])}`);
                        }
                        break;
                    // case FieldsList.COURSEUSER_ENROLLMENT_CODE:
                        // TODO: create a view for the aggregations of those fields
                        // break;
                    // case FieldsList.COURSEUSER_ENROLLMENT_CODESET:
                        // TODO: create a view for the aggregations of those fields
                        // break;
                    // Statistic fields
                    case FieldsList.STATS_USER_COURSE_COMPLETION_PERCENTAGE:
                        if (!joinLearningOrganizationCount) {
                            joinLearningOrganizationCount = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_ORGANIZATION_COUNT} AS ${TablesListAliases.LEARNING_ORGANIZATION_COUNT} ON ${TablesListAliases.LEARNING_ORGANIZATION_COUNT}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        if (!joinLearningCommontrackCompleted) {
                            joinLearningCommontrackCompleted = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK_COMPLETED} AS ${TablesListAliases.LEARNING_COMMONTRACK_COMPLETED} ON ${TablesListAliases.LEARNING_COMMONTRACK_COMPLETED}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_COMMONTRACK_COMPLETED}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.LEARNING_COMMONTRACK_COMPLETED}.completed IS NOT NULL AND ${TablesListAliases.LEARNING_ORGANIZATION_COUNT}.count > 0 THEN (${TablesListAliases.LEARNING_COMMONTRACK_COMPLETED}.completed * 100) / ${TablesListAliases.LEARNING_ORGANIZATION_COUNT}.count
                                ELSE 0
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_COMPLETION_PERCENTAGE])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_COMPLETION_PERCENTAGE])}`);
                        break;
                    case FieldsList.STATS_TOTAL_TIME_IN_COURSE:
                        if (!joinLearningTracksessionAggregate) {
                            joinLearningTracksessionAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTime AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_TOTAL_TIME_IN_COURSE])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_TOTAL_TIME_IN_COURSE])}`);
                        break;
                    case FieldsList.STATS_TOTAL_SESSIONS_IN_COURSE:
                        if (!joinLearningTracksessionAggregate) {
                            joinLearningTracksessionAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.actions AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_TOTAL_SESSIONS_IN_COURSE])}`);
                        archivedSelect.push(`enrollment_sessions_count AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_TOTAL_SESSIONS_IN_COURSE])}`);
                        break;
                    case FieldsList.STATS_NUMBER_OF_ACTIONS:
                        if (!joinLearningTracksessionAggregate) {
                            joinLearningTracksessionAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.numberOfActions AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_NUMBER_OF_ACTIONS])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_NUMBER_OF_ACTIONS])}`);
                        break;
                    case FieldsList.STATS_SESSION_TIME:
                        if (!joinCourseSessionTimeAggregate) {
                            joinCourseSessionTimeAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.COURSE_SESSION_TIME_AGGREGATE} AS ${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE} ON ${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE}.id_user = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE}.course_id = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE}.session_time AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_SESSION_TIME])}`);
                        archivedSelect.push(`enrollment_time_spent AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_SESSION_TIME])}`);
                        break;
                    case FieldsList.STATS_USER_FLOW_YES_NO:
                        if (this.session.platform.checkPluginFlowEnabled()) {
                            if (!joinLearningTracksessionAggregate) {
                                joinLearningTracksessionAggregate = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                            }
                            select.push(`
                            IF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.numberOfActionsFlow != 0, ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}, ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])})
                            AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_YES_NO])}`);
                            archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_YES_NO])}`);
                        }
                        break;
                    case FieldsList.STATS_USER_COURSE_FLOW_PERCENTAGE:
                        if (this.session.platform.checkPluginFlowEnabled()) {
                            if (!joinLearningTracksessionAggregate) {
                                joinLearningTracksessionAggregate = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                            }
                            select.push(`
                            IF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTime > 0, (${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTimeFlow * 100)/${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTime, 0)
                            AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_FLOW_PERCENTAGE])}`);
                            archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_FLOW_PERCENTAGE])}`);
                        }
                        break;
                    case FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW:
                        if (this.session.platform.checkPluginFlowEnabled()) {
                            if (!joinLearningTracksessionAggregate) {
                                joinLearningTracksessionAggregate = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                            }
                            select.push(`
                            IF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTimeFlow IS NOT NULL,${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTimeFlow, 0)
                            AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW])}`);
                            archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW])}`);
                        }
                        break;
                    case FieldsList.STATS_COURSE_ACCESS_FROM_MOBILE:
                        if (!joinLearningTracksessionAggregate) {
                            joinLearningTracksessionAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`
                            IF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.numberOfActionsGoLearn != 0, ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}, ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])})
                            AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_COURSE_ACCESS_FROM_MOBILE])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_COURSE_ACCESS_FROM_MOBILE])}`);
                        break;
                    case FieldsList.STATS_PERCENTAGE_OF_COURSE_FROM_MOBILE:
                        if (!joinLearningTracksessionAggregate) {
                            joinLearningTracksessionAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`
                            IF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTime > 0, (${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTimeGoLearn * 100)/${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTime, 0)
                            AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_PERCENTAGE_OF_COURSE_FROM_MOBILE])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_PERCENTAGE_OF_COURSE_FROM_MOBILE])}`);
                        break;
                    case FieldsList.STATS_TIME_SPENT_FROM_MOBILE:
                        if (!joinLearningTracksessionAggregate) {
                            joinLearningTracksessionAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`
                            IF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTimeGoLearn IS NOT NULL,${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTimeGoLearn, 0)
                            AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_TIME_SPENT_FROM_MOBILE])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_TIME_SPENT_FROM_MOBILE])}`);
                        break;
                    case FieldsList.STATS_USER_FLOW_MS_TEAMS_YES_NO:
                        if (this.session.platform.checkPluginFlowMsTeamsEnabled()) {
                            if (!joinLearningTracksessionAggregate) {
                                joinLearningTracksessionAggregate = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                            }
                            select.push(`
                                IF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.numberOfActionsFlowMsTeams != 0, ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}, ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])})
                                AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_MS_TEAMS_YES_NO])}`);
                            archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_MS_TEAMS_YES_NO])}`);
                        }
                        break;
                    case FieldsList.STATS_USER_COURSE_FLOW_MS_TEAMS_PERCENTAGE:
                        if (this.session.platform.checkPluginFlowMsTeamsEnabled()) {
                            if (!joinLearningTracksessionAggregate) {
                                joinLearningTracksessionAggregate = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                            }
                            select.push(`
                            IF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTime > 0, (${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTimeFlowMsTeams * 100)/${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTime, 0)
                            AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_FLOW_MS_TEAMS_PERCENTAGE])}`);
                            archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_FLOW_MS_TEAMS_PERCENTAGE])}`);
                        }
                        break;
                    case FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW_MS_TEAMS:
                        if (this.session.platform.checkPluginFlowMsTeamsEnabled()) {
                            if (!joinLearningTracksessionAggregate) {
                                joinLearningTracksessionAggregate = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                            }
                            select.push(`
                                IF ( ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTimeFlowMsTeams IS NOT NULL,${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTimeFlowMsTeams, 0)
                                AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW_MS_TEAMS])}`);
                            archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW_MS_TEAMS])}`);
                        }
                        break;

                    // Additional fields
                    default:
                        if (this.isUserExtraField(field)) {
                            const fieldId = parseInt(field.replace('user_extrafield_', ''), 10);

                            for (const userField of userExtraFields.data.items) {
                                if (parseInt(userField.id, 10) === fieldId) {
                                    if (await this.checkUserAdditionalFieldInAthena(fieldId) === false) {
                                        select.push(`'' AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                    } else {
                                        if (!joinCoreUserFieldValue) {
                                            joinCoreUserFieldValue = true;
                                            from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                                        }
                                        switch (userField.type) {
                                            case AdditionalFieldsTypes.CodiceFiscale:
                                            case AdditionalFieldsTypes.FreeText:
                                            case AdditionalFieldsTypes.GMail:
                                            case AdditionalFieldsTypes.ICQ:
                                            case AdditionalFieldsTypes.MSN:
                                            case AdditionalFieldsTypes.Skype:
                                            case AdditionalFieldsTypes.Textfield:
                                            case AdditionalFieldsTypes.Yahoo:
                                            case AdditionalFieldsTypes.Upload:
                                                select.push(`${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                    select.push(`DATE_FORMAT(${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}, '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                    archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} AND ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.YesNo:
                                                select.push(`
                                                CASE
                                                    WHEN ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                                    WHEN ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                                    ELSE ''
                                                END AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Country:
                                                from.push(`LEFT JOIN ${TablesList.CORE_COUNTRY} AS ${TablesListAliases.CORE_COUNTRY}_${fieldId} ON ${TablesListAliases.CORE_COUNTRY}_${fieldId}.id_country = ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}`);
                                                select.push(`${TablesListAliases.CORE_COUNTRY}_${fieldId}.name_country AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                        }
                                    }
                                    break;
                                }
                            }
                        } else if (this.isCourseExtraField(field)) {
                            const fieldId = parseInt(field.replace('course_extrafield_', ''), 10);
                            for (const courseField of courseExtraFields.data.items) {
                                if (courseField.id === fieldId) {
                                    if (await this.checkCourseAdditionalFieldInAthena(fieldId) === false) {
                                        const additionalField = this.setAdditionalFieldTranslation(courseField);
                                        select.push(`'' AS ${athena.renderStringInQuerySelect(additionalField)}`);
                                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(additionalField)}`);
                                    } else {
                                        if (!joinLearningCourseFieldValue) {
                                            joinLearningCourseFieldValue = true;
                                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_VALUE} AS ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE} ON ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.id_course = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                                        }
                                        switch (courseField.type) {
                                            case AdditionalFieldsTypes.Textarea:
                                            case AdditionalFieldsTypes.Textfield:
                                                select.push(`${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId} AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}, '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId} AND ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.YesNo:
                                                select.push(`
                                                CASE
                                                    WHEN ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId} = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                                    ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                                END AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                        }
                                    }
                                    break;
                                }
                            }
                        } else if (this.isCourseUserExtraField(field)) {
                            const fieldId = parseInt(field.replace('courseuser_extrafield_', ''), 10);
                            for (const courseuserField of courseuserExtraFields.data) {
                                if (courseuserField.id === fieldId) {
                                    if (await this.checkEnrollmentAdditionalFieldInAthena(fieldId) === false) {
                                        select.push(`'' AS ${athena.renderStringInQuerySelect(courseuserField.name)}`);
                                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(courseuserField.name)}`);
                                    } else  {
                                        switch (courseuserField.type) {
                                            case AdditionalFieldsTypes.Textarea:
                                            case AdditionalFieldsTypes.Text:
                                            case AdditionalFieldsTypes.Date:
                                                select.push(`JSON_EXTRACT_SCALAR(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.enrollment_fields, '$.${fieldId}') AS ${athena.renderStringInQuerySelect(courseuserField.name)}`);
                                                archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(courseuserField.name)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.LEARNING_ENROLLMENT_FIELDS_DROPDOWN} AS ${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId} ON ${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.id = CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.enrollment_fields, '$.${fieldId}') AS INTEGER)`);
                                                select.push(`IF(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.translation LIKE '%"${this.session.user.getLangCode()}":%', JSON_EXTRACT_SCALAR(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.translation, '$["${this.session.user.getLangCode()}"]'), JSON_EXTRACT_SCALAR(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.translation, '$["${this.session.platform.getDefaultLanguageCode()}"]')) AS ${athena.renderStringInQuerySelect(courseuserField.name)}`);
                                                archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(courseuserField.name)}`);
                                                break;
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                        break;
                }
            }
        }

        // Workaround because we cannot filter for Dates if Enrollment Archive Date is enabled. In this case we consider only the "archived" query, without doing the Union
        const isOnlyArchivedDateFilter = !this.info.archivingDate?.any && this.info.completionDate.any && this.info.enrollmentDate.any;
        const isArchivedAndOtherDateFiltersWithAllConditionsSatisfied = this.info.conditions === DateOptions.CONDITIONS && !this.info.archivingDate?.any && (!this.info.completionDate.any || !this.info.enrollmentDate.any);
        const isOnlyArchivedQuery = this.info.enrollment.enrollmentTypes === EnrollmentTypes.activeAndArchived && (isOnlyArchivedDateFilter || isArchivedAndOtherDateFiltersWithAllConditionsSatisfied);

        let query = '';
        if (this.info.fields.includes(FieldsList.COURSE_SKILLS)) {
            query += `WITH ${TablesList.SKILLS_WITH} AS (
                        SELECT  ${TablesListAliases.SKILL_SKILLS_OBJECTS}.idObject as idCourse, 
                                ARRAY_JOIN(ARRAY_SORT(ARRAY_AGG(DISTINCT(${TablesListAliases.SKILL_SKILLS}.title))), ', ') as skillsInCourse 
                        FROM ${TablesList.SKILL_SKILLS_OBJECTS} AS sso LEFT JOIN ${TablesList.SKILL_SKILLS} AS ${TablesListAliases.SKILL_SKILLS} 
                            ON ${TablesListAliases.SKILL_SKILLS}.id = ${TablesListAliases.SKILL_SKILLS_OBJECTS}.idSkill
                        WHERE ${TablesListAliases.SKILL_SKILLS_OBJECTS}.objectType = 1`;
            if (fullCourses !== '') {
                query += ` AND ${TablesListAliases.SKILL_SKILLS_OBJECTS}.idObject IN (${fullCourses})`;
            }
            query += ` GROUP BY ${TablesListAliases.SKILL_SKILLS_OBJECTS}.idObject) `;
        }
        if (this.showActive() && !isOnlyArchivedQuery || !this.session.platform.isToggleMultipleEnrollmentCompletions()) {
            query += `
            SELECT ${select.join(', ')}
            FROM ${from.join(' ')}
            ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}`;
        }

        if (this.info.enrollment.enrollmentTypes === EnrollmentTypes.activeAndArchived && this.session.platform.isToggleMultipleEnrollmentCompletions() && !isOnlyArchivedQuery) {
            query += `
            UNION
            `;
        }

        if (this.showArchived() && this.session.platform.isToggleMultipleEnrollmentCompletions()) {
            query += `
            SELECT ${archivedSelect.join(', ')}
            FROM ${archivedFrom.join(' ')}
            ${archivedWhere.length > 0 ? ` WHERE TRUE ${archivedWhere.join(' ')}` : ''}
            `;
        }

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(select, translations, {user: userExtraFields.data.items, course: courseExtraFields.data.items, userCourse: courseuserExtraFields.data, webinar: [], classroom: []});
        }

        // get the sql like LIMIT for the query
        query += this.getQueryExportLimit(limit);

        return query;
    }

    public async getQuerySnowflake(limit = 0, isPreview: boolean, checkPuVisibility = true, fromSchedule = false): Promise<string> {

        const translations = await this.loadTranslations();
        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;
        let fullUsers = '';
        const archivedWhere = [];

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilterSnowflake(checkPuVisibility);
        }

        const fullCourses = await this.calculateCourseFilterSnowflake(false, false, checkPuVisibility);

        // Needed to save some info for the select switch statement
        const queryHelper = {
            select: [],
            archivedSelect: [],
            from: [],
            archivedFrom: [],
            join: [],
            cte: [],
            groupBy: [],
            userAdditionalFieldsSelect: [],
            userAdditionalFieldsFrom: [],
            userAdditionalFieldsId: [],
            archivedGroupBy: [],
            courseAdditionalFieldsSelect: [],
            courseAdditionalFieldsFrom: [],
            courseAdditionalFieldsId: [],
            translations,
            checkPuVisibility
        };

        let table = `SELECT * FROM ${TablesList.LEARNING_COURSEUSER_AGGREGATE} WHERE TRUE`;
        queryHelper.archivedFrom.push(`${TablesList.ARCHIVED_ENROLLMENT_COURSE} AS ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}`);

        if (fullUsers !== '') {
            table += ` AND "iduser" IN (${fullUsers})`;
            archivedWhere.push(` AND "user_id" IN (${fullUsers})`);
        }

        if (fullCourses !== '') {
            table += ` AND "idcourse" IN (${fullCourses})`;
            archivedWhere.push(` AND "course_id" IN (${fullCourses})`);
        }

        // Show only learners
        if (this.info.users && this.info.users.showOnlyLearners) {
            table += ` AND "level" = ${CourseuserLevels.Student}`;
            archivedWhere.push(` AND "enrollment_level" = ${CourseuserLevels.Student}`);
        }

        // manage the date options - enrollmentDate and completionDate
        table += this.composeDateOptionsFilter();
        archivedWhere.push(this.composeDateOptionsWithArchivedEnrollmentFilter(
            `${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_enrolled_at`,
            `${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_completed_at`,
            `${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.created_at`)
        );

        queryHelper.from.push(`(${table}) AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}`);

        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `"valid" = 1 ` : 'true';

        if (hideExpiredUsers) {
            table += ` AND ("expiration" IS NULL OR "expiration" > current_timestamp())`;
        }
        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);

        // JOIN LEARNING COURSE
        table = `SELECT * FROM ${TablesList.LEARNING_COURSE} WHERE TRUE`;

        if (fullCourses !== '') {
            table += ` AND "idcourse" IN (${fullCourses})`;
        }

        if (this.info.courses && this.info.courses.courseType !== CourseTypeFilter.ALL) {
            if (this.info.courses.courseType === CourseTypeFilter.E_LEARNING) {
                table += ` AND "course_type" = '${CourseTypes.Elearning}'`;
                archivedWhere.push(` AND JSON_EXTRACT_PATH_TEXT("course_info", 'type') = '${CourseTypes.Elearning}'`);
            }

            if (this.info.courses.courseType === CourseTypeFilter.ILT) {
                table += ` AND "course_type" = '${CourseTypes.Classroom}'`;
                archivedWhere.push(` AND JSON_EXTRACT_PATH_TEXT("course_info", 'type') = '${CourseTypes.Classroom}'`);
            }
        }

        if (this.info.courseExpirationDate) {
            table += this.buildDateFilter('date_end', this.info.courseExpirationDate, 'AND', true);
            archivedWhere.push(this.buildDateFilter('json_extract_path_text("course_info", \'$.end_at\')', this.info.courseExpirationDate, 'AND', true));
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);

        let where = [
            `AND ${TablesListAliases.CORE_USER}."userid" <> '/Anonymous'`
        ];

        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        let translationValue = this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');

        let courseExtraFields = {data: { items: []} } as CourseExtraFieldsResponse;
        let courseuserExtraFields = {data: [] } as CourseuserExtraFieldsResponse;

        // load the translations of additional fields only if are selected
        if (this.info.fields.find(item => item.includes('course_extrafield_'))) {
            courseExtraFields = await this.session.getHydra().getCourseExtraFields();
            translationValue = this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course', translationValue);
        }

        if (this.info.fields.find(item => item.includes('courseuser_extrafield_'))) {
            courseuserExtraFields = await this.session.getHydra().getCourseuserExtraFields();
            this.updateExtraFieldsDuplicated(courseuserExtraFields.data, translations, 'course-user', translationValue);
        }

        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            this.getAdditionalUsersFieldsFiltersSnowflake(queryHelper, userExtraFields);
        }

        // Course status filter
        if (this.info.enrollment && (this.info.enrollment.completed || this.info.enrollment.inProgress || this.info.enrollment.waitingList || this.info.enrollment.enrollmentsToConfirm || this.info.enrollment.notStarted || this.info.enrollment.suspended || this.info.enrollment.overbooking)) {
            // if all enrollment has set we don't put the filter!
            const allValuesAreTrue = Object.keys(this.info.enrollment).every((k) => this.info.enrollment[k]);

            if (!allValuesAreTrue) {
                const statuses: number[] = [];
                let tmp = '';

                if (this.info.enrollment.notStarted) {
                    statuses.push(EnrollmentStatuses.Subscribed);
                }
                if (this.info.enrollment.inProgress) {
                    statuses.push(EnrollmentStatuses.InProgress);
                }
                if (this.info.enrollment.completed) {
                    statuses.push(EnrollmentStatuses.Completed);
                }
                if (this.info.enrollment.suspended) {
                    statuses.push(EnrollmentStatuses.Suspend);
                }
                if (this.info.enrollment.overbooking) {
                    statuses.push(EnrollmentStatuses.Overbooking);
                }
                let statusesQuery = '';
                const waitingListQuery = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = -2 OR (${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."waiting" = 1 AND ${TablesListAliases.LEARNING_COURSE}."course_type" = 'elearning')`;
                const enrollmentsToConfirmQuery = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = -1`;

                if (statuses.length > 0) {
                    statusesQuery = `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" IN (${statuses.join(',')})`;
                }

                if (statuses.length > 0 && this.info.enrollment.waitingList && this.info.enrollment.enrollmentsToConfirm) {
                    tmp = `AND ( (${statusesQuery}) OR (${waitingListQuery}) OR (${enrollmentsToConfirmQuery}))`;
                } else if (statuses.length > 0 && this.info.enrollment.enrollmentsToConfirm) {
                    tmp = `AND ( (${statusesQuery}) OR (${enrollmentsToConfirmQuery}) )`;
                } else if (statuses.length > 0 && this.info.enrollment.waitingList) {
                    tmp = `AND ( (${statusesQuery}) OR (${waitingListQuery}) )`;
                } else if (statuses.length > 0) {
                    tmp = `AND (${statusesQuery})`;
                } else if (this.info.enrollment.waitingList && !this.info.enrollment.enrollmentsToConfirm) {
                    tmp = `AND (${waitingListQuery})`;
                } else if (!this.info.enrollment.waitingList && this.info.enrollment.enrollmentsToConfirm) {
                    tmp = `AND (${enrollmentsToConfirmQuery})`;
                } else if (this.info.enrollment.waitingList && this.info.enrollment.enrollmentsToConfirm) {
                    tmp = `AND ( (${waitingListQuery}) OR (${enrollmentsToConfirmQuery}) )`;
                }

                if (tmp !== '') {
                    where = where.concat(tmp);
                    tmp = tmp.replace(`OR (${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."waiting" = 1 AND ${TablesListAliases.LEARNING_COURSE}."course_type" = 'elearning')`, '');
                    archivedWhere.push(tmp.replaceAll(`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status"`, '"enrollment_status"'));
                }
            }
        }

        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged = this.querySelectUserFields(field, queryHelper) ||
                    this.querySelectCourseFields(field, queryHelper) ||
                    this.querySelectEnrollmentFields(field, queryHelper) ||
                    this.querySelectUsageStatisticsFields(field, queryHelper) ||
                    this.querySelectMobileAppStatisticsFields(field, queryHelper) ||
                    this.querySelectFlowStatisticsFields(field, queryHelper) ||
                    this.querySelectFlowMsTeamsStatisticsFields(field, queryHelper) ||
                    this.queryWithUserAdditionalFields(field, queryHelper, userExtraFields) ||
                    this.queryWithCourseAdditionalFields(field, queryHelper, courseExtraFields) ||
                    this.queryWithCourseUserAdditionalFields(field, queryHelper, courseuserExtraFields);
            }
        }

        // Workaround because we cannot filter for Dates if Enrollment Archive Date is enabled. In this case we consider only the "archived" query, without doing the Union
        const isOnlyArchivedDateFilter = !this.info.archivingDate?.any && this.info.completionDate.any && this.info.enrollmentDate.any;
        const isArchivedAndOtherDateFiltersWithAllConditionsSatisfied = this.info.conditions === DateOptions.CONDITIONS && !this.info.archivingDate?.any && (!this.info.completionDate.any || !this.info.enrollmentDate.any);
        const isOnlyArchivedQuery = this.info.enrollment.enrollmentTypes === EnrollmentTypes.activeAndArchived && (isOnlyArchivedDateFilter || isArchivedAndOtherDateFiltersWithAllConditionsSatisfied);

        if (queryHelper.userAdditionalFieldsId.length > 0) {
            queryHelper.cte.push(this.additionalUserFieldQueryWith(queryHelper.userAdditionalFieldsId));
            queryHelper.cte.push(this.additionalFieldQueryWith(queryHelper.userAdditionalFieldsFrom, queryHelper.userAdditionalFieldsSelect, queryHelper.userAdditionalFieldsId, 'id_user', TablesList.CORE_USER_FIELD_VALUE_WITH, TablesList.USERS_ADDITIONAL_FIELDS_TRANSLATIONS));
        }
        if (queryHelper.courseAdditionalFieldsId.length > 0) {
            queryHelper.cte.push(this.additionalCourseFieldQueryWith(queryHelper.courseAdditionalFieldsId));
            queryHelper.cte.push(this.additionalFieldQueryWith(queryHelper.courseAdditionalFieldsFrom, queryHelper.courseAdditionalFieldsSelect, queryHelper.courseAdditionalFieldsId, 'id_course', TablesList.LEARNING_COURSE_FIELD_VALUE_WITH, TablesList.COURSES_ADDITIONAL_FIELDS_TRANSLATIONS));
        }

        let query = '';
        if (this.showActive() && !isOnlyArchivedQuery || !this.session.platform.isToggleMultipleEnrollmentCompletions()) {
            const groupByUniqueFields = queryHelper.groupBy.length > 0 && this.info.fields.includes(FieldsList.COURSE_SKILLS)
                ? [...new Set(queryHelper.groupBy)] : [];
            query += `
            ${queryHelper.cte.length > 0 ? `WITH ${queryHelper.cte.join(', ')}` : ''}
            SELECT ${queryHelper.select.join(', ')}
            FROM ${queryHelper.from.join(' ')}
            ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}
            ${groupByUniqueFields.length > 0 ? ` GROUP BY ${groupByUniqueFields.join(', ')}` : ''}`;
        }

        if (this.info.enrollment.enrollmentTypes === EnrollmentTypes.activeAndArchived && this.session.platform.isToggleMultipleEnrollmentCompletions() && !isOnlyArchivedQuery) {
            query += `
            UNION
            `;
        }

        if (this.showArchived() && this.session.platform.isToggleMultipleEnrollmentCompletions()) {
            const archivedGroupByUniqueFields = [...new Set(queryHelper.archivedGroupBy)];
            query += `
            SELECT ${queryHelper.archivedSelect.join(', ')}
            FROM ${queryHelper.archivedFrom.join(' ')}
            ${archivedWhere.length > 0 ? ` WHERE TRUE ${archivedWhere.join(' ')}` : ''}
            ${archivedGroupByUniqueFields.length > 0 ? ` GROUP BY ${archivedGroupByUniqueFields.join(', ')}` : ''}`;
        }

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(
                queryHelper.select,
                queryHelper.translations,
                {
                    user: userExtraFields.data.items,
                    course: courseExtraFields.data.items,
                    userCourse: courseuserExtraFields.data,
                    webinar: [],
                    classroom: []
                },
                fromSchedule
            );
        }

        // get the sql like LIMIT for the query
        query += this.getQueryExportLimit(limit);

        return query;
    }

    protected getReportDefaultStructure(report: ReportManagerInfo, title: string, platform: string, idUser: number, description: string): ReportManagerInfo {
        const id = v4();
        const date = new Date();

        const tmpFields: string[] = [];

        this.mandatoryFields.user.forEach(element => {
            tmpFields.push(element);
        });

        this.mandatoryFields.course.forEach(element => {
            tmpFields.push(element);
        });

        report.fields = tmpFields;
        report.deleted = false;

        report.idReport = id;
        report.type = this.reportType;
        report.timezone = this.session.user.getTimezone();
        report.title = title;
        report.platform = platform;

        report.users = new ReportManagerInfoUsersFilter();
        report.users.all = true;

        report.courses = new ReportManagerInfoCoursesFilter();
        report.courses.all = true;

        report.learningPlans = new ReportManagerLearningPlansFilter();
        report.learningPlans.all = true;

        report.standard = false;

        report.visibility = new ReportManagerInfoVisibility();
        report.visibility.type = VisibilityTypes.ALL_GODADMINS;

        report.author = idUser;
        report.lastEditBy = {
            idUser,
            firstname: '',
            lastname: '',
            username: '',
            avatar: ''
        };
        report.creationDate = this.convertDateObjectToDatetime(date);
        report.lastEdit = this.convertDateObjectToDatetime(date);

        // manage the planning default fields
        report.planning = this.getDefaultPlanningFields();
        report.conditions = DateOptions.CONDITIONS;
        report.enrollmentDate = this.getDefaultDateOptions();
        report.courseExpirationDate = this.getDefaultCourseExpDate();
        report.sortingOptions = this.getSortingOptions();
        report.completionDate = this.getDefaultDateOptions();
        report.enrollment = this.getDefaultEnrollment();

        if (this.session.platform.isToggleMultipleEnrollmentCompletions()) {
            report.archivingDate = this.getDefaultDateOptions();
        }

        if (description) {
            report.description = description;
        }

        return report;
    }

    /**
     * Get the default value for the Sorting Options
     */
    protected getSortingOptions(): SortingOptions {
        return {
            selector: 'default',
            selectedField: FieldsList.USER_USERID,
            orderBy: 'asc',
        };
    }

    public async getAvailablesFields(): Promise<ReportAvailablesFields> {
        const result: any = await this.getBaseAvailableFields();

        const userExtraFields = await this.getAvailableUserExtraFields();
        result.user.push(...userExtraFields);

        const courseExtraFields = await this.getAvailableCourseExtraFields();
        result.course.push(...courseExtraFields);

        const courseuserExtraFields = await this.getAvailableEnrollmentExtraFields();
        result.courseuser.push(...courseuserExtraFields);

        return result;
    }

    public async getBaseAvailableFields(): Promise<ReportAvailablesFields> {
        const result: ReportAvailablesFields = {};
        const translations = await this.loadTranslations(true);

        // Recover user fields
        result.user = [];
        for (const field of this.allFields.user) {
            result.user.push({
                field,
                idLabel: field,
                mandatory: this.mandatoryFields.user.includes(field),
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        // Recover course fields
        result.course = [];
        for (const field of this.allFields.course) {
            result.course.push({
                field,
                idLabel: field,
                mandatory: this.mandatoryFields.course.includes(field),
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        // Recover courseuser fields
        result.courseuser = [];
        for (const field of this.allFields.courseuser) {
            result.courseuser.push({
                field,
                idLabel: field,
                mandatory: false,
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        // Recover Usage Statistics fields
        result.usageStatistics = [];
        for (const field of this.allFields.usageStatistics) {
            result.usageStatistics.push({
                field,
                idLabel: field,
                mandatory: false,
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        // Recover Mobile App Statistics fields
        result.mobileAppStatistics = [];
        for (const field of this.allFields.mobileAppStatistics) {
            result.mobileAppStatistics.push({
                field,
                idLabel: field,
                mandatory: false,
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        // Recover Flow Statistics fields
        if (this.session.platform.checkPluginFlowEnabled()) {
            result.flowStatistics = [];
            for (const field of this.allFields.flowStatistics) {
                result.flowStatistics.push({
                    field,
                    idLabel: field,
                    mandatory: false,
                    isAdditionalField: false,
                    translation: translations[field]
                });
            }
        }

        // Recover Flow For MS TEAM Statistics fields
        if (this.session.platform.checkPluginFlowMsTeamsEnabled()) {
            result.flowMsTeamsStatistics = [];
            for (const field of this.allFields.flowMsTeamsStatistics) {
                result.flowMsTeamsStatistics.push({
                    field,
                    idLabel: field,
                    mandatory: false,
                    isAdditionalField: false,
                    translation: translations[field]
                });
            }
        }

        return result;
    }

    /**
     * Set the sortingOptions object with the input passed
     * @param sortingOptions The object that describes a sortingOptions
     */
    setSortingOptions(item: SortingOptions): void {
        this.info.sortingOptions = {
            selector: item && item.selector ? item.selector : 'default',
            selectedField: item && item.selectedField ? item.selectedField : FieldsList.USER_USERID,
            orderBy: item && item.orderBy ? item.orderBy : 'asc'
        };
    }

    public parseLegacyReport(legacyReport: LegacyReport, platform: string, visibilityRules: VisibilityRule[]): ReportManagerInfo {
        const utils = new Utils();
        // get a default structure for our report type
        let report = this.getReportDefaultStructure(new ReportManagerInfo(), legacyReport.filter_name, platform, +legacyReport.author, '');
        // set title, dates and visibility options
        report = this.setCommonFieldsBetweenReportTypes(report, legacyReport, visibilityRules);
        // and now the report type specific section
        // users, groups and branches
        const filterData = JSON.parse(legacyReport.filter_data);

        /**
         * USERS IMPORT - populate the users field of the aamon report
         */
        this.legacyUserImport(filterData, report, legacyReport.id_filter);
        /**
         * COURSES IMPORT
         */
        this.legacyCourseImport(filterData, report, legacyReport.id_filter);

        // check filters section validity
        if (!filterData.filters) {
            this.logger.error(`No legacy filters section for id report: ${legacyReport.id_filter}`);
            throw new Error('No legacy filters section');
        }

        const filters = filterData.filters;
        if (filters.courses_expiring_in && filters.courses_expiring_in.indexOf('/') === -1) {
            report.courseExpirationDate = {
                ...report.courseExpirationDate as DateOptionsValueDescriptor,
                any: false,
                type: 'relative',
                days: +filters.courses_expiring_in,
                operator: 'expiringIn',
            };
        }

        // import courses_expiring_before only if courses_expiring_in is not populated
        if (filters.courses_expiring_before && !(filters.courses_expiring_in && filters.courses_expiring_in.indexOf('/') === -1)) {
            report.courseExpirationDate = {
                ...report.courseExpirationDate as DateOptionsValueDescriptor,
                any: false,
                type: 'range',
                days: 0,
                operator: 'range',
                from: '1970-01-01',
                to: filters.courses_expiring_before,
            };
        }
        // Enrollment Date
        if (filters.start_date.type !== 'any' && filters.start_date.data.days_count.indexOf('/') === -1) {
            report.enrollmentDate = utils.parseLegacyFilterDate(report.enrollmentDate as DateOptionsValueDescriptor, filters.start_date);
        }
        // Completion Date
        if (filters.end_date.type !== 'any' && filters.end_date.data.days_count.indexOf('/') === -1) {
            report.completionDate = utils.parseLegacyFilterDate(report.completionDate as DateOptionsValueDescriptor, filters.end_date);
        }

        // Conditions
        if (filters.condition_status) {
            report.conditions = filters.condition_status === 'and' ? 'allConditions' : 'atLeastOneCondition';
        }

        // Enrollment Status
        this.extractLegacyEnrollmentStatus(filters, report);

        // map selected fields and manage order by
        if (filterData.fields) {
            let legacyOrderField: LegacyOrderByParsed | undefined;
            const userMandatoryFieldsMap = this.mandatoryFields.user.reduce((previousValue: {[key: string]: string}, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});
            const courseMandatoryFieldsMap = this.mandatoryFields.course.reduce((previousValue: {[key: string]: string}, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});
            // user fields and order by
            const userFieldsDescriptor = this.mapUserSelectedFields(filterData.fields.user, filterData.order, userMandatoryFieldsMap);
            report.fields.push(...userFieldsDescriptor.fields);
            if (userFieldsDescriptor.orderByDescriptor) legacyOrderField = userFieldsDescriptor.orderByDescriptor;

            const courseFieldsDescriptor = this.mapCourseSelectedFields(filterData.fields.course, filterData.order, courseMandatoryFieldsMap);
            report.fields.push(...courseFieldsDescriptor.fields);
            if (courseFieldsDescriptor.orderByDescriptor) legacyOrderField = courseFieldsDescriptor.orderByDescriptor;

            const enrollmentFieldsDescriptor = this.mapEnrollmentSelectedFields(filterData.fields.enrollment, filterData.order);
            report.fields.push(...enrollmentFieldsDescriptor.fields);
            if (enrollmentFieldsDescriptor.orderByDescriptor) legacyOrderField = enrollmentFieldsDescriptor.orderByDescriptor;

            if (legacyOrderField) {
                report.sortingOptions = {
                    orderBy: legacyOrderField.direction,
                    selector: 'custom',
                    selectedField: legacyOrderField.field,
                };
            }
        }

        return report;
    }
}
