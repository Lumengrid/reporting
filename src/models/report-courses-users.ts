import { v4 } from 'uuid';

import SessionManager from '../services/session/session-manager.session';
import {
    DateOptions,
    DateOptionsValueDescriptor,
    SortingOptions,
    VisibilityTypes
} from './custom-report';
import { LegacyOrderByParsed, LegacyReport, VisibilityRule } from './migration-component';
import {
    FieldsList,
    ReportAvailablesFields,
    ReportManagerInfo,
    ReportManagerInfoCoursesFilter,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility,
    TablesListAliases,
    TablesList,
    FieldTranslation,
    ReportManagerLearningPlansFilter,
} from './report-manager';
import {
    EnrollmentStatuses,
    AdditionalFieldsTypes,
    CourseTypes,
    CourseuserLevels,
    CourseTypeFilter
} from './base';
import { UserLevels } from '../services/session/user-manager.session';
import { ReportsTypes } from '../reports/constants/report-types';
import { BaseReportManager } from './base-report-manager';
import { CourseExtraFieldsResponse } from '../services/hydra';

export class CoursesUsersManager extends BaseReportManager {
    reportType = ReportsTypes.COURSES_USERS;

    allFields = {
        course: [
            FieldsList.COURSE_NAME,
            FieldsList.COURSE_CATEGORY_CODE,
            FieldsList.COURSE_CATEGORY_NAME,
            FieldsList.COURSE_TYPE,
            FieldsList.COURSE_CODE,
            FieldsList.COURSE_ID,
            FieldsList.COURSE_STATUS,
            FieldsList.COURSE_DURATION,
            FieldsList.COURSE_CREDITS,
            FieldsList.COURSE_EXPIRED,
            FieldsList.COURSE_CREATION_DATE,
            FieldsList.COURSE_DATE_BEGIN,
            FieldsList.COURSE_DATE_END,
            FieldsList.COURSE_LANGUAGE,
            FieldsList.COURSE_UNIQUE_ID,
            FieldsList.COURSE_SKILLS,
        ],
        usageStatistics: [
            FieldsList.STATS_COMPLETED_USERS,
            FieldsList.STATS_COMPLETED_USERS_PERCENTAGE,
            FieldsList.STATS_IN_PROGRESS_USERS,
            FieldsList.STATS_IN_PROGRESS_USERS_PERCENTAGE,
            FieldsList.STATS_NOT_STARTED_USERS,
            FieldsList.STATS_NOT_STARTED_USERS_PERCENTAGE,
            FieldsList.STATS_COURSE_RATING,
            FieldsList.STATS_ENROLLED_USERS,
            FieldsList.STATS_SESSION_TIME,
            FieldsList.STATS_TOTAL_TIME_IN_COURSE,
        ],
        mobileAppStatistics: [
            FieldsList.STATS_ACCESS_FROM_MOBILE,
            FieldsList.STATS_PERCENTAGE_ACCESS_FROM_MOBILE,
        ],
        flowStatistics: [
            FieldsList.STATS_USER_FLOW,
            FieldsList.STATS_USER_FLOW_PERCENTAGE,
        ],
        flowMsTeamsStatistics: [
            FieldsList.STATS_USER_FLOW_MS_TEAMS,
            FieldsList.STATS_USER_FLOW_MS_TEAMS_PERCENTAGE,
        ],
    };

    mandatoryFields = {
        course: [
            FieldsList.COURSE_NAME
        ]
    };

    constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap|undefined) {
        super(session, reportDetails);

        if (this.session.platform.checkPluginESignatureEnabled()) {
            this.allFields.course.push(FieldsList.COURSE_E_SIGNATURE);
        }

        if (!this.session.platform.checkPluginFlowEnabled()) {
            this.allFields.flowStatistics = [];
        }

         if (!this.session.platform.checkPluginFlowMsTeamsEnabled()) {
            this.allFields.flowMsTeamsStatistics = [];
        }
    }

    public async getQuery(limit = 0, isPreview: boolean): Promise<string> {
        const translations = await this.loadTranslations();
        const hydra = this.session.getHydra();
        const athena = this.session.getAthena();

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;
        let fullUsers = '';

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilter();
        }

        const fullCourses = await this.calculateCourseFilter();

        const select = [];
        const from = [];

        let table = `SELECT * FROM ${TablesList.LEARNING_COURSE} WHERE TRUE`;

        if (fullCourses !== '') {
            table += ` AND idCourse IN (${fullCourses})`;
        }

        if (this.info.courses && this.info.courses.courseType !== CourseTypeFilter.ALL) {
            if (this.info.courses.courseType === CourseTypeFilter.E_LEARNING) {
                table += ` AND course_type = '${CourseTypes.Elearning}'`;
            }

            if (this.info.courses.courseType === CourseTypeFilter.ILT) {
                table += ` AND course_type = '${CourseTypes.Classroom}'`;
            }
        }

        if (this.info.courseExpirationDate) {
            table += this.buildDateFilter('date_end', this.info.courseExpirationDate, 'AND', true);
        }

        from.push(`(${table}) AS ${TablesListAliases.LEARNING_COURSE}`);

        table = `SELECT * FROM ${TablesList.LEARNING_COURSEUSER_AGGREGATE} WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND idUser IN (${fullUsers})`;
        }

        if (fullCourses !== '') {
            table += ` AND idCourse IN (${fullCourses})`;
        }

        if (this.info.users && this.info.users.showOnlyLearners) {
            table += ` AND level = ${CourseuserLevels.Student}`;
        }

        // manage the date options - enrollmentDate and completionDate
        table += this.composeDateOptionsFilter();

        from.push(`LEFT JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);

        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `valid ${this.getCheckIsValidFieldClause()}` : 'true';

        if (hideExpiredUsers) {
            table += ` AND (expiration IS NULL OR expiration > NOW())`;
        }
        from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);

        let where = [
            `AND (${TablesListAliases.CORE_USER}.userid IS NULL OR ${TablesListAliases.CORE_USER}.userid <> '/Anonymous')`
        ];

        // Variables to check if the specified table was already joined in the query
        let joinCourseCategories = false;
        let joinCoreUserFieldValue = false;
        let joinLearningCourseFieldValue = false;
        let joinLearningCourseRating = false;

        // Variables to check if the specified materialized tables was already joined in the query
        let joinLearningTracksessionAggregate = false;
        let joinCoreLangLanguageFieldValue = false;
        let joinCourseSessionTimeAggregate = false;
        let joinSkillSkills = false;
        let joinSkillSkillsObjects = false;

        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        const translationValue = this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');
        const courseExtraFields = await this.session.getHydra().getCourseExtraFields();
        this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course', translationValue);

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

        if (this.info.fields) {
            for (const field of this.info.fields) {
                switch (field) {
                    // Course fields
                    case FieldsList.COURSE_ID:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.idCourse) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_ID])}`);
                        break;
                    case FieldsList.COURSE_UNIQUE_ID:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.uidCourse) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_UNIQUE_ID])}`);
                        break;
                    case FieldsList.COURSE_CODE:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.code) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CODE])}`);
                        break;
                    case FieldsList.COURSE_NAME:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.name) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_NAME])}`);
                        break;
                    case FieldsList.COURSE_CATEGORY_CODE:
                        if (!joinCourseCategories) {
                            joinCourseCategories = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_CATEGORY} AS ${TablesListAliases.LEARNING_CATEGORY} ON ${TablesListAliases.LEARNING_CATEGORY}.idCategory = ${TablesListAliases.LEARNING_COURSE}.idCategory AND ${TablesListAliases.LEARNING_CATEGORY}.lang_code = '${this.session.user.getLang()}'`);
                        }
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_CATEGORY}.code) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_CODE])}`);
                        break;
                    case FieldsList.COURSE_CATEGORY_NAME:
                        if (!joinCourseCategories) {
                            joinCourseCategories = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_CATEGORY} AS ${TablesListAliases.LEARNING_CATEGORY} ON ${TablesListAliases.LEARNING_CATEGORY}.idCategory = ${TablesListAliases.LEARNING_COURSE}.idCategory AND ${TablesListAliases.LEARNING_CATEGORY}.lang_code = '${this.session.user.getLang()}'`);
                        }
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_CATEGORY}.translation) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_NAME])}`);
                        break;
                    case FieldsList.COURSE_STATUS:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSE}.status) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_STATUS_PREPARATION])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_STATUS_EFFECTIVE])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_STATUS])}`);
                        break;
                    case FieldsList.COURSE_CREDITS:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.credits) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CREDITS])}`);
                        break;
                    case FieldsList.COURSE_DURATION:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.mediumTime) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DURATION])}`);
                        break;
                    case FieldsList.COURSE_TYPE:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSE}.course_type) = ${athena.renderStringInQueryCase(CourseTypes.Elearning)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_ELEARNING])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSE}.course_type) = ${athena.renderStringInQueryCase(CourseTypes.Classroom)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_CLASSROOM])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_WEBINAR])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_TYPE])}`);
                        break;
                    case FieldsList.COURSE_DATE_BEGIN:
                        select.push(`${this.mapDateDefaultValueWithDLV2(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.date_begin)`)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_BEGIN])}`);
                        break;
                    case FieldsList.COURSE_DATE_END:
                        select.push(`${this.mapDateDefaultValueWithDLV2(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.date_end)`)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_END])}`);
                        break;
                    case FieldsList.COURSE_EXPIRED:
                        const dateEndColumnDLV2Fix = `(${this.mapDateDefaultValueWithDLV2(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.date_end)`)})`;
                        select.push(`
                            CASE
                                WHEN ${dateEndColumnDLV2Fix} < CURRENT_DATE THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_EXPIRED])}`);
                        break;
                    case FieldsList.COURSE_CREATION_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSE}.create_date) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CREATION_DATE])}`);
                        break;
                    case FieldsList.COURSE_E_SIGNATURE:
                        if (this.session.platform.checkPluginESignatureEnabled()) {
                            select.push(`
                                CASE
                                    WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSE}.has_esignature_enabled) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                    ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                END AS ${athena.renderStringInQuerySelect(FieldsList.COURSE_E_SIGNATURE)}`);
                        }
                        break;
                    case FieldsList.COURSE_LANGUAGE:
                        if (!joinCoreLangLanguageFieldValue) {
                            joinCoreLangLanguageFieldValue = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_LANG_LANGUAGE} AS ${TablesListAliases.CORE_LANG_LANGUAGE} ON ${TablesListAliases.LEARNING_COURSE}.lang_code = ${TablesListAliases.CORE_LANG_LANGUAGE}.lang_code`);

                        }
                        select.push(`ARBITRARY(${TablesListAliases.CORE_LANG_LANGUAGE}.lang_description) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_LANGUAGE])}`);
                        break;
                    case FieldsList.COURSE_SKILLS:
                        if (!joinSkillSkillsObjects) {
                            joinSkillSkillsObjects = true;
                            from.push(`LEFT JOIN ${TablesList.SKILL_SKILLS_OBJECTS} AS ${TablesListAliases.SKILL_SKILLS_OBJECTS} ON ${TablesListAliases.SKILL_SKILLS_OBJECTS}.idObject = ${TablesListAliases.LEARNING_COURSE}.idCourse AND ${TablesListAliases.SKILL_SKILLS_OBJECTS}.objectType = 1`);
                        }
                        if (!joinSkillSkills) {
                            joinSkillSkills = true;
                            from.push(`LEFT JOIN ${TablesList.SKILL_SKILLS} AS ${TablesListAliases.SKILL_SKILLS} ON ${TablesListAliases.SKILL_SKILLS}.id = ${TablesListAliases.SKILL_SKILLS_OBJECTS}.idSkill`);
                        }
                        select.push(`ARRAY_JOIN(ARRAY_SORT(ARRAY_AGG(DISTINCT(${TablesListAliases.SKILL_SKILLS}.title))), ', ') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_SKILLS])}`);
                        break;
                    // Statistic fields
                    case FieldsList.STATS_ENROLLED_USERS:
                        select.push(`COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_ENROLLED_USERS])}`);
                        break;
                    case FieldsList.STATS_IN_PROGRESS_USERS_PERCENTAGE:
                        select.push(`
                            CASE
                                WHEN COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser)) = 0 THEN 0
                                ELSE (CAST(SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.InProgress} AND ${TablesListAliases.CORE_USER}.idst IS NOT NULL THEN 1 ELSE 0 END) AS DECIMAL) * 100) / COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser))
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_IN_PROGRESS_USERS_PERCENTAGE])}`);
                        break;
                    case FieldsList.STATS_IN_PROGRESS_USERS:
                        select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.InProgress} AND ${TablesListAliases.CORE_USER}.idst IS NOT NULL THEN 1 ELSE 0 END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_IN_PROGRESS_USERS])}`);
                        break;
                    case FieldsList.STATS_COMPLETED_USERS_PERCENTAGE:
                        select.push(`
                            CASE
                                WHEN COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser)) = 0 THEN 0
                                ELSE (CAST(SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.Completed} AND ${TablesListAliases.CORE_USER}.idst IS NOT NULL THEN 1 ELSE 0 END) AS DECIMAL) * 100) / COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser))
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_COMPLETED_USERS_PERCENTAGE])}`);
                        break;
                    case FieldsList.STATS_COMPLETED_USERS:
                        select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.Completed} AND ${TablesListAliases.CORE_USER}.idst IS NOT NULL THEN 1 ELSE 0 END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_COMPLETED_USERS])}`);
                        break;
                    case FieldsList.STATS_NOT_STARTED_USERS_PERCENTAGE:
                        select.push(`
                            CASE
                                WHEN COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser)) = 0 THEN 0
                                ELSE (CAST(SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status <> ${EnrollmentStatuses.InProgress} AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status <> ${EnrollmentStatuses.Completed} AND ${TablesListAliases.CORE_USER}.idst IS NOT NULL THEN 1 ELSE 0 END) AS DECIMAL) * 100) / COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser))
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_NOT_STARTED_USERS_PERCENTAGE])}`);
                        break;
                    case FieldsList.STATS_NOT_STARTED_USERS:
                        select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status <> ${EnrollmentStatuses.InProgress} AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status <> ${EnrollmentStatuses.Completed} AND ${TablesListAliases.CORE_USER}.idst IS NOT NULL THEN 1 ELSE 0 END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_NOT_STARTED_USERS])}`);
                        break;
                    case FieldsList.STATS_TOTAL_TIME_IN_COURSE:
                        if (!joinLearningTracksessionAggregate) {
                            joinLearningTracksessionAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.CORE_USER}.idst AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`SUM(${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totalTime) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_TOTAL_TIME_IN_COURSE])}`);
                        break;
                    case FieldsList.STATS_COURSE_RATING:
                        if (!joinLearningCourseRating) {
                            joinLearningCourseRating = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_RATING} AS ${TablesListAliases.LEARNING_COURSE_RATING} ON ${TablesListAliases.LEARNING_COURSE_RATING}.idCourse = ${TablesListAliases.LEARNING_COURSE}.idCourse`);
                        }
                        select.push(`CAST(ARBITRARY(${TablesListAliases.LEARNING_COURSE_RATING}.rate_average) AS INTEGER) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_COURSE_RATING])}`);
                        break;
                    case FieldsList.STATS_SESSION_TIME:
                        if (!joinCourseSessionTimeAggregate) {
                            joinCourseSessionTimeAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.COURSE_SESSION_TIME_AGGREGATE} AS ${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE} ON ${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE}.id_user = ${TablesListAliases.CORE_USER}.idst AND ${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE}.course_id = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`SUM(${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE}.session_time) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_SESSION_TIME])}`);
                        break;
                    case FieldsList.STATS_USER_FLOW:
                        if (this.session.platform.checkPluginFlowEnabled()) {
                            if (!joinLearningTracksessionAggregate) {
                                joinLearningTracksessionAggregate = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.CORE_USER}.idst AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                            }
                            select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.userFlow > 0 THEN 1 ELSE 0 END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW])}`);
                        }
                        break;
                    case FieldsList.STATS_USER_FLOW_PERCENTAGE:
                        if (this.session.platform.checkPluginFlowEnabled()) {
                            if (!joinLearningTracksessionAggregate) {
                                joinLearningTracksessionAggregate = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.CORE_USER}.idst AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                            }
                            select.push(`(SUM(CASE WHEN ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.userFlow > 0 THEN 1 ELSE 0 END) * 100 / COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser))) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_PERCENTAGE])}`);
                        }
                        break;
                    case FieldsList.STATS_USER_FLOW_MS_TEAMS:
                        if (this.session.platform.checkPluginFlowMsTeamsEnabled()) {
                            if (!joinLearningTracksessionAggregate) {
                                joinLearningTracksessionAggregate = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.CORE_USER}.idst AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                            }
                            select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.userFlowMsTeams > 0 THEN 1 ELSE 0 END)
                            AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_MS_TEAMS])}`);
                        }
                        break;
                    case FieldsList.STATS_USER_FLOW_MS_TEAMS_PERCENTAGE:
                        if (this.session.platform.checkPluginFlowMsTeamsEnabled()) {
                            if (!joinLearningTracksessionAggregate) {
                                joinLearningTracksessionAggregate = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.CORE_USER}.idst AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                            }
                            select.push(`(SUM(CASE WHEN ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.userFlowMsTeams > 0 THEN 1 ELSE 0 END) * 100 / COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser)))
                            AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_USER_FLOW_MS_TEAMS_PERCENTAGE])}`);
                        }
                        break;
                    case FieldsList.STATS_ACCESS_FROM_MOBILE:
                        if (!joinLearningTracksessionAggregate) {
                            joinLearningTracksessionAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.CORE_USER}.idst AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.userGoLearn > 0 THEN 1 ELSE 0 END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_ACCESS_FROM_MOBILE])}`);
                        break;
                    case FieldsList.STATS_PERCENTAGE_ACCESS_FROM_MOBILE:
                        if (!joinLearningTracksessionAggregate) {
                            joinLearningTracksessionAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.CORE_USER}.idst AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.userGoLearn > 0 THEN 1 ELSE 0 END) * 100 / COUNT(DISTINCT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser))  AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_PERCENTAGE_ACCESS_FROM_MOBILE])}`);
                        break;

                    // Additional fields
                    default:
                        if (this.isCourseExtraField(field)) {
                            const fieldId = parseInt(field.replace('course_extrafield_', ''), 10);
                            for (const courseField of courseExtraFields.data.items) {
                                if (courseField.id === fieldId) {
                                    if (await this.checkCourseAdditionalFieldInAthena(fieldId) === false) {
                                        const additionalField = this.setAdditionalFieldTranslation(courseField);
                                        select.push(`'' AS ${athena.renderStringInQuerySelect(additionalField)}`);
                                    } else {
                                        if (!joinLearningCourseFieldValue) {
                                            joinLearningCourseFieldValue = true;
                                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_VALUE} AS ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE} ON ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.id_course = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                                        }
                                        switch (courseField.type) {
                                            case AdditionalFieldsTypes.Textarea:
                                            case AdditionalFieldsTypes.Textfield:
                                                select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}) AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId} AND ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation) AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.YesNo:
                                                select.push(`
                                                CASE
                                                    WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                                    ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                                END AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
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

        let query = `SELECT ${select.join(', ')}
        FROM ${from.join(' ')}
        ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}
        GROUP BY ${TablesListAliases.LEARNING_COURSE}.idCourse`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(select, translations, {user: [], course: courseExtraFields.data.items, userCourse: [], webinar: [], classroom: []});
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
            groupBy: [`${TablesListAliases.LEARNING_COURSE}."idcourse"`],
            archivedGroupBy: [],
            userAdditionalFieldsSelect: [],
            userAdditionalFieldsFrom: [],
            userAdditionalFieldsId: [],
            courseAdditionalFieldsSelect: [],
            courseAdditionalFieldsFrom: [],
            courseAdditionalFieldsId: [],
            translations,
            checkPuVisibility
        };

        let table = `SELECT * FROM ${TablesList.LEARNING_COURSE} WHERE TRUE`;

        if (fullCourses !== '') {
            table += ` AND "idcourse" IN (${fullCourses})`;
        }

        if (this.info.courses && this.info.courses.courseType !== CourseTypeFilter.ALL) {
            if (this.info.courses.courseType === CourseTypeFilter.E_LEARNING) {
                table += ` AND "course_type" = '${CourseTypes.Elearning}'`;
            }

            if (this.info.courses.courseType === CourseTypeFilter.ILT) {
                table += ` AND "course_type" = '${CourseTypes.Classroom}'`;
            }
        }


        if (this.info.courseExpirationDate) {
            table += this.buildDateFilter('date_end', this.info.courseExpirationDate, 'AND', true);
        }

        queryHelper.from.push(`(${table}) AS ${TablesListAliases.LEARNING_COURSE}`);

        table = `SELECT * FROM ${TablesList.LEARNING_COURSEUSER_AGGREGATE} WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND "iduser" IN (${fullUsers})`;
        }

        if (fullCourses !== '') {
            table += ` AND "idcourse" IN (${fullCourses})`;
        }

        if (this.info.users && this.info.users.showOnlyLearners) {
            table += ` AND "level" = ${CourseuserLevels.Student}`;
        }

        // manage the date options - enrollmentDate and completionDate
        table += this.composeDateOptionsFilter();

        queryHelper.from.push(`LEFT JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE} ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);

        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `"valid" = 1` : 'true';

        if (hideExpiredUsers) {
            table += ` AND ("expiration" IS NULL OR "expiration" > current_timestamp())`;
        }
        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);

        const where = [
            `AND (${TablesListAliases.CORE_USER}."userid" IS NULL OR ${TablesListAliases.CORE_USER}."userid" <> '/Anonymous')`
        ];
        // load the translations of additional fields only if are selected
        let courseExtraFields = {data: { items: []} } as CourseExtraFieldsResponse;
        if (this.info.fields.find(item => item.includes('course_extrafield_'))) {
            courseExtraFields = await this.session.getHydra().getCourseExtraFields();
            this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course');
        }

        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            const userExtraFields = await this.session.getHydra().getUserExtraFields();
            this.getAdditionalUsersFieldsFiltersSnowflake(queryHelper, userExtraFields);
        }

        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged = this.querySelectCourseFields(field, queryHelper) ||
                    this.querySelectUsageStatisticsFields(field, queryHelper) ||
                    this.querySelectMobileAppStatisticsFields(field, queryHelper) ||
                    this.querySelectFlowStatisticsFields(field, queryHelper) ||
                    this.querySelectFlowMsTeamsStatisticsFields(field, queryHelper) ||
                    this.queryWithCourseAdditionalFields(field, queryHelper, courseExtraFields);
            }
        }

        if (queryHelper.userAdditionalFieldsId.length > 0) {
            queryHelper.cte.push(this.additionalUserFieldQueryWith(queryHelper.userAdditionalFieldsId));
            queryHelper.cte.push(this.additionalFieldQueryWith(queryHelper.userAdditionalFieldsFrom, queryHelper.userAdditionalFieldsSelect, queryHelper.userAdditionalFieldsId, 'id_user', TablesList.CORE_USER_FIELD_VALUE_WITH, TablesList.USERS_ADDITIONAL_FIELDS_TRANSLATIONS));
        }
        if (queryHelper.courseAdditionalFieldsId.length > 0) {
            queryHelper.cte.push(this.additionalCourseFieldQueryWith(queryHelper.courseAdditionalFieldsId));
            queryHelper.cte.push(this.additionalFieldQueryWith(queryHelper.courseAdditionalFieldsFrom, queryHelper.courseAdditionalFieldsSelect, queryHelper.courseAdditionalFieldsId, 'id_course', TablesList.LEARNING_COURSE_FIELD_VALUE_WITH, TablesList.COURSES_ADDITIONAL_FIELDS_TRANSLATIONS));
        }

        let query = '';
        query += `
            ${queryHelper.cte.length > 0 ? `WITH ${queryHelper.cte.join(', ')}` : ''}
            SELECT ${queryHelper.select.join(', ')}
            FROM ${queryHelper.from.join(' ')}
            WHERE TRUE ${where.join(' ')}
            GROUP BY ${[...new Set(queryHelper.groupBy)].join(', ')}`;

        // custom columns sorting
        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(
                queryHelper.select,
                queryHelper.translations,
                {
                    user: [],
                    course: courseExtraFields.data.items,
                    userCourse: [],
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

        this.mandatoryFields.course.forEach(element => {
            tmpFields.push(element);
        });

        report.fields = tmpFields;

        report.idReport = id;
        report.type = this.reportType;
        report.timezone = this.session.user.getTimezone();
        report.title = title;
        report.description = description ? description : '';
        report.platform = platform;
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

        // Manage filters default values
        report.courses = new ReportManagerInfoCoursesFilter();
        report.courses.all = true;
        report.courseExpirationDate = this.getDefaultCourseExpDate();
        report.users = new ReportManagerInfoUsersFilter();
        report.users.all = true;
        report.users.showOnlyLearners = true;
        report.enrollmentDate = this.getDefaultDateOptions();
        report.completionDate = this.getDefaultDateOptions();
        report.conditions = DateOptions.CONDITIONS;
        report.learningPlans = new ReportManagerLearningPlansFilter();
        report.learningPlans.all = true;

        // View options
        report.sortingOptions = this.getSortingOptions();

        // Schedule
        report.planning = this.getDefaultPlanningFields();

        return report;
    }

    /**
     * Get the default value for the Sorting Options
     */
    protected getSortingOptions(): SortingOptions {
        return {
            selector: 'default',
            selectedField: FieldsList.COURSE_NAME,
            orderBy: 'asc',
        };
    }

    public async getAvailablesFields(): Promise<ReportAvailablesFields> {
        const result: ReportAvailablesFields = {};
        const translations = await this.loadTranslations(true);

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

        const courseExtraFields = await this.getAvailableCourseExtraFields();
        result.course.push(...courseExtraFields);

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

    public getBaseAvailableFields(): Promise<ReportAvailablesFields> {
        throw new Error('Method not implemented.');
    }

    /**
     * Set the sortingOptions object with the input passed
     * @param sortingOptions The object that describes a sortingOptions
     */
    setSortingOptions(item: SortingOptions): void {
        this.info.sortingOptions = {
            selector: item && item.selector ? item.selector : 'default',
            selectedField: item && item.selectedField ? item.selectedField : FieldsList.COURSE_NAME,
            orderBy: item && item.orderBy ? item.orderBy : 'asc'
        };
    }

    // Parse old reports
    public parseLegacyReport(legacyReport: LegacyReport, platform: string, visibilityRules: VisibilityRule[]): ReportManagerInfo {
        // get a default structure for our report type
        let report = this.getReportDefaultStructure(new ReportManagerInfo(), legacyReport.filter_name, platform, +legacyReport.author, '');
        // set title, dates and visibility options
        report = this.setCommonFieldsBetweenReportTypes(report, legacyReport, visibilityRules);
        // and now the report type specific section
        // users, groups and branches
        const filterData = JSON.parse(legacyReport.filter_data);


        /**
         * FILTERS
         */

        // COURSES IMPORT
        this.legacyCourseImport(filterData, report, legacyReport.id_filter);

        // USERS IMPORT - populate the users field of the aamon report
        this.legacyUserImport(filterData, report, legacyReport.id_filter);

        // check filters section validity
        if (!filterData.filters) {
            this.logger.error(`No legacy filters section for id report: ${legacyReport.id_filter}`);
            throw new Error('No legacy filters section');
        }

        const filters = filterData.filters;

        // Enrollment Date
        if (filters.start_date.type !== 'any') {
            report.enrollmentDate = this.parseLegacyFilterDate(report.enrollmentDate as DateOptionsValueDescriptor, filters.start_date);
        }
        // Completion Date
        if (filters.end_date.type !== 'any') {
            report.completionDate = this.parseLegacyFilterDate(report.completionDate as DateOptionsValueDescriptor, filters.end_date);
        }
        // Conditions
        if (filters.condition_status) {
            report.conditions = filters.condition_status === 'and' ? 'allConditions' : 'atLeastOneCondition';
        }


        /**
         * VIEW OPTIONS
         */

        // map selected fields and manage order by
        if (filterData.fields) {
            let legacyOrderField: LegacyOrderByParsed | undefined;

            const courseMandatoryFieldsMap = this.mandatoryFields.course.reduce((previousValue: { [key: string]: string }, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});

            const courseFieldsDescriptor = this.mapCourseSelectedFields(filterData.fields.course, filterData.order, courseMandatoryFieldsMap);
            report.fields.push(...courseFieldsDescriptor.fields);
            if (courseFieldsDescriptor.orderByDescriptor) legacyOrderField = courseFieldsDescriptor.orderByDescriptor;

            const statsFieldsDescriptor = this.mapStatsSelectedFields(filterData.fields.stat, filterData.order);
            report.fields.push(...statsFieldsDescriptor.fields);
            if (statsFieldsDescriptor.orderByDescriptor) legacyOrderField = statsFieldsDescriptor.orderByDescriptor;

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


    private parseLegacyFilterDate(newReportDateFilter: DateOptionsValueDescriptor, legacyDateFilter: any): DateOptionsValueDescriptor {
        switch (legacyDateFilter.type) {
            case 'ndago':
                return this.parseLegacyNDaysAgo(newReportDateFilter, legacyDateFilter);
            case 'range':
                return this.parseLegacyRange(newReportDateFilter, legacyDateFilter);
            default:
                return newReportDateFilter;
        }
    }

    private parseLegacyNDaysAgo(newReportDateFilter: DateOptionsValueDescriptor, legacyDateFilter: any): DateOptionsValueDescriptor {
        switch (legacyDateFilter.data.combobox) {
            case '<':
                newReportDateFilter.operator = 'isAfter';
                newReportDateFilter.days = +legacyDateFilter.data.days_count;
                break;
            case '<=':
                newReportDateFilter.operator = 'isAfter';
                newReportDateFilter.days = +legacyDateFilter.data.days_count + 1;
                break;
            case '>':
                newReportDateFilter.operator = 'isBefore';
                newReportDateFilter.days = +legacyDateFilter.data.days_count;
                break;
            case '>=':
                newReportDateFilter.operator = 'isBefore';
                newReportDateFilter.days = (+legacyDateFilter.data.days_count > 0) ? +legacyDateFilter.data.days_count - 1 : 0;
                break;
            case '=':
                // new type of operator - we need to map it in the FE and in Query composition
                newReportDateFilter.operator = 'isEqual';
                newReportDateFilter.days = +legacyDateFilter.data.days_count;
                break;
            default:
                return newReportDateFilter;
        }
        newReportDateFilter.type = 'relative';
        newReportDateFilter.any = false;

        return newReportDateFilter;
    }

    private parseLegacyRange(newReportDateFilter: DateOptionsValueDescriptor, legacyDateFilter: any): DateOptionsValueDescriptor {
        return {
            ...newReportDateFilter,
            any: false,
            type: 'range',
            days: 0,
            operator: 'range',
            from: legacyDateFilter.data.from,
            to: legacyDateFilter.data.to,
        };
    }

}
