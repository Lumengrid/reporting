import {
    FieldsList,
    FieldTranslation,
    ReportAvailablesFields,
    ReportManagerInfo,
    ReportManagerInfoCoursesFilter,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility,
    TablesList,
    TablesListAliases,
    ReportManagerLearningPlansFilter
} from './report-manager';
import {
    DateOptions,
    InstructorsFilter,
    SessionDates,
    SortingOptions,
    VisibilityTypes
} from './custom-report';
import { LegacyReport, VisibilityRule } from './migration-component';
import SessionManager from '../services/session/session-manager.session';
import { v4 } from 'uuid';
import { UserLevels } from '../services/session/user-manager.session';
import { AdditionalFieldsTypes, CourseTypes, CourseuserLevels, EnrollmentStatuses, UserLevelsGroups, SessionEvaluationStatus } from './base';
import { ReportsTypes } from '../reports/constants/report-types';
import { BaseReportManager } from './base-report-manager';
import { MethodNotImplementedException } from '../exceptions';

export class UsersWebinarManager extends BaseReportManager {

    reportType = ReportsTypes.USERS_WEBINAR;

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
            FieldsList.USER_DIRECT_MANAGER
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
            FieldsList.COURSE_LANGUAGE
        ],
        session: [
            FieldsList.WEBINAR_SESSION_NAME,
            FieldsList.WEBINAR_SESSION_EVALUATION_SCORE_BASE,
            FieldsList.WEBINAR_SESSION_START_DATE,
            FieldsList.WEBINAR_SESSION_END_DATE,
            FieldsList.WEBINAR_SESSION_SESSION_TIME,
            FieldsList.WEBINAR_SESSION_WEBINAR_TOOL,
            FieldsList.WEBINAR_SESSION_TOOL_TIME_IN_SESSION
        ],
        webinarSessionUser: [
            FieldsList.WEBINAR_SESSION_USER_LEVEL,
            FieldsList.WEBINAR_SESSION_USER_ENROLL_DATE,
            FieldsList.WEBINAR_SESSION_USER_STATUS,
            FieldsList.WEBINAR_SESSION_USER_LEARN_EVAL,
            FieldsList.WEBINAR_SESSION_USER_EVAL_STATUS,
            FieldsList.WEBINAR_SESSION_USER_INSTRUCTOR_FEEDBACK,
            FieldsList.WEBINAR_SESSION_USER_ENROLLMENT_STATUS,
            FieldsList.WEBINAR_SESSION_USER_SUBSCRIBE_DATE,
            FieldsList.WEBINAR_SESSION_USER_COMPLETE_DATE,
            FieldsList.COURSEUSER_DATE_COMPLETE,
        ],
    };

    mandatoryFields = {
        user : [
            FieldsList.USER_USERID
        ],
        course: [
            FieldsList.COURSE_NAME
        ],
        session: [],
        webinarSessionUser: [],
    };

    public constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap | undefined) {
        super(session, reportDetails);


        if (this.session.platform.checkPluginESignatureEnabled()) {
            this.allFields.course.push(FieldsList.COURSE_E_SIGNATURE);
            this.allFields.webinarSessionUser.push(FieldsList.COURSE_E_SIGNATURE_HASH);
        }
    }

    async getAvailablesFields(): Promise<ReportAvailablesFields> {
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

        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        for (const field of userExtraFields.data.items) {
            result.user.push({
                field: 'user_extrafield_' + field.id,
                idLabel: field.title,
                mandatory: false,
                isAdditionalField: true,
                translation: field.title
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

        const courseExtraFields = await this.session.getHydra().getCourseExtraFields();
        for (const field of courseExtraFields.data.items) {
            result.course.push({
                field: 'course_extrafield_' + field.id,
                idLabel: this.setAdditionalFieldTranslation(field),
                mandatory: false,
                isAdditionalField: true,
                translation: this.setAdditionalFieldTranslation(field)
            });
        }

        // Recover session fields
        result.session = [];
        for (const field of this.allFields.session) {
            result.session.push({
                field,
                idLabel: field,
                mandatory: false,
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        const sessionExtraFields = await this.session.getHydra().getWebinarExtraFields();
        for (const field of sessionExtraFields.data.items) {
            result.session.push({
                field: 'webinar_extrafield_' + field.id,
                idLabel: this.setAdditionalFieldTranslation(field),
                mandatory: false,
                isAdditionalField: true,
                translation: this.setAdditionalFieldTranslation(field)
            });
        }

        // Recover webinarSessionUser fields
        result.webinarSessionUser = [];
        for (const field of this.allFields.webinarSessionUser) {
            result.webinarSessionUser.push({
                field,
                idLabel: field,
                mandatory: false,
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        return result;
    }

    public getBaseAvailableFields(): Promise<ReportAvailablesFields> {
        throw new Error('Method not implemented.');
    }

    async getQuery(limit: number, isPreview: boolean): Promise<string> {
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

        const select: string[] = [];
        const from: string[] = [];
        let table = `SELECT * FROM ${TablesList.LEARNING_COURSEUSER_AGGREGATE} WHERE TRUE`;
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

        from.push(`(${table}) AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}`);


        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `valid ${this.getCheckIsValidFieldClause()}` : 'true';

        if (hideExpiredUsers) {
            table += ` AND (expiration IS NULL OR expiration > NOW())`;
        }
        from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);


        // JOIN LEARNING COURSE
        table = `SELECT * FROM ${TablesList.LEARNING_COURSE} WHERE course_type = 'webinar' AND TRUE`;

        if (fullCourses !== '') {
            table += ` AND idCourse IN (${fullCourses})`;
        }
        if (this.info.courseExpirationDate) {
            table += this.buildDateFilter('date_end', this.info.courseExpirationDate, 'AND', true);
        }
        from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);

        table = `SELECT * FROM ${TablesList.WEBINAR_SESSION_USER_DETAILS} WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND id_user IN (${fullUsers})`;
        }

        if (fullCourses !== '') {
            table += ` AND course_id IN (${fullCourses})`;
        }

        from.push(`JOIN (${table}) AS ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS} ON ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.course_id = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse  AND ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.id_user = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);

        let where = [
            `AND ${TablesListAliases.CORE_USER}.userid <> '/Anonymous'`,
        ];

        // Filter enrollment status based on table ${TablesList.WEBINAR_SESSION_USER_DETAILS}
        if (this.info.enrollment && (this.info.enrollment.completed || this.info.enrollment.inProgress || this.info.enrollment.waitingList || this.info.enrollment.notStarted || this.info.enrollment.suspended)) {
            // if all enrollment has set we don't put the filter!
            const allValuesAreTrue = Object.keys(this.info.enrollment).every((k) => this.info.enrollment[k]);

            if (!allValuesAreTrue) {

                const statuses: number[] = [];

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

                if (statuses.length > 0 && this.info.enrollment.waitingList) {
                    where.push(`AND ((${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.status IN (${statuses.join(',')}) AND ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.waiting = 0) OR (${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.status = -2 AND ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.waiting = 1))`);
                } else if (statuses.length > 0) {
                    where.push(`AND (${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.status IN (${statuses.join(',')}) AND ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.waiting = 0)`);
                } else if (this.info.enrollment.waitingList) {
                    where.push(`AND (${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.status = -2 AND ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.waiting = 1)`);
                }
            }
        }

        // Session date filter
        const dateSessionFilter = this.composeSessionDateOptionsFilter(`${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.date_begin`, `${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.date_end`);
        if (dateSessionFilter !== '') {
            where.push(`AND (${this.composeTableField(TablesListAliases.WEBINAR_SESSION_USER_DETAILS, 'id_user')} IS NULL OR (${dateSessionFilter}))`);
        }

        // Variables to check if the specified table was already joined in the query
        let joinCourseCategories = false;
        let joinCoreGroupLevel = false;
        let joinCoreGroupMembers = false;
        let joinCoreUserFieldValue = false;
        let joinLearningCourseFieldValue = false;
        let joinWebinarSessionFieldValue = false;
        let joinLearningCourseuserSign = false;
        let joinCoreLangLanguageFieldValue = false;
        let joinWebinarSessionDate = false;
        let joinWebinarSessionDateAttendance = false;

        let joinCoreUserBranches = false;
        let joinSkillManagersValue = false;

        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        let translationValue = this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');
        const courseExtraFields = await this.session.getHydra().getCourseExtraFields();
        translationValue = this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course', translationValue);
        const webinarExtraFields = await this.session.getHydra().getWebinarExtraFields();
        translationValue = this.updateExtraFieldsDuplicated(webinarExtraFields.data.items, translations, 'webinar', translationValue);
        const courseuserExtraFields = await this.session.getHydra().getCourseuserExtraFields();
        this.updateExtraFieldsDuplicated(courseuserExtraFields.data, translations, 'course-user', translationValue);

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
                    // User fields
                    case FieldsList.USER_ID:
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER}.idst) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_ID])}`);
                        break;
                    case FieldsList.USER_USERID:
                        select.push(`ARBITRARY(SUBSTR(${TablesListAliases.CORE_USER}.userid, 2)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_USERID])}`);
                        break;
                    case FieldsList.USER_FIRSTNAME:
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER}.firstname) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FIRSTNAME])}`);
                        break;
                    case FieldsList.USER_LASTNAME:
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER}.lastname) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LASTNAME])}`);
                        break;
                    case FieldsList.USER_FULLNAME:
                        if (this.session.platform.getShowFirstNameFirst()) {
                            select.push(`CONCAT(ARBITRARY(${TablesListAliases.CORE_USER}.firstname), ' ', ARBITRARY(${TablesListAliases.CORE_USER}.lastname)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                        } else {
                            select.push(`CONCAT(ARBITRARY(${TablesListAliases.CORE_USER}.lastname), ' ', ARBITRARY(${TablesListAliases.CORE_USER}.firstname)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                        }
                        break;
                    case FieldsList.USER_EMAIL:
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER}.email) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL])}`);
                        break;
                    case FieldsList.USER_EMAIL_VALIDATION_STATUS:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.CORE_USER}.email_status) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL_VALIDATION_STATUS])}`);
                        break;
                    case FieldsList.USER_LEVEL:
                        if (!joinCoreGroupMembers) {
                            joinCoreGroupMembers = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS} ON ${TablesListAliases.CORE_GROUP_MEMBERS}.idstMember = ${TablesListAliases.CORE_USER}.idst`);
                        }
                        if (!joinCoreGroupLevel) {
                            joinCoreGroupLevel = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_GROUP} AS ${TablesListAliases.CORE_GROUP_MEMBERS}l ON ${TablesListAliases.CORE_GROUP_MEMBERS}l.idst = ${TablesListAliases.CORE_GROUP_MEMBERS}.idst AND ${TablesListAliases.CORE_GROUP_MEMBERS}l.groupid LIKE '/framework/level/%'`);
                        }
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.CORE_GROUP_MEMBERS}l.groupid) = ${athena.renderStringInQueryCase(UserLevelsGroups.GodAdmin)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_GODADMIN])}
                                WHEN ARBITRARY(${TablesListAliases.CORE_GROUP_MEMBERS}l.groupid) = ${athena.renderStringInQueryCase(UserLevelsGroups.PowerUser)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_POWERUSER])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_USER])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LEVEL])}`);
                        break;
                    case FieldsList.USER_DEACTIVATED:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.CORE_USER}.valid) ${this.getCheckIsValidFieldClause()} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_DEACTIVATED])}`);
                        break;
                    case FieldsList.USER_EXPIRATION:
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER}.expiration) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EXPIRATION])}`);
                        break;
                    case FieldsList.USER_SUSPEND_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.CORE_USER}.suspend_date) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_SUSPEND_DATE])}`);
                        break;
                    case FieldsList.USER_REGISTER_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.CORE_USER}.register_date) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_REGISTER_DATE])}`);
                        break;
                    case FieldsList.USER_LAST_ACCESS_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.CORE_USER}.lastenter) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LAST_ACCESS_DATE])}`);
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
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER_BRANCHES_NAMES}.${FieldsList.USER_BRANCH_NAME}) AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_NAME])}`);
                        break;
                    case FieldsList.USER_BRANCH_PATH:
                        if (!joinCoreUserBranches) {
                            joinCoreUserBranches = true;
                            let userBranchesTable = '';
                            if (this.session.user.getLevel() === UserLevels.POWER_USER) {
                                userBranchesTable = await this.createPuUserBranchesTable();
                            } else {
                                userBranchesTable = this.createUserBranchesTable();
                            }

                            from.push(`LEFT JOIN ${userBranchesTable} AS ${TablesListAliases.CORE_USER_BRANCHES} ON ${TablesListAliases.CORE_USER_BRANCHES}.idst = ${TablesListAliases.CORE_USER}.idst`);
                        }
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER_BRANCHES}.branches) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_PATH])}`);
                        break;
                    case FieldsList.USER_BRANCHES_CODES:
                        if (!joinCoreUserBranches) {
                            joinCoreUserBranches = true;
                            let userBranchesTable = '';
                            if (this.session.user.getLevel() === UserLevels.POWER_USER) {
                                userBranchesTable = await this.createPuUserBranchesTable();
                            } else {
                                userBranchesTable = this.createUserBranchesTable();
                            }

                            from.push(`LEFT JOIN ${userBranchesTable} AS ${TablesListAliases.CORE_USER_BRANCHES} ON ${TablesListAliases.CORE_USER_BRANCHES}.idst = ${TablesListAliases.CORE_USER}.idst`);
                        }
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER_BRANCHES}.codes) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_BRANCHES_CODES])}`);
                        break;
                    case FieldsList.USER_DIRECT_MANAGER:
                        if (!joinSkillManagersValue) {
                            joinSkillManagersValue = true;
                            from.push(`LEFT JOIN ${TablesList.SKILL_MANAGERS} AS ${TablesListAliases.SKILL_MANAGERS} ON ${TablesListAliases.SKILL_MANAGERS}.idEmployee = ${TablesListAliases.CORE_USER}.idst AND ${TablesListAliases.SKILL_MANAGERS}.type = 1`);
                            from.push(`LEFT JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}s ON ${TablesListAliases.CORE_USER}s.idst = ${TablesListAliases.SKILL_MANAGERS}.idManager`);
                        }
                        let directManagerFullName = '';
                        if (this.session.platform.getShowFirstNameFirst()) {
                            directManagerFullName = `ARBITRARY(CONCAT(${TablesListAliases.CORE_USER}s.firstname, ' ', ${TablesListAliases.CORE_USER}s.lastname))`;
                        } else {
                            directManagerFullName = `ARBITRARY(CONCAT(${TablesListAliases.CORE_USER}s.lastname, ' ', ${TablesListAliases.CORE_USER}s.firstname))`;
                        }
                        select.push(`IF(${directManagerFullName} = ' ', ARBITRARY(SUBSTR(${TablesListAliases.CORE_USER}s.userid, 2)), ${directManagerFullName}) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_DIRECT_MANAGER])}`);
                        break;

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
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSE}.date_end) < CURRENT_DATE THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                            END AS ${FieldsList.COURSE_EXPIRED}`);
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

                        // Session fields
                    case FieldsList.WEBINAR_SESSION_NAME:
                        select.push(`ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.name) AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_NAME])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_EVALUATION_SCORE_BASE:
                        select.push(`ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.score_base) AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_EVALUATION_SCORE_BASE])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_START_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.date_begin) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_START_DATE])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_END_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.date_end) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_END_DATE])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_WEBINAR_TOOL:
                        if (!joinWebinarSessionDate) {
                            joinWebinarSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.WEBINAR_SESSION_DATE} AS ${TablesListAliases.WEBINAR_SESSION_DATE} ON ${TablesListAliases.WEBINAR_SESSION_DATE}.id_session = ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.id_session`);
                        }
                        select.push(`ARRAY_JOIN(ARRAY_AGG(DISTINCT(${TablesListAliases.WEBINAR_SESSION_DATE}.webinar_tool)), ',') AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_WEBINAR_TOOL])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_SESSION_TIME:
                        if (!joinWebinarSessionDate) {
                            joinWebinarSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.WEBINAR_SESSION_DATE} AS ${TablesListAliases.WEBINAR_SESSION_DATE} ON ${TablesListAliases.WEBINAR_SESSION_DATE}.id_session = ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.id_session`);
                        }
                        if (!joinWebinarSessionDateAttendance) {
                            joinWebinarSessionDateAttendance = true;
                            from.push(`LEFT JOIN ${TablesList.WEBINAR_SESSION_DATE_ATTENDANCE} AS ${TablesListAliases.WEBINAR_SESSION_DATE_ATTENDANCE} ON ${TablesListAliases.WEBINAR_SESSION_DATE_ATTENDANCE}.id_session = ${TablesListAliases.WEBINAR_SESSION_DATE}.id_session AND ${TablesListAliases.WEBINAR_SESSION_DATE_ATTENDANCE}.id_user = ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.id_user AND ${TablesListAliases.WEBINAR_SESSION_DATE_ATTENDANCE}.day = ${TablesListAliases.WEBINAR_SESSION_DATE}.day`);
                        }
                        select.push(`
                        SUM(
                            CASE WHEN (${TablesListAliases.WEBINAR_SESSION_DATE_ATTENDANCE}.watched_live = 1 OR ${TablesListAliases.WEBINAR_SESSION_DATE_ATTENDANCE}.watched_recording = 1 OR ${TablesListAliases.WEBINAR_SESSION_DATE_ATTENDANCE}.watched_externally = 1) THEN ${TablesListAliases.WEBINAR_SESSION_DATE}.duration_minutes
                            ELSE 0
                            END
                        ) AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_SESSION_TIME])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_TOOL_TIME_IN_SESSION:
                        if (!joinWebinarSessionDate) {
                            joinWebinarSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.WEBINAR_SESSION_DATE} AS ${TablesListAliases.WEBINAR_SESSION_DATE} ON ${TablesListAliases.WEBINAR_SESSION_DATE}.id_session = ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.id_session`);
                        }
                        if (!joinWebinarSessionDateAttendance) {
                            joinWebinarSessionDateAttendance = true;
                            from.push(`LEFT JOIN ${TablesList.WEBINAR_SESSION_DATE_ATTENDANCE} AS ${TablesListAliases.WEBINAR_SESSION_DATE_ATTENDANCE} ON ${TablesListAliases.WEBINAR_SESSION_DATE_ATTENDANCE}.id_session = ${TablesListAliases.WEBINAR_SESSION_DATE}.id_session AND ${TablesListAliases.WEBINAR_SESSION_DATE_ATTENDANCE}.id_user = ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.id_user AND ${TablesListAliases.WEBINAR_SESSION_DATE_ATTENDANCE}.day = ${TablesListAliases.WEBINAR_SESSION_DATE}.day`);
                        }
                        select.push(`
                            SUM(
                                ${TablesListAliases.WEBINAR_SESSION_DATE_ATTENDANCE}.watched_externally
                            ) AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_TOOL_TIME_IN_SESSION])}`);
                        break;

                    // Courseuser fields
                    case FieldsList.WEBINAR_SESSION_USER_LEVEL:
                        select.push(`CASE WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.level) = ${CourseuserLevels.Teacher} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TEACHER])} WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.level) = ${CourseuserLevels.Tutor} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TUTOR])} ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_STUDENT])} END AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_USER_LEVEL])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_USER_ENROLL_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_inscr) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_USER_ENROLL_DATE])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_USER_STATUS:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = -1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = -2 AND ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.waiting) ${this.getCheckIsValidFieldClause()} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 3 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 4 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING])}
                                ELSE CAST (ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) as varchar)
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_USER_STATUS])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_USER_ENROLLMENT_STATUS:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.status) = -2 AND ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.waiting) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                                WHEN ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.status) = 0 AND ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.waiting) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                                WHEN ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.status) = 1 AND ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.waiting) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                                WHEN ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.status) = 2 AND ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.waiting) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                                WHEN ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.status) = 3 AND ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.waiting) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                                ELSE ''
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_USER_ENROLLMENT_STATUS])}`);
                        break;

                    case FieldsList.WEBINAR_SESSION_USER_SUBSCRIBE_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.date_subscribed) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_USER_SUBSCRIBE_DATE])}`);
                        break;

                    case FieldsList.WEBINAR_SESSION_USER_COMPLETE_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.date_completed) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_USER_COMPLETE_DATE])}`);
                        break;

                    case FieldsList.COURSEUSER_DATE_COMPLETE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_complete) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_COMPLETE])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_USER_LEARN_EVAL:
                        select.push(`ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.evaluation_score) AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_USER_LEARN_EVAL])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_USER_EVAL_STATUS:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.evaluation_status) = ${SessionEvaluationStatus.PASSED} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.WEBINAR_SESSION_USER_EVAL_STATUS_PASSED])}
                                WHEN ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.evaluation_status) = ${SessionEvaluationStatus.FAILED} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.WEBINAR_SESSION_USER_EVAL_STATUS_FAILED])}
                                ELSE ''
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_USER_EVAL_STATUS])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_USER_INSTRUCTOR_FEEDBACK:
                        select.push(`ARBITRARY(${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.evaluation_text) AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_USER_INSTRUCTOR_FEEDBACK])}`);
                        break;
                    case FieldsList.COURSE_E_SIGNATURE_HASH:
                        if (this.session.platform.checkPluginESignatureEnabled()) {
                            if (!joinLearningCourseuserSign) {
                                joinLearningCourseuserSign = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEUSER_SIGN} AS ${TablesListAliases.LEARNING_COURSEUSER_SIGN} ON ${TablesListAliases.LEARNING_COURSEUSER_SIGN}.course_id = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse AND ${TablesListAliases.LEARNING_COURSEUSER_SIGN}.user_id = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                            }
                            select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_SIGN}.signature) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_E_SIGNATURE_HASH])}`);
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
                                                select.push(`ARBITRARY(${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}) AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                    select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} AND ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`ARBITRARY(${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation) AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.YesNo:
                                                select.push(`
                                                CASE
                                                    WHEN ARBITRARY(${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                                    WHEN ARBITRARY(${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}) = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                                    ELSE ''
                                                END AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Country:
                                                from.push(`LEFT JOIN ${TablesList.CORE_COUNTRY} AS ${TablesListAliases.CORE_COUNTRY}_${fieldId} ON ${TablesListAliases.CORE_COUNTRY}_${fieldId}.id_country = ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}`);
                                                select.push(`ARBITRARY(${TablesListAliases.CORE_COUNTRY}_${fieldId}.name_country) AS ${athena.renderStringInQuerySelect(userField.title)}`);
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
                                    } else {
                                        if (!joinLearningCourseFieldValue) {
                                            joinLearningCourseFieldValue = true;
                                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_VALUE} AS ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE} ON ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.id_course = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                                        }
                                        switch (courseField.type) {
                                            case AdditionalFieldsTypes.Textarea:
                                            case AdditionalFieldsTypes.Textfield:
                                                select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}) AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(courseField))}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(courseField))}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId} AND ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation) AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(courseField))}`);
                                                break;
                                            case AdditionalFieldsTypes.YesNo:
                                                select.push(`
                                                CASE
                                                    WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                                    ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                                END AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(courseField))}`);
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
                                    } else  {
                                        switch (courseuserField.type) {
                                            case AdditionalFieldsTypes.Textarea:
                                            case AdditionalFieldsTypes.Text:
                                            case AdditionalFieldsTypes.Date:
                                                select.push(`JSON_EXTRACT_SCALAR(ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.enrollment_fields), '$.${fieldId}') AS ${athena.renderStringInQuerySelect(courseuserField.name)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.LEARNING_ENROLLMENT_FIELDS_DROPDOWN} AS ${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId} ON ${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.id = CAST(JSON_EXTRACT_SCALAR(ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.enrollment_fields), '$.${fieldId}') AS INTEGER)`);
                                                select.push(`IF(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.translation LIKE '%"${this.session.user.getLangCode()}":%', JSON_EXTRACT_SCALAR(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.translation, '$["${this.session.user.getLangCode()}"]'), JSON_EXTRACT_SCALAR(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.translation, '$["${this.session.platform.getDefaultLanguageCode()}"]')) AS ${athena.renderStringInQuerySelect(courseuserField.name)}`);
                                                break;
                                        }
                                    }
                                    break;
                                }
                            }
                        } else if (this.isWebinarExtraField(field)) {
                            const fieldId = parseInt(field.replace('webinar_extrafield_', ''), 10);
                            for (const webinarField of webinarExtraFields.data.items) {
                                if (webinarField.id === fieldId) {
                                    if (await this.checkCourseAdditionalFieldInAthena(fieldId) === false) {
                                        const additionalField = this.setAdditionalFieldTranslation(webinarField);
                                        select.push(`'' AS ${athena.renderStringInQuerySelect(additionalField)}`);
                                    } else {
                                        if (!joinWebinarSessionFieldValue) {
                                            joinWebinarSessionFieldValue = true;
                                            from.push(`LEFT JOIN ${TablesList.WEBINAR_SESSION_FIELD_VALUE} AS ${TablesListAliases.WEBINAR_SESSION_FIELD_VALUE} ON ${TablesListAliases.WEBINAR_SESSION_FIELD_VALUE}.id_session = ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.id_session`);
                                        }
                                        switch (webinarField.type) {
                                            case AdditionalFieldsTypes.Textarea:
                                            case AdditionalFieldsTypes.Textfield:
                                                select.push(`ARBITRARY(${TablesListAliases.WEBINAR_SESSION_FIELD_VALUE}.field_${fieldId}) AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(webinarField))}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.WEBINAR_SESSION_FIELD_VALUE}.field_${fieldId}), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(webinarField))}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.WEBINAR_SESSION_FIELD_VALUE}.field_${fieldId} AND ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation) AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(webinarField))}`);
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
        GROUP BY ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser, ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse, ${TablesListAliases.WEBINAR_SESSION_USER_DETAILS}.id_session`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(select, translations, {user: userExtraFields.data.items, course: courseExtraFields.data.items, userCourse: courseuserExtraFields.data, webinar: webinarExtraFields.data.items, classroom: []});
        }

        // get the sql like LIMIT for the query
        query += this.getQueryExportLimit(limit);

        return query;
    }

    public async getQuerySnowflake(limit: number, isPreview: boolean, fromSchedule = false): Promise<string> {
        throw new MethodNotImplementedException('Method not implemented.');
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
        this.mandatoryFields.session.forEach(element => {
            tmpFields.push(element);
        });
        this.mandatoryFields.webinarSessionUser.forEach(element => {
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
        report.sortingOptions = this.getSortingOptions();


        report.users = new ReportManagerInfoUsersFilter();
        report.users.all = true;
        report.courses = new ReportManagerInfoCoursesFilter();
        report.courses.all = true;
        report.sessionDates = this.getDefaultSessionDates();
        report.instructors = this.getDefaultInstructorsFilter();
        report.learningPlans = new ReportManagerLearningPlansFilter();
        report.learningPlans.all = true;

        // manage the planning default fields
        report.planning = this.getDefaultPlanningFields();
        report.conditions = DateOptions.CONDITIONS;
        report.enrollmentDate = this.getDefaultDateOptions();
        report.courseExpirationDate = this.getDefaultCourseExpDate();
        report.completionDate = this.getDefaultDateOptions();
        report.enrollment = this.getDefaultEnrollment();

        return report;
    }

    protected getSortingOptions(): SortingOptions {
        return {
            selector: 'default',
            selectedField: FieldsList.USER_USERID,
            orderBy: 'asc',
        };
    }

    parseLegacyReport(legacyReport: LegacyReport, platform: string, visibilityRules: VisibilityRule[]): ReportManagerInfo {
        throw new Error('Method not implemented.');
    }

    setSortingOptions(item: SortingOptions): void {
        throw new Error('Method not implemented.');
    }

    private getDefaultSessionDates(): SessionDates {
        return {
            conditions: DateOptions.CONDITIONS,
            startDate: this.getDefaultDateOptions(),
            endDate: this.getDefaultDateOptions(),
        };
    }

    private getDefaultInstructorsFilter(): InstructorsFilter {
        return {
            all: true,
            instructors: [],
        };
    }

}
