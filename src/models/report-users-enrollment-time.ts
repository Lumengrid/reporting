import { v4 } from 'uuid';
import { ReportsTypes } from '../reports/constants/report-types';
import { Utils } from '../reports/utils';
import SessionManager from '../services/session/session-manager.session';
import { UserLevels } from '../services/session/user-manager.session';
import { AdditionalFieldsTypes, CourseTypes, CourseuserLevels, EnrollmentStatuses, UserLevelsGroups } from './base';
import { DateOptionsValueDescriptor, SortingOptions, VisibilityTypes } from './custom-report';
import { LegacyOrderByParsed, LegacyReport, VisibilityRule } from './migration-component';
import {
    FieldsList,
    FieldTranslation,
    ReportAvailablesFields,
    ReportManagerInfo,
    ReportManagerInfoCoursesFilter,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility,
    ReportManagerLearningPlansFilter,
    TablesList,
    TablesListAliases,
} from './report-manager';
import { BaseReportManager } from './base-report-manager';
import { CourseExtraFieldsResponse, CourseuserExtraFieldsResponse } from '../services/hydra';

export class UsersEnrollmentTimeManager extends BaseReportManager {
    reportType = ReportsTypes.USERS_ENROLLMENT_TIME;

    // View Options Fields
    allFields = {
        user: [
            FieldsList.USER_USERID,
            FieldsList.USER_BRANCH_NAME,
            FieldsList.USER_BRANCH_PATH,
            FieldsList.USER_BRANCHES_CODES,
            FieldsList.USER_DEACTIVATED,
            FieldsList.USER_EMAIL,
            FieldsList.USER_EMAIL_VALIDATION_STATUS,
            FieldsList.USER_FIRSTNAME,
            FieldsList.USER_FULLNAME,
            FieldsList.USER_LASTNAME,
            FieldsList.USER_REGISTER_DATE,
            FieldsList.USER_EXPIRATION,
            FieldsList.USER_LAST_ACCESS_DATE,
            FieldsList.USER_LEVEL,
            FieldsList.USER_SUSPEND_DATE,
            FieldsList.USER_ID,
            FieldsList.USER_DIRECT_MANAGER,
        ],
        course: [
            FieldsList.COURSE_NAME,
            FieldsList.COURSE_CATEGORY_CODE,
            FieldsList.COURSE_CATEGORY_NAME,
            FieldsList.COURSE_CODE,
            FieldsList.COURSE_CREATION_DATE,
            FieldsList.COURSE_DURATION,
            FieldsList.COURSE_DATE_END,
            FieldsList.COURSE_EXPIRED,
            FieldsList.COURSE_ID,
            FieldsList.COURSE_DATE_BEGIN,
            FieldsList.COURSE_STATUS,
            FieldsList.COURSE_TYPE,
            FieldsList.COURSE_UNIQUE_ID,
            FieldsList.COURSE_CREDITS,
            FieldsList.COURSE_LANGUAGE,
            FieldsList.COURSE_SKILLS,
        ],
        courseuser: [
            FieldsList.COURSEUSER_DATE_FIRST_ACCESS,
            FieldsList.COURSEUSER_DATE_LAST_ACCESS,
            FieldsList.COURSEUSER_EXPIRATION_DATE,
            FieldsList.COURSEUSER_STATUS,
            FieldsList.COURSEUSER_LEVEL,
            FieldsList.COURSEUSER_DAYS_LEFT
        ],
    };
    mandatoryFields = {
        user : [
            FieldsList.USER_USERID
        ],
        course: [
            FieldsList.COURSE_NAME,
        ],
        courseuser: [
            FieldsList.COURSEUSER_DATE_FIRST_ACCESS,
            FieldsList.COURSEUSER_DATE_LAST_ACCESS,
            FieldsList.COURSEUSER_EXPIRATION_DATE,
            FieldsList.COURSEUSER_STATUS,
            FieldsList.COURSEUSER_LEVEL,
            FieldsList.COURSEUSER_DAYS_LEFT
        ]
    };

    public constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap | undefined) {
        super(session, reportDetails);

        if (this.session.platform.checkPluginESignatureEnabled()) {
            this.allFields.course.push(FieldsList.COURSE_E_SIGNATURE);
            this.allFields.courseuser.push(FieldsList.COURSE_E_SIGNATURE_HASH);
        }

        if (this.session.platform.isCoursesAssignmentTypeActive()) {
            this.allFields.courseuser.push(FieldsList.COURSEUSER_ASSIGNMENT_TYPE);
        }
    }

    protected getReportDefaultStructure(report: ReportManagerInfo, title: string, platform: string, idUser: number, description: string): ReportManagerInfo {
        const id = v4();
        const date = new Date();

        /**
         * Report's infos
         */
        report.idReport = id;
        report.author = idUser;
        report.creationDate = this.convertDateObjectToDatetime(date);
        report.platform = platform;
        report.standard = false;


        /**
         * Properties Tab
         */
        report.title = title;
        report.description = description ? description : '';
        report.type = this.reportType;
        report.timezone = this.session.user.getTimezone();
        report.visibility = new ReportManagerInfoVisibility();
        report.visibility.type = VisibilityTypes.ALL_GODADMINS;

        // Report last update infos (floating save bar)
        report.lastEditBy = {
            idUser,
            firstname: '',
            lastname: '',
            username: '',
            avatar: ''
        };
        report.lastEdit = this.convertDateObjectToDatetime(date);


        /**
         * Filters Tab
         */

         // Users
        report.users = new ReportManagerInfoUsersFilter();
        report.users.all = true;

        // Courses
        report.courses = new ReportManagerInfoCoursesFilter();
        report.courses.all = true;
        report.courseExpirationDate = this.getDefaultCourseExpDate();
        report.learningPlans = new ReportManagerLearningPlansFilter();
        report.learningPlans.all = true;

        /**
         * View Options Tab
         */
        const tmpFields: string[] = [];

        this.mandatoryFields.user.forEach(element => {
            tmpFields.push(element);
        });
        this.mandatoryFields.course.forEach(element => {
            tmpFields.push(element);
        });
        this.mandatoryFields.courseuser.forEach(element => {
            tmpFields.push(element);
        });
        report.fields = tmpFields;
        report.sortingOptions = this.getSortingOptions();


        /**
         * Schedule Tab
         */
        report.planning = this.getDefaultPlanningFields();

        return report;
    }

    /**
     * View Options Fields
     */
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

        const userExtraFields = await this.getAvailableUserExtraFields();
        result.user.push(...userExtraFields);

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

        // Recover courseuser fields
        result.courseuser = [];
        for (const field of this.allFields.courseuser) {
            result.courseuser.push({
                field,
                idLabel: field,
                mandatory: this.mandatoryFields.courseuser.includes(field),
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        const courseuserExtraFields = await this.getAvailableEnrollmentExtraFields();
        result.courseuser.push(...courseuserExtraFields);

        return result;
    }

    public getBaseAvailableFields(): Promise<ReportAvailablesFields> {
        throw new Error('Method not implemented.');
    }

    protected getSortingOptions(): SortingOptions {
        return {
            selector: 'default',
            selectedField: FieldsList.USER_USERID,
            orderBy: 'asc',
        };
    }
    setSortingOptions(item: SortingOptions): void {
        this.info.sortingOptions = {
            selector: item && item.selector ? item.selector : 'default',
            selectedField: item && item.selectedField ? item.selectedField : FieldsList.USER_USERID,
            orderBy: item && item.orderBy ? item.orderBy : 'asc'
        };
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

        if (this.info.courseExpirationDate) {
            table += this.buildDateFilter('date_end', this.info.courseExpirationDate, 'AND', true);
        }

        from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);

        let where = [
            `AND ${TablesListAliases.CORE_USER}.userid <> '/Anonymous'`
        ];

        // Add filter the enrollment time report to show
        let expireValidityTimeNotNull = '';
        if (this.session.platform.isDatalakeV2Active()) {
            expireValidityTimeNotNull = `AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_expire_validity != TIMESTAMP '0002-11-30 00:00:00.000'`;
        }
        const enrollmentTimeFilter = ` AND ( ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.Subscribed} OR ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.InProgress}) AND ( ${TablesListAliases.LEARNING_COURSE}.valid_time > 0 OR (${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_expire_validity IS NOT NULL ${expireValidityTimeNotNull}))`;
        where = where.concat(enrollmentTimeFilter);

        // Variables to check if the specified table was already joined in the query
        let joinCourseCategories = false;
        let joinCoreUserFieldValue = false;
        let joinLearningCourseFieldValue = false;
        let joinLearningCourseuserSign = false;
        let joinSkillManagersValue = false;

        // Variables to check if the specified materialized tables was already joined in the query
        let joinCoreUserLevels = false;
        let joinCoreUserBranches = false;

        let joinCoreLangLanguageFieldValue = false;
        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        let translationValue = this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');
        const courseExtraFields = await this.session.getHydra().getCourseExtraFields();
        translationValue = this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course', translationValue);
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
                        select.push(`${TablesListAliases.CORE_USER}.idst AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_ID])}`);
                        break;
                    case FieldsList.USER_USERID:
                        select.push(`SUBSTR(${TablesListAliases.CORE_USER}.userid, 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_USERID])}`);
                        break;
                    case FieldsList.USER_FIRSTNAME:
                        select.push(`${TablesListAliases.CORE_USER}.firstname AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FIRSTNAME])}`);
                        break;
                    case FieldsList.USER_LASTNAME:
                        select.push(`${TablesListAliases.CORE_USER}.lastname AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LASTNAME])}`);
                        break;
                    case FieldsList.USER_FULLNAME:
                        if (this.session.platform.getShowFirstNameFirst()) {
                            select.push(`CONCAT(${TablesListAliases.CORE_USER}.firstname, ' ', ${TablesListAliases.CORE_USER}.lastname) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                        } else {
                            select.push(`CONCAT(${TablesListAliases.CORE_USER}.lastname, ' ', ${TablesListAliases.CORE_USER}.firstname) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                        }
                        break;
                    case FieldsList.USER_EMAIL:
                        select.push(`${TablesListAliases.CORE_USER}.email AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL])}`);
                        break;
                    case FieldsList.USER_EMAIL_VALIDATION_STATUS:
                        select.push(`
                    CASE
                        WHEN ${TablesListAliases.CORE_USER}.email_status = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                        ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                    END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL_VALIDATION_STATUS])}`);
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
                        break;

                    case FieldsList.USER_DEACTIVATED:
                        select.push(`
                    CASE
                        WHEN ${TablesListAliases.CORE_USER}.valid ${this.getCheckIsValidFieldClause()} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                        ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                    END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_DEACTIVATED])}`);
                        break;
                    case FieldsList.USER_EXPIRATION:
                        select.push(`${TablesListAliases.CORE_USER}.expiration AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EXPIRATION])}`);
                        break;
                    case FieldsList.USER_SUSPEND_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.CORE_USER}.suspend_date AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_SUSPEND_DATE])}`);
                        break;
                    case FieldsList.USER_REGISTER_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.CORE_USER}.register_date AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_REGISTER_DATE])}`);
                        break;
                    case FieldsList.USER_LAST_ACCESS_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.CORE_USER}.lastenter AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LAST_ACCESS_DATE])}`);
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
                        select.push(`${TablesListAliases.CORE_USER_BRANCHES}.branches AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_PATH])}`);
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
                        select.push(`${TablesListAliases.CORE_USER_BRANCHES}.codes AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_BRANCHES_CODES])}`);
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
                        break;

                    // Course fields
                    case FieldsList.COURSE_ID:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.idCourse AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_ID])}`);
                        break;
                    case FieldsList.COURSE_UNIQUE_ID:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.uidCourse AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_UNIQUE_ID])}`);
                        break;
                    case FieldsList.COURSE_CODE:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.code AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CODE])}`);
                        break;
                    case FieldsList.COURSE_NAME:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.name AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_NAME])}`);
                        break;
                    case FieldsList.COURSE_CATEGORY_CODE:
                        if (!joinCourseCategories) {
                            joinCourseCategories = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_CATEGORY} AS ${TablesListAliases.LEARNING_CATEGORY} ON ${TablesListAliases.LEARNING_CATEGORY}.idCategory = ${TablesListAliases.LEARNING_COURSE}.idCategory AND ${TablesListAliases.LEARNING_CATEGORY}.lang_code = '${this.session.user.getLang()}'`);
                        }
                        select.push(`${TablesListAliases.LEARNING_CATEGORY}.code AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_CODE])}`);
                        break;
                    case FieldsList.COURSE_CATEGORY_NAME:
                        if (!joinCourseCategories) {
                            joinCourseCategories = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_CATEGORY} AS ${TablesListAliases.LEARNING_CATEGORY} ON ${TablesListAliases.LEARNING_CATEGORY}.idCategory = ${TablesListAliases.LEARNING_COURSE}.idCategory AND ${TablesListAliases.LEARNING_CATEGORY}.lang_code = '${this.session.user.getLang()}'`);
                        }
                        select.push(`${TablesListAliases.LEARNING_CATEGORY}.translation AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_NAME])}`);
                        break;
                    case FieldsList.COURSE_STATUS:
                        select.push(`
                    CASE
                        WHEN ${TablesListAliases.LEARNING_COURSE}.status = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_STATUS_PREPARATION])}
                        ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_STATUS_EFFECTIVE])}
                    END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_STATUS])}`);
                        break;
                    case FieldsList.COURSE_CREDITS:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.credits AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CREDITS])}`);
                        break;
                    case FieldsList.COURSE_DURATION:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.mediumTime AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DURATION])}`);
                        break;
                    case FieldsList.COURSE_TYPE:
                        select.push(`
                    CASE
                        WHEN ${TablesListAliases.LEARNING_COURSE}.course_type = ${athena.renderStringInQueryCase(CourseTypes.Elearning)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_ELEARNING])}
                        WHEN ${TablesListAliases.LEARNING_COURSE}.course_type = ${athena.renderStringInQueryCase(CourseTypes.Classroom)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_CLASSROOM])}
                        ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_WEBINAR])}
                    END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_TYPE])}`);
                        break;
                    case FieldsList.COURSE_DATE_BEGIN:
                        select.push(`${this.mapDateDefaultValueWithDLV2(`${TablesListAliases.LEARNING_COURSE}.date_begin`)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_BEGIN])}`);
                        break;
                    case FieldsList.COURSE_DATE_END:
                        select.push(`${this.mapDateDefaultValueWithDLV2(`${TablesListAliases.LEARNING_COURSE}.date_end`)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_END])}`);
                        break;
                    case FieldsList.COURSE_EXPIRED:
                        let dateEndNotNull = ``;
                        if (this.session.platform.isDatalakeV2Active()) {
                            dateEndNotNull = `AND ${TablesListAliases.LEARNING_COURSE}.date_end != DATE '0101-01-01'`;
                        }
                        select.push(`
                        CASE
                        WHEN (${TablesListAliases.LEARNING_COURSE}.date_end < CURRENT_DATE ${dateEndNotNull}) THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                        ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                    END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_EXPIRED])}`);
                        break;
                    case FieldsList.COURSE_CREATION_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSE}.create_date AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CREATION_DATE])}`);
                        break;
                    case FieldsList.COURSE_LANGUAGE:
                        if (!joinCoreLangLanguageFieldValue) {
                            joinCoreLangLanguageFieldValue = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_LANG_LANGUAGE} AS ${TablesListAliases.CORE_LANG_LANGUAGE} ON ${TablesListAliases.LEARNING_COURSE}.lang_code = ${TablesListAliases.CORE_LANG_LANGUAGE}.lang_code`);

                        }
                        select.push(`${TablesListAliases.CORE_LANG_LANGUAGE}.lang_description AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_LANGUAGE])}`);
                        break;
                    case FieldsList.COURSE_E_SIGNATURE:
                        if (this.session.platform.checkPluginESignatureEnabled()) {
                            select.push(`
                                CASE
                                    WHEN ${TablesListAliases.LEARNING_COURSE}.has_esignature_enabled = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                    ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                END AS ${athena.renderStringInQuerySelect(FieldsList.COURSE_E_SIGNATURE)}`);
                        }
                        break;
                    case FieldsList.COURSE_SKILLS:
                        from.push(`LEFT JOIN ${TablesList.SKILLS_WITH} AS ${TablesListAliases.SKILLS_WITH} ON ${TablesListAliases.SKILLS_WITH}.idCourse = ${TablesListAliases.LEARNING_COURSE}.idCourse`);
                        select.push(`${TablesListAliases.SKILLS_WITH}.skillsInCourse AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_SKILLS])}`);
                        break;
                    // Courseuser fields
                    case FieldsList.COURSEUSER_LEVEL:
                        select.push(`
                    CASE
                        WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.level = ${CourseuserLevels.Teacher} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TEACHER])}
                        WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.level = ${CourseuserLevels.Tutor} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TUTOR])}
                        ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_STUDENT])} END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_LEVEL])}`);
                        break;
                    case FieldsList.COURSEUSER_DATE_FIRST_ACCESS:
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_first_access AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_FIRST_ACCESS])}`);
                        break;
                    case FieldsList.COURSEUSER_DATE_LAST_ACCESS:
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_last_access AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_LAST_ACCESS])}`);
                        break;
                    case FieldsList.COURSEUSER_EXPIRATION_DATE:
                        const formatDateExpireValidity = `DATE_FORMAT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_expire_validity AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s')`;
                        if (this.session.platform.isDatalakeV2Active()) {
                            select.push(`IF(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_expire_validity IS NOT NULL AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_expire_validity != TIMESTAMP '0002-11-30 00:00:00.000', ${formatDateExpireValidity}, '') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_EXPIRATION_DATE])}`);
                        } else {
                            select.push(`${formatDateExpireValidity} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_EXPIRATION_DATE])}`);
                        }
                        break;
                    case FieldsList.COURSEUSER_STATUS:
                        select.push(`
                    CASE
                        WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.waiting ${this.getCheckIsValidFieldClause()} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING])}
                        WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.WaitingList} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                        WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.Confirmed} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_CONFIRMED])}
                        WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.Subscribed} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                        WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.InProgress} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                        WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.Completed} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                        WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.Suspend} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                        ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING])}
                    END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_STATUS])}`);
                        break;
                    case FieldsList.COURSEUSER_DAYS_LEFT:
                        select.push(`
                    CASE
                        WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_expire_validity IS NULL THEN NULL
                        ELSE DATE_DIFF('day', DATE_TRUNC('day', NOW()), DATE_TRUNC('day', ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_expire_validity))
                    END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DAYS_LEFT])}`);
                        break;
                    case FieldsList.COURSE_E_SIGNATURE_HASH:
                        if (this.session.platform.checkPluginESignatureEnabled()) {
                            if (!joinLearningCourseuserSign) {
                                joinLearningCourseuserSign = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEUSER_SIGN} AS ${TablesListAliases.LEARNING_COURSEUSER_SIGN} ON ${TablesListAliases.LEARNING_COURSEUSER_SIGN}.course_id = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse AND ${TablesListAliases.LEARNING_COURSEUSER_SIGN}.user_id = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                            }
                            select.push(`${TablesListAliases.LEARNING_COURSEUSER_SIGN}.signature AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_E_SIGNATURE_HASH])}`);
                        }
                        break;
                    case FieldsList.COURSEUSER_ASSIGNMENT_TYPE:
                        if (this.session.platform.isCoursesAssignmentTypeActive()) {
                            select.push(this.getCourseAssignmentTypeSelectField(false, translations));
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
                                                select.push(`${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                    select.push(`DATE_FORMAT(${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}, '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} AND ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.YesNo:
                                                select.push(`
                                                CASE
                                                    WHEN ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                                    WHEN ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                                    ELSE ''
                                                END AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Country:
                                                from.push(`LEFT JOIN ${TablesList.CORE_COUNTRY} AS ${TablesListAliases.CORE_COUNTRY}_${fieldId} ON ${TablesListAliases.CORE_COUNTRY}_${fieldId}.id_country = ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}`);
                                                select.push(`${TablesListAliases.CORE_COUNTRY}_${fieldId}.name_country AS ${athena.renderStringInQuerySelect(userField.title)}`);
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
                                                select.push(`${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId} AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}, '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId} AND ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.YesNo:
                                                select.push(`
                                                CASE
                                                    WHEN ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId} = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                                    ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                                END AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
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
                                                select.push(`JSON_EXTRACT_SCALAR(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.enrollment_fields, '$.${fieldId}') AS ${athena.renderStringInQuerySelect(courseuserField.name)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.LEARNING_ENROLLMENT_FIELDS_DROPDOWN} AS ${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId} ON ${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.id = CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.enrollment_fields, '$.${fieldId}') AS INTEGER)`);
                                                select.push(`IF(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.translation LIKE '%"${this.session.user.getLangCode()}":%', JSON_EXTRACT_SCALAR(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.translation, '$["${this.session.user.getLangCode()}"]'), JSON_EXTRACT_SCALAR(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.translation, '$["${this.session.platform.getDefaultLanguageCode()}"]')) AS ${athena.renderStringInQuerySelect(courseuserField.name)}`);
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
        let query = ``;
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
        query += `SELECT ${select.join(', ')}
        FROM ${from.join(' ')}
        ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(select, translations, {user: userExtraFields.data.items, course: courseExtraFields.data.items, userCourse: [], webinar: [], classroom: []});
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
            groupBy: [],
            archivedGroupBy: [],
            userAdditionalFieldsSelect: [],
            userAdditionalFieldsFrom: [],
            userAdditionalFieldsId: [],
            courseAdditionalFieldsSelect: [],
            courseAdditionalFieldsFrom: [],
            courseAdditionalFieldsId: [],
            courseUserAdditionalFields: [],
            translations,
            checkPuVisibility
        };

        let table = `SELECT * FROM ${TablesList.LEARNING_COURSEUSER_AGGREGATE} WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND "iduser" IN (${fullUsers})`;
        }

        if (fullCourses !== '') {
            table += ` AND "idcourse" IN (${fullCourses})`;
        }

        if (this.info.users && this.info.users.showOnlyLearners) {
            table += ` AND "level" = ${CourseuserLevels.Student}`;
        }

        queryHelper.from.push(`(${table}) AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}`);


        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `"valid" = 1` : 'true';

        if (hideExpiredUsers) {
            table += ` AND ("expiration" IS NULL OR "expiration" > CURRENT_TIMESTAMP())`;
        }
        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);


        // JOIN LEARNING COURSE
        table = `SELECT * FROM ${TablesList.LEARNING_COURSE} WHERE TRUE`;

        if (fullCourses !== '') {
            table += ` AND "idcourse" IN (${fullCourses})`;
        }

        if (this.info.courseExpirationDate) {
            table += this.buildDateFilter('date_end', this.info.courseExpirationDate, 'AND', true);
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);

        let where = [
            `AND ${TablesListAliases.CORE_USER}."userid" <> '/Anonymous'`
        ];

        // Add filter the enrollment time report to show
        const enrollmentTimeFilter = ` AND ( ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.Subscribed} OR ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."status" = ${EnrollmentStatuses.InProgress}) AND ( ${TablesListAliases.LEARNING_COURSE}."valid_time" > 0 OR (${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."date_expire_validity" IS NOT NULL))`;
        where = where.concat(enrollmentTimeFilter);

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

        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged = this.querySelectUserFields(field, queryHelper) ||
                    this.querySelectCourseFields(field, queryHelper) ||
                    this.querySelectEnrollmentFields(field, queryHelper) ||
                    this.queryWithUserAdditionalFields(field, queryHelper, userExtraFields) ||
                    this.queryWithCourseAdditionalFields(field, queryHelper, courseExtraFields) ||
                    this.queryWithCourseUserAdditionalFields(field, queryHelper, courseuserExtraFields);
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
        const groupByUniqueFields = queryHelper.groupBy.length > 0 && this.info.fields.includes(FieldsList.COURSE_SKILLS)
            ? [...new Set(queryHelper.groupBy)] : [];
        let query = `
                ${queryHelper.cte.length > 0 ? `WITH ${queryHelper.cte.join(', ')}` : ''}
                SELECT ${queryHelper.select.join(', ')}
                FROM ${queryHelper.from.join(' ')}
                ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}
                ${groupByUniqueFields.length > 0 ? ` GROUP BY ${groupByUniqueFields.join(', ')}` : ''}`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(
                queryHelper.select,
                queryHelper.translations,
                {
                    user: userExtraFields.data.items,
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

    parseLegacyReport(legacyReport: LegacyReport, platform: string, visibilityRules: VisibilityRule[]): ReportManagerInfo {
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
        if (filters.courses_expiring_in) {
            report.courseExpirationDate = {
                ...report.courseExpirationDate as DateOptionsValueDescriptor,
                any: false,
                type: 'relative',
                days: +filters.courses_expiring_in,
                operator: 'expiringIn',
            };
        }

        // import courses_expiring_before only if courses_expiring_in is not populated
        if (filters.courses_expiring_before && !filters.courses_expiring_in) {
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

        // map selected fields and manage order by
        if (filterData.fields) {
            let legacyOrderField: LegacyOrderByParsed | undefined;
            const userMandatoryFieldsMap = this.mandatoryFields.user.reduce((previousValue: { [key: string]: string }, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});
            const courseMandatoryFieldsMap = this.mandatoryFields.course.reduce((previousValue: { [key: string]: string }, currentValue) => {
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
