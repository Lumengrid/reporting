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
    TablesListAliases
} from './report-manager';
import SessionManager from '../services/session/session-manager.session';
import { LegacyOrderByParsed, LegacyReport, VisibilityRule } from './migration-component';
import {
    DateOptions,
    DateOptionsValueDescriptor,
    SortingOptions,
    VisibilityTypes
} from './custom-report';
import httpContext from 'express-http-context';
import { SessionLoggerService } from '../services/logger/session-logger.service';
import { v4 } from 'uuid';
import { ReportsTypes } from '../reports/constants/report-types';
import { UserLevels } from '../services/session/user-manager.session';
import {
    AdditionalFieldsTypes,
    CourseTypeFilter,
    CourseTypes,
    CourseuserLevels,
    LOStatus,
    LOTypes,
    ScoresTypes,
    UserLevelsGroups
} from './base';
import { Utils } from '../reports/utils';
import { BaseReportManager } from './base-report-manager';
import { CourseExtraFieldsResponse, CourseuserExtraFieldsResponse } from '../services/hydra';

export class UsersLearningObjectManager extends BaseReportManager {
    // Renamed in Users - Training Material
    reportType = ReportsTypes.USERS_LEARNINGOBJECTS;
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
            FieldsList.COURSE_LANGUAGE
        ],
        trainingMaterials: [
            FieldsList.LO_TITLE,
            FieldsList.LO_BOOKMARK,
            FieldsList.LO_DATE_ATTEMPT,
            FieldsList.LO_FIRST_ATTEMPT,
            FieldsList.LO_SCORE,
            FieldsList.LO_STATUS,
            FieldsList.LO_TYPE,
            FieldsList.LO_VERSION,
            FieldsList.LO_DATE_COMPLETE,
        ],
        courseuser: [
            FieldsList.COURSEUSER_DATE_INSCR,
            FieldsList.COURSEUSER_DATE_COMPLETE,
        ]
    };

    mandatoryFields = {
        user : [
            FieldsList.USER_USERID
        ],
        course: [
            FieldsList.COURSE_NAME
        ],
        trainingMaterials: [
            FieldsList.LO_BOOKMARK,
            FieldsList.LO_TITLE,
        ],
        courseuser: []
    };
    logger: SessionLoggerService;

    public constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap | undefined) {
        super(session, reportDetails);
        this.logger = httpContext.get('logger');

        if (this.session.platform.checkPluginESignatureEnabled()) {
            this.allFields.course.push(FieldsList.COURSE_E_SIGNATURE);
            this.allFields.courseuser.push(FieldsList.COURSE_E_SIGNATURE_HASH);
        }

        if (this.session.platform.isCoursesAssignmentTypeActive()) {
            this.allFields.courseuser.push(FieldsList.COURSEUSER_ASSIGNMENT_TYPE);
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

        result.trainingMaterials = [];
        for (const field of this.allFields.trainingMaterials) {
            result.trainingMaterials.push({
                field,
                idLabel: field,
                mandatory: this.mandatoryFields.trainingMaterials.includes(field),
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

        return result;
    }

    public getBaseAvailableFields(): Promise<ReportAvailablesFields> {
        throw new Error('Method not implemented.');
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
        table = `SELECT * FROM ${TablesList.LEARNING_COURSE} WHERE TRUE`;

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

        from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);

        table = `SELECT * FROM ${TablesList.LEARNING_ORGANIZATION} WHERE objectType <> ''`;

        const loFilter: string[] = [];
        let applyLOFilter = false;

        if (this.info.loTypes) {
            for (const key in this.info.loTypes) {
                if (this.info.loTypes[key]) {
                    loFilter.push(key);
                }
            }

            applyLOFilter = Object.keys(this.info.loTypes).length !== loFilter.length;
        }

        if (loFilter.length > 0 && applyLOFilter) {
            table += ` AND objectType IN ('${loFilter.join("','")}')`;
        }

        from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_ORGANIZATION} ON ${TablesListAliases.LEARNING_ORGANIZATION}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);

        let where = [
            `AND ${TablesListAliases.CORE_USER}.userid <> '/Anonymous'`
        ];

        // Variables to check if the specified table was already joined in the query
        let joinCourseCategories = false;
        let joinCoreUserFieldValue = false;
        let joinLearningCourseFieldValue = false;
        let joinLearningCommontrack = false;
        let joinLearningRepositoryObjectVersion = false;
        let joinCoreLangLanguageFieldValue = false;
        let joinLearningCourseuserSign = false;
        let joinSkillManagersValue = false;


        // Variables to check if the specified materialized tables was already joined in the query
        let joinCoreUserLevels = false;
        let joinCoreUserBranches = false;

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
                        const dateEndColumnDLV2Fix = `(${this.mapDateDefaultValueWithDLV2(`${TablesListAliases.LEARNING_COURSE}.date_end`)})`;
                        select.push(`
                            CASE
                                WHEN ${dateEndColumnDLV2Fix} < CURRENT_DATE THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_EXPIRED])}`);
                        break;
                    case FieldsList.COURSE_CREATION_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSE}.create_date AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CREATION_DATE])}`);
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
                    case FieldsList.COURSE_LANGUAGE:
                        if (!joinCoreLangLanguageFieldValue) {
                            joinCoreLangLanguageFieldValue = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_LANG_LANGUAGE} AS ${TablesListAliases.CORE_LANG_LANGUAGE} ON ${TablesListAliases.LEARNING_COURSE}.lang_code = ${TablesListAliases.CORE_LANG_LANGUAGE}.lang_code`);

                        }
                        select.push(`${TablesListAliases.CORE_LANG_LANGUAGE}.lang_description AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_LANGUAGE])}`);
                        break;
                    // Training materials fields
                    case FieldsList.LO_BOOKMARK:
                        select.push(`
                        CASE
                            WHEN ${TablesListAliases.LEARNING_COURSE}.initial_object = ${TablesListAliases.LEARNING_ORGANIZATION}.idOrg AND ${TablesListAliases.LEARNING_COURSE}.final_object = ${TablesListAliases.LEARNING_ORGANIZATION}.idOrg AND ${TablesListAliases.LEARNING_COURSE}.initial_score_mode = '${ScoresTypes.INITIAL_SCORE_TYPE_KEY_OBJECT}' AND ${TablesListAliases.LEARNING_COURSE}.final_score_mode = '${ScoresTypes.FINAL_SCORE_TYPE_KEY_OBJECT}' THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_BOOKMARK_START] + ' - ' + translations[FieldTranslation.LO_BOOKMARK_FINAL])}
                            WHEN ${TablesListAliases.LEARNING_COURSE}.initial_object = ${TablesListAliases.LEARNING_ORGANIZATION}.idOrg AND ${TablesListAliases.LEARNING_COURSE}.initial_score_mode = '${ScoresTypes.INITIAL_SCORE_TYPE_KEY_OBJECT}' THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_BOOKMARK_START])}
                            WHEN ${TablesListAliases.LEARNING_COURSE}.final_object = ${TablesListAliases.LEARNING_ORGANIZATION}.idOrg AND ${TablesListAliases.LEARNING_COURSE}.final_score_mode = '${ScoresTypes.FINAL_SCORE_TYPE_KEY_OBJECT}' THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_BOOKMARK_FINAL])}
                            ELSE '-'
                        END AS ${athena.renderStringInQuerySelect(translations[FieldsList.LO_BOOKMARK])}`);
                        break;
                    case FieldsList.LO_DATE_ATTEMPT:
                        if (!joinLearningCommontrack) {
                            joinLearningCommontrack = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK} ON ${TablesListAliases.LEARNING_COMMONTRACK}.idReference = ${TablesListAliases.LEARNING_ORGANIZATION}.idOrg AND ${TablesListAliases.LEARNING_COMMONTRACK}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                        }
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COMMONTRACK}.dateAttempt AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.LO_DATE_ATTEMPT])}`);
                        break;
                    case FieldsList.LO_FIRST_ATTEMPT:
                        if (!joinLearningCommontrack) {
                            joinLearningCommontrack = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK} ON ${TablesListAliases.LEARNING_COMMONTRACK}.idReference = ${TablesListAliases.LEARNING_ORGANIZATION}.idOrg AND ${TablesListAliases.LEARNING_COMMONTRACK}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                        }
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COMMONTRACK}.firstAttempt AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.LO_FIRST_ATTEMPT])}`);
                        break;
                    case FieldsList.LO_SCORE:
                        if (!joinLearningCommontrack) {
                            joinLearningCommontrack = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK} ON ${TablesListAliases.LEARNING_COMMONTRACK}.idReference = ${TablesListAliases.LEARNING_ORGANIZATION}.idOrg AND ${TablesListAliases.LEARNING_COMMONTRACK}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                        }
                        select.push(`${TablesListAliases.LEARNING_COMMONTRACK}.score AS ${athena.renderStringInQuerySelect(translations[FieldsList.LO_SCORE])}`);
                        break;
                    case FieldsList.LO_STATUS:
                        if (!joinLearningCommontrack) {
                            joinLearningCommontrack = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK} ON ${TablesListAliases.LEARNING_COMMONTRACK}.idReference = ${TablesListAliases.LEARNING_ORGANIZATION}.idOrg AND ${TablesListAliases.LEARNING_COMMONTRACK}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                        }
                        select.push(`
                        CASE
                            WHEN ${TablesListAliases.LEARNING_COMMONTRACK}.status = ${athena.renderStringInQueryCase(LOStatus.AB_INITIO)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_STATUS_NOT_STARTED])}
                            WHEN ${TablesListAliases.LEARNING_COMMONTRACK}.status = ${athena.renderStringInQueryCase(LOStatus.ATTEMPTED)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_STATUS_IN_ITINERE])}
                            WHEN ${TablesListAliases.LEARNING_COMMONTRACK}.status = ${athena.renderStringInQueryCase(LOStatus.COMPLETED)} OR ${TablesListAliases.LEARNING_COMMONTRACK}.status = ${athena.renderStringInQueryCase(LOStatus.PASSED)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_STATUS_COMPLETED])}
                            WHEN ${TablesListAliases.LEARNING_COMMONTRACK}.status = ${athena.renderStringInQueryCase(LOStatus.FAILED)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_STATUS_FAILED])}
                            ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_STATUS_NOT_STARTED])}
                        END AS ${athena.renderStringInQuerySelect(translations[FieldsList.LO_STATUS])}`);
                        break;
                    case FieldsList.LO_TITLE:
                        select.push(`${TablesListAliases.LEARNING_ORGANIZATION}.title AS ${athena.renderStringInQuerySelect(translations[FieldsList.LO_TITLE])}`);
                        break;
                    case FieldsList.LO_TYPE:
                        select.push(`
                        CASE
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${athena.renderStringInQueryCase(LOTypes.AUTHORING)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_AUTHORING])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${athena.renderStringInQueryCase(LOTypes.DELIVERABLE)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_DELIVERABLE])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${athena.renderStringInQueryCase(LOTypes.FILE)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_FILE])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${athena.renderStringInQueryCase(LOTypes.HTMLPAGE)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_HTMLPAGE])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${athena.renderStringInQueryCase(LOTypes.POLL)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_POLL])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${athena.renderStringInQueryCase(LOTypes.SCORM)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_SCORM])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${athena.renderStringInQueryCase(LOTypes.TEST)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_TEST])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${athena.renderStringInQueryCase(LOTypes.TINCAN)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_TINCAN])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${athena.renderStringInQueryCase(LOTypes.VIDEO)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_VIDEO])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${athena.renderStringInQueryCase(LOTypes.AICC)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_AICC])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${athena.renderStringInQueryCase(LOTypes.ELUCIDAT)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_ELUCIDAT])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${athena.renderStringInQueryCase(LOTypes.GOOGLEDRIVE)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_GOOGLEDRIVE])}
                            WHEN ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${athena.renderStringInQueryCase(LOTypes.LTI)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LO_TYPE_LTI])}
                            ELSE '-'
                        END AS ${athena.renderStringInQuerySelect(translations[FieldsList.LO_TYPE])}`);
                        break;
                    case FieldsList.LO_VERSION:
                        if (!joinLearningCommontrack) {
                            joinLearningCommontrack = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK} ON ${TablesListAliases.LEARNING_COMMONTRACK}.idReference = ${TablesListAliases.LEARNING_ORGANIZATION}.idOrg AND ${TablesListAliases.LEARNING_COMMONTRACK}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                        }
                        if (!joinLearningRepositoryObjectVersion) {
                            joinLearningRepositoryObjectVersion = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_REPOSITORY_OBJECT_VERSION} AS ${TablesListAliases.LEARNING_REPOSITORY_OBJECT_VERSION} ON (CASE WHEN ${TablesListAliases.LEARNING_COMMONTRACK}.idResource is null THEN ${TablesListAliases.LEARNING_ORGANIZATION}.idResource ELSE ${TablesListAliases.LEARNING_COMMONTRACK}.idResource END) = ${TablesListAliases.LEARNING_REPOSITORY_OBJECT_VERSION}.id_resource AND ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = ${TablesListAliases.LEARNING_REPOSITORY_OBJECT_VERSION}.object_type`);
                        }
                        select.push(`${TablesListAliases.LEARNING_REPOSITORY_OBJECT_VERSION}.version_name AS ${athena.renderStringInQuerySelect(translations[FieldsList.LO_VERSION])}`);
                        break;

                    // Courseuser fields
                    case FieldsList.COURSEUSER_DATE_INSCR:
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_inscr AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_INSCR])}`);
                        break;
                    case FieldsList.COURSEUSER_DATE_COMPLETE:
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_complete AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_COMPLETE])}`);
                        break;
                    case FieldsList.LO_DATE_COMPLETE:
                        if (!joinLearningCommontrack) {
                            joinLearningCommontrack = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK} ON ${TablesListAliases.LEARNING_COMMONTRACK}.idReference = ${TablesListAliases.LEARNING_ORGANIZATION}.idOrg AND ${TablesListAliases.LEARNING_COMMONTRACK}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                        }
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COMMONTRACK}.last_complete AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.LO_DATE_COMPLETE])}`);
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
                        }
                        break;
                }
            }
        }

        let query = `SELECT ${select.join(', ')}
        FROM ${from.join(' ')}
        ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}`;

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

        // Show only learners
        if (this.info.users && this.info.users.showOnlyLearners) {
            table += ` AND "level" = ${CourseuserLevels.Student}`;
        }

        // manage the date options - enrollmentDate and completionDate
        table += this.composeDateOptionsFilter();

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
            }

            if (this.info.courses.courseType === CourseTypeFilter.ILT) {
                table += ` AND "course_type" = '${CourseTypes.Classroom}'`;
            }
        }

        if (this.info.courseExpirationDate) {
            table += this.buildDateFilter('date_end', this.info.courseExpirationDate, 'AND', true);
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);

        table = `SELECT * FROM ${TablesList.LEARNING_ORGANIZATION} WHERE "objecttype" <> ''`;

        const loFilter: string[] = [];
        let applyLOFilter = false;

        if (this.info.loTypes) {
            for (const key in this.info.loTypes) {
                if (this.info.loTypes[key]) {
                    loFilter.push(key);
                }
            }

            applyLOFilter = Object.keys(this.info.loTypes).length !== loFilter.length;
        }

        if (loFilter.length > 0 && applyLOFilter) {
            table += ` AND "objecttype" IN ('${loFilter.join("','")}')`;
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_ORGANIZATION} ON ${TablesListAliases.LEARNING_ORGANIZATION}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);

        const where = [
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

        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged = this.querySelectUserFields(field, queryHelper) ||
                    this.querySelectCourseFields(field, queryHelper) ||
                    this.querySelectEnrollmentFields(field, queryHelper) ||
                    this.querySelectTrainingMaterialsFields(field, queryHelper) ||
                    this.queryWithUserAdditionalFields(field, queryHelper, userExtraFields) ||
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

        let query = `
            ${queryHelper.cte.length > 0 ? `WITH ${queryHelper.cte.join(', ')}` : ''}
            SELECT ${queryHelper.select.join(', ')}
            FROM ${queryHelper.from.join(' ')}
            ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}`;

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

    /**
     * Step that can populate more fields of a report that is being created for the first time in dynamo
     * @param report{ReportManagerInfo} the report model
     */
    public async onBeforeSaveNewReport(report: ReportManagerInfo): Promise<ReportManagerInfo> {
        report.loTypes = await this.getAllLOTypesAvailable();
        return report;
    }

    // get all lo types and set the state of each one to selected
    public async getAllLOTypesAvailable(): Promise<{[key: string]: boolean}> {
        const selectedTypes: {[key: string]: boolean} = {};
        const types = await this.session.getHydra().getAllLOTypes();
        for (const type of types.data) {
            selectedTypes[type] = true;
        }
        return selectedTypes;
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

        this.mandatoryFields.trainingMaterials.forEach(element => {
            tmpFields.push(element);
        });

        this.mandatoryFields.courseuser.forEach(element => {
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

        if (description) {
            report.description = description;
        }

        // set as undefined - the values are retrieved in the onBeforeCreation
        report.loTypes = undefined;

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
        const utils = new Utils();
        let report = this.getReportDefaultStructure(new ReportManagerInfo(), legacyReport.filter_name, platform, +legacyReport.author, '');
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

        // Enrollment Date
        if (filters.start_date.type !== 'any') {
            report.enrollmentDate = utils.parseLegacyFilterDate(report.enrollmentDate as DateOptionsValueDescriptor, filters.start_date);
        }
        // Completion Date
        if (filters.end_date.type !== 'any') {
            report.completionDate = utils.parseLegacyFilterDate(report.completionDate as DateOptionsValueDescriptor, filters.end_date);
        }

        // Conditions
        if (filters.condition_status) {
            report.conditions = filters.condition_status === 'and' ? 'allConditions' : 'atLeastOneCondition';
        }

        // Learning Object types
        report.loTypes = utils.parseLearningObjectTypes(filters.lo_type ? filters.lo_type : []);

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
            const trainingMaterialsMandatoryFieldsMap = this.mandatoryFields.trainingMaterials.reduce((previousValue: {[key: string]: string}, currentValue) => {
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

            const trainingMaterialsDescriptor = this.mapTrainingMaterialFields(filterData.fields.learning_object, filterData.order, trainingMaterialsMandatoryFieldsMap);
            report.fields.push(...trainingMaterialsDescriptor.fields);
            if (trainingMaterialsDescriptor.orderByDescriptor) legacyOrderField = trainingMaterialsDescriptor.orderByDescriptor;

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

    setSortingOptions(item: SortingOptions): void {
        this.info.sortingOptions = {
            selector: item && item.selector ? item.selector : 'default',
            selectedField: item && item.selectedField ? item.selectedField : FieldsList.USER_USERID,
            orderBy: item && item.orderBy ? item.orderBy : 'asc'
        };
    }
}
