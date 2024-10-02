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
import SessionManager from '../services/session/session-manager.session';
import { v4 } from 'uuid';
import { SortingOptions, VisibilityTypes } from './custom-report';
import { UserLevels } from '../services/session/user-manager.session';
import {
    AdditionalFieldsTypes,
    CourseTypes,
    CourseuserLevels,
    EnrollmentStatuses,
} from './base';
import { LegacyOrderByParsed, LegacyReport, VisibilityRule } from './migration-component';
import { ReportsTypes } from '../reports/constants/report-types';
import { BaseReportManager } from './base-report-manager';

export class GroupsCoursesManager extends BaseReportManager {

    reportType = ReportsTypes.GROUPS_COURSES;

    allFields = {
        group: [
            FieldsList.GROUP_GROUP_OR_BRANCH_NAME,
            FieldsList.GROUP_MEMBERS_COUNT
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
        statistics: [
            FieldsList.STATS_ENROLLED_USERS,
            FieldsList.STATS_NOT_STARTED_USERS,
            FieldsList.STATS_NOT_STARTED_USERS_PERCENTAGE,
            FieldsList.STATS_IN_PROGRESS_USERS,
            FieldsList.STATS_IN_PROGRESS_USERS_PERCENTAGE,
            FieldsList.STATS_COMPLETED_USERS,
            FieldsList.STATS_COMPLETED_USERS_PERCENTAGE,
            FieldsList.STATS_TOTAL_TIME_IN_COURSE,
            FieldsList.STATS_SESSION_TIME,
        ]
    };

    mandatoryFields = {
        group: [
            FieldsList.GROUP_GROUP_OR_BRANCH_NAME,
            FieldsList.GROUP_MEMBERS_COUNT
        ],
        course: [
            FieldsList.COURSE_NAME
        ],
        statistics: []
    };

    constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap|undefined) {
        super(session, reportDetails);

        if (this.session.platform.checkPluginESignatureEnabled()) {
            this.allFields.course.push(FieldsList.COURSE_E_SIGNATURE);
        }
    }

    public async getAvailablesFields(): Promise<ReportAvailablesFields> {
        const result: ReportAvailablesFields = {};
        const translations = await this.loadTranslations(true);

        // Recover user fields
        result.group = [];
        for (const field of this.allFields.group) {
            result.group.push({
                field,
                idLabel: field,
                mandatory: this.mandatoryFields.group.includes(field),
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

        const courseExtraFields = await this.getAvailableCourseExtraFields();
        result.course.push(...courseExtraFields);

        // Recover statistics fields
        result.statistics = [];
        for (const field of this.allFields.statistics) {
            result.statistics.push({
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

    async getQuery(limit = 0, isPreview: boolean): Promise<string> {
        const translations = await this.loadTranslations();
        const hydra = this.session.getHydra();
        const athena = this.session.getAthena();

        const allUsers = this.info.users ? this.info.users.all : false;
        let fullUsers = '';
        let fullGroups = '';

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilter();
            fullGroups = await this.calculateGroupsFilter();
        }

        const fullCourses = await this.calculateCourseFilter();

        const select: string[] = [];
        const from: string[] = [];
        const where: string[] = [];

        // core_group table
        let table = `SELECT * FROM ${TablesList.CORE_GROUP} WHERE TRUE`;

        if (fullGroups !== '') {
            table += ` AND idst IN (${fullGroups})`;
        }

        table += ` AND (hidden = 'false' OR groupid LIKE '/oc|_%' ESCAPE '|')`;

        if (isPreview) {
            // adding "order by" to the query to return always the same ordered result
            table += ' ORDER BY idst';
            table += ' LIMIT ' + limit;
        }

        from.push(`(${table}) AS ${TablesListAliases.CORE_GROUP}`);

        // core_group_members table
        table = `SELECT * FROM ${TablesList.CORE_GROUP_MEMBERS} WHERE TRUE`;

        if (fullGroups !== '') {
            table += ` AND idst IN (${fullGroups})`;
        }

        from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_GROUP_MEMBERS} ON ${TablesListAliases.CORE_GROUP_MEMBERS}.idst = ${TablesListAliases.CORE_GROUP}.idst`);

        // core_group_members table for group users count
        table = `SELECT idst, COUNT(idstMember) AS idstMemberCount FROM ${TablesList.CORE_GROUP_MEMBERS} WHERE TRUE`;

        if (fullGroups !== '') {
            table += ` AND idst IN (${fullGroups})`;
        }

        table += ' GROUP BY idst';

        from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_GROUP_MEMBERS}Count ON ${TablesListAliases.CORE_GROUP_MEMBERS}Count.idst = ${TablesListAliases.CORE_GROUP}.idst`);

        // core_user table
        from.push(`JOIN (SELECT * FROM ${TablesList.CORE_USER} WHERE valid ${this.getCheckIsValidFieldClause()}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.CORE_GROUP_MEMBERS}.idstMember`);

        // learning_courseuser_aggregate table
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

        from.push(` JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE} ON ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser = ${TablesListAliases.CORE_GROUP_MEMBERS}.idstMember`);

        // learning_course table
        table = `SELECT * FROM ${TablesList.LEARNING_COURSE} WHERE TRUE`;

        if (fullCourses !== '') {
            table += ` AND idCourse IN (${fullCourses})`;
        }

        if (this.info.courseExpirationDate) {
            table += this.buildDateFilter('date_end', this.info.courseExpirationDate, 'AND', true);
        }

        from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);

        // core_org_chart_tree table
        from.push(`LEFT JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE} ON ${TablesListAliases.CORE_GROUP}.idst = ${TablesListAliases.CORE_ORG_CHART_TREE}.idst_oc`);

        // core_org_chart table
        from.push(`LEFT JOIN ${TablesList.CORE_ORG_CHART} AS ${TablesListAliases.CORE_ORG_CHART} ON ${TablesListAliases.CORE_ORG_CHART_TREE}.idOrg = ${TablesListAliases.CORE_ORG_CHART}.id_dir AND ${TablesListAliases.CORE_ORG_CHART}.lang_code = '${this.session.user.getLang()}'`);

        // Variables to check if the specified table was already joined in the query
        let joinCourseCategories = false;
        let joinLearningTracksessionAggregate = false;
        let joinLearningCourseFieldValue = false;
        let joinCoreLangLanguageFieldValue = false;
        let joinCourseSessionTimeAggregate = false;
        let joinSkillSkills = false;
        let joinSkillSkillsObjects = false;
        const courseExtraFields = await this.session.getHydra().getCourseExtraFields();
        this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course');

        if (this.info.fields) {
            for (const field of this.info.fields) {
                switch (field) {
                    // Group fields
                    case FieldsList.GROUP_GROUP_OR_BRANCH_NAME:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.CORE_ORG_CHART_TREE}.idOrg) IS NOT NULL THEN CONCAT(CASE WHEN ARBITRARY(${TablesListAliases.CORE_ORG_CHART_TREE}.code) <> '' THEN CONCAT('(',ARBITRARY(${TablesListAliases.CORE_ORG_CHART_TREE}.code), ') ') ELSE '' END, ARBITRARY(${TablesListAliases.CORE_ORG_CHART}.translation))
                                ELSE ARBITRARY(SUBSTR(${TablesListAliases.CORE_GROUP}.groupid, 2))
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.GROUP_GROUP_OR_BRANCH_NAME])}`);
                        break;
                    case FieldsList.GROUP_MEMBERS_COUNT:
                        select.push(`ARBITRARY(${TablesListAliases.CORE_GROUP_MEMBERS}Count.idstMemberCount) AS ${athena.renderStringInQuerySelect(translations[FieldsList.GROUP_MEMBERS_COUNT])}`);
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
                    case FieldsList.STATS_NOT_STARTED_USERS:
                        select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.Subscribed} THEN 1 ELSE 0 END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_NOT_STARTED_USERS])}`);
                        break;
                    case FieldsList.STATS_NOT_STARTED_USERS_PERCENTAGE:
                        select.push(`ROUND(((SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.Subscribed} THEN 1 ELSE 0 END) * 100.0) / COUNT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser)), 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_NOT_STARTED_USERS_PERCENTAGE])}`);
                        break;
                    case FieldsList.STATS_IN_PROGRESS_USERS:
                        select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.InProgress} THEN 1 ELSE 0 END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_IN_PROGRESS_USERS])}`);
                        break;
                    case FieldsList.STATS_IN_PROGRESS_USERS_PERCENTAGE:
                        select.push(`ROUND(((SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.InProgress} THEN 1 ELSE 0 END) * 100.0) / COUNT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser)), 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_IN_PROGRESS_USERS_PERCENTAGE])}`);
                        break;
                    case FieldsList.STATS_COMPLETED_USERS:
                        select.push(`SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.Completed} THEN 1 ELSE 0 END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_COMPLETED_USERS])}`);
                        break;
                    case FieldsList.STATS_COMPLETED_USERS_PERCENTAGE:
                        select.push(`ROUND(((SUM(CASE WHEN ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status = ${EnrollmentStatuses.Completed} THEN 1 ELSE 0 END) * 100.0) / COUNT(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser)), 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_COMPLETED_USERS_PERCENTAGE])}`);
                        break;
                    case FieldsList.STATS_TOTAL_TIME_IN_COURSE:
                        if (!joinLearningTracksessionAggregate) {
                            joinLearningTracksessionAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idUser = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`SUM(${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}.totaltime) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_TOTAL_TIME_IN_COURSE])}`);
                        break;
                    case FieldsList.STATS_SESSION_TIME:
                        if (!joinCourseSessionTimeAggregate) {
                            joinCourseSessionTimeAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.COURSE_SESSION_TIME_AGGREGATE} AS ${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE} ON ${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE}.id_user = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser AND ${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE}.course_id = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);
                        }
                        select.push(`SUM(${TablesListAliases.COURSE_SESSION_TIME_AGGREGATE}.session_time) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_SESSION_TIME])}`);
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
        GROUP BY ${TablesListAliases.CORE_GROUP}.idst, ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`;

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
        let fullUsers = '';
        let fullGroups = '';

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilterSnowflake(checkPuVisibility);
            fullGroups = await this.calculateGroupFilterSnowflake(checkPuVisibility);
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
            groupBy: [`${TablesListAliases.CORE_GROUP}."idst"`, `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`],
            archivedGroupBy: [],
            courseAdditionalFieldsFrom: [],
            courseAdditionalFieldsSelect: [],
            courseAdditionalFieldsId: [],
            translations,
            checkPuVisibility
        };

        // core_group table
        let table = `SELECT * FROM ${TablesList.CORE_GROUP} WHERE TRUE`;

        if (fullGroups !== '') {
            table += ` AND "idst" IN (${fullGroups})`;
        }

        table += ` AND ("hidden" = 'false' OR "groupid" LIKE '/oc|_%' ESCAPE '|')`;

        if (isPreview) {
            table += ' LIMIT ' + limit;
        }

        queryHelper.from.push(`(${table}) AS ${TablesListAliases.CORE_GROUP}`);

        // core_group_members table
        table = `SELECT * FROM ${TablesList.CORE_GROUP_MEMBERS} WHERE TRUE`;

        if (fullGroups !== '') {
            table += ` AND "idst" IN (${fullGroups})`;
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_GROUP_MEMBERS} ON ${TablesListAliases.CORE_GROUP_MEMBERS}."idst" = ${TablesListAliases.CORE_GROUP}."idst"`);

        // core_group_members table for group users count
        table = `SELECT "idst", COUNT("idstmember") AS "idstmembercount" FROM ${TablesList.CORE_GROUP_MEMBERS} WHERE TRUE`;

        if (fullGroups !== '') {
            table += ` AND "idst" IN (${fullGroups})`;
        }

        table += ' GROUP BY "idst"';

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_GROUP_MEMBERS}Count ON ${TablesListAliases.CORE_GROUP_MEMBERS}Count."idst" = ${TablesListAliases.CORE_GROUP}."idst"`);

        // core_user table
        queryHelper.from.push(`JOIN (SELECT * FROM ${TablesList.CORE_USER} WHERE "valid" = 1) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember"`);

        // learning_courseuser_aggregate table
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

        queryHelper.from.push(` JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE} ON ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember"`);

        // learning_course table
        table = `SELECT * FROM ${TablesList.LEARNING_COURSE} WHERE TRUE`;

        if (fullCourses !== '') {
            table += ` AND "idcourse" IN (${fullCourses})`;
        }

        if (this.info.courseExpirationDate) {
            table += this.buildDateFilter('date_end', this.info.courseExpirationDate, 'AND', true);
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);

        // core_org_chart_tree table
        queryHelper.from.push(`LEFT JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE} ON ${TablesListAliases.CORE_GROUP}."idst" = ${TablesListAliases.CORE_ORG_CHART_TREE}."idst_oc"`);

        // core_org_chart table
        queryHelper.from.push(`LEFT JOIN ${TablesList.CORE_ORG_CHART} AS ${TablesListAliases.CORE_ORG_CHART} ON ${TablesListAliases.CORE_ORG_CHART_TREE}."idorg" = ${TablesListAliases.CORE_ORG_CHART}."id_dir" AND ${TablesListAliases.CORE_ORG_CHART}."lang_code" = '${this.session.user.getLang()}'`);

        const courseExtraFields = await this.session.getHydra().getCourseExtraFields();
        this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course');


        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged = this.querySelectGroupFields(field, queryHelper) ||
                    this.querySelectCourseFields(field, queryHelper) ||
                    this.querySelectUsageStatisticsFields(field, queryHelper) ||
                    this.queryWithCourseAdditionalFields(field, queryHelper, courseExtraFields);
            }
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

    /**
     * Set the sortingOptions object with the input passed
     * @param sortingOptions The object that describes a sortingOptions
     */
    setSortingOptions(item: SortingOptions): void {
        this.info.sortingOptions = {
            selector: item && item.selector ? item.selector : 'default',
            selectedField: item && item.selectedField ? item.selectedField : FieldsList.GROUP_GROUP_OR_BRANCH_NAME,
            orderBy: item && item.orderBy ? item.orderBy : 'asc'
        };
    }

    protected getSortingOptions(): SortingOptions {
        return {
            selector: 'default',
            selectedField: FieldsList.GROUP_GROUP_OR_BRANCH_NAME,
            orderBy: 'asc',
        };
    }

    public parseLegacyReport(legacyReport: LegacyReport, platform: string, visibilityRules: VisibilityRule[]): ReportManagerInfo {
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

        // map selected fields
        if (filterData.fields) {
            let legacyOrderField: LegacyOrderByParsed | undefined;
            const groupMandatoryFieldsMap = this.mandatoryFields.group.reduce((previousValue: {[key: string]: string}, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});
            const courseMandatoryFieldsMap = this.mandatoryFields.course.reduce((previousValue: {[key: string]: string}, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});

            const courseFieldsDescriptor = this.mapCourseSelectedFields(filterData.fields.course, filterData.order, courseMandatoryFieldsMap);
            report.fields.push(...courseFieldsDescriptor.fields);
            if (courseFieldsDescriptor.orderByDescriptor) legacyOrderField = courseFieldsDescriptor.orderByDescriptor;

            const statsFieldsDescriptor = this.mapStatsSelectedFields(filterData.fields.stat, filterData.order);
            report.fields.push(...statsFieldsDescriptor.fields);
            if (statsFieldsDescriptor.orderByDescriptor) legacyOrderField = statsFieldsDescriptor.orderByDescriptor;

            const groupFieldsDescriptor = this.mapGroupSelectedFields(filterData.fields.group, filterData.order, groupMandatoryFieldsMap);
            report.fields.push(...groupFieldsDescriptor.fields);
            if (groupFieldsDescriptor.orderByDescriptor) legacyOrderField = groupFieldsDescriptor.orderByDescriptor;

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

    protected getReportDefaultStructure(report: ReportManagerInfo, title: string, platform: string, idUser: number, description: string): ReportManagerInfo {
        const id = v4();
        const date = new Date();

        const tmpFields: string[] = [];
        this.mandatoryFields.group.forEach(element => {
            tmpFields.push(element);
        });

        this.mandatoryFields.course.forEach(element => {
            tmpFields.push(element);
        });

        this.mandatoryFields.statistics.forEach(element => {
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

        // Manage the planning default fields
        report.planning = this.getDefaultPlanningFields();

        // Manage filters default values
        report.users = new ReportManagerInfoUsersFilter();
        report.users.all = true;

        report.courses = new ReportManagerInfoCoursesFilter();
        report.courses.all = true;
        report.learningPlans = new ReportManagerLearningPlansFilter();
        report.learningPlans.all = true;

        report.courseExpirationDate = this.getDefaultCourseExpDate();

        return report;
    }

}
