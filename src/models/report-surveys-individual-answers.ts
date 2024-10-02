import { v4 } from 'uuid';

import { Exception } from '../exceptions/exception';
import { ReportsTypes } from '../reports/constants/report-types';
import SessionManager from '../services/session/session-manager.session';
import { UserLevels } from '../services/session/user-manager.session';
import { AdditionalFieldsTypes, CourseTypeFilter, CourseTypes, CourseuserLevels, LOQuestTypes, LOTypes } from './base';
import { BaseReportManager } from './base-report-manager';
import { DateOptions, SortingOptions, VisibilityTypes } from './custom-report';
import { LegacyReport, VisibilityRule } from './migration-component';
import {
    FieldsList,
    FieldTranslation,
    ReportAvailablesFields,
    ReportManagerInfo,
    ReportManagerInfoCoursesFilter,
    ReportManagerInfoSurveysFilter,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility,
    TablesList,
    TablesListAliases,
} from './report-manager';

export class SurveysIndividualAnswersManager extends BaseReportManager {

    reportType = ReportsTypes.SURVEYS_INDIVIDUAL_ANSWERS;

    allFields = {
        group: [
            FieldsList.GROUP_GROUP_OR_BRANCH_NAME
        ],
        survey: [
            FieldsList.SURVEY_ID,
            FieldsList.SURVEY_TITLE,
            FieldsList.SURVEY_DESCRIPTION,
            FieldsList.SURVEY_TRACKING_TYPE,
        ],
        surveyQuestionAnswer: [
            FieldsList.SURVEY_COMPLETION_ID,
            FieldsList.SURVEY_COMPLETION_DATE,
            FieldsList.QUESTION_ID,
            FieldsList.QUESTION,
            FieldsList.QUESTION_TYPE,
            FieldsList.QUESTION_MANDATORY,
            FieldsList.ANSWER_USER,
        ],
        course: [
            FieldsList.COURSE_NAME,
            FieldsList.COURSE_CODE,
            FieldsList.COURSE_DURATION,
            FieldsList.COURSE_DATE_END,
            FieldsList.COURSE_STATUS,
            FieldsList.COURSE_CATEGORY_NAME,
            FieldsList.COURSE_CATEGORY_CODE,
            FieldsList.COURSE_DATE_BEGIN,
            FieldsList.COURSE_EXPIRED,
            FieldsList.COURSE_TYPE,
        ]
    };

    mandatoryFields = {
        group: [],
        survey: [
            FieldsList.SURVEY_TITLE
        ],
        surveyQuestionAnswer: [
            FieldsList.SURVEY_COMPLETION_ID
        ],
        course: [],
    };

    constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap|undefined) {
        super(session, reportDetails);
    }

    public async getAvailablesFields(): Promise<ReportAvailablesFields> {
        const result: ReportAvailablesFields = await this.getBaseAvailableFields();

        const courseExtraFields = await this.getAvailableCourseExtraFields();
        result.course.push(...courseExtraFields);

        return result;
    }

    public async getBaseAvailableFields(): Promise<ReportAvailablesFields> {
        const result: ReportAvailablesFields = {};
        const translations = await this.loadTranslations(true);

        // Recover group fields
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

        // Recover survey fields
        result.survey = [];
        for (const field of this.allFields.survey) {
            result.survey.push({
                field,
                idLabel: field,
                mandatory: this.mandatoryFields.survey.includes(field),
                isAdditionalField: false,
                translation: translations[field]
            });
        }
        // Recover survey Question Answer fields
        result.surveyQuestionAnswer = [];
        for (const field of this.allFields.surveyQuestionAnswer) {
            result.surveyQuestionAnswer.push({
                field,
                idLabel: field,
                mandatory: this.mandatoryFields.surveyQuestionAnswer.includes(field),
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

        return result;
    }

    async getQuery(limit = 0, isPreview: boolean): Promise<string> {

        const translations = await this.loadTranslations();
        const athena = this.session.getAthena();

        const allUsers = this.info.users ? this.info.users.all : false;
        const allSurveys = this.info.surveys ? this.info.surveys.all : false;

        let fullUsers = '';
        let fullGroups = '';
        let surveyCompletionDateFilter = '';

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilter();
            fullGroups = await this.calculateGroupsFilter();
        }

        const fullCourses = await this.calculateCourseFilter();

        const select: string[] = [];
        const from: string[] = [];
        const groupBy = [
            `${TablesListAliases.CORE_USER}.idst`,
            `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`,
            `${TablesListAliases.LEARNING_POLLQUEST}.id_quest`,
            `${TablesListAliases.LEARNING_POLLQUEST}.title_quest`
        ];

        // core_group table
        let table = `SELECT * FROM ${TablesList.CORE_GROUP} WHERE TRUE`;
        if (fullGroups !== '') {
            table += ` AND idst IN (${fullGroups})`;
        }
        table += ` AND (hidden = 'false' OR groupid LIKE '/oc|_%' ESCAPE '|')`;
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
        from.push(`JOIN (SELECT * FROM ${TablesList.CORE_USER} WHERE valid ${this.getCheckIsValidFieldClause()}) AS ${TablesListAliases.CORE_USER}
            ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.CORE_GROUP_MEMBERS}.idstMember`);

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

        // manage the date options - enrollmentDate, completionDate
        table += this.composeDateOptionsFilter();

        from.push(` JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}
            ON ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser = ${TablesListAliases.CORE_GROUP_MEMBERS}.idstMember`);

        // learning_course table
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

        // survey tables
        from.push(`JOIN ${TablesList.LEARNING_ORGANIZATION} AS ${TablesListAliases.LEARNING_ORGANIZATION}
            ON ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse = ${TablesListAliases.LEARNING_ORGANIZATION}.idCourse
                AND ${TablesListAliases.LEARNING_ORGANIZATION}.objectType = '${LOTypes.POLL}'`);
        from.push(`JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK}
            ON ${TablesListAliases.LEARNING_COMMONTRACK}.idReference = ${TablesListAliases.LEARNING_ORGANIZATION}.idOrg
                AND ${TablesListAliases.LEARNING_COMMONTRACK}.idUser = ${TablesListAliases.CORE_USER}.idst`);

        // Survey Date options filters
        table = `SELECT * FROM ${TablesList.LEARNING_POLLTRACK} WHERE status = 'valid'`;
        if (this.info.surveyCompletionDate) {
            surveyCompletionDateFilter = this.buildDateFilter(TablesListAliases.LEARNING_COMMONTRACK + '.last_complete', this.info.surveyCompletionDate, 'AND', true);
        }
        from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_POLLTRACK}
            ON ${TablesListAliases.LEARNING_POLLTRACK}.id_track = ${TablesListAliases.LEARNING_COMMONTRACK}.idTrack
                AND ${TablesListAliases.LEARNING_COMMONTRACK}.objectType = '${LOTypes.POLL}'
                AND ${TablesListAliases.LEARNING_POLLTRACK}.id_user = ${TablesListAliases.CORE_USER}.idst ${surveyCompletionDateFilter}`);

        // Survey filter
        table = `SELECT * FROM ${TablesList.LEARNING_POLL} WHERE TRUE`;
        if (this.info.surveys && !allSurveys) {
            const surveySelection = this.info.surveys.surveys.map(a => a.id);
            table += ` AND id_poll IN (${surveySelection.join(',')})`;
        }
        from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_POLL} ON ${TablesListAliases.LEARNING_POLL}.id_poll = ${TablesListAliases.LEARNING_POLLTRACK}.id_poll`);

        from.push(`LEFT JOIN ${TablesList.LEARNING_POLLTRACK_ANSWER} AS ${TablesListAliases.LEARNING_POLLTRACK_ANSWER}
            ON ${TablesListAliases.LEARNING_POLLTRACK_ANSWER}.id_track = ${TablesListAliases.LEARNING_POLLTRACK}.id_track`);
        from.push(`JOIN ${TablesList.LEARNING_POLLQUEST_WITH} AS ${TablesListAliases.LEARNING_POLLQUEST}
            ON ${TablesListAliases.LEARNING_POLLQUEST}.id_quest = ${TablesListAliases.LEARNING_POLLTRACK_ANSWER}.id_quest
            AND ${TablesListAliases.LEARNING_POLLQUEST}.type_quest NOT IN ('${LOQuestTypes.TITLE}', '${LOQuestTypes.BREAK_PAGE}')`);

        // core_org_chart_tree table
        from.push(`LEFT JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE} ON ${TablesListAliases.CORE_GROUP}.idst = ${TablesListAliases.CORE_ORG_CHART_TREE}.idst_oc`);

        // core_org_chart table
        from.push(`LEFT JOIN ${TablesList.CORE_ORG_CHART} AS ${TablesListAliases.CORE_ORG_CHART}
            ON ${TablesListAliases.CORE_ORG_CHART_TREE}.idOrg = ${TablesListAliases.CORE_ORG_CHART}.id_dir AND ${TablesListAliases.CORE_ORG_CHART}.lang_code = '${this.session.user.getLang()}'`);

        const courseExtraFields = await this.session.getHydra().getCourseExtraFields();
        this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course');

        // Variables to check if the specified table was already joined in the query
        let joinCourseCategories = false;
        let joinLearningCourseFieldValue = false;
        let joinLearningPollQuestAnswer = false;
        let joinLearningPollLikertScale = false;
        let joinLearningRepositoryObject = false;

        if (this.info.fields) {
            for (const field of this.info.fields) {
                switch (field) {
                    // Group fields
                    case FieldsList.GROUP_GROUP_OR_BRANCH_NAME:
                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.CORE_ORG_CHART_TREE}.idOrg IS NOT NULL
                                    THEN CONCAT(CASE
                                            WHEN ${TablesListAliases.CORE_ORG_CHART_TREE}.code <> ''
                                            THEN CONCAT('(',${TablesListAliases.CORE_ORG_CHART_TREE}.code, ') ')
                                            ELSE '' END, ${TablesListAliases.CORE_ORG_CHART}.translation
                                    )
                                ELSE SUBSTR(${TablesListAliases.CORE_GROUP}.groupid, 2)
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.GROUP_GROUP_OR_BRANCH_NAME])}`);
                        groupBy.push(`${TablesListAliases.CORE_ORG_CHART_TREE}.idOrg`);
                        groupBy.push(`${TablesListAliases.CORE_ORG_CHART_TREE}.code`);
                        groupBy.push(`${TablesListAliases.CORE_ORG_CHART}.translation`);
                        groupBy.push(`${TablesListAliases.CORE_GROUP}.groupid`);
                        break;

                    // Surveys
                    case FieldsList.SURVEY_TITLE:
                        select.push(`${TablesListAliases.LEARNING_POLL}.title AS ${this.renderStringInQuerySelect(translations[FieldsList.SURVEY_TITLE])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_POLL}.title`);
                        break;
                    case FieldsList.SURVEY_ID:
                        select.push(`${TablesListAliases.LEARNING_POLL}.id_poll AS ${this.renderStringInQuerySelect(translations[FieldsList.SURVEY_ID])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_POLL}.id_poll`);
                        break;
                    case FieldsList.SURVEY_DESCRIPTION:
                        select.push(`${TablesListAliases.LEARNING_POLL}.description AS ${this.renderStringInQuerySelect(translations[FieldsList.SURVEY_DESCRIPTION])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_POLL}.description`);
                        break;
                    case FieldsList.SURVEY_TRACKING_TYPE:
                        if (!joinLearningRepositoryObject) {
                            joinLearningRepositoryObject = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_REPOSITORY_OBJECT} AS ${TablesListAliases.LEARNING_REPOSITORY_OBJECT}
                                ON ${TablesListAliases.LEARNING_REPOSITORY_OBJECT}.id_object = ${TablesListAliases.LEARNING_ORGANIZATION}.id_object`);
                        }
                        select.push(`CASE
                            WHEN ${TablesListAliases.LEARNING_REPOSITORY_OBJECT}.shared_tracking > 0
                                THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.SHARED_TRACKING])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.LOCAL_TRACKING])}
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.SURVEY_TRACKING_TYPE])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_REPOSITORY_OBJECT}.shared_tracking`);
                        break;

                    // Survey Question And Answer
                    case FieldsList.SURVEY_COMPLETION_ID:
                        select.push(`${TablesListAliases.LEARNING_POLLTRACK}.id_track AS ${this.renderStringInQuerySelect(translations[FieldsList.SURVEY_COMPLETION_ID])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_POLLTRACK}.id_track`);
                        break;
                    case FieldsList.SURVEY_COMPLETION_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COMMONTRACK}.last_complete AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s')
                            AS ${this.renderStringInQuerySelect(translations[FieldsList.SURVEY_COMPLETION_DATE])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_COMMONTRACK}.last_complete`);
                        break;
                    case FieldsList.QUESTION:
                        select.push(`${TablesListAliases.LEARNING_POLLQUEST}.title_quest AS ${this.renderStringInQuerySelect(translations[FieldsList.QUESTION])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_POLLQUEST}.title_quest`);
                        break;
                    case FieldsList.QUESTION_ID:
                        select.push(`${TablesListAliases.LEARNING_POLLQUEST}.id_quest AS ${this.renderStringInQuerySelect(translations[FieldsList.QUESTION_ID])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_POLLQUEST}.id_quest`);
                        break;
                    case FieldsList.QUESTION_MANDATORY:
                        select.push(`CASE WHEN CAST(${TablesListAliases.LEARNING_POLLQUEST}.mandatory AS INTEGER) > 0
                                        THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                        ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.QUESTION_MANDATORY])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_POLLQUEST}.mandatory`);
                        break;
                    case FieldsList.QUESTION_TYPE:
                        select.push(`CASE
                                    WHEN ${TablesListAliases.LEARNING_POLLQUEST}.type_quest = '${LOQuestTypes.CHOICE}' THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.CHOICE])}
                                    WHEN ${TablesListAliases.LEARNING_POLLQUEST}.type_quest = '${LOQuestTypes.CHOICE_MULTIPLE}' THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.CHOICE_MULTIPLE])}
                                    WHEN ${TablesListAliases.LEARNING_POLLQUEST}.type_quest = '${LOQuestTypes.INLINE_CHOICE}' THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.INLINE_CHOICE])}
                                    WHEN ${TablesListAliases.LEARNING_POLLQUEST}.type_quest = '${LOQuestTypes.EXTENDED_TEXT}' THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.EXTENDED_TEXT])}
                                    WHEN ${TablesListAliases.LEARNING_POLLQUEST}.type_quest = '${LOQuestTypes.LIKERT_SCALE}' THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LIKERT_SCALE])}
                                    ELSE ${TablesListAliases.LEARNING_POLLQUEST}.type_quest
                                END AS ${this.renderStringInQuerySelect(translations[FieldsList.QUESTION_TYPE])}
                            `);
                    groupBy.push(`${TablesListAliases.LEARNING_POLLQUEST}.type_quest`);
                        break;
                    case FieldsList.ANSWER_USER:
                        if (!joinLearningPollQuestAnswer) {
                            joinLearningPollQuestAnswer = true;
                            this.joinLearningPollQuestAnswer(from, LOQuestTypes);
                        }
                        if (!joinLearningPollLikertScale) {
                            joinLearningPollLikertScale = true;
                            this.joinLearningPollLikertScale(from, LOQuestTypes);
                        }
                        select.push(`
                                    CASE
                                        WHEN ${TablesListAliases.LEARNING_POLLQUEST}.type_quest = '${LOQuestTypes.CHOICE}' THEN MAX(${TablesListAliases.LEARNING_POLLQUEST_ANSWER}.answer)
                                        WHEN ${TablesListAliases.LEARNING_POLLQUEST}.type_quest = '${LOQuestTypes.CHOICE_MULTIPLE}' THEN ARRAY_JOIN(ARRAY_AGG(DISTINCT(${TablesListAliases.LEARNING_POLLQUEST_ANSWER}.answer)), ', ')
                                        WHEN ${TablesListAliases.LEARNING_POLLQUEST}.type_quest = '${LOQuestTypes.INLINE_CHOICE}' THEN MAX(${TablesListAliases.LEARNING_POLLQUEST_ANSWER}.answer)
                                        WHEN ${TablesListAliases.LEARNING_POLLQUEST}.type_quest = '${LOQuestTypes.EXTENDED_TEXT}' THEN MAX(${TablesListAliases.LEARNING_POLLTRACK_ANSWER}.more_info)
                                        WHEN ${TablesListAliases.LEARNING_POLLQUEST}.type_quest = '${LOQuestTypes.LIKERT_SCALE}' THEN MAX(${TablesListAliases.LEARNING_POLL_LIKERT_SCALE}.title)
                                        ELSE NULL
                                    END AS ${this.renderStringInQuerySelect(translations[FieldsList.ANSWER_USER])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_POLLQUEST}.type_quest`);
                        break;

                    case FieldsList.COURSE_NAME:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.name AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_NAME])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_COURSE}.name`);
                        break;
                    case FieldsList.COURSE_CATEGORY_NAME:
                        if (!joinCourseCategories) {
                            joinCourseCategories = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_CATEGORY} AS ${TablesListAliases.LEARNING_CATEGORY}
                                ON ${TablesListAliases.LEARNING_CATEGORY}.idCategory = ${TablesListAliases.LEARNING_COURSE}.idCategory
                                AND ${TablesListAliases.LEARNING_CATEGORY}.lang_code = '${this.session.user.getLang()}'`);
                        }
                        select.push(`${TablesListAliases.LEARNING_CATEGORY}.translation AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_NAME])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_CATEGORY}.translation`);
                        break;
                    case FieldsList.COURSE_CATEGORY_CODE:
                        if (!joinCourseCategories) {
                            joinCourseCategories = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_CATEGORY} AS ${TablesListAliases.LEARNING_CATEGORY}
                                ON ${TablesListAliases.LEARNING_CATEGORY}.idCategory = ${TablesListAliases.LEARNING_COURSE}.idCategory
                                AND ${TablesListAliases.LEARNING_CATEGORY}.lang_code = '${this.session.user.getLang()}'`);
                        }
                        select.push(`${TablesListAliases.LEARNING_CATEGORY}.code AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_CODE])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_CATEGORY}.code`);
                        break;
                    case FieldsList.COURSE_CODE:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.code AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CODE])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_COURSE}.code`);
                        break;
                    case FieldsList.COURSE_DURATION:
                        select.push(`${TablesListAliases.LEARNING_COURSE}.mediumTime AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DURATION])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_COURSE}.mediumTime`);
                        break;
                    case FieldsList.COURSE_DATE_END:
                        select.push(`${this.mapDateDefaultValueWithDLV2(`${TablesListAliases.LEARNING_COURSE}.date_end`)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_END])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_COURSE}.date_end`);
                        break;
                    case FieldsList.COURSE_DATE_BEGIN:
                        select.push(`${this.mapDateDefaultValueWithDLV2(`${TablesListAliases.LEARNING_COURSE}.date_begin`)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_BEGIN])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_COURSE}.date_begin`);
                        break;
                    case FieldsList.COURSE_EXPIRED:
                        const dateEndColumnDLV2Fix = `(${this.mapDateDefaultValueWithDLV2(`${TablesListAliases.LEARNING_COURSE}.date_end`)})`;
                        select.push(`
                                CASE
                                    WHEN ${dateEndColumnDLV2Fix} < CURRENT_DATE THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                    ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_EXPIRED])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_COURSE}.date_end`);
                        break;
                    case FieldsList.COURSE_STATUS:
                        select.push(`
                                CASE
                                    WHEN ${TablesListAliases.LEARNING_COURSE}.status = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_STATUS_PREPARATION])}
                                    ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_STATUS_EFFECTIVE])}
                                END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_STATUS])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_COURSE}.status`);
                        break;
                    case FieldsList.COURSE_TYPE:
                        select.push(`
                                CASE
                                    WHEN ${TablesListAliases.LEARNING_COURSE}.course_type = ${athena.renderStringInQueryCase(CourseTypes.Elearning)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_ELEARNING])}
                                    WHEN ${TablesListAliases.LEARNING_COURSE}.course_type = ${athena.renderStringInQueryCase(CourseTypes.Classroom)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_CLASSROOM])}
                                    ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_WEBINAR])}
                                END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_TYPE])}`);
                        groupBy.push(`${TablesListAliases.LEARNING_COURSE}.course_type`);
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
                                                select.push(`${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId} AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                groupBy.push(`${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                select.push(`DATE_FORMAT(${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}, '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                groupBy.push(`${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId} AND ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                groupBy.push(`${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation`);
                                                break;
                                            case AdditionalFieldsTypes.YesNo:
                                                select.push(`
                                                    CASE
                                                        WHEN ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId} = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                                        ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                                    END AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                groupBy.push(`${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}`);
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

        let query = `${this.withLearningPollQuest()}
        SELECT ${select.join(', ')}
            FROM ${from.join(' ')}
            GROUP BY ${[...new Set(groupBy)].join(', ')}`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(select, translations, { user: [], course: courseExtraFields.data.items, userCourse: [], webinar: [], classroom: [] });
        }

        // get the sql like LIMIT for the query
        query += this.getQueryExportLimit(limit);

        return query;

    }

    public async getQuerySnowflake(limit = 0, isPreview: boolean, checkPuVisibility = true, fromSchedule = false): Promise<string> {
        const translations = await this.loadTranslations();
        const allUsers = this.info.users ? this.info.users.all : false;
        const allSurveys = this.info.surveys ? this.info.surveys.all : false;
        let fullUsers = '';
        let fullGroups = '';
        let surveyCompletionDateFilter = '';

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
            groupBy: [
                `${TablesListAliases.CORE_USER}."idst"`,
                `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`,
                `${TablesListAliases.LEARNING_POLLQUEST}."id_quest"`,
            ],
            archivedGroupBy: [],
            courseAdditionalFieldsSelect: [],
            courseAdditionalFieldsFrom: [],
            courseAdditionalFieldsId: [],
            translations,
            checkPuVisibility
        };

        // core_group table
        let table = `SELECT * FROM ${TablesList.CORE_GROUP} WHERE ("hidden" = 'false' OR "groupid" LIKE '/oc|_%' ESCAPE '|')`;

        if (fullGroups !== '') {
            table += ` AND "idst" IN (${fullGroups})`;
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

        // manage the date options - enrollmentDate and completionDate
        table += this.composeDateOptionsFilter();

        queryHelper.from.push(` JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE} ON ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember"`);

        // learning_course table
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

        // survey tables
        queryHelper.from.push(`JOIN ${TablesList.LEARNING_ORGANIZATION} AS ${TablesListAliases.LEARNING_ORGANIZATION} ON ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse" = ${TablesListAliases.LEARNING_ORGANIZATION}."idcourse"
            AND ${TablesListAliases.LEARNING_ORGANIZATION}."objecttype" = '${LOTypes.POLL}'`);
        queryHelper.from.push(`JOIN ${TablesList.LEARNING_COMMONTRACK} AS ${TablesListAliases.LEARNING_COMMONTRACK} ON ${TablesListAliases.LEARNING_COMMONTRACK}."idreference" = ${TablesListAliases.LEARNING_ORGANIZATION}."idorg"
            AND ${TablesListAliases.LEARNING_COMMONTRACK}."iduser" = ${TablesListAliases.CORE_USER}."idst"`);

        table = `SELECT * FROM ${TablesList.LEARNING_POLLTRACK} WHERE "status" = 'valid'`;
        // manage the date options - surveyCompletionDate
        if (this.info.surveyCompletionDate) {
            surveyCompletionDateFilter = this.buildDateFilter(TablesListAliases.LEARNING_COMMONTRACK + '.last_complete', this.info.surveyCompletionDate, 'AND', true);
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_POLLTRACK}
            ON ${TablesListAliases.LEARNING_POLLTRACK}."id_track" = ${TablesListAliases.LEARNING_COMMONTRACK}."idtrack"
            AND ${TablesListAliases.LEARNING_COMMONTRACK}."objecttype" = '${LOTypes.POLL}'
            AND ${TablesListAliases.LEARNING_POLLTRACK}."id_user" = ${TablesListAliases.CORE_USER}."idst" ${surveyCompletionDateFilter}`);

        // learning_poll table
        table = `SELECT * FROM ${TablesList.LEARNING_POLL} WHERE TRUE`;

        if (this.info.surveys && !allSurveys) {
            const surveySelection = this.info.surveys.surveys.map(a => a.id);
            table += ` AND "id_poll" IN (${surveySelection.join(',')})`;
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_POLL} ON ${TablesListAliases.LEARNING_POLL}."id_poll" = ${TablesListAliases.LEARNING_POLLTRACK}."id_poll"`);
        queryHelper.from.push(`LEFT JOIN ${TablesList.LEARNING_POLLTRACK_ANSWER} AS ${TablesListAliases.LEARNING_POLLTRACK_ANSWER} ON ${TablesListAliases.LEARNING_POLLTRACK_ANSWER}."id_track" = ${TablesListAliases.LEARNING_POLLTRACK}."id_track"`);
        queryHelper.from.push(`JOIN ${TablesList.LEARNING_POLLQUEST_WITH} AS ${TablesListAliases.LEARNING_POLLQUEST} ON ${TablesListAliases.LEARNING_POLLQUEST}."id_quest" = ${TablesListAliases.LEARNING_POLLTRACK_ANSWER}."id_quest"
            AND ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" NOT IN ('${LOQuestTypes.TITLE}', '${LOQuestTypes.BREAK_PAGE}')`);

        // core_org_chart_tree table
        queryHelper.from.push(`LEFT JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE} ON ${TablesListAliases.CORE_GROUP}."idst" = ${TablesListAliases.CORE_ORG_CHART_TREE}."idst_oc"`);

        // core_org_chart table
        queryHelper.from.push(`LEFT JOIN ${TablesList.CORE_ORG_CHART} AS ${TablesListAliases.CORE_ORG_CHART} ON ${TablesListAliases.CORE_ORG_CHART_TREE}."idorg" = ${TablesListAliases.CORE_ORG_CHART}."id_dir" AND ${TablesListAliases.CORE_ORG_CHART}."lang_code" = '${this.session.user.getLang()}'`);

        const courseExtraFields = await this.session.getHydra().getCourseExtraFields();
        this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course');

        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged = this.querySelectGroupFields(field, queryHelper) ||
                    this.querySelectSurveyFields(field, queryHelper) ||
                    this.querySelectSurveyQuestionAnswerFields(field, queryHelper) ||
                    this.querySelectCourseFields(field, queryHelper) ||
                    this.queryWithCourseAdditionalFields(field, queryHelper, courseExtraFields);
            }
        }

        const pollQuestFieldsWith = `${TablesList.LEARNING_POLLQUEST_WITH} AS (
            SELECT "id_quest", "id_poll", "id_category", null as "id_answer", "type_quest", "title_quest", "sequence", "page", "mandatory"
            FROM ${TablesList.LEARNING_POLLQUEST}
            WHERE "type_quest" <> '${LOQuestTypes.LIKERT_SCALE}'
            UNION
            SELECT
               ${TablesListAliases.LEARNING_POLLQUEST}."id_quest", ${TablesListAliases.LEARNING_POLLQUEST}."id_poll", ${TablesListAliases.LEARNING_POLLQUEST}."id_category", ${TablesListAliases.LEARNING_POLLQUEST_ANSWER}."id_answer",
               ${TablesListAliases.LEARNING_POLLQUEST}."type_quest", CONCAT(${TablesListAliases.LEARNING_POLLQUEST}."title_quest", ' - ', ${TablesListAliases.LEARNING_POLLQUEST_ANSWER}."answer"),
               ${TablesListAliases.LEARNING_POLLQUEST}."sequence", ${TablesListAliases.LEARNING_POLLQUEST}."page", ${TablesListAliases.LEARNING_POLLQUEST}."mandatory"
            FROM ${TablesList.LEARNING_POLLQUEST} as ${TablesListAliases.LEARNING_POLLQUEST}
            JOIN ${TablesList.LEARNING_POLLQUEST_ANSWER} as ${TablesListAliases.LEARNING_POLLQUEST_ANSWER}
               ON ${TablesListAliases.LEARNING_POLLQUEST_ANSWER}."id_quest" = ${TablesListAliases.LEARNING_POLLQUEST}."id_quest"
               AND ${TablesListAliases.LEARNING_POLLQUEST}."type_quest" = '${LOQuestTypes.LIKERT_SCALE}')`;
        queryHelper.cte.push(pollQuestFieldsWith);
        if (queryHelper.courseAdditionalFieldsId.length > 0) {
            queryHelper.cte.push(this.additionalCourseFieldQueryWith(queryHelper.courseAdditionalFieldsId));
            queryHelper.cte.push(this.additionalFieldQueryWith(queryHelper.courseAdditionalFieldsFrom, queryHelper.courseAdditionalFieldsSelect, queryHelper.courseAdditionalFieldsId, 'id_course', TablesList.LEARNING_COURSE_FIELD_VALUE_WITH, TablesList.COURSES_ADDITIONAL_FIELDS_TRANSLATIONS));
        }

        let query = '';
        query += `
            WITH ${queryHelper.cte.join(', ')}
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
     * @param item The object that describes a sortingOptions
     */
    setSortingOptions(item: SortingOptions): void {
        this.info.sortingOptions = {
            selector: item && item.selector ? item.selector : 'default',
            selectedField: item && item.selectedField ? item.selectedField : FieldsList.SURVEY_COMPLETION_ID,
            orderBy: item && item.orderBy ? item.orderBy : 'desc'
        };
    }

    protected getSortingOptions(): SortingOptions {
        return {
            selector: 'default',
            selectedField: FieldsList.SURVEY_COMPLETION_ID,
            orderBy: 'desc',
        };
    }

    protected getReportDefaultStructure(report: ReportManagerInfo, title: string, platform: string, idUser: number, description: string): ReportManagerInfo {
        const id = v4();
        const date = new Date();

        const tmpFields: string[] = [];
        this.mandatoryFields.group.forEach(element => {
            tmpFields.push(element);
        });
        this.mandatoryFields.survey.forEach(element => {
            tmpFields.push(element);
        });
        this.mandatoryFields.surveyQuestionAnswer.forEach(element => {
            tmpFields.push(element);
        });
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
        report.sortingOptions = this.getSortingOptions();

        // Manage the planning default fields
        report.planning = this.getDefaultPlanningFields();

        // Manage filters default values
        report.users = new ReportManagerInfoUsersFilter();
        report.users.all = true;
        report.surveys = new ReportManagerInfoSurveysFilter();
        report.surveys.all = true;
        report.courses = new ReportManagerInfoCoursesFilter();
        report.courses.all = true;
        report.courseExpirationDate = this.getDefaultCourseExpDate();
        report.enrollmentDate = this.getDefaultDateOptions();
        report.completionDate = this.getDefaultDateOptions();
        report.surveyCompletionDate = this.getDefaultDateOptions();
        report.conditions = DateOptions.CONDITIONS;

        return report;
    }

    parseLegacyReport(legacyReport: LegacyReport, platform: string, visibilityRules: VisibilityRule[]): ReportManagerInfo {
        throw new Exception('Function not implemented');
    }

    /** UTILS */
    private withLearningPollQuest(): string {
        return `WITH ${TablesList.LEARNING_POLLQUEST_WITH} AS (
             SELECT
                 id_quest,
                 id_poll,
                 id_category,
                 null as id_answer,
                 type_quest,
                 title_quest,
                 sequence,
                 page,
                 mandatory
             FROM ${TablesList.LEARNING_POLLQUEST} where type_quest <> '${LOQuestTypes.LIKERT_SCALE}'
             UNION
             SELECT
                ${TablesListAliases.LEARNING_POLLQUEST}.id_quest,
                ${TablesListAliases.LEARNING_POLLQUEST}.id_poll,
                ${TablesListAliases.LEARNING_POLLQUEST}.id_category,
                ${TablesListAliases.LEARNING_POLLQUEST_ANSWER}.id_answer,
                ${TablesListAliases.LEARNING_POLLQUEST}.type_quest,
                CONCAT(${TablesListAliases.LEARNING_POLLQUEST}.title_quest, ' - ', ${TablesListAliases.LEARNING_POLLQUEST_ANSWER}.answer),
                ${TablesListAliases.LEARNING_POLLQUEST}.sequence,
                ${TablesListAliases.LEARNING_POLLQUEST}.page,
                ${TablesListAliases.LEARNING_POLLQUEST}.mandatory
             FROM ${TablesList.LEARNING_POLLQUEST} as ${TablesListAliases.LEARNING_POLLQUEST}
             JOIN ${TablesList.LEARNING_POLLQUEST_ANSWER} as ${TablesListAliases.LEARNING_POLLQUEST_ANSWER}
                ON ${TablesListAliases.LEARNING_POLLQUEST_ANSWER}.id_quest = ${TablesListAliases.LEARNING_POLLQUEST}.id_quest
                AND ${TablesListAliases.LEARNING_POLLQUEST}.type_quest = '${LOQuestTypes.LIKERT_SCALE}')`;
     }
    private joinLearningPollQuestAnswer(from: string[], loQuestTypes: any): void {
        from.push(`LEFT JOIN ${TablesList.LEARNING_POLLQUEST_ANSWER} AS ${TablesListAliases.LEARNING_POLLQUEST_ANSWER}
                ON ${TablesListAliases.LEARNING_POLLQUEST_ANSWER}.id_answer = ${TablesListAliases.LEARNING_POLLTRACK_ANSWER}.id_answer
                AND ${TablesListAliases.LEARNING_POLLQUEST}.type_quest IN ('${loQuestTypes.CHOICE}', '${loQuestTypes.CHOICE_MULTIPLE}', '${loQuestTypes.INLINE_CHOICE}', '${loQuestTypes.LIKERT_SCALE}')`);
    }
    private joinLearningPollLikertScale(from: string[], loQuestTypes: any): void {
        from.push(`LEFT JOIN ${TablesList.LEARNING_POLL_LIKERT_SCALE} AS ${TablesListAliases.LEARNING_POLL_LIKERT_SCALE}
        ON ${TablesListAliases.LEARNING_POLL_LIKERT_SCALE}.id_poll = ${TablesListAliases.LEARNING_POLLTRACK}.id_poll
            AND ${TablesListAliases.LEARNING_POLLQUEST}.type_quest = '${loQuestTypes.LIKERT_SCALE}'
            AND ${TablesListAliases.LEARNING_POLLTRACK_ANSWER}.id_answer = ${TablesListAliases.LEARNING_POLLQUEST}.id_answer
            AND ${TablesListAliases.LEARNING_POLL_LIKERT_SCALE}.id = try_cast(${TablesListAliases.LEARNING_POLLTRACK_ANSWER}.more_info as integer)`);
    }
}
