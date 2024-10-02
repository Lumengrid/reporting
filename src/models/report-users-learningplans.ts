import { v4 } from 'uuid';

import {
    CourseExtraFieldsResponse,
    CourseuserExtraFieldsResponse,
    LearningPlanExtraFieldsResponse,
    UserExtraFieldsResponse
} from '../services/hydra';
import { ReportsTypes } from '../reports/constants/report-types';
import SessionManager from '../services/session/session-manager.session';
import { UserLevels } from '../services/session/user-manager.session';
import { AdditionalFieldsTypes, CourseTypes, CourseuserLevels, joinedTables, UserLevelsGroups } from './base';
import { BaseReportManager } from './base-report-manager';
import { DateOptions, SortingOptions, VisibilityTypes } from './custom-report';
import { LegacyOrderByParsed, LegacyReport, VisibilityRule } from './migration-component';
import {
    FieldsList,
    FieldTranslation,
    ReportAvailablesFields,
    ReportManagerInfo,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility,
    ReportManagerLearningPlansFilter,
    TablesList,
    TablesListAliases,
} from './report-manager';

export class UsersLearningPlansManager extends BaseReportManager {

    reportType = ReportsTypes.USERS_LP;

    allFields = {
        user: [
            FieldsList.USER_USERID,
            FieldsList.USER_LAST_ACCESS_DATE,
            FieldsList.USER_ID,
            FieldsList.USER_DEACTIVATED,
            FieldsList.USER_FIRSTNAME,
            FieldsList.USER_SUSPEND_DATE,
            FieldsList.USER_LASTNAME,
            FieldsList.USER_EXPIRATION,
            FieldsList.USER_FULLNAME,
            FieldsList.USER_EMAIL_VALIDATION_STATUS,
            FieldsList.USER_EMAIL,
            FieldsList.USER_BRANCH_NAME,
            FieldsList.USER_BRANCH_PATH,
            FieldsList.USER_BRANCHES_CODES,
            FieldsList.USER_REGISTER_DATE,
            FieldsList.USER_DIRECT_MANAGER
        ],
        lp: [
            FieldsList.LP_NAME,
            FieldsList.LP_CODE,
            FieldsList.LP_CREDITS
        ],
        lpenrollment: [
            FieldsList.LP_ENROLLMENT_DATE,
            FieldsList.LP_ENROLLMENT_COMPLETION_DATE,
            FieldsList.LP_ENROLLMENT_STATUS,
        ],
        learningPlansStatistics: [],
        course: [],
        courseEnrollments: []
    };

    mandatoryFields = {
        user: [
            FieldsList.USER_USERID
        ],
        lp: [
            FieldsList.LP_NAME
        ],
        lpenrollment: []
    };

    public constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap | undefined) {
        super(session, reportDetails);

        if (session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) {
            this.allFields.lpenrollment.push(FieldsList.LP_ENROLLMENT_START_OF_VALIDITY);
            this.allFields.lpenrollment.push(FieldsList.LP_ENROLLMENT_END_OF_VALIDITY);
            this.allFields.lp = [
                ...this.allFields.lp,
                FieldsList.LP_UUID,
                FieldsList.LP_LAST_EDIT,
                FieldsList.LP_CREATION_DATE,
                FieldsList.LP_DESCRIPTION,
                FieldsList.LP_ASSOCIATED_COURSES,
                FieldsList.LP_MANDATORY_ASSOCIATED_COURSES,
                FieldsList.LP_STATUS,
                FieldsList.LP_LANGUAGE,
            ];
            this.allFields.learningPlansStatistics = [
                ...this.allFields.learningPlansStatistics,
                FieldsList.LP_ENROLLMENT_COMPLETION_PERCENTAGE,
                FieldsList.LP_STAT_PROGRESS_PERCENTAGE_MANDATORY,
                FieldsList.LP_STAT_PROGRESS_PERCENTAGE_OPTIONAL,
                FieldsList.LP_STAT_DURATION,
                FieldsList.LP_STAT_DURATION_MANDATORY,
                FieldsList.LP_STAT_DURATION_OPTIONAL,
            ];
        } else {
            this.allFields.lpenrollment.push(FieldsList.LP_ENROLLMENT_COMPLETION_PERCENTAGE);
        }
        if (this.session.platform.isLearningplansAssignmentTypeActive()) {
            this.allFields.lpenrollment.push(FieldsList.LP_ENROLLMENT_ASSIGNMENT_TYPE);
        }

        if (session.platform.isToggleUsersLearningPlansReportEnhancement()) {
            this.allFields.course = [
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
                FieldsList.LP_COURSE_LANGUAGE,
                FieldsList.COURSE_UNIQUE_ID,
                FieldsList.COURSE_SKILLS,
            ];
            this.allFields.courseEnrollments = [
                FieldsList.COURSE_ENROLLMENT_DATE_INSCR,
                FieldsList.COURSE_ENROLLMENT_DATE_COMPLETE,
                FieldsList.COURSE_ENROLLMENT_DATE_BEGIN_VALIDITY,
                FieldsList.COURSE_ENROLLMENT_DATE_EXPIRE_VALIDITY,
                FieldsList.COURSE_ENROLLMENT_STATUS,
                FieldsList.COURSEUSER_DATE_FIRST_ACCESS,
                FieldsList.COURSEUSER_DATE_LAST_ACCESS,
                FieldsList.COURSEUSER_SCORE_GIVEN,
                FieldsList.COURSEUSER_LEVEL,
            ];
            if (this.session.platform.checkPluginESignatureEnabled()) {
                this.allFields.course.push(FieldsList.COURSE_E_SIGNATURE);
                this.allFields.courseEnrollments.push(FieldsList.COURSE_E_SIGNATURE_HASH);
            }
        }
    }

    async getAvailablesFields(): Promise<ReportAvailablesFields> {
        const result: ReportAvailablesFields = await this.getBaseAvailableFields();

        // User additional fields
        const userExtraFields = await this.getAvailableUserExtraFields();
        result.user.push(...userExtraFields);

        if (this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) {
            // Learning Plan additional fields
            const lpExtraFields = await this.getAvailableLearningPlanExtraFields();
            result.learningPlans.push(...lpExtraFields);
        }

        if (this.session.platform.isToggleUsersLearningPlansReportEnhancement()) {
            const courseExtraFields = await this.getAvailableCourseExtraFields();
            result.course.push(...courseExtraFields);
            const courseuserExtraFields = await this.getAvailableEnrollmentExtraFields();
            result.courseEnrollments.push(...courseuserExtraFields);
        }

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

        // Recover Learning Plans fields
        result.learningPlans = [];
        for (const field of this.allFields.lp) {
            result.learningPlans.push({
                field,
                idLabel: field,
                mandatory: this.mandatoryFields.lp.includes(field),
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        result.learningPlansEnrollments = [];
        for (const field of this.allFields.lpenrollment) {
            result.learningPlansEnrollments.push({
                field,
                idLabel: field,
                mandatory: false,
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        if (this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) {
            result.learningPlansStatistics = [];
            for (const field of this.allFields.learningPlansStatistics) {
                result.learningPlansStatistics.push({
                    field,
                    idLabel: field,
                    mandatory: false,
                    isAdditionalField: false,
                    translation: translations[field]
                });
            }
        }

        if (this.session.platform.isToggleUsersLearningPlansReportEnhancement()) {
            result.course = [];
            for (const field of this.allFields.course) {
                result.course.push({
                    field,
                    idLabel: field,
                    mandatory: false,
                    isAdditionalField: false,
                    translation: translations[field]
                });
            }
            result.courseEnrollments = [];
            for (const field of this.allFields.courseEnrollments) {
                result.courseEnrollments.push({
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

    public async getQuery(limit = 0, isPreview: boolean, checkPuVisibility = true): Promise<string> {
        const translations = await this.loadTranslations();
        const hydra = this.session.getHydra();
        const athena = this.session.getAthena();

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;
        let fullUsers = '';

        let allLPs = this.info.learningPlans ? this.info.learningPlans.all : false;
        let fullLPs: number[] = [];

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilter(checkPuVisibility);
        }

        if (!allLPs) {
            fullLPs = this.info.learningPlans ? this.info.learningPlans.learningPlans.map(a => parseInt(a.id as string, 10)) : [];
        }

        if (this.session.user.getLevel() === UserLevels.POWER_USER && checkPuVisibility === true) {
            const puCourses = await hydra.getPuLPs();
            if (allLPs) {
                allLPs = false;
                fullLPs = puCourses.data;
            } else {
                fullLPs = fullLPs.filter(x => puCourses.data.includes(x));
            }
        }

        const select = [];
        const from = [];

        let table = `SELECT * FROM ${TablesList.LEARNING_COURSEPATH_USER} WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND idUser IN (${fullUsers})`;
        }

        if (!allLPs) {
            if (fullLPs.length > 0) {
                table += ` AND id_path IN (${fullLPs.join(',')})`;
            } else {
                table += ' AND FALSE';
            }
        }

        from.push(`(${table}) AS ${TablesListAliases.LEARNING_COURSEPATH_USER}`);

        table = `SELECT * FROM ${TablesList.LEARNING_COURSEPATH} WHERE TRUE`;

        if (!allLPs) {
            if (fullLPs.length > 0) {
                table += ` AND id_path IN (${fullLPs.join(',')})`;
            } else {
                table += ' AND FALSE';
            }
        }

        from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSEPATH} ON ${TablesListAliases.LEARNING_COURSEPATH}.id_path = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);


        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `valid ${this.getCheckIsValidFieldClause()}` : 'true';

        if (hideExpiredUsers) {
            table += ` AND (expiration IS NULL OR expiration > NOW())`;
        }
        from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.LEARNING_COURSEPATH_USER}.idUser`);


        let where = [
            `AND ${TablesListAliases.CORE_USER}.userid <> '/Anonymous'`
        ];

        // Variables to check if the specified table was already joined in the query
        let joinCoreGroupLevel = false;
        let joinCoreGroupMembers = false;
        let joinCoreUserFieldValue = false;
        let joinLearningCourseFieldValue = false;
        let joinLearningCoursePathFieldValue = false;
        let joinLearningCoursepatCoursesCount = false;
        let joinLearningCoursepathUserCompletedCourses = false;
        let joinCoreUserBranches = false;
        let joinSkillManagersValue = false;
        let joinCoreLangLanguageField = false;
        let joinLearningCoursepathCourses = false;
        let joinLearningCourse = false;
        let joinCourseCategories = false;
        let joinSkillSkills = false;
        let joinSkillSkillsObjects = false;
        let joinLearningCourseuserSign = false;
        let joinLearningCourseuserAggregate = false;

        // Additional Fields
        let translationValue = [];
        let userExtraFields = {data: { items: []} } as UserExtraFieldsResponse;
        let lpExtraFields = {data: { items: []} } as LearningPlanExtraFieldsResponse;
        let courseExtraFields = {data: { items: []} } as CourseExtraFieldsResponse;
        let courseuserExtraFields = {data: [] } as CourseuserExtraFieldsResponse;

        // load the translations of additional fields only if are selected
        if (this.info.fields.find(item => item.includes('user_extrafield_'))) {
            userExtraFields = await this.session.getHydra().getUserExtraFields();
            translationValue = this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');
        }

        if (this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) {
            if (this.info.fields.find(item => item.includes('lp_extrafield_'))) {
                lpExtraFields = await this.session.getHydra().getLearningPlanExtraFields();
                translationValue = this.updateExtraFieldsDuplicated(lpExtraFields.data.items, translations, 'lp', translationValue);
            }
        }

        if (this.session.platform.isToggleUsersLearningPlansReportEnhancement()) {
            if (this.info.fields.find(item => item.includes('course_extrafield_'))) {
                courseExtraFields = await this.session.getHydra().getCourseExtraFields();
                this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course', translationValue);
            }
        }

        if (this.session.platform.isToggleUsersLearningPlansReportEnhancement()) {
            if (this.info.fields.find(item => item.includes('courseuser_extrafield_'))) {
                courseuserExtraFields = await this.session.getHydra().getCourseuserExtraFields();
                this.updateExtraFieldsDuplicated(courseuserExtraFields.data, translations, 'course-user', translationValue);
            }
        }

        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            const tmp = await this.getAdditionalFieldsFilters(userExtraFields);
            if (tmp.length) {
                if (!joinCoreUserFieldValue) {
                    joinCoreUserFieldValue = true;
                    from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = ${TablesListAliases.LEARNING_COURSEPATH_USER}.idUser`);
                }
                where = where.concat(tmp);
            }
        }

        // If new Learning Plan consider only the mandatory courses for the LP status completed
        const { completedCoursesColumn, coursesColumn, lastDateCompleteColumn } = this.switchColumnsForNewLPEnrollmentStatus();

        // LP subscription status filter
        if (this.info.enrollment && (this.info.enrollment.completed || this.info.enrollment.inProgress || this.info.enrollment.notStarted || this.info.enrollment.waitingList)) {
            const statusFilter = [];
            if (this.info.enrollment.notStarted) {
                if (!joinLearningCoursepathUserCompletedCourses) {
                    joinLearningCoursepathUserCompletedCourses = true;
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idUser = ${TablesListAliases.LEARNING_COURSEPATH_USER}.idUser AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idPath = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                }

                statusFilter.push(`${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.completedCourses = 0 AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.startedCourses = 0`);
            }

            if (this.info.enrollment.inProgress) {
                if (!joinLearningCoursepathUserCompletedCourses) {
                    joinLearningCoursepathUserCompletedCourses = true;
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idUser = ${TablesListAliases.LEARNING_COURSEPATH_USER}.idUser AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idPath = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                }
                if (!joinLearningCoursepatCoursesCount) {
                    joinLearningCoursepatCoursesCount = true;
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT} ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.id_path = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                }

                statusFilter.push(`${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.completedCourses < ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.courses AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.startedCourses > 0`);
            }

            if (this.info.enrollment.completed) {
                if (!joinLearningCoursepathUserCompletedCourses) {
                    joinLearningCoursepathUserCompletedCourses = true;
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idUser = ${TablesListAliases.LEARNING_COURSEPATH_USER}.idUser AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idPath = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                }
                if (!joinLearningCoursepatCoursesCount) {
                    joinLearningCoursepatCoursesCount = true;
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT} ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.id_path = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                }

                statusFilter.push(`${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.${completedCoursesColumn} = ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.${coursesColumn}`);
            }

            if (this.info.enrollment.waitingList) {
                statusFilter.push(`${TablesListAliases.LEARNING_COURSEPATH_USER}.waiting ${this.getCheckIsValidFieldClause()}`);
            }

            where.push(`AND (${statusFilter.join(' OR ')})`);
        }

        if (this.session.platform.isToggleUsersLearningPlansReportEnhancement() && this.reportHasAtLeastACourseuserField(this.info.fields)) {
            if (!joinLearningCoursepathCourses) {
                joinLearningCoursepathCourses = true;
                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                                ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_path = ${TablesListAliases.LEARNING_COURSEPATH}.id_path`);
            }
            if (!joinLearningCourse) {
                joinLearningCourse = true;
                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_item`);
            }
            if (!joinLearningCourseuserAggregate) {
                joinLearningCourseuserAggregate = true;
                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEUSER_AGGREGATE} AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}
                            ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse
                            AND ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
            }
        }

        if (this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) {
            if (this.info.completionDate) {
                if (!joinLearningCoursepathUserCompletedCourses) {
                    joinLearningCoursepathUserCompletedCourses = true;
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idUser = ${TablesListAliases.LEARNING_COURSEPATH_USER}.idUser AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idPath = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                }
                if (!joinLearningCoursepatCoursesCount) {
                    joinLearningCoursepatCoursesCount = true;
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT} ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.id_path = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                }
            }
            where.push(this.composeLearningPlanDateOptionsFilter(`${TablesListAliases.LEARNING_COURSEPATH_USER}.date_assign`, `${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.${lastDateCompleteColumn}`, `${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.${completedCoursesColumn} >= ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.${coursesColumn}`));
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
                            if (this.session.user.getLevel() === UserLevels.POWER_USER && checkPuVisibility === true) {
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
                            if (this.session.user.getLevel() === UserLevels.POWER_USER && checkPuVisibility === true) {
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

                    // LP Fields
                    case FieldsList.LP_NAME:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH}.path_name) AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_NAME])}`);
                        break;
                    case FieldsList.LP_CODE:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH}.path_code) AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_CODE])}`);
                        break;
                    case FieldsList.LP_CREDITS:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH}.credits) AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_CREDITS])}`);
                        break;
                    case FieldsList.LP_UUID:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH}.uuid) AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_UUID])}`);
                        break;
                    case FieldsList.LP_LAST_EDIT:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH}.last_update) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_LAST_EDIT])}`);
                        break;
                    case FieldsList.LP_CREATION_DATE:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH}.create_date) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_CREATION_DATE])}`);
                        break;
                    case FieldsList.LP_DESCRIPTION:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH}.path_descr) AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_DESCRIPTION])}`);
                        break;
                    case FieldsList.LP_STATUS:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH}.status) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LP_UNDER_MAINTENANCE])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH}.status) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LP_PUBLISHED])}
                                ELSE CAST (ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH}.status) as varchar)
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_STATUS])}`);
                        break;
                    case FieldsList.LP_LANGUAGE:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        if (!joinCoreLangLanguageField) {
                            joinCoreLangLanguageField = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_LANG_LANGUAGE} AS ${TablesListAliases.CORE_LANG_LANGUAGE} ON ${TablesListAliases.LEARNING_COURSEPATH}.lang_code = ${TablesListAliases.CORE_LANG_LANGUAGE}.lang_code`);
                        }
                    select.push(`ARBITRARY(${TablesListAliases.CORE_LANG_LANGUAGE}.lang_description) AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_LANGUAGE])}`);
                    break;
                    case FieldsList.LP_ASSOCIATED_COURSES:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        if (!joinLearningCoursepathCourses) {
                            joinLearningCoursepathCourses = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                                ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_path = ${TablesListAliases.LEARNING_COURSEPATH}.id_path`);
                        }
                        select.push(`count(${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_item) AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_ASSOCIATED_COURSES])}`);
                        break;
                    case FieldsList.LP_MANDATORY_ASSOCIATED_COURSES:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        if (!joinLearningCoursepathCourses) {
                            joinLearningCoursepathCourses = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                                    ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_path = ${TablesListAliases.LEARNING_COURSEPATH}.id_path`);
                        }
                        select.push(`count( CASE WHEN ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.is_required = 1 THEN ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_item END )
                                        AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_MANDATORY_ASSOCIATED_COURSES])}`);
                        break;

                    // LP subscription fields
                    case FieldsList.LP_ENROLLMENT_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER}.date_assign) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_DATE])}`);
                        break;
                    case FieldsList.LP_ENROLLMENT_COMPLETION_DATE:
                        if (!joinLearningCoursepathUserCompletedCourses) {
                            joinLearningCoursepathUserCompletedCourses = true;
                            from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idUser = ${TablesListAliases.LEARNING_COURSEPATH_USER}.idUser AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idPath = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                        }
                        if (!joinLearningCoursepatCoursesCount) {
                            joinLearningCoursepatCoursesCount = true;
                            from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT} ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.id_path = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                        }
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.${completedCoursesColumn}) = ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.${coursesColumn})
                                THEN DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.${lastDateCompleteColumn}) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s')
                            ELSE ''
                        END AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_COMPLETION_DATE])}`);
                        break;
                    case FieldsList.LP_ENROLLMENT_STATUS:
                        if (!joinLearningCoursepathUserCompletedCourses) {
                            joinLearningCoursepathUserCompletedCourses = true;
                            from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}
                                    ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idUser = ${TablesListAliases.LEARNING_COURSEPATH_USER}.idUser
                                    AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idPath = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                        }
                        if (!joinLearningCoursepatCoursesCount) {
                            joinLearningCoursepatCoursesCount = true;
                            from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}
                                    ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.id_path = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                        }
                        select.push(`${this.querySelectLpEnrollmentStatus(athena, translations)}`);
                        break;
                    case FieldsList.LP_ENROLLMENT_START_OF_VALIDITY:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        const dateBeginValidityColumn = `ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER}.date_begin_validity)`;
                        const dateBeginValidityQuery = `${this.mapTimestampDefaultValueWithDLV2(dateBeginValidityColumn, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_START_OF_VALIDITY])}`;
                        select.push(dateBeginValidityQuery);
                        break;
                    case FieldsList.LP_ENROLLMENT_END_OF_VALIDITY:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        const dateExpireValidityColumn = `ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER}.date_end_validity)`;
                        const dateExpireValidityQuery = `${this.mapTimestampDefaultValueWithDLV2(dateExpireValidityColumn, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_END_OF_VALIDITY])}`;
                        select.push(dateExpireValidityQuery);
                        break;
                    case FieldsList.LP_ENROLLMENT_ASSIGNMENT_TYPE:
                        if (this.session.platform.isLearningplansAssignmentTypeActive()) {
                            select.push(`
                                CASE
                                    WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER}.assignment_type) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.ASSIGNMENT_TYPE_MANDATORY])}
                                    WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER}.assignment_type) = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.ASSIGNMENT_TYPE_REQUIRED])}
                                    WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER}.assignment_type) = 3 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.ASSIGNMENT_TYPE_RECOMMENDED])}
                                    WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER}.assignment_type) = 4 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.ASSIGNMENT_TYPE_OPTIONAL])}
                                    ELSE CAST (ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER}.assignment_type) as varchar)
                                END AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_ASSIGNMENT_TYPE])}`);
                        }
                        break;

                    // LP statistics fields
                    case FieldsList.LP_ENROLLMENT_COMPLETION_PERCENTAGE:
                        if (!joinLearningCoursepathUserCompletedCourses) {
                            joinLearningCoursepathUserCompletedCourses = true;
                            from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}
                                        ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idUser = ${TablesListAliases.LEARNING_COURSEPATH_USER}.idUser
                                        AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idPath = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                        }
                        if (!joinLearningCoursepatCoursesCount) {
                            joinLearningCoursepatCoursesCount = true;
                            from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT} ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.id_path = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                        }
                        select.push(`CASE
                                        WHEN CAST(ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.${coursesColumn}) as INTEGER) = 0 THEN 0
                                        ELSE
                                            CAST((ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.${completedCoursesColumn}) * 100) as INTEGER)
                                            / CAST(ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.${coursesColumn}) as INTEGER )
                                    END
                                    AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_COMPLETION_PERCENTAGE])}`);
                        break;
                    case FieldsList.LP_STAT_PROGRESS_PERCENTAGE_MANDATORY:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        if (!joinLearningCoursepathUserCompletedCourses) {
                            joinLearningCoursepathUserCompletedCourses = true;
                            from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}
                                            ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idUser = ${TablesListAliases.LEARNING_COURSEPATH_USER}.idUser
                                            AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idPath = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                        }
                        if (!joinLearningCoursepatCoursesCount) {
                            joinLearningCoursepatCoursesCount = true;
                            from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT} ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.id_path = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                        }
                        select.push(`CASE
                                        WHEN CAST(ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.coursesMandatory) as INTEGER) = 0 THEN 0
                                        ELSE
                                            CAST((ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.completedCoursesMandatory) * 100) as INTEGER)
                                            / CAST(ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.coursesMandatory) as INTEGER)
                                    END
                                    AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_STAT_PROGRESS_PERCENTAGE_MANDATORY])}`);
                        break;
                    case FieldsList.LP_STAT_PROGRESS_PERCENTAGE_OPTIONAL:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        if (!joinLearningCoursepathUserCompletedCourses) {
                            joinLearningCoursepathUserCompletedCourses = true;
                            from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}
                                                ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idUser = ${TablesListAliases.LEARNING_COURSEPATH_USER}.idUser
                                                AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.idPath = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                        }
                        if (!joinLearningCoursepatCoursesCount) {
                            joinLearningCoursepatCoursesCount = true;
                            from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT} ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.id_path = ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`);
                        }
                        select.push(` CASE
                                        WHEN CAST(ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.coursesOptional) AS INTEGER) = 0 THEN 0
                                        ELSE
                                            CAST((ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.completedCoursesOptional) * 100) as INTEGER)
                                            / CAST (ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.coursesOptional) as INTEGER )
                                      END
                                    AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_STAT_PROGRESS_PERCENTAGE_OPTIONAL])}`);
                        break;
                    case FieldsList.LP_STAT_DURATION:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        if (!joinLearningCoursepathCourses) {
                            joinLearningCoursepathCourses = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                                ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_path = ${TablesListAliases.LEARNING_COURSEPATH}.id_path`);
                        }
                        if (!joinLearningCourse) {
                            joinLearningCourse = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_item`);
                        }
                        select.push(`
                            CASE
                                WHEN SUM(${TablesListAliases.LEARNING_COURSE}.mediumtime) = 0 THEN NULL
                            ELSE
                                CONCAT(
                                    CAST(FLOOR( SUM(${TablesListAliases.LEARNING_COURSE}.mediumtime) / 3600)  AS VARCHAR),
                                    ${athena.renderStringInQueryCase(translations[FieldTranslation.HR])},
                                    ' ',
                                    CAST(FLOOR((SUM(${TablesListAliases.LEARNING_COURSE}.mediumtime) % 3600) / 60) AS VARCHAR),
                                    ${athena.renderStringInQueryCase(translations[FieldTranslation.MIN])}
                                )
                            END
                        AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_STAT_DURATION])}`);
                        break;
                    case FieldsList.LP_STAT_DURATION_MANDATORY:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        if (!joinLearningCoursepathCourses) {
                            joinLearningCoursepathCourses = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                                    ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_path = ${TablesListAliases.LEARNING_COURSEPATH}.id_path`);
                        }
                        if (!joinLearningCourse) {
                            joinLearningCourse = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_item`);
                        }

                        select.push(`${this.querySelectLpStatDuration(athena, translations)}  AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_STAT_DURATION_MANDATORY])}`);
                        break;
                    case FieldsList.LP_STAT_DURATION_OPTIONAL:
                        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                        if (!joinLearningCoursepathCourses) {
                            joinLearningCoursepathCourses = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                                    ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_path = ${TablesListAliases.LEARNING_COURSEPATH}.id_path`);
                        }
                        if (!joinLearningCourse) {
                            joinLearningCourse = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.id_item`);
                        }

                        select.push(`${this.querySelectLpStatDuration(athena, translations, false)}  AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_STAT_DURATION_OPTIONAL])}`);
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
                    case FieldsList.LP_COURSE_LANGUAGE:
                        if (!joinCoreLangLanguageField) {
                            joinCoreLangLanguageField = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_LANG_LANGUAGE} AS ${TablesListAliases.CORE_LANG_LANGUAGE} ON ${TablesListAliases.LEARNING_COURSE}.lang_code = ${TablesListAliases.CORE_LANG_LANGUAGE}.lang_code`);
                        }
                        select.push(`ARBITRARY(${TablesListAliases.CORE_LANG_LANGUAGE}.lang_description) AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_COURSE_LANGUAGE])}`);
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

                    // Courseuser fields
                    case FieldsList.COURSE_ENROLLMENT_DATE_INSCR:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_inscr) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_ENROLLMENT_DATE_INSCR])}`);
                        break;
                    case FieldsList.COURSE_ENROLLMENT_DATE_COMPLETE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_complete) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_ENROLLMENT_DATE_COMPLETE])}`);
                        break;
                    case FieldsList.COURSE_ENROLLMENT_DATE_BEGIN_VALIDITY:
                        const courseuserDateBeginValidityColumn = `ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_begin_validity)`;
                        const courseuserDateBeginValidityQuery = `${this.mapTimestampDefaultValueWithDLV2(courseuserDateBeginValidityColumn, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_ENROLLMENT_DATE_BEGIN_VALIDITY])}`;
                        select.push(courseuserDateBeginValidityQuery);
                        break;
                    case FieldsList.COURSE_ENROLLMENT_DATE_EXPIRE_VALIDITY:
                        const courseuserDateExpireValidityColumn = `ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_expire_validity)`;
                        const courseuserDateExpireValidityQuery = `${this.mapTimestampDefaultValueWithDLV2(courseuserDateExpireValidityColumn, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_ENROLLMENT_DATE_EXPIRE_VALIDITY])}`;
                        select.push(courseuserDateExpireValidityQuery);
                        break;
                    case FieldsList.COURSE_ENROLLMENT_STATUS:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = -1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = -2 OR (
                                    ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.waiting) ${this.getCheckIsValidFieldClause()} 
                                    AND ARBITRARY(${TablesListAliases.LEARNING_COURSE}.course_type) = 'elearning'
                                ) THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 3 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 4 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING])}
                                ELSE CAST(ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) as varchar)
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_ENROLLMENT_STATUS])}`);
                        break;
                    case FieldsList.COURSEUSER_DATE_FIRST_ACCESS:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_first_access) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_FIRST_ACCESS])}`);
                        break;
                    case FieldsList.COURSEUSER_DATE_LAST_ACCESS:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_last_access) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_LAST_ACCESS])}`);
                        break;
                    case FieldsList.COURSEUSER_SCORE_GIVEN:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.score_given) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_SCORE_GIVEN])}`);
                        break;
                    case FieldsList.COURSEUSER_LEVEL:
                        const courseUserLevel = (field: string) => `
                            CASE
                                WHEN ARBITRARY(${field}) = ${CourseuserLevels.Teacher} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TEACHER])}
                                WHEN ARBITRARY(${field}) = ${CourseuserLevels.Tutor} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TUTOR])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_STUDENT])} END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_LEVEL])}`;
                        select.push(courseUserLevel(TablesListAliases.LEARNING_COURSEUSER_AGGREGATE + '.level'));
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
                                            from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = ${TablesListAliases.LEARNING_COURSEPATH_USER}.idUser`);
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
                        } else if (this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement() && this.isLearningPlanExtraField(field)) {

                            const fieldId = parseInt(field.replace('lp_extrafield_', ''), 10);

                            for (const lpField of lpExtraFields.data.items) {
                                if (lpField.id === fieldId) {
                                    if (await this.checkCourseAdditionalFieldInAthena(fieldId) === false) {
                                        const additionalField = this.setAdditionalFieldTranslation(lpField);
                                        select.push(`'' AS ${athena.renderStringInQuerySelect(additionalField)}`);
                                    } else {

                                        if (!joinLearningCoursePathFieldValue) {
                                            joinLearningCoursePathFieldValue = true;
                                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_FIELD_VALUE} AS ${TablesListAliases.LEARNING_COURSEPATH_FIELD_VALUE}
                                                ON ${TablesListAliases.LEARNING_COURSEPATH_FIELD_VALUE}.id_path = ${TablesListAliases.LEARNING_COURSEPATH}.id_path`);
                                        }

                                        switch (lpField.type) {
                                            case AdditionalFieldsTypes.Textarea:
                                            case AdditionalFieldsTypes.Textfield:
                                                select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_FIELD_VALUE}.field_${fieldId}) AS ${athena.renderStringInQuerySelect(lpField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_FIELD_VALUE}.field_${fieldId}), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(lpField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}
                                                            ON ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.LEARNING_COURSEPATH_FIELD_VALUE}.field_${fieldId}
                                                            AND ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation) AS ${athena.renderStringInQuerySelect(lpField.name.value)}`);
                                                break;
                                        }
                                    }
                                    break;
                                }
                            }
                        } else if (this.session.platform.isToggleUsersLearningPlansReportEnhancement() && this.isCourseExtraField(field)) {
                            const fieldId = parseInt(field.replace('course_extrafield_', ''), 10);
                            for (const courseField of courseExtraFields.data.items) {
                                if (courseField.id === fieldId) {
                                    if (await this.checkCourseAdditionalFieldInAthena(fieldId) === false) {
                                        const additionalField = this.setAdditionalFieldTranslation(courseField);
                                        select.push(`'' AS ${athena.renderStringInQuerySelect(additionalField)}`);
                                    } else {
                                        if (!joinLearningCourseFieldValue) {
                                            joinLearningCourseFieldValue = true;
                                            from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEUSER_AGGREGATE} AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
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
                        } else if (this.session.platform.isToggleUsersLearningPlansReportEnhancement() && this.isCourseUserExtraField(field)) {
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
                                                from.push(`LEFT JOIN ${TablesList.LEARNING_ENROLLMENT_FIELDS_DROPDOWN} AS ${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId} ON ${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.id = CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.enrollment_fields, '$.${fieldId}') AS INTEGER)`);
                                                select.push(`IF(ARBITRARY(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.translation) LIKE '%"${this.session.user.getLangCode()}":%', JSON_EXTRACT_SCALAR(ARBITRARY(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.translation), '$["${this.session.user.getLangCode()}"]'), JSON_EXTRACT_SCALAR(ARBITRARY(${TablesListAliases.LEARNING_ENROLLMENT_FIELDS_DROPDOWN}_${fieldId}.translation), '$["${this.session.platform.getDefaultLanguageCode()}"]')) AS ${athena.renderStringInQuerySelect(courseuserField.name)}`);
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
        GROUP BY ${TablesListAliases.LEARNING_COURSEPATH_USER}.idUser, ${TablesListAliases.LEARNING_COURSEPATH_USER}.id_path`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(select, translations, {
                user: userExtraFields.data.items,
                course: [],
                userCourse: this.session.platform.isToggleUsersLearningPlansReportEnhancement() ? courseuserExtraFields.data : [],
                webinar: [],
                classroom: [],
                transcripts: [],
                learningPlan: this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement() ? lpExtraFields.data.items : []
            });
        }

        // get the sql like LIMIT for the query
        query += this.getQueryExportLimit(limit);

        return query;
    }

    private switchColumnsForNewLPEnrollmentStatus(isSnowflake = false): { completedCoursesColumn: string, coursesColumn: string, lastDateCompleteColumn: string} {
        let completedCoursesColumn = 'completedCoursesMandatory';
        let coursesColumn = 'coursesMandatory';
        let lastDateCompleteColumn = 'lastDateMandatoryCourseComplete';

        if (!this.session.platform.isToggleNewLearningPlanManagement() && !this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) {
            completedCoursesColumn = isSnowflake ? 'completedcourses' : 'completedCourses';
            coursesColumn = 'courses';
            lastDateCompleteColumn = isSnowflake ? 'lastdatecomplete' : 'lastDateComplete';
        }

        return {
            completedCoursesColumn,
            coursesColumn,
            lastDateCompleteColumn
        };
    }

    private querySelectLpEnrollmentStatus(athena: any, translations: any): string {
        if (!this.session.platform.isToggleNewLearningPlanManagement() && !this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) {
            return `
                CASE
                    WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER}.waiting ${this.getCheckIsValidFieldClause()}) THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING])}
                    WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.completedCourses) < ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.courses)
                        AND ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.startedCourses) > 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                    WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.completedCourses) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                    ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                END AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_STATUS])}`;
        }

        return `
            CASE
                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER}.waiting ${this.getCheckIsValidFieldClause()}) THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING])}
                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.completedCourses) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.completedCoursesMandatory) = ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.coursesMandatory)
                    THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.completedCourses) < ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}.courses)
                    AND ARBITRARY(${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}.startedCourses) > 0
                THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                ELSE NULL
            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_STATUS])}`;
    }

    private querySelectLpStatDuration(athena: any, translations: any, isMandatory = true): string {
        const isRequired = isMandatory ? 1 : 0;
        return `
            CASE
                WHEN
                    SUM(CASE
                            WHEN ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.is_required = ${isRequired}
                            THEN ${TablesListAliases.LEARNING_COURSE}.mediumtime
                            ELSE 0
                        END
                    ) = 0 THEN NULL
                ELSE
                    CONCAT(
                        CAST(FLOOR(
                            SUM(CASE
                                    WHEN ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.is_required = ${isRequired}
                                    THEN ${TablesListAliases.LEARNING_COURSE}.mediumtime
                                    ELSE 0
                                END
                            ) / 3600
                        ) AS VARCHAR),
                        ${athena.renderStringInQueryCase(translations[FieldTranslation.HR])},
                        ' ',
                        CAST(FLOOR(
                            (SUM(CASE
                                    WHEN ${TablesListAliases.LEARNING_COURSEPATH_COURSES}.is_required = ${isRequired}
                                    THEN ${TablesListAliases.LEARNING_COURSE}.mediumtime
                                    ELSE 0
                                END
                            ) % 3600) / 60
                        ) AS VARCHAR),
                        ${athena.renderStringInQueryCase(translations[FieldTranslation.MIN])}
                    )
                END`;
    }


    public async getQuerySnowflake(limit = 0, isPreview: boolean, checkPuVisibility = true, fromSchedule = false): Promise<string> {
        const translations = await this.loadTranslations();

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;
        let fullUsers = '';

        let allLPs = this.info.learningPlans ? this.info.learningPlans.all : false;
        let fullLPs: number[] = [];
        let lpInCondition = '';

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilterSnowflake(checkPuVisibility);
        }

        if (!allLPs) {
            fullLPs = this.info.learningPlans ? this.info.learningPlans.learningPlans.map(a => parseInt(a.id as string, 10)) : [];
            lpInCondition = fullLPs.join(',');
        }

        if (this.session.user.getLevel() === UserLevels.POWER_USER && checkPuVisibility === true) {
            if (allLPs) {
                allLPs = false;
                lpInCondition = this.getLPSubQuery(this.session.user.getIdUser());
            } else {
                lpInCondition = this.getLPSubQuery(this.session.user.getIdUser(), fullLPs);
            }
        }

        const queryHelper = {
            select: [],
            archivedSelect: [],
            from: [],
            archivedFrom: [],
            join: [],
            cte: [],
            groupBy: [`${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser", ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`],
            archivedGroupBy: [],
            userAdditionalFieldsSelect: [],
            userAdditionalFieldsFrom: [],
            userAdditionalFieldsId: [],
            courseAdditionalFieldsSelect: [],
            courseAdditionalFieldsFrom: [],
            courseAdditionalFieldsId: [],
            lpAdditionalFieldsSelect: [],
            lpAdditionalFieldsFrom: [],
            lpAdditionalFieldsIds: [],
            checkPuVisibility,
            translations,
            newLPEnrollmentStatusColumns: this.switchColumnsForNewLPEnrollmentStatus(true) // If new Learning Plan consider only the mandatory courses for the LP status
        };

        let table = `SELECT * FROM ${TablesList.LEARNING_COURSEPATH_USER} WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND "iduser" IN (${fullUsers})`;
        }

        if (!allLPs) {
            if (lpInCondition !== '') {
                table += ` AND "id_path" IN (${lpInCondition})`;
            } else {
                table += ' AND FALSE';
            }
        }

        queryHelper.from.push(`(${table}) AS ${TablesListAliases.LEARNING_COURSEPATH_USER}`);

        table = `SELECT * FROM ${TablesList.LEARNING_COURSEPATH} WHERE TRUE`;

        if (!allLPs) {
            if (lpInCondition !== '') {
                table += ` AND "id_path" IN (${lpInCondition})`;
            } else {
                table += ' AND FALSE';
            }
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSEPATH} ON ${TablesListAliases.LEARNING_COURSEPATH}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);


        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `"valid" = 1 ` : 'true';

        if (hideExpiredUsers) {
            table += ` AND ("expiration" IS NULL OR "expiration" > current_timestamp())`;
        }
        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser"`);

        const where = [
            `AND ${TablesListAliases.CORE_USER}."userid" <> '/Anonymous'`
        ];

        if (this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) {
            if (this.info.completionDate) {
                if (!queryHelper.join.includes(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES)) {
                    queryHelper.join.push(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES);
                    queryHelper.from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}
                        ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."iduser" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser"
                        AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."idpath" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }

                if (!queryHelper.join.includes(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT)) {
                    queryHelper.join.push(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT);
                    queryHelper.from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}
                        ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
            }
            where.push(this.composeLearningPlanDateOptionsFilter(`
                ${TablesListAliases.LEARNING_COURSEPATH_USER}."date_assign"`,
                `${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."lastdatecomplete"`,
                `${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."completedcourses" >= ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."courses"`));
        }

        // Additional Fields
        let translationValue = [];
        let userExtraFields = { data: { items: [] } } as UserExtraFieldsResponse;
        let lpExtraFields = { data: { items: [] } } as LearningPlanExtraFieldsResponse;
        let courseExtraFields = {data: { items: []} } as CourseExtraFieldsResponse;
        let courseuserExtraFields = {data: [] } as CourseuserExtraFieldsResponse;

        if (this.info.fields.find(item => item.includes('user_extrafield_'))) {
             userExtraFields = await this.session.getHydra().getUserExtraFields();
             translationValue = this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');
        }

        if (this.session.platform.isToggleUsersLearningPlansReportEnhancement()) {
            if (this.info.fields.find(item => item.includes('course_extrafield_'))) {
                courseExtraFields = await this.session.getHydra().getCourseExtraFields();
                translationValue = this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course', translationValue);
            }

            if (this.info.fields.find(item => item.includes('courseuser_extrafield_'))) {
                courseuserExtraFields = await this.session.getHydra().getCourseuserExtraFields();
                translationValue = this.updateExtraFieldsDuplicated(courseuserExtraFields.data, translations, 'course-user', translationValue);
            }
        }

        if (this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) {
            if (this.info.fields.find(item => item.includes('lp_extrafield_'))) {
                lpExtraFields = await this.session.getHydra().getLearningPlanExtraFields();
                this.updateExtraFieldsDuplicated(lpExtraFields.data.items, translations, 'lp', translationValue);
            }
        }

        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            this.getAdditionalUsersFieldsFiltersSnowflake(queryHelper, userExtraFields);
        }

        // If new Learning Plan consider only the mandatory courses for the LP status completed
        const { completedCoursesColumn, coursesColumn, lastDateCompleteColumn} = queryHelper.newLPEnrollmentStatusColumns;

        // LP subscription status filter
        if (this.info.enrollment && (this.info.enrollment.completed || this.info.enrollment.inProgress || this.info.enrollment.notStarted || this.info.enrollment.waitingList)) {
            const statusFilter = [];
            if (this.info.enrollment.notStarted) {
                if (!queryHelper.join.includes(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES)) {
                    queryHelper.join.push(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES);
                    queryHelper.from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."iduser" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser" AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."idpath" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }

                statusFilter.push(`${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."${completedCoursesColumn}" = 0 AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."startedCourses" = 0`);
            }

            if (this.info.enrollment.inProgress) {
                if (!queryHelper.join.includes(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES)) {
                    queryHelper.join.push(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES);
                    queryHelper.from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."iduser" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser" AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."idpath" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
                if (!queryHelper.join.includes(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT)) {
                    queryHelper.join.push(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT);
                    queryHelper.from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT} ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }

                statusFilter.push(`${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."${completedCoursesColumn}" < ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."${coursesColumn}" AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."startedCourses" > 0`);
            }

            if (this.info.enrollment.completed) {
                if (!queryHelper.join.includes(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES)) {
                    queryHelper.join.push(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES);
                    queryHelper.from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."iduser" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser" AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."idpath" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
                if (!queryHelper.join.includes(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT)) {
                    queryHelper.join.push(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT);
                    queryHelper.from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT} ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }

                statusFilter.push(`${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."${completedCoursesColumn}" = ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."${coursesColumn}"`);
            }

            if (this.info.enrollment.waitingList) {
                statusFilter.push(`${TablesListAliases.LEARNING_COURSEPATH_USER}."waiting" = 1 `);
            }

            where.push(`AND (${statusFilter.map(filter => `(${filter})`).join(' OR ')})`);
        }

        if (this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) {
            if (this.info.completionDate) {
                if (!queryHelper.join.includes(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES)) {
                    queryHelper.join.push(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES);
                    queryHelper.from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}
                        ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."iduser" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser"
                        AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."idpath" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
                if (!queryHelper.join.includes(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT)) {
                    queryHelper.join.push(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT);
                    queryHelper.from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}
                            ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
            }
            where.push(this.composeLearningPlanDateOptionsFilter(
                `${TablesListAliases.LEARNING_COURSEPATH_USER}."date_assign"`,
                `${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."${lastDateCompleteColumn}"`,
                `${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."${completedCoursesColumn}" >= ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."${coursesColumn}"`,
                true
            ));
        }

        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged =
                    this.querySelectUserFields(field, queryHelper) ||
                    this.querySelectLPFields(field, queryHelper) ||
                    this.querySelectLPSubscriptionFields(field, queryHelper) ||
                    this.querySelectLPStatisticsFields(field, queryHelper) ||
                    this.queryWithUserAdditionalFields(field, queryHelper, userExtraFields) ||
                    this.queryWithLpAdditionalFields(field, queryHelper, lpExtraFields) ||
                    this.manageSelectCoursesFields(field, queryHelper, courseExtraFields) ||
                    this.manageSelectCourseEnrollmentsFields(field, queryHelper, courseuserExtraFields);
            }
        }

        if (queryHelper.userAdditionalFieldsId.length > 0) {
            queryHelper.cte.push(this.additionalUserFieldQueryWith(queryHelper.userAdditionalFieldsId));
            queryHelper.cte.push(this.additionalFieldQueryWith(queryHelper.userAdditionalFieldsFrom, queryHelper.userAdditionalFieldsSelect, queryHelper.userAdditionalFieldsId, 'id_user', TablesList.CORE_USER_FIELD_VALUE_WITH, TablesList.USERS_ADDITIONAL_FIELDS_TRANSLATIONS));
        }
        if (this.session.platform.isToggleUsersLearningPlansReportEnhancement() && queryHelper.courseAdditionalFieldsId.length > 0) {
            queryHelper.cte.push(this.additionalCourseFieldQueryWith(queryHelper.courseAdditionalFieldsId));
            queryHelper.cte.push(this.additionalFieldQueryWith(
                queryHelper.courseAdditionalFieldsFrom,
                queryHelper.courseAdditionalFieldsSelect,
                queryHelper.courseAdditionalFieldsId,
                'id_course',
                TablesList.LEARNING_COURSE_FIELD_VALUE_WITH,
                TablesList.COURSES_ADDITIONAL_FIELDS_TRANSLATIONS)
            )
        }

        if (this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement() && queryHelper.lpAdditionalFieldsIds.length > 0) {
            queryHelper.cte.push(this.additionalLpFieldQueryWith(queryHelper.lpAdditionalFieldsIds));
            queryHelper.cte.push(this.additionalFieldQueryWith(
                queryHelper.lpAdditionalFieldsFrom,
                queryHelper.lpAdditionalFieldsSelect,
                queryHelper.lpAdditionalFieldsIds,
                'id_path',
                TablesList.LEARNING_COURSEPATH_FIELD_VALUE_WITH,
                TablesList.LEARNING_PLAN_ADDITIONAL_FIELDS_TRANSLATIONS)
            );
        }

        let query = `
        ${queryHelper.cte.length > 0 ? `WITH ${queryHelper.cte.join(', ')}` : ''}
        SELECT ${queryHelper.select.join(', ')}
        FROM ${queryHelper.from.join(' ')}
        ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}
        GROUP BY ${queryHelper.groupBy.join(', ')}`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(queryHelper.select, translations, {
                user: userExtraFields.data.items,
                course: this.session.platform.isToggleUsersLearningPlansReportEnhancement() ? courseExtraFields.data.items : [],
                userCourse: this.session.platform.isToggleUsersLearningPlansReportEnhancement() ? courseuserExtraFields.data : [],
                webinar: [],
                classroom: [],
                transcripts: [],
                learningPlan: this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement() ? lpExtraFields.data.items : []
            }, fromSchedule);
        }

        // get the sql like LIMIT for the query
        query += this.getQueryExportLimit(limit);

        return query;
    }

    private querySelectLPSubscriptionFields(field: string, queryHelper: any): boolean {

        const {select, from, join, groupBy, translations,  newLPEnrollmentStatusColumns : {completedCoursesColumn, coursesColumn, lastDateCompleteColumn} } = queryHelper;

        switch (field) {
            case FieldsList.LP_ENROLLMENT_DATE:
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEPATH_USER}."date_assign"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_DATE])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH_USER}."date_assign"`);
                break;
            case FieldsList.LP_ENROLLMENT_COMPLETION_DATE:
                if (!join.includes(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES)) {
                    join.push(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES);
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."iduser" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser" AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."idpath" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
                if (!join.includes(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT)) {
                    join.push(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT);
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT} ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
                select.push(`
                            CASE
                                WHEN ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."${completedCoursesColumn}" < ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."${coursesColumn}" THEN ''
                                ELSE ${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."${lastDateCompleteColumn}"`)}
                            END AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_COMPLETION_DATE])}`);
                groupBy.push(`${this.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_COMPLETION_DATE])}`);
                break;
            case FieldsList.LP_ENROLLMENT_STATUS:
                if (!join.includes(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES)) {
                    join.push(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES);
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."iduser" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser" AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."idpath" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
                if (!join.includes(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT)) {
                    join.push(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT);
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT} ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
                select.push(`${this.querySelectLpEnrollmentStatusSnowflake(translations)}`);
                groupBy.push(`${this.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_STATUS])}`);
                break;
            case FieldsList.LP_ENROLLMENT_COMPLETION_PERCENTAGE:
                if (!join.includes(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES)) {
                    join.push(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES);
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."iduser" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser" AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."idpath" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
                if (!join.includes(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT)) {
                    join.push(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT);
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT} ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
                select.push(`TRUNCATE(CASE
                                WHEN CAST(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."${coursesColumn}" AS INTEGER) = 0 THEN 0
                                ELSE
                                    CAST((${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."${completedCoursesColumn}" * 100) as INTEGER)
                                    / CAST(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."${coursesColumn}" as INTEGER)
                            END)
                        AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_COMPLETION_PERCENTAGE])}`);
                groupBy.push(`${this.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_COMPLETION_PERCENTAGE])}`);
                break;
            case FieldsList.LP_ENROLLMENT_START_OF_VALIDITY:
                if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEPATH_USER}."date_begin_validity"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_START_OF_VALIDITY])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH_USER}."date_begin_validity"`);
                break;
            case FieldsList.LP_ENROLLMENT_END_OF_VALIDITY:
                if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) break;
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.LEARNING_COURSEPATH_USER}."date_end_validity"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_END_OF_VALIDITY])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH_USER}."date_end_validity"`);
                break;
            case FieldsList.LP_ENROLLMENT_ASSIGNMENT_TYPE:
                if (this.session.platform.isLearningplansAssignmentTypeActive()) {
                    select.push(`
                                CASE
                                    WHEN ${TablesListAliases.LEARNING_COURSEPATH_USER}."assignment_type" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.ASSIGNMENT_TYPE_MANDATORY])}
                                    WHEN ${TablesListAliases.LEARNING_COURSEPATH_USER}."assignment_type" = 2 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.ASSIGNMENT_TYPE_REQUIRED])}
                                    WHEN ${TablesListAliases.LEARNING_COURSEPATH_USER}."assignment_type" = 3 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.ASSIGNMENT_TYPE_RECOMMENDED])}
                                    WHEN ${TablesListAliases.LEARNING_COURSEPATH_USER}."assignment_type" = 4 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.ASSIGNMENT_TYPE_OPTIONAL])}
                                    ELSE CAST (${TablesListAliases.LEARNING_COURSEPATH_USER}."assignment_type" as varchar)
                                END AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_ASSIGNMENT_TYPE])}`);
                    groupBy.push(`${this.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_ASSIGNMENT_TYPE])}`);
                }
                break;
            default:
                return false;
        }
        return true;
    }

    private querySelectLPStatisticsFields(field: string, queryHelper: any): boolean {
        if (!this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) return false;

        const { select, from, join, groupBy, translations } = queryHelper;

        switch (field) {
            case FieldsList.LP_STAT_PROGRESS_PERCENTAGE_MANDATORY:

                if (!joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES) {
                    join.push(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES);
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}
                                    ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."iduser" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser"
                                    AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."idpath" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
                if (!joinedTables.LEARNING_COURSEPATH_COURSES_COUNT) {
                    join.push(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT);
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}
                                ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
                select.push(`TRUNCATE(CASE
                                WHEN CAST(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."coursesMandatory" as INTEGER) = 0 THEN 0
                                ELSE
                                    CAST((${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."completedCoursesMandatory" * 100) as INTEGER)
                                    / CAST(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."coursesMandatory" as INTEGER)
                             END)
                        AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_STAT_PROGRESS_PERCENTAGE_MANDATORY])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."coursesMandatory"`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."completedCoursesMandatory"`);
                break;

            case FieldsList.LP_STAT_PROGRESS_PERCENTAGE_OPTIONAL:
                if (!joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES) {
                    join.push(joinedTables.LEARNING_COURSEPATH_USER_COMPLETED_COURSES);
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_USER_COMPLETED_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}
                                    ON ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."iduser" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser"
                                    AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."idpath" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
                if (!joinedTables.LEARNING_COURSEPATH_COURSES_COUNT) {
                    join.push(joinedTables.LEARNING_COURSEPATH_COURSES_COUNT);
                    from.push(`JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}
                                ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
                }
                select.push(`TRUNCATE(CASE
                                WHEN CAST(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."coursesOptional" AS INTEGER) = 0 THEN 0
                                ELSE
                                    CAST((${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."completedCoursesOptional" * 100) as INTEGER)
                                    / CAST(${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."coursesOptional" as INTEGER )
                            END)
                        AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_STAT_PROGRESS_PERCENTAGE_OPTIONAL])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."coursesOptional"`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."completedCoursesOptional"`);
                break;

            case FieldsList.LP_STAT_DURATION:
                if (!join.includes(joinedTables.LEARNING_COURSEPATH_COURSES)) {
                    join.push(joinedTables.LEARNING_COURSEPATH_COURSES);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                                ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH}."id_path"`);
                }
                if (!join.includes(joinedTables.LEARNING_COURSE)) {
                    join.push(joinedTables.LEARNING_COURSE);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE}
                                ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_item"`);
                }
                select.push(`
                    CASE
                        WHEN SUM(${TablesListAliases.LEARNING_COURSE}."mediumtime") = 0 THEN NULL
                        ELSE
                            CONCAT(
                                CAST(FLOOR(SUM(${TablesListAliases.LEARNING_COURSE}."mediumtime") / 3600) AS VARCHAR),
                                ${this.renderStringInQueryCase(translations[FieldTranslation.HR])},
                                ' ',
                                CAST(FLOOR((SUM(${TablesListAliases.LEARNING_COURSE}."mediumtime") % 3600) / 60) AS VARCHAR),
                                ${this.renderStringInQueryCase(translations[FieldTranslation.MIN])}
                            )
                        END
                    AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_STAT_DURATION])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."mediumtime"`);
                break;

            case FieldsList.LP_STAT_DURATION_MANDATORY:
                if (!join.includes(joinedTables.LEARNING_COURSEPATH_COURSES)) {
                    join.push(joinedTables.LEARNING_COURSEPATH_COURSES);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                                ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH}."id_path"`);
                }
                if (!join.includes(joinedTables.LEARNING_COURSE)) {
                    join.push(joinedTables.LEARNING_COURSE);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE}
                                ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_item"`);
                }
                select.push(`${this.querySnowflakeSelectLpStatDuration(translations)} AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_STAT_DURATION_MANDATORY])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH_COURSES}."is_required"`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."mediumtime"`);
                break;

            case FieldsList.LP_STAT_DURATION_OPTIONAL:
                if (!join.includes(joinedTables.LEARNING_COURSEPATH_COURSES)) {
                    join.push(joinedTables.LEARNING_COURSEPATH_COURSES);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                                    ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH}."id_path"`);
                }
                if (!join.includes(joinedTables.LEARNING_COURSE)) {
                    join.push(joinedTables.LEARNING_COURSE);
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE}
                                    ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_item"`);
                }
                select.push(`${this.querySnowflakeSelectLpStatDuration(translations, false)}  AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_STAT_DURATION_OPTIONAL])}`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSEPATH_COURSES}."is_required"`);
                groupBy.push(`${TablesListAliases.LEARNING_COURSE}."mediumtime"`);
                break;

            default:
                return false;
        }

    }

    private querySnowflakeSelectLpStatDuration(translations: any, isMandatory = true): string {
        const isRequired = isMandatory ? 1 : 0;
        return `
            CASE
                WHEN
                    SUM(CASE
                            WHEN ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."is_required" = ${isRequired}
                            THEN ${TablesListAliases.LEARNING_COURSE}."mediumtime"
                            ELSE 0
                        END
                    ) = 0 THEN NULL
                ELSE
                    CONCAT(
                        CAST(FLOOR(
                            SUM(CASE
                                    WHEN ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."is_required" = ${isRequired}
                                    THEN ${TablesListAliases.LEARNING_COURSE}."mediumtime"
                                    ELSE 0
                                END
                            ) / 3600
                        ) AS VARCHAR),
                        ${this.renderStringInQueryCase(translations[FieldTranslation.HR])},
                        ' ',
                        CAST(FLOOR(
                            (SUM(CASE
                                    WHEN ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."is_required" = ${isRequired}
                                    THEN ${TablesListAliases.LEARNING_COURSE}."mediumtime"
                                    ELSE 0
                                END
                            ) % 3600) / 60
                        ) AS VARCHAR),
                        ${this.renderStringInQueryCase(translations[FieldTranslation.MIN])}
                    )
                END`;
    }

    private querySelectLpEnrollmentStatusSnowflake(translations: any): string {
        if (!this.session.platform.isToggleNewLearningPlanManagement() && !this.session.platform.isToggleNewLearningPlanManagementAndReportEnhancement()) {
            return `
                CASE
                    WHEN ${TablesListAliases.LEARNING_COURSEPATH_USER}."waiting" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING])}
                    WHEN ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."completedcourses" < ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."courses"
                        AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."startedCourses" > 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                    WHEN ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."completedcourses" = 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                    ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                END AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_STATUS])}`;
        }

        return `
            CASE
                WHEN ${TablesListAliases.LEARNING_COURSEPATH_USER}."waiting" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING])}
                WHEN ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."startedCourses" = 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                WHEN ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."completedCoursesMandatory" = ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."coursesMandatory"
                    THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                WHEN ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."completedcourses" < ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."courses"
                    AND ${TablesListAliases.LEARNING_COURSEPATH_USER_COMPLETED_COURSES}."startedCourses" > 0
                THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                ELSE NULL
            END AS ${this.renderStringInQuerySelect(translations[FieldsList.LP_ENROLLMENT_STATUS])}`;
    }

    private manageSelectCoursesFields(field: any, queryHelper: any, courseExtraFields: CourseExtraFieldsResponse): boolean {
        if (!this.session.platform.isToggleUsersLearningPlansReportEnhancement()) {
            return false;
        }
        return (
            this.querySelectCourseFields(field, queryHelper) ||
            this.queryWithCourseAdditionalFields(field, queryHelper, courseExtraFields)
        );
    }

    private manageSelectCourseEnrollmentsFields(field: any, queryHelper: any, courseuserExtraFields: any): boolean {
        if (!this.session.platform.isToggleUsersLearningPlansReportEnhancement()) {
            return false;
        }
        return (
             this.querySelectEnrollmentFields(field, queryHelper) ||
             this.queryWithCourseUserAdditionalFields(field, queryHelper, courseuserExtraFields)
        );
    }

    protected getReportDefaultStructure(report: ReportManagerInfo, title: string, platform: string, idUser: number, description: string): ReportManagerInfo {
        const id = v4();
        const date = new Date();

        const tmpFields: string[] = [];
        this.mandatoryFields.user.forEach(element => {
            tmpFields.push(element);
        });
        this.mandatoryFields.lp.forEach(element => {
            tmpFields.push(element);
        });
        this.mandatoryFields.lpenrollment.forEach(element => {
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

        report.learningPlans = new ReportManagerLearningPlansFilter();
        report.learningPlans.all = true;

        // manage the planning default fields
        report.planning = this.getDefaultPlanningFields();
        report.conditions = DateOptions.CONDITIONS;
        report.enrollmentDate = this.getDefaultDateOptions();
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
         * LEARNING PLANS IMPORT
         */
        this.legacyLearningPlansImport(filterData, report, legacyReport.id_filter);

        // check filters section validity
        if (!filterData.filters) {
            this.logger.error(`No legacy filters section for id report: ${legacyReport.id_filter}`);
            throw new Error('No legacy filters section');
        }

        const filters = filterData.filters;
        // Enrollment Status
        this.extractLegacyEnrollmentStatus(filters, report);
        // map selected fields and manage order by
        if (filterData.fields) {
            let legacyOrderField: LegacyOrderByParsed | undefined;
            const userMandatoryFieldsMap = this.mandatoryFields.user.reduce((previousValue: { [key: string]: string }, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});
            const lpMandatoryFieldsMap = this.mandatoryFields.lp.reduce((previousValue: { [key: string]: string }, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});
            // user fields and order by
            const userFieldsDescriptor = this.mapUserSelectedFields(filterData.fields.user, filterData.order, userMandatoryFieldsMap);
            report.fields.push(...userFieldsDescriptor.fields);
            if (userFieldsDescriptor.orderByDescriptor) legacyOrderField = userFieldsDescriptor.orderByDescriptor;

            // lp fields
            const lpFieldsDescriptor = this.mapLearningPlanSelectedFields(filterData.fields.coursepaths, filterData.order, lpMandatoryFieldsMap);
            report.fields.push(...lpFieldsDescriptor.fields);
            if (lpFieldsDescriptor.orderByDescriptor) legacyOrderField = lpFieldsDescriptor.orderByDescriptor;

            // lp enrollment fields
            const lpEnrollmentFieldsDescriptor = this.mapLearningPlanEnrollmentSelectedFields(filterData.fields.plansUsers, filterData.order);
            report.fields.push(...lpEnrollmentFieldsDescriptor.fields);
            if (lpEnrollmentFieldsDescriptor.orderByDescriptor) legacyOrderField = lpEnrollmentFieldsDescriptor.orderByDescriptor;

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

    private reportHasAtLeastACourseuserField(fields: string[]): boolean {
        const courseuserFields: string[] = [
            FieldsList.COURSE_ENROLLMENT_DATE_INSCR,
            FieldsList.COURSE_ENROLLMENT_DATE_COMPLETE,
            FieldsList.COURSE_ENROLLMENT_DATE_BEGIN_VALIDITY,
            FieldsList.COURSE_ENROLLMENT_DATE_EXPIRE_VALIDITY,
            FieldsList.COURSE_ENROLLMENT_STATUS,
            FieldsList.COURSEUSER_DATE_FIRST_ACCESS,
            FieldsList.COURSEUSER_DATE_LAST_ACCESS,
            FieldsList.COURSEUSER_SCORE_GIVEN,
            FieldsList.COURSEUSER_LEVEL,
            FieldsList.COURSE_E_SIGNATURE_HASH
        ];
        return fields.find((field) => courseuserFields.includes(field)) ? true : false;
    }
}
