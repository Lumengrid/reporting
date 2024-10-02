import { v4 } from 'uuid';

import { ReportsTypes } from '../reports/constants/report-types';
import { Utils } from '../reports/utils';
import SessionManager from '../services/session/session-manager.session';
import { UserLevels } from '../services/session/user-manager.session';
import { AdditionalFieldsTypes, AttendancesTypes, CourseTypes, CourseuserLevels, EnrollmentStatuses, UserLevelsGroups, SessionEvaluationStatus, joinedTables } from './base';
import { DateOptions, DateOptionsValueDescriptor, EnrollmentTypes, SessionDates, SortingOptions, VisibilityTypes } from './custom-report';
import { LegacyOrderByParsed, LegacyReport, VisibilityRule } from './migration-component';
import {
    FieldsList,
    FieldTranslation,
    ReportAvailablesFields,
    ReportManagerInfo,
    ReportManagerInfoCoursesFilter,
    ReportManagerInfoSessionsFilter,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility,
    ReportManagerLearningPlansFilter,
    TablesList,
    TablesListAliases,
} from './report-manager';
import { CourseExtraFieldsResponse, UserExtraFieldsResponse } from '../services/hydra';
import { BaseReportManager } from './base-report-manager';

export class UsersClassroomSessionsManager extends BaseReportManager {
    reportType = ReportsTypes.USERS_CLASSROOM_SESSIONS;

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
            FieldsList.USER_DIRECT_MANAGER
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
        session: [
            FieldsList.SESSION_NAME,
            FieldsList.SESSION_CODE,
            FieldsList.SESSION_END_DATE,
            FieldsList.SESSION_EVALUATION_SCORE_BASE,
            FieldsList.SESSION_TIME_SESSION,
            FieldsList.SESSION_START_DATE,
            FieldsList.SESSION_UNIQUE_ID,
            FieldsList.WEBINAR_SESSION_WEBINAR_TOOL,
            FieldsList.WEBINAR_SESSION_TOOL_TIME_IN_SESSION,
            FieldsList.SESSION_INSTRUCTOR_USERIDS,
            FieldsList.SESSION_INSTRUCTOR_FULLNAMES,
            FieldsList.SESSION_ATTENDANCE_TYPE,
            FieldsList.SESSION_MINIMUM_ENROLLMENTS,
            FieldsList.SESSION_MAXIMUM_ENROLLMENTS,
        ],
        event: [
            FieldsList.SESSION_EVENT_NAME,
            FieldsList.SESSION_EVENT_ID,
            FieldsList.SESSION_EVENT_DATE,
            FieldsList.SESSION_EVENT_START_DATE,
            FieldsList.SESSION_EVENT_DURATION,
            FieldsList.SESSION_EVENT_TIMEZONE,
            FieldsList.SESSION_EVENT_TYPE,
            FieldsList.SESSION_EVENT_INSTRUCTOR_USER_NAME,
            FieldsList.SESSION_EVENT_INSTRUCTOR_FULLNAME,
        ],
        enrollment: [
            FieldsList.ENROLLMENT_ATTENDANCE,
            FieldsList.ENROLLMENT_DATE,
            FieldsList.ENROLLMENT_ENROLLMENT_STATUS,
            FieldsList.ENROLLMENT_EVALUATION_STATUS,
            FieldsList.ENROLLMENT_INSTRUCTOR_FEEDBACK,
            FieldsList.ENROLLMENT_LEARNER_EVALUATION,
            FieldsList.ENROLLMENT_USER_COURSE_LEVEL,
            FieldsList.ENROLLMENT_USER_SESSION_STATUS,
            FieldsList.ENROLLMENT_USER_SESSION_SUBSCRIBE_DATE,
            FieldsList.ENROLLMENT_USER_SESSION_COMPLETE_DATE,
            FieldsList.COURSEUSER_DATE_COMPLETE,
            FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_HOURS,
            FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS
        ],
    };
    mandatoryFields = {
        user : [
            FieldsList.USER_USERID
        ],
        course: [
            FieldsList.COURSE_NAME,
        ]
    };

    public constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap | undefined) {
        super(session, reportDetails);

        if (this.session.platform.checkPluginESignatureEnabled()) {
            this.allFields.course.push(FieldsList.COURSE_E_SIGNATURE);
            this.allFields.enrollment.push(FieldsList.COURSE_E_SIGNATURE_HASH);
        }

        if (session.platform.isToggleMultipleEnrollmentCompletions()) {
            this.allFields.enrollment.push(FieldsList.ENROLLMENT_ARCHIVED);
            this.allFields.enrollment.push(FieldsList.ENROLLMENT_ARCHIVING_DATE);
        }

        if (this.session.platform.isCoursesAssignmentTypeActive()) {
            this.allFields.enrollment.push(FieldsList.COURSEUSER_ASSIGNMENT_TYPE);
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

        // Courses (Classroom)
        report.courses = new ReportManagerInfoCoursesFilter();
        report.courses.all = true;
        report.courseExpirationDate = this.getDefaultCourseExpDate();
        report.learningPlans = new ReportManagerLearningPlansFilter();
        report.learningPlans.all = true;

        // Sessions
        report.sessions = new ReportManagerInfoSessionsFilter();
        report.sessions.all = true;
        report.sessions.entitiesLimits = this.session.platform.getEntitiesLimits().classrooms.sessionLimit;

        // Date options
        report.enrollmentDate = this.getDefaultDateOptions();
        report.completionDate = this.getDefaultDateOptions();
        report.conditions = DateOptions.CONDITIONS;
        report.sessionDates = this.getDefaultSessionDates();
        report.sessionAttendanceType = this.getDefaultSessionAttendanceType();
        if (this.session.platform.isToggleMultipleEnrollmentCompletions()) {
            report.archivingDate = this.getDefaultDateOptions();
        }

        // Enrollments
        report.enrollment = this.getDefaultEnrollment();


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
        const result: ReportAvailablesFields = await this.getBaseAvailableFields();

        const userExtraFields = await this.getAvailableUserExtraFields();
        result.user.push(...userExtraFields);

        const courseExtraFields = await this.getAvailableCourseExtraFields();
        result.course.push(...courseExtraFields);

        const sessionExtraFields = await this.getAvailableILTExtraFields();
        result.session.push(...sessionExtraFields);

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

        // Session fields
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

        // Event fields
        result.event = [];
        for (const field of this.allFields.event) {
            result.event.push({
                field,
                idLabel: field,
                mandatory: false,
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        // Enrollment fields
        result.enrollment = [];
        for (const field of this.allFields.enrollment) {
            result.enrollment.push({
                field,
                idLabel: field,
                mandatory: false,
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        return result;
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

    public async getQuery(limit = 0, isPreview: boolean, checkPuVisibility = true): Promise<string> {
        const translations = await this.loadTranslations();
        const athena = this.session.getAthena();

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;

        const archivedWhere = [];

        let fullUsers = '';

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilter(checkPuVisibility);
        }

        const fullCourses = await this.calculateCourseFilter(false, true, checkPuVisibility);
        const instructorSelection = typeof this.info.courses !== 'undefined' && typeof this.info.courses.instructors !== 'undefined' ? this.info.courses.instructors.map(a => a.id) : [];

        const select: string[] = [];
        const from: string[] = [];

        const archivedSelect: string[] = [];
        const archivedFrom: string[] = [];

        archivedFrom.push(`${TablesList.ARCHIVED_ENROLLMENT_COURSE} AS ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}`);
        archivedFrom.push(`JOIN ${TablesList.ARCHIVED_ENROLLMENT_SESSION} AS ${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION} ON ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.id = ${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.id_archived_enrollment_course`);

        let table = `SELECT * FROM ${TablesList.LEARNING_COURSEUSER_AGGREGATE} WHERE TRUE`;
        if (fullUsers !== '') {
            table += ` AND idUser IN (${fullUsers})`;
            archivedWhere.push(` AND ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.user_id IN (${fullUsers})`);
        }

        if (fullCourses !== '') {
            table += ` AND idCourse IN (${fullCourses})`;
            archivedWhere.push(` AND ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_id IN (${fullCourses})`);
        }

        // manage the date options - enrollmentDate and completionDate
        table += this.composeDateOptionsFilter();
        archivedWhere.push(this.composeDateOptionsWithArchivedEnrollmentFilter(
            `${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_enrolled_at`,
            `${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_completed_at`,
            'created_at')
        );

        from.push(`(${table}) AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}`);


        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `valid ${this.getCheckIsValidFieldClause()}` : 'true';

        if (hideExpiredUsers) {
            table += ` AND (expiration IS NULL OR expiration > NOW())`;
        }
        from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);


        // JOIN LEARNING COURSE
        table = `SELECT * FROM ${TablesList.LEARNING_COURSE} WHERE course_type = 'classroom' AND TRUE`;

        if (fullCourses !== '') {
            table += ` AND idCourse IN (${fullCourses})`;
        }
        if (this.info.courseExpirationDate) {
            table += this.buildDateFilter('date_end', this.info.courseExpirationDate, 'AND', true);
            archivedWhere.push(this.buildDateFilter(`DATE_PARSE(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.end_at'), '%Y-%m-%d %H:%i:%s')`, this.info.courseExpirationDate, 'AND', true));
        }

        from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse`);

        table = `SELECT * FROM ${TablesList.LT_COURSEUSER_SESSION_DETAILS} WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND id_user IN (${fullUsers})`;
        }

        if (fullCourses !== '') {
            table += ` AND course_id IN (${fullCourses})`;
        }

        if (instructorSelection.length > 0) {
            table += ` AND id_session IN (SELECT id_session FROM ${TablesList.LT_COURSE_SESSION_INSTRUCTOR} WHERE id_user IN (${instructorSelection.join(',')}))`;
            archivedWhere.push(`AND ${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.session_id IN (SELECT id_session FROM ${TablesList.LT_COURSE_SESSION_INSTRUCTOR} WHERE id_user IN (${instructorSelection.join(',')}))`);
        }

        from.push(`JOIN (${table}) AS ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS} ON ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.course_id = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse  AND ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_user = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);

        let where = [
            `AND ${TablesListAliases.CORE_USER}.userid <> '/Anonymous'`,
        ];

        // Filter enrollment status based on table ${TablesList.LT_COURSEUSER_SESSION_DETAILS}
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
                    where.push(`AND ((${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.status IN (${statuses.join(',')}) AND ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.waiting = 0) OR (${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.status = -2))`);
                    archivedWhere.push(`AND (CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.enrollment_info, '$.status') AS int) IN (${statuses.join(',')}) OR (CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.enrollment_info, '$.status') AS int) = -2))`);
                } else if (statuses.length > 0) {
                    where.push(`AND (${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.status IN (${statuses.join(',')}) AND ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.waiting = 0)`);
                    archivedWhere.push(`AND CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.enrollment_info, '$.status') AS int) IN (${statuses.join(',')})`);
                } else if (this.info.enrollment.waitingList) {
                    where.push(`AND (${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.status = -2)`);
                    archivedWhere.push(`AND (CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.enrollment_info, '$.status') AS int) = -2)`);
                }
            }
        }

        // Session filter
        if (this.info.sessions && this.info.sessions.all === false) {
            if (this.info.sessions.sessions.length === 0) {
                where.push(`AND FALSE`);
                archivedWhere.push(`AND FALSE`);
            } else {
                const sessions = this.info.sessions.sessions.map(a => a.id);
                where.push(`AND ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session IN (${sessions.join(',')})`);
                archivedWhere.push(`AND ${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.session_id IN (${sessions.join(',')})`);
            }
        }

        // Filter data by session attendance type
        const attendanceSession = this.composeSessionAttendanceFilter(TablesListAliases.LT_COURSEUSER_SESSION_DETAILS);
        if (attendanceSession !== '') {
            where.push(`AND (${attendanceSession})`);
        }

        // Session date filter
        const dateSessionFilter = this.composeSessionDateOptionsFilter(`${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.date_begin`, `${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.date_end`);
        if (dateSessionFilter !== '') {
            where.push(`AND (${this.composeTableField(TablesListAliases.LT_COURSEUSER_SESSION_DETAILS, 'id_user')} IS NULL OR (${dateSessionFilter}))`);
            archivedWhere.push(`AND (${this.composeSessionDateOptionsFilter(`DATE_PARSE(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.session_info, '$.start_at'), '%Y-%m-%d %H:%i:%s')`, `DATE_PARSE(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.session_info, '$.end_at'), '%Y-%m-%d %H:%i:%s')`)})`);
        }

        // Variable to check if we need to switch to the alternate group by clause
        let alternateGroupByClause = false;

        // Variables to check if the specified table was already joined in the query
        let joinCourseCategories = false;
        let joinCoreGroupLevel = false;
        let joinCoreGroupMembers = false;
        let joinCoreUserFieldValue = false;
        let joinLearningCourseFieldValue = false;
        let joinCoreLangLanguageFieldValue = false;
        let joinLtCourseSessionFieldValue = false;
        let joinCoreUserBranches = false;
        let joinLearningCourseuserSign = false;
        let joinLtCourseSessionDateAttendanceAggregate = false;
        let joinSkillManagersValue = false;
        let joinLtCourseSessionInstructor = false;
        let joinLtCourseSessionDate = false;
        let joinLtCourseSessionDateWebinarSetting = false;
        let joinLtCourseSessionInstructorAggregate = false;
        let joinLtCourseSessionDateAttendance = false;
        let userExtraFields = {data: { items: []} } as UserExtraFieldsResponse;
        let courseExtraFields = {data: { items: []} } as CourseExtraFieldsResponse;
        let classroomExtraFields = {data: { items: []} } as CourseExtraFieldsResponse;

        let translationValue = [];

        if (this.info.users && this.info.users.showOnlyLearners) {
            table += ` AND level = ${CourseuserLevels.Student}`;
            if (!joinLtCourseSessionInstructor) {
                from.push(`LEFT JOIN (${TablesList.LT_COURSE_SESSION_INSTRUCTOR}) AS ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR} ON ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session AND ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}.id_user = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_user`);
                joinLtCourseSessionInstructor = true;
            }
            where.push(`AND ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}.id_user IS NULL`);
        }

        // load the translations of additional fields only if are selected
        if (this.info.fields.find(item => item.includes('user_extrafield_'))) {
            userExtraFields = await this.session.getHydra().getUserExtraFields();
            translationValue = this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');
        }

        if (this.info.fields.find(item => item.includes('course_extrafield_'))) {
            courseExtraFields = await this.session.getHydra().getCourseExtraFields();
            translationValue = this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course', translationValue);
        }
        if (this.info.fields.find(item => item.includes('classroom_extrafield_'))) {
            classroomExtraFields = await this.session.getHydra().getILTExtraFields();
            this.updateExtraFieldsDuplicated(classroomExtraFields.data.items, translations, 'classroom', translationValue);
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

        if (this.info.fields) {
            for (const field of this.info.fields) {
                switch (field) {
                    // User fields
                    case FieldsList.USER_ID:
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER}.idst) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_ID])}`);
                        archivedSelect.push(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.user_id AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_ID])}`);
                        break;
                    case FieldsList.USER_USERID:
                        select.push(`ARBITRARY(SUBSTR(${TablesListAliases.CORE_USER}.userid, 2)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_USERID])}`);
                        archivedSelect.push(`SUBSTR(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.user_info, '$.username'), 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_USERID])}`);
                        break;
                    case FieldsList.USER_FIRSTNAME:
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER}.firstname) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FIRSTNAME])}`);
                        archivedSelect.push(`JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.user_info, '$.firstname') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FIRSTNAME])}`);
                        break;
                    case FieldsList.USER_LASTNAME:
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER}.lastname) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LASTNAME])}`);
                        archivedSelect.push(`JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.user_info, '$.lastname') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LASTNAME])}`);
                        break;
                    case FieldsList.USER_FULLNAME:
                        if (this.session.platform.getShowFirstNameFirst()) {
                            select.push(`CONCAT(ARBITRARY(${TablesListAliases.CORE_USER}.firstname), ' ', ARBITRARY(${TablesListAliases.CORE_USER}.lastname)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                            archivedSelect.push(`CONCAT(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.user_info, '$.firstname'), ' ', JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.user_info, '$.lastname')) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                        } else {
                            select.push(`CONCAT(ARBITRARY(${TablesListAliases.CORE_USER}.lastname), ' ', ARBITRARY(${TablesListAliases.CORE_USER}.firstname)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                            archivedSelect.push(`CONCAT(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.user_info, '$.lastname'), ' ', JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.user_info, '$.firstname')) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                        }
                        break;
                    case FieldsList.USER_EMAIL:
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER}.email) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL])}`);
                        archivedSelect.push(`JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.user_info, '$.email') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL])}`);
                        break;
                    case FieldsList.USER_EMAIL_VALIDATION_STATUS:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.CORE_USER}.email_status) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL_VALIDATION_STATUS])}`);
                            archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL_VALIDATION_STATUS])}`);
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
                            archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LEVEL])}`);
                            break;
                    case FieldsList.USER_DEACTIVATED:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.CORE_USER}.valid) ${this.getCheckIsValidFieldClause()} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_DEACTIVATED])}`);
                            archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_DEACTIVATED])}`);
                            break;
                    case FieldsList.USER_EXPIRATION:
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER}.expiration) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EXPIRATION])}`);
                        archivedSelect.push(`DATE_PARSE(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.user_info, '$.expires_at'), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EXPIRATION])}`);
                        break;
                    case FieldsList.USER_SUSPEND_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.CORE_USER}.suspend_date) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_SUSPEND_DATE])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_SUSPEND_DATE])}`);
                        break;
                    case FieldsList.USER_REGISTER_DATE:
                        const registerDateColumn = `ARBITRARY(${TablesListAliases.CORE_USER}.register_date)`;
                        const registerDateQuery = `${this.mapTimestampDefaultValueWithDLV2(registerDateColumn, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_REGISTER_DATE])}`;
                        select.push(`${registerDateQuery}`);
                        archivedSelect.push(`${this.mapTimestampDefaultValueWithDLV2(`DATE_PARSE(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.user_info, '$.registered_at'),'%Y-%m-%d %H:%i:%s')`, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_REGISTER_DATE])}`);
                        break;
                    case FieldsList.USER_LAST_ACCESS_DATE:
                        const lastenterColumn = `ARBITRARY(${TablesListAliases.CORE_USER}.lastenter)`;
                        const lastenterQuery = `${this.mapTimestampDefaultValueWithDLV2(lastenterColumn, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LAST_ACCESS_DATE])}`;
                        select.push(`${lastenterQuery}`);
                        archivedSelect.push(`${this.mapTimestampDefaultValueWithDLV2(`DATE_PARSE(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.user_info, '$.last_login'),'%Y-%m-%d %H:%i:%s')`, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LAST_ACCESS_DATE])}`);
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
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_NAME])}`);
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
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_PATH])}`);
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
                            directManagerFullName = `ARBITRARY(CONCAT(${TablesListAliases.CORE_USER}s.firstname, ' ', ${TablesListAliases.CORE_USER}s.lastname))`;
                        } else {
                            directManagerFullName = `ARBITRARY(CONCAT(${TablesListAliases.CORE_USER}s.lastname, ' ', ${TablesListAliases.CORE_USER}s.firstname))`;
                        }
                        select.push(`IF(${directManagerFullName} = ' ', ARBITRARY(SUBSTR(${TablesListAliases.CORE_USER}s.userid, 2)), ${directManagerFullName}) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_DIRECT_MANAGER])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_DIRECT_MANAGER])}`);
                        break;

                    // Course fields
                    case FieldsList.COURSE_ID:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.idCourse) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_ID])}`);
                        archivedSelect.push(`CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.id') AS INT) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_ID])}`);
                        break;
                    case FieldsList.COURSE_UNIQUE_ID:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.uidCourse) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_UNIQUE_ID])}`);
                        archivedSelect.push(`JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.uid') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_UNIQUE_ID])}`);
                        break;
                    case FieldsList.COURSE_CODE:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.code) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CODE])}`);
                        archivedSelect.push(`JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.code') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CODE])}`);
                        break;
                    case FieldsList.COURSE_NAME:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.name) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_NAME])}`);
                        archivedSelect.push(`JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.name') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_NAME])}`);
                        break;
                    case FieldsList.COURSE_CATEGORY_CODE:
                        if (!joinCourseCategories) {
                            joinCourseCategories = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_CATEGORY} AS ${TablesListAliases.LEARNING_CATEGORY} ON ${TablesListAliases.LEARNING_CATEGORY}.idCategory = ${TablesListAliases.LEARNING_COURSE}.idCategory AND ${TablesListAliases.LEARNING_CATEGORY}.lang_code = '${this.session.user.getLang()}'`);
                        }
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_CATEGORY}.code) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_CODE])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_CODE])}`);
                        break;
                    case FieldsList.COURSE_CATEGORY_NAME:
                        if (!joinCourseCategories) {
                            joinCourseCategories = true;
                            from.push(`LEFT JOIN ${TablesList.LEARNING_CATEGORY} AS ${TablesListAliases.LEARNING_CATEGORY} ON ${TablesListAliases.LEARNING_CATEGORY}.idCategory = ${TablesListAliases.LEARNING_COURSE}.idCategory AND ${TablesListAliases.LEARNING_CATEGORY}.lang_code = '${this.session.user.getLang()}'`);
                        }
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_CATEGORY}.translation) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_NAME])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CATEGORY_NAME])}`);
                        break;
                    case FieldsList.COURSE_STATUS:
                        const courseStatus = (field: string) => `CASE
                                WHEN ${field} = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_STATUS_PREPARATION])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_STATUS_EFFECTIVE])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_STATUS])}`;
                        select.push(courseStatus(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.status)`));
                        archivedSelect.push(courseStatus(`CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.status') AS int)`));
                        break;
                    case FieldsList.COURSE_CREDITS:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.credits) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CREDITS])}`);
                        archivedSelect.push(`ROUND(CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.credits') AS DOUBLE), 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CREDITS])}`);
                        break;
                    case FieldsList.COURSE_DURATION:
                        select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.mediumTime) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DURATION])}`);
                        archivedSelect.push(`CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.duration') AS int) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DURATION])}`);
                        break;
                    case FieldsList.COURSE_TYPE:
                        const courseType = (field: string) => `
                            CASE
                                WHEN ${field} = ${athena.renderStringInQueryCase(CourseTypes.Elearning)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_ELEARNING])}
                                WHEN ${field} = ${athena.renderStringInQueryCase(CourseTypes.Classroom)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_CLASSROOM])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_WEBINAR])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_TYPE])}`;
                        select.push(courseType(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.course_type)`));
                        archivedSelect.push(courseType(`JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.type')`));
                        break;
                    case FieldsList.COURSE_DATE_BEGIN:
                        select.push(`${this.mapDateDefaultValueWithDLV2(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.date_begin)`)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_BEGIN])}`);
                        archivedSelect.push(`DATE_PARSE(json_extract_scalar(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.start_at'), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_BEGIN])}`);
                        break;
                    case FieldsList.COURSE_DATE_END:
                        select.push(`${this.mapDateDefaultValueWithDLV2(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.date_end)`)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_END])}`);
                        archivedSelect.push(`DATE_PARSE(json_extract_scalar(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.end_at'), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_DATE_END])}`);
                        break;
                    case FieldsList.COURSE_EXPIRED:
                        const courseExpired = (field: string) => `
                            CASE
                                WHEN ${field} < CURRENT_DATE THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_EXPIRED])}`;

                        const dateEndColumnDLV2Fix = `(${this.mapDateDefaultValueWithDLV2(`ARBITRARY(${TablesListAliases.LEARNING_COURSE}.date_end)`)})`;
                        select.push(courseExpired(`${dateEndColumnDLV2Fix}`));
                        archivedSelect.push(courseExpired(`DATE_PARSE(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.end_at'), '%Y-%m-%d')`));
                        break;
                    case FieldsList.COURSE_CREATION_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSE}.create_date) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CREATION_DATE])}`);
                        archivedSelect.push(`DATE_FORMAT(DATE_PARSE(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.created_at'),
                        '%Y-%m-%d %H:%i:%s') AT TIME ZONE '${this.info.timezone}',
                        '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_CREATION_DATE])}`);
                        break;
                    case FieldsList.COURSE_E_SIGNATURE:
                        if (this.session.platform.checkPluginESignatureEnabled()) {
                            select.push(`
                                CASE
                                    WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSE}.has_esignature_enabled) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
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
                        select.push(`ARBITRARY(${TablesListAliases.CORE_LANG_LANGUAGE}.lang_description) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_LANGUAGE])}`);
                        archivedSelect.push(`JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_info, '$.language') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_LANGUAGE])}`);
                        break;
                    case FieldsList.COURSE_SKILLS:
                        from.push(`LEFT JOIN ${TablesList.SKILLS_WITH} AS ${TablesListAliases.SKILLS_WITH} ON ${TablesListAliases.SKILLS_WITH}.idCourse = ${TablesListAliases.LEARNING_COURSE}.idCourse`);
                        archivedFrom.push(`LEFT JOIN ${TablesList.SKILLS_WITH} AS ${TablesListAliases.SKILLS_WITH} ON ${TablesListAliases.SKILLS_WITH}.idCourse = ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.course_id`);
                        select.push(`ARBITRARY(${TablesListAliases.SKILLS_WITH}.skillsInCourse) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_SKILLS])}`);
                        archivedSelect.push(`${TablesListAliases.SKILLS_WITH}.skillsInCourse AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_SKILLS])}`);
                        break;
                    // Session fields
                    case FieldsList.SESSION_NAME:
                        select.push(`ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.name) AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_NAME])}`);
                        archivedSelect.push(`JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.session_info, '$.name') AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_NAME])}`);
                        break;
                    case FieldsList.SESSION_CODE:
                        select.push(`ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.session_code) AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_CODE])}`);
                        archivedSelect.push(`JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.session_info, '$.code') AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_CODE])}`);
                        break;
                    case FieldsList.SESSION_START_DATE:
                        const sessionDateBeginColumn = `ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.date_begin)`;
                        const sessionDateBeginQuery = `${this.mapTimestampDefaultValueWithDLV2(sessionDateBeginColumn, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_START_DATE])}`;
                        select.push(sessionDateBeginQuery);
                        archivedSelect.push(`DATE_FORMAT(DATE_PARSE(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.session_info, '$.start_at'), '%Y-%m-%d %H:%i:%s') AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_START_DATE])}`);
                        break;
                    case FieldsList.SESSION_END_DATE:
                        const sessionDateEndColumn = `ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.date_end)`;
                        const sessionDateEndQuery = `${this.mapTimestampDefaultValueWithDLV2(sessionDateEndColumn, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_END_DATE])}`;
                        select.push(sessionDateEndQuery);
                        archivedSelect.push(`DATE_FORMAT(DATE_PARSE(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.session_info, '$.end_at'), '%Y-%m-%d %H:%i:%s') AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_END_DATE])}`);
                        break;
                    case FieldsList.SESSION_EVALUATION_SCORE_BASE:
                        select.push(`CONCAT(CAST(ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.evaluation_score) AS VARCHAR), '/', CAST(ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.score_base) AS VARCHAR)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVALUATION_SCORE_BASE])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVALUATION_SCORE_BASE])}`);
                        break;
                    case FieldsList.SESSION_TIME_SESSION:
                        select.push(`
                        CASE
                         WHEN CAST( ((ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.total_hours) / 60) * 3600) % 60 AS INTEGER) = 0
                          THEN
                           CONCAT(CAST( CAST(ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.total_hours) AS INTEGER) AS VARCHAR), 'h')
                          ELSE
                             CONCAT(
                               CAST(CAST(FLOOR(ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.total_hours)) AS INTEGER) AS VARCHAR),
                               'h ',
                               CAST( CAST( ((ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.total_hours) / 60) * 3600) % 60 AS INTEGER) AS VARCHAR),
                               'm'
                             )
                         END
                        AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_TIME_SESSION])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_TIME_SESSION])}`);
                        break;
                    case FieldsList.SESSION_UNIQUE_ID:
                        select.push(`ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.uid_session) AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_UNIQUE_ID])}`);
                        archivedSelect.push(`JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.session_info, '$.uid') AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_UNIQUE_ID])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_WEBINAR_TOOL:
                        if (!joinLtCourseSessionDate) {
                            joinLtCourseSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        if (!joinLtCourseSessionDateWebinarSetting) {
                            joinLtCourseSessionDateWebinarSetting = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_WEBINAR_SETTING} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING} ON ${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}.id_date = ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_date`);
                        }
                        select.push(`ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}.webinar_tool) AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_WEBINAR_TOOL])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_WEBINAR_TOOL])}`);
                        break;
                    case FieldsList.WEBINAR_SESSION_TOOL_TIME_IN_SESSION:
                        if (!joinLtCourseSessionDateAttendanceAggregate) {
                            joinLtCourseSessionDateAttendanceAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}.id_user = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_user`);
                        }
                        select.push(`ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}.webinar_tool_session_time) AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_TOOL_TIME_IN_SESSION])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.WEBINAR_SESSION_TOOL_TIME_IN_SESSION])}`);
                        break;
                    case FieldsList.SESSION_INSTRUCTOR_USERIDS:
                        if (!joinLtCourseSessionInstructorAggregate) {
                            joinLtCourseSessionInstructorAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} ON ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        select.push(`ARRAY_JOIN(ARRAY_AGG(DISTINCT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.userid)) FILTER(WHERE ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.id_date IS NULL), ', ') AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_INSTRUCTOR_USERIDS])}`);
                        archivedSelect.push(`IF(JSON_FORMAT(JSON_EXTRACT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.session_info, '$.instructors')) = 'null', '', JSON_FORMAT(JSON_EXTRACT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.session_info, '$.instructors'))) AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_INSTRUCTOR_USERIDS])}`);
                        break;
                    case FieldsList.SESSION_INSTRUCTOR_FULLNAMES:
                        if (!joinLtCourseSessionInstructorAggregate) {
                            joinLtCourseSessionInstructorAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} ON ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        if (this.session.platform.getShowFirstNameFirst()) {
                            select.push(`ARRAY_JOIN(
                                ${this.sortArrayValuesInSelectStatementAsc(`ARRAY_AGG(
                                    DISTINCT(CONCAT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.firstname, ' ', ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.lastname))
                                ) FILTER(WHERE ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.id_date IS NULL)`)},
                                ', ') AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_INSTRUCTOR_FULLNAMES])}`);
                        } else {
                            select.push(`ARRAY_JOIN(
                                ${this.sortArrayValuesInSelectStatementAsc(`ARRAY_AGG(
                                    DISTINCT(CONCAT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.lastname, ' ', ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.firstname))
                                ) FILTER(WHERE ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.id_date IS NULL)`)},
                                ', ') AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_INSTRUCTOR_FULLNAMES])}`);
                        }
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_INSTRUCTOR_FULLNAMES])}`);
                        break;
                    case FieldsList.SESSION_ATTENDANCE_TYPE:
                        const attendanceTypeTranslation = (field: string) => `
                            CASE
                                WHEN ${field} = '${AttendancesTypes.BLENDED}' THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_BLEENDED])}
                                WHEN ${field} = '${AttendancesTypes.FLEXIBLE}' THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_FLEXIBLE])}
                                WHEN ${field} = '${AttendancesTypes.FULLONLINE}' THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONLINE])}
                                WHEN ${field} = '${AttendancesTypes.FULLONSITE}' THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONSITE])}
                                ELSE ''
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_ATTENDANCE_TYPE])}`;
                            select.push(attendanceTypeTranslation(`ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.attendance_type)`));
                            archivedSelect.push(attendanceTypeTranslation(`JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.attendance_info, '$.type')`));
                        break;
                    case FieldsList.SESSION_MAXIMUM_ENROLLMENTS:
                        select.push(`ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.max_enroll) AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_MAXIMUM_ENROLLMENTS])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_MAXIMUM_ENROLLMENTS])}`);
                        break;
                    case FieldsList.SESSION_MINIMUM_ENROLLMENTS:
                        select.push(`ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.min_enroll) AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_MINIMUM_ENROLLMENTS])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_MINIMUM_ENROLLMENTS])}`);
                        break;

                    // Session Fields
                    case FieldsList.SESSION_EVENT_NAME:
                        alternateGroupByClause = true;
                        if (!joinLtCourseSessionDate) {
                            joinLtCourseSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        select.push(`ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE}.name) AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_NAME])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_NAME])}`);
                        break;
                    case FieldsList.SESSION_EVENT_DATE:
                        alternateGroupByClause = true;
                        if (!joinLtCourseSessionDate) {
                            joinLtCourseSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE}.day), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_DATE])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_DATE])}`);
                        break;
                    case FieldsList.SESSION_EVENT_DURATION:
                        alternateGroupByClause = true;
                        if (!joinLtCourseSessionDate) {
                            joinLtCourseSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        select.push(`ROUND(ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE}.effective_duration) / 60, 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_DURATION])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_DURATION])}`);
                        break;
                    case FieldsList.SESSION_EVENT_ID:
                        alternateGroupByClause = true;
                        if (!joinLtCourseSessionDate) {
                            joinLtCourseSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        select.push(`ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE}.id_date) AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_ID])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_ID])}`);
                        break;
                    case FieldsList.SESSION_EVENT_INSTRUCTOR_FULLNAME:
                        alternateGroupByClause = true;
                        if (!joinLtCourseSessionDate) {
                            joinLtCourseSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        if (!joinLtCourseSessionInstructorAggregate) {
                            joinLtCourseSessionInstructorAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} ON ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        if (this.session.platform.getShowFirstNameFirst()) {
                            select.push(`
                                ARRAY_JOIN(
                                    ARRAY_AGG(
                                        DISTINCT(CONCAT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.firstname, ' ', ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.lastname))
                                    ) FILTER(WHERE ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.id_date = ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_date),
                                ', ') AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_INSTRUCTOR_FULLNAME])}`);
                        } else {
                            select.push(`
                                ARRAY_JOIN(
                                    ARRAY_AGG(
                                        DISTINCT(CONCAT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.lastname, ' ', ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.firstname))
                                    ) FILTER(WHERE ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.id_date = ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_date),
                                ', ') AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_INSTRUCTOR_FULLNAME])}`);
                        }
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_INSTRUCTOR_FULLNAME])}`);
                        break;
                    case FieldsList.SESSION_EVENT_INSTRUCTOR_USER_NAME:
                        alternateGroupByClause = true;
                        if (!joinLtCourseSessionDate) {
                            joinLtCourseSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        if (!joinLtCourseSessionInstructorAggregate) {
                            joinLtCourseSessionInstructorAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE} ON ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        select.push(`
                            ARRAY_JOIN(
                                ARRAY_AGG(
                                    DISTINCT(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.userid)
                                ) FILTER(WHERE ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE}.id_date = ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_date),
                            ', ') AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_INSTRUCTOR_USER_NAME])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_INSTRUCTOR_USER_NAME])}`);
                        break;
                    case FieldsList.SESSION_EVENT_TIMEZONE:
                        alternateGroupByClause = true;
                        if (!joinLtCourseSessionDate) {
                            joinLtCourseSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        select.push(`ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE}.timezone) AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_TIMEZONE])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_TIMEZONE])}`);
                        break;
                    case FieldsList.SESSION_EVENT_TYPE:
                        alternateGroupByClause = true;
                        if (!joinLtCourseSessionDate) {
                            joinLtCourseSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        if (!joinLtCourseSessionDateWebinarSetting) {
                            joinLtCourseSessionDateWebinarSetting = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_WEBINAR_SETTING} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING} ON ${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}.id_date = ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_date`);
                        }
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE}.id_location) IS NOT NULL AND ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}.webinar_tool) IS NOT NULL THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_BLEENDED])}
                                WHEN ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE}.id_location) IS NULL AND ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}.webinar_tool) IS NOT NULL THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONLINE])}
                                WHEN ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE}.id_location) IS NOT NULL AND ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_WEBINAR_SETTING}.webinar_tool) IS NULL THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONSITE])}
                                ELSE ''
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_TYPE])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_TYPE])}`);
                        break;
                    case FieldsList.SESSION_EVENT_START_DATE:
                        alternateGroupByClause = true;
                        if (!joinLtCourseSessionDate) {
                            joinLtCourseSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        let fieldDay = `ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE}.day)`;
                        let fieldTime = `DATE_FORMAT(ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE}.time_begin), '%H:%i:%s')`;

                        if (this.session.platform.isDatalakeV2Active()) {
                            fieldDay = `CAST( ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE}.day) as TIMESTAMP)`;
                            fieldTime = `ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE}.time_begin)`;
                        }

                        select.push(`CONCAT(DATE_FORMAT(${fieldDay}, '%Y-%m-%d'), ' ', ${fieldTime}) AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_START_DATE])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.SESSION_EVENT_START_DATE])}`);
                        break;

                    // Enrollment fields
                    case FieldsList.ENROLLMENT_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_inscr) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_DATE])}`);
                        archivedSelect.push(`DATE_FORMAT(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_enrolled_at AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_DATE])}`);
                        break;

                    case FieldsList.ENROLLMENT_ENROLLMENT_STATUS:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = -1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = -2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 3 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                                WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) = 4 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING])}
                                ELSE CAST (ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.status) as varchar)
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ENROLLMENT_STATUS])}`);
                        archivedSelect.push(`
                            CASE
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_status = -1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM])}
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_status = -2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_status = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_status = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_status = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_status = 3 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                                WHEN ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_status = 4 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING])}
                                ELSE CAST (${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_status as varchar)
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ENROLLMENT_STATUS])}`);
                        break;

                    case FieldsList.ENROLLMENT_USER_COURSE_LEVEL:
                        if (!joinLtCourseSessionInstructor) {
                            from.push(`LEFT JOIN (${TablesList.LT_COURSE_SESSION_INSTRUCTOR}) AS ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR} ON ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session AND ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}.id_user = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_user`);
                            joinLtCourseSessionInstructor = true;
                        }
                        select.push(`
                                CASE
                                    WHEN ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}.id_user) IS NOT NULL THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TEACHER])}
                                    WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.level) = ${CourseuserLevels.Tutor} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TUTOR])}
                                    ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_STUDENT])}
                                END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_COURSE_LEVEL])}`);
                        const courseUserLevel = (field: string) => `
                            CASE
                                WHEN ${field} = ${CourseuserLevels.Teacher} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TEACHER])}
                                WHEN ${field} = ${CourseuserLevels.Tutor} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_TUTOR])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_LEVEL_STUDENT])} END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_COURSE_LEVEL])}`;
                        archivedSelect.push(courseUserLevel(`${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_level`));
                        break;
                    case FieldsList.ENROLLMENT_USER_SESSION_STATUS:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.status) = -2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                                WHEN ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.status) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLED])}
                                WHEN ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.status) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                                WHEN ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.status) = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                                WHEN ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.status) = 3 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                                ELSE ''
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_STATUS])}`);
                            archivedSelect.push(`
                            CASE
                                WHEN CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.enrollment_info, '$.status') AS int) = -2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                                WHEN CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.enrollment_info, '$.status') AS int) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLED])}
                                WHEN CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.enrollment_info, '$.status') AS int) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                                WHEN CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.enrollment_info, '$.status') AS int) = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                                WHEN CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.enrollment_info, '$.status') AS int) = 3 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                                ELSE ''
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_STATUS])}`);
                        break;
                    case FieldsList.ENROLLMENT_USER_SESSION_SUBSCRIBE_DATE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.date_subscribed) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_SUBSCRIBE_DATE])}`);
                        archivedSelect.push(`${this.mapTimestampDefaultValueWithDLV2(`DATE_PARSE(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.enrollment_info, '$.created_at'),'%Y-%m-%d %H:%i:%s')`, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_SUBSCRIBE_DATE])}`);
                        break;
                    case FieldsList.ENROLLMENT_USER_SESSION_COMPLETE_DATE:
                        const columnName = `ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.date_completed)`;
                        const columnQuery = `${this.mapTimestampDefaultValueWithDLV2(columnName, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_COMPLETE_DATE])}`;
                        select.push(`${columnQuery}`);
                        archivedSelect.push(`${this.mapTimestampDefaultValueWithDLV2(`DATE_PARSE(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.enrollment_info, '$.completed_at'),'%Y-%m-%d %H:%i:%s')`, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_COMPLETE_DATE])}`);
                        break;
                    case FieldsList.COURSEUSER_DATE_COMPLETE:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.date_complete) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_COMPLETE])}`);
                        archivedSelect.push(`DATE_FORMAT(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.enrollment_completed_at AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_DATE_COMPLETE])}`);
                        break;
                    case FieldsList.ENROLLMENT_EVALUATION_STATUS:
                        const evaluationStatus = (field: string) => `
                                CASE
                                    WHEN ${field} = ${SessionEvaluationStatus.PASSED} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.WEBINAR_SESSION_USER_EVAL_STATUS_PASSED])}
                                    WHEN ${field} = ${SessionEvaluationStatus.FAILED} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.WEBINAR_SESSION_USER_EVAL_STATUS_FAILED])}
                                    ELSE ''
                                END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_EVALUATION_STATUS])}`;
                        select.push(evaluationStatus(`ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.evaluation_status)`));
                        archivedSelect.push(evaluationStatus(`CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}.attendance_info, '$.attendance.status') AS int)`));
                        break;

                    case FieldsList.ENROLLMENT_LEARNER_EVALUATION:
                        select.push(`ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.evaluation_score) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_LEARNER_EVALUATION])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_LEARNER_EVALUATION])}`);
                        break;

                    case FieldsList.ENROLLMENT_INSTRUCTOR_FEEDBACK:
                        select.push(`ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.evaluation_text) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_INSTRUCTOR_FEEDBACK])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_INSTRUCTOR_FEEDBACK])}`);
                        break;

                    case FieldsList.ENROLLMENT_ATTENDANCE:
                        if (!joinLtCourseSessionDateAttendanceAggregate) {
                            joinLtCourseSessionDateAttendanceAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}.id_user = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_user`);
                        }
                        select.push(`
                            CASE
                             WHEN ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}.attendance_time_spent) = '0h' AND ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}.session_total_time) = '0h'
                              THEN
                               (CASE
                                 WHEN ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.total_hours) % 60 = 0
                                  THEN CONCAT( CAST(ARBITRARY(${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.total_hours) AS varchar), ' h' )
                                 ELSE CONCAT('0h / ', '0h')
                                END
                                )
                             ELSE CONCAT( CAST(ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}.attendance_time_spent) AS VARCHAR), ' / ' , CAST(ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE}.session_total_time) AS VARCHAR) )
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ATTENDANCE])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ATTENDANCE])}`);
                        break;
                    case FieldsList.COURSE_E_SIGNATURE_HASH:
                        if (this.session.platform.checkPluginESignatureEnabled()) {
                            if (!joinLearningCourseuserSign) {
                                joinLearningCourseuserSign = true;
                                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEUSER_SIGN} AS ${TablesListAliases.LEARNING_COURSEUSER_SIGN} ON ${TablesListAliases.LEARNING_COURSEUSER_SIGN}.course_id = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse AND ${TablesListAliases.LEARNING_COURSEUSER_SIGN}.user_id = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                            }
                            select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSEUSER_SIGN}.signature) AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_E_SIGNATURE_HASH])}`);
                        }
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSE_E_SIGNATURE_HASH])}`);
                        break;
                    case FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_HOURS:
                        alternateGroupByClause = true;
                        if (!joinLtCourseSessionDate) {
                            joinLtCourseSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        if (!joinLtCourseSessionDateAttendance) {
                            joinLtCourseSessionDateAttendance = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_ATTENDANCE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session
                            AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}.id_date = ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_date
                            AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}.id_user = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_user`);
                        }
                        select.push(`ROUND(ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}.effective_duration) / 60, 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_HOURS])}`);
                        archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_HOURS])}`);
                        break;
                    case FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS:
                        alternateGroupByClause = true;
                        if (!joinLtCourseSessionDate) {
                            joinLtCourseSessionDate = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE} ON ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                        }
                        if (!joinLtCourseSessionDateAttendance) {
                            joinLtCourseSessionDateAttendance = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE_ATTENDANCE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}
                            ON ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session
                            AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}.id_date = ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_date
                            AND ${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}.id_user = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_user`);
                        }
                        select.push(`CASE
                            WHEN ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}.attendance) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_PRESENT])}
                            WHEN ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}.attendance) IS NULL AND ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}.effective_duration) > 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_PRESENT])}
                            WHEN ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_DATE_ATTENDANCE}.attendance) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_ABSENT])}
                            ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_NOT_SET])}
                        END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS])}`);
                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS])}`);
                        break;
                    case FieldsList.COURSEUSER_ASSIGNMENT_TYPE:
                        if (this.session.platform.isCoursesAssignmentTypeActive()) {
                            select.push(this.getCourseAssignmentTypeSelectField(true, translations));
                            archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.COURSEUSER_ASSIGNMENT_TYPE])}`);
                        }
                        break;
                    // Archived enrollment specific fiedls
                    case FieldsList.ENROLLMENT_ARCHIVING_DATE:
                        if (this.session.platform.isToggleMultipleEnrollmentCompletions()) {
                            select.push(`NULL AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVING_DATE])}`);
                            archivedSelect.push(`DATE_FORMAT(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}.created_at AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVING_DATE])}`);
                        }
                        break;
                    case FieldsList.ENROLLMENT_ARCHIVED:
                        if (this.session.platform.isToggleMultipleEnrollmentCompletions()) {
                            select.push(`${athena.renderStringInQueryCase(translations[FieldTranslation.NO])} AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVED])}`);
                            archivedSelect.push(`${athena.renderStringInQueryCase(translations[FieldTranslation.YES])} AS ${athena.renderStringInQuerySelect(translations[FieldsList.ENROLLMENT_ARCHIVED])}`);
                        }
                        break;

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
                                                archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} AND ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`ARBITRARY(${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation) AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.YesNo:
                                                select.push(`
                                                CASE
                                                    WHEN ARBITRARY(${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                                    WHEN ARBITRARY(${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}) = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                                    ELSE ''
                                                END AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Country:
                                                from.push(`LEFT JOIN ${TablesList.CORE_COUNTRY} AS ${TablesListAliases.CORE_COUNTRY}_${fieldId} ON ${TablesListAliases.CORE_COUNTRY}_${fieldId}.id_country = ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}`);
                                                select.push(`ARBITRARY(${TablesListAliases.CORE_COUNTRY}_${fieldId}.name_country) AS ${athena.renderStringInQuerySelect(userField.title)}`);
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
                                                select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}) AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(courseField))}`);
                                                archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(courseField))}`);
                                                archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId} AND ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation) AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(courseField))}`);
                                                archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                            case AdditionalFieldsTypes.YesNo:
                                                select.push(`
                                                CASE
                                                    WHEN ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_VALUE}.field_${fieldId}) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                                    ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                                END AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(courseField))}`);
                                                archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(courseField.name.value)}`);
                                                break;
                                        }
                                    }
                                    break;
                                }
                            }
                        } else if (this.isClassroomExtraField(field)) {
                            const fieldId = parseInt(field.replace('classroom_extrafield_', ''), 10);
                            for (const classroomField of classroomExtraFields.data.items) {
                                if (classroomField.id === fieldId) {
                                    if (await this.checkCourseAdditionalFieldInAthena(fieldId) === false) {
                                        const additionalField = this.setAdditionalFieldTranslation(classroomField);
                                        select.push(`'' AS ${athena.renderStringInQuerySelect(additionalField)}`);
                                        archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(additionalField)}`);
                                    } else {
                                        if (!joinLtCourseSessionFieldValue) {
                                            joinLtCourseSessionFieldValue = true;
                                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_FIELD_VALUES} AS ${TablesListAliases.LT_COURSE_SESSION_FIELD_VALUES} ON ${TablesListAliases.LT_COURSE_SESSION_FIELD_VALUES}.id_session = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session`);
                                        }
                                        switch (classroomField.type) {
                                            case AdditionalFieldsTypes.Textarea:
                                            case AdditionalFieldsTypes.Textfield:
                                                select.push(`ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_FIELD_VALUES}.field_${fieldId}) AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(classroomField))}`);
                                                archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(classroomField))}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.LT_COURSE_SESSION_FIELD_VALUES}.field_${fieldId}), '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(classroomField))}`);
                                                archivedSelect.push(`NULL AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(classroomField))}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.LT_COURSE_SESSION_FIELD_VALUES}.field_${fieldId} AND ${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`ARBITRARY(${TablesListAliases.LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation) AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(classroomField))}`);
                                                archivedSelect.push(`'' AS ${athena.renderStringInQuerySelect(this.setAdditionalFieldTranslation(classroomField))}`);
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
        if (this.showActive() && !isOnlyArchivedQuery || !this.session.platform.isToggleMultipleEnrollmentCompletions()) {
            query += `
                SELECT ${select.join(', ')}
                FROM ${from.join(' ')}
                ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}
                GROUP BY ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser, ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idCourse, ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}.id_session` + (alternateGroupByClause ? `, ${TablesListAliases.LT_COURSE_SESSION_DATE}.id_date` : '');
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
            ${archivedWhere.length > 0 ? ` WHERE TRUE ${archivedWhere.join(' ')}` : ''}`;
        }

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(select, translations, {user: userExtraFields.data.items, course: courseExtraFields.data.items, userCourse: [], webinar: [], classroom: classroomExtraFields.data.items});
        }

        // get the sql like LIMIT for the query
        query += this.getQueryExportLimit(limit);

        return query;
    }

    public async getQuerySnowflake(limit = 0, isPreview: boolean, checkPuVisibility = true, fromSchedule = false): Promise<string> {
        const translations = await this.loadTranslations();

        // Needed because of https://docebo.atlassian.net/browse/DD-38954
        const isAtLeastOneEventFieldsSelected = this.info.fields.some(item => this.allFields.event.includes(item as FieldsList));

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;

        const archivedWhere = [];

        let fullUsers = '';

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilterSnowflake(checkPuVisibility);
        }

        const fullCourses = await this.calculateCourseFilterSnowflake(false, true, checkPuVisibility);
        const instructorSelection = typeof this.info.courses !== 'undefined' && typeof this.info.courses.instructors !== 'undefined' ? this.info.courses.instructors.map(a => a.id) : [];

        // Needed to save some info for the select switch statement
        const queryHelper = {
            select: [],
            archivedSelect: [],
            from: [],
            archivedFrom: [],
            join: [],
            cte: [],
            groupBy: [`${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`, `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`,
           `${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level"`, `${TablesListAliases.CORE_USER}."userid"`],
            archivedGroupBy: [],
            userAdditionalFieldsSelect: [],
            userAdditionalFieldsFrom: [],
            userAdditionalFieldsId: [],
            courseAdditionalFieldsSelect: [],
            courseAdditionalFieldsFrom: [],
            courseAdditionalFieldsId: [],
            classroomAdditionalFieldsSelect: [],
            classroomAdditionalFieldsFrom: [],
            classroomAdditionalFieldsId: [],
            checkPuVisibility,
            translations
        };
        queryHelper.archivedFrom.push(`${TablesList.ARCHIVED_ENROLLMENT_COURSE} AS ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}`);
        queryHelper.archivedFrom.push(`JOIN ${TablesList.ARCHIVED_ENROLLMENT_SESSION} AS ${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION} ON ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."id" = ${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."id_archived_enrollment_course"`);
        let table = `SELECT * FROM ${TablesList.LEARNING_COURSEUSER_AGGREGATE} WHERE TRUE`;
        if (fullUsers !== '') {
            table += ` AND "iduser" IN (${fullUsers})`;
            archivedWhere.push(` AND ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."user_id" IN (${fullUsers})`);
        }

        if (fullCourses !== '') {
            table += ` AND "idcourse" IN (${fullCourses})`;
            archivedWhere.push(` AND ${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_id" IN (${fullCourses})`);
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
            table += ` AND ("expiration" IS NULL OR "expiration" > CURRENT_TIMESTAMP())`;
        }
        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);


        // JOIN LEARNING COURSE
        table = `SELECT * FROM ${TablesList.LEARNING_COURSE} WHERE "course_type" = 'classroom' AND TRUE`;

        if (fullCourses !== '') {
            table += ` AND "idcourse" IN (${fullCourses})`;
        }
        if (this.info.courseExpirationDate) {
            table += this.buildDateFilter('date_end', this.info.courseExpirationDate, 'AND', true);
            archivedWhere.push(this.buildDateFilter(`JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_COURSE}."course_info", '$.end_at')`, this.info.courseExpirationDate, 'AND', true));
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"`);

        table = `SELECT * FROM ${TablesList.LT_COURSEUSER_SESSION_DETAILS} WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND "id_user" IN (${fullUsers})`;
        }

        if (fullCourses !== '') {
            table += ` AND "course_id" IN (${fullCourses})`;
        }

        if (instructorSelection.length > 0) {
            table += ` AND "id_session" IN (SELECT "id_session" FROM ${TablesList.LT_COURSE_SESSION_INSTRUCTOR} WHERE "id_user" IN (${instructorSelection.join(',')}))`;
            archivedWhere.push(`AND ${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_id" IN (SELECT "id_session" FROM ${TablesList.LT_COURSE_SESSION_INSTRUCTOR} WHERE "id_user" IN (${instructorSelection.join(',')}))`);
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS} ON ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."course_id" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."idcourse"  AND ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_user" = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."iduser"`);

        if (isAtLeastOneEventFieldsSelected) {
            if (!queryHelper.join.includes(joinedTables.LT_COURSE_SESSION_DATE)) {
                queryHelper.join.push(joinedTables.LT_COURSE_SESSION_DATE);
                queryHelper.from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION_DATE} AS ${TablesListAliases.LT_COURSE_SESSION_DATE}
                        ON ${TablesListAliases.LT_COURSE_SESSION_DATE}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"`);
            }
        }

        queryHelper.from.push(`LEFT JOIN (${TablesList.LT_COURSE_SESSION_INSTRUCTOR}) AS ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}
            ON ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session"
            AND ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}."id_user" = ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_user"
            AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" = ${CourseuserLevels.Teacher}
            ${isAtLeastOneEventFieldsSelected ? `AND (${TablesListAliases.LT_COURSE_SESSION_DATE}."id_date" = ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}."id_date" OR (${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}."id_date" IS NULL AND ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}."id_session" IS NOT NULL))` : ''}
            `);

        const where = [
            `AND ${TablesListAliases.CORE_USER}."userid" <> '/Anonymous'`,
            `AND (
                (${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" <> 6 AND ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."date_subscribed" IS NOT NULL)
                OR (${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" = 6 AND ${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}."id_session" IS NOT NULL)
            )`
        ];

        if (isAtLeastOneEventFieldsSelected) {
            where.push(`AND (${TablesListAliases.LT_COURSE_SESSION_INSTRUCTOR}."id_user" IS NOT NULL OR ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" <> 6)`);
        }

        // Session filter
        if (this.info.sessions && this.info.sessions.all === false) {
            if (this.info.sessions.sessions.length === 0) {
                where.push(`AND FALSE`);
                archivedWhere.push(`AND FALSE`);
            } else {
                const sessions = this.info.sessions.sessions.map(a => a.id);
                where.push(`AND ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."id_session" IN (${sessions.join(',')})`);
                archivedWhere.push(`AND ${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_id" IN (${sessions.join(',')})`);
            }
        }

        // Filter enrollment status based on table ${TablesList.LT_COURSEUSER_SESSION_DETAILS}
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
                    where.push(`AND ((${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."status" IN (${statuses.join(',')}) AND ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."waiting" = 0) OR (${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."status" = -2))`);
                    archivedWhere.push(`AND (CAST(JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info", '$.status') AS int) IN (${statuses.join(',')}) OR (CAST(JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info", '$.status') AS int) = -2))`);
                } else if (statuses.length > 0) {
                    where.push(`AND (${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."status" IN (${statuses.join(',')}) AND ${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."waiting" = 0)`);
                    archivedWhere.push(`AND CAST(JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info", '$.status') AS int) IN (${statuses.join(',')})`);
                } else if (this.info.enrollment.waitingList) {
                    where.push(`AND (${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."status" = -2)`);
                    archivedWhere.push(`AND (CAST(JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."enrollment_info", '$.status') AS int) = -2)`);
                }
            }
        }

        // Filter data by session attendance type
        const attendanceSession = this.composeSessionAttendanceFilter(TablesListAliases.LT_COURSEUSER_SESSION_DETAILS);
        if (attendanceSession !== '') {
            where.push(`AND (${attendanceSession})`);
        }

        // Session date filter
        const dateSessionFilter = this.composeSessionDateOptionsFilter(`${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."date_begin"`, `${TablesListAliases.LT_COURSEUSER_SESSION_DETAILS}."date_end"`);

        if (dateSessionFilter !== '') {
            where.push(`AND (${this.composeTableField(TablesListAliases.LT_COURSEUSER_SESSION_DETAILS, 'id_user')} IS NULL OR (${dateSessionFilter}))`);
            archivedWhere.push(`AND (${this.composeSessionDateOptionsFilter(`JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info", '$.start_at')`, `JSON_EXTRACT_PATH_TEXT(${TablesListAliases.ARCHIVED_ENROLLMENT_SESSION}."session_info", '$.end_at')`)})`);
        }

        let userExtraFields = {data: { items: []} } as UserExtraFieldsResponse;
        let courseExtraFields = {data: { items: []} } as CourseExtraFieldsResponse;
        let classroomExtraFields = {data: { items: []} } as CourseExtraFieldsResponse;

        let translationValue = [];

        // load the translations of additional fields only if are selected
        if (this.info.fields.find(item => item.includes('user_extrafield_'))) {
            userExtraFields = await this.session.getHydra().getUserExtraFields();
            translationValue = this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');
        }

        if (this.info.fields.find(item => item.includes('course_extrafield_'))) {
            courseExtraFields = await this.session.getHydra().getCourseExtraFields();
            translationValue = this.updateExtraFieldsDuplicated(courseExtraFields.data.items, translations, 'course', translationValue);
        }
        if (this.info.fields.find(item => item.includes('classroom_extrafield_'))) {
            classroomExtraFields = await this.session.getHydra().getILTExtraFields();
            this.updateExtraFieldsDuplicated(classroomExtraFields.data.items, translations, 'classroom', translationValue);
        }

        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            await this.getAdditionalUsersFieldsFiltersSnowflake(queryHelper, userExtraFields);
        }

        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged = this.querySelectUserFields(field, queryHelper)
                    || this.querySelectCourseFields(field, queryHelper)
                    || this.querySelectSessionFields(field, queryHelper)
                    || this.querySelectEventFields(field, queryHelper)
                    || this.querySelectSessionEnrollmentFields(field, queryHelper, isAtLeastOneEventFieldsSelected)
                    || this.queryWithUserAdditionalFields(field, queryHelper, userExtraFields)
                    || this.queryWithCourseAdditionalFields(field, queryHelper, courseExtraFields)
                    || this.queryWithClassroomAdditionalFields(field, queryHelper, classroomExtraFields);
            }
        }

        // Must remain here, after the select statement switch ( https://docebo.atlassian.net/browse/DD-39331 )
        if (this.info.users && this.info.users.showOnlyLearners) {
            where.push(`AND ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}."level" = ${CourseuserLevels.Student}`);
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
        if (queryHelper.classroomAdditionalFieldsId.length > 0) {
            queryHelper.cte.push(this.additionalClassroomFieldQueryWith(queryHelper.classroomAdditionalFieldsId));
            queryHelper.cte.push(this.additionalFieldQueryWith(queryHelper.classroomAdditionalFieldsFrom, queryHelper.classroomAdditionalFieldsSelect, queryHelper.classroomAdditionalFieldsId, 'id_session', TablesList.LT_COURSE_SESSION_FIELD_VALUES_WITH, TablesList.CLASSROOM_ADDITIONAL_FIELDS_TRANSLATIONS));
        }

        let query = '';
        if (this.showActive() && !isOnlyArchivedQuery || !this.session.platform.isToggleMultipleEnrollmentCompletions()) {
            const groupByUniqueFields = [...new Set(queryHelper.groupBy)];
            query += `
                ${queryHelper.cte.length > 0 ? `WITH ${queryHelper.cte.join(', ')}` : ''}
                SELECT ${queryHelper.select.join(', ')}
                FROM ${queryHelper.from.join(' ')}
                ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}
                GROUP BY ${groupByUniqueFields.join(', ')}`;
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
            query += this.addOrderByClause(queryHelper.select, translations, {user: userExtraFields.data.items, course: courseExtraFields.data.items, userCourse: [], webinar: [], classroom: classroomExtraFields.data.items}, fromSchedule);
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

        // Enrollment Date
        if (filters.start_date.type !== 'any') {
            report.enrollmentDate = utils.parseLegacyFilterDate(report.enrollmentDate as DateOptionsValueDescriptor, filters.start_date);
        }
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

            const iltSessionFieldsDescriptor = this.mapILTSessionSelectedFields(filterData.fields.session, filterData.order, courseMandatoryFieldsMap);
            report.fields.push(...iltSessionFieldsDescriptor.fields);
            if (iltSessionFieldsDescriptor.orderByDescriptor) legacyOrderField = iltSessionFieldsDescriptor.orderByDescriptor;

            const enrollmentSessionDescriptor = this.mapLegacyFieldsToAmmonByEntity(
                filterData.fields.enrollment,
                filterData.order,
                {},
                {
                    'level': FieldsList.ENROLLMENT_USER_COURSE_LEVEL,
                    'date_inscr': FieldsList.ENROLLMENT_DATE,
                    'status': FieldsList.ENROLLMENT_ENROLLMENT_STATUS,
                    'learningCourseuserSessions.evaluation_score': FieldsList.ENROLLMENT_LEARNER_EVALUATION,
                    'learningCourseuserSessions.evaluation_status': FieldsList.ENROLLMENT_EVALUATION_STATUS,
                    'learningCourseuserSessions.evaluation_text': FieldsList.ENROLLMENT_INSTRUCTOR_FEEDBACK,
                    'learningCourseuserSessions.attendance_hours': FieldsList.ENROLLMENT_ATTENDANCE,
                },
                'enrollment');
            report.fields.push(...enrollmentSessionDescriptor.fields);
            if (enrollmentSessionDescriptor.orderByDescriptor) legacyOrderField = enrollmentSessionDescriptor.orderByDescriptor;

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

    private getDefaultSessionDates(): SessionDates {
        return {
            conditions: DateOptions.CONDITIONS,
            startDate: this.getDefaultDateOptions(),
            endDate: this.getDefaultDateOptions(),
        };
    }
}
