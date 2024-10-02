import { BaseDashboardManager } from '../manager/base-dashboard-manager';
import { Request } from 'express';
import {
    BranchChildren,
    BranchEnrollments,
    BranchesList,
    BranchesSummary,
    BranchUserEnrollments,
    ExportTranslation,
    OrgChartTree
} from '../interfaces/dashboard.interface';
import SessionManager from '../../services/session/session-manager.session';
import { Utils } from '../../reports/utils';
import { TablesList, TablesListAliases } from '../../models/report-manager';
import {
    DashboardTypes,
    ENROLLMENT_STATUSES_MAP,
    EnrollmentStatus,
    FieldTranslation,
    LearningCourseuser
} from '../constants/dashboard-types';
import { NotFoundException } from '../../exceptions/not-found.exception';
import { CourseTypes, EnrollmentStatuses } from '../../models/base';
import { Snowflake } from '../../services/snowflake/snowflake';

export class DashboardBranches extends BaseDashboardManager {
    translatableFields = [
        FieldTranslation.BRANCH_ID,
        FieldTranslation.COMPLETION_DATE,
        FieldTranslation.COURSEUSER_STATUS_COMPLETED,
        FieldTranslation.COURSEUSER_STATUS_ENROLLED,
        FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS,
        FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED,
        FieldTranslation.COURSE_CODE,
        FieldTranslation.COURSE_NAME,
        FieldTranslation.COURSE_TYPE,
        FieldTranslation.COURSE_TYPE_CLASSROOM,
        FieldTranslation.COURSE_TYPE_ELEARNING,
        FieldTranslation.COURSE_TYPE_WEBINAR,
        FieldTranslation.CREDITS,
        FieldTranslation.ENROLLMENT_DATE,
        FieldTranslation.FULL_NAME,
        FieldTranslation.HAS_CHILDREN,
        FieldTranslation.OVERDUE,
        FieldTranslation.OVERDUE,
        FieldTranslation.SCORE,
        FieldTranslation.SESSION_TIME,
        FieldTranslation.STATUS,
        FieldTranslation.TITLE,
        FieldTranslation.TOTAL_USERS,
        FieldTranslation.USERNAME,
    ];

    public async getBranchesSummary(req: Request): Promise<BranchesSummary> {
        const session: SessionManager = this.session;
        const snowflakeDriver = session.getSnowflake();
        const hideDeactivatedUsers: boolean = req.query.hide_deactivated_users !== undefined ? Utils.stringToBoolean(req.query.hide_deactivated_users.toString()) : false;
        const branchId: number = Number(req.query.branch_id.toString());
        const orgChartTree: OrgChartTree = await this.getOrgChartTree(branchId, snowflakeDriver);
        if (orgChartTree === undefined) {
            throw new NotFoundException('The requested branch does not exist', 1002);
        }

        /** Get all distinct user from the branch and sub-branches */
        let queryTotalUsers: string = this.generateTotalUsers(orgChartTree);
        /** If the current user is PU filter to users that he can manage only */
        if (this.session.user.isPowerUser() && !this.session.user.isGodAdmin()) {
            queryTotalUsers += this.generatePuUserBranchesQuery();
        }

        let queryTotalUsersWhereCondition = ' WHERE TRUE';
        /** Exclude ERP Admins from Statistics */
        if (!session.user.isERPAdmin()) {
            queryTotalUsersWhereCondition += ` AND ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember" NOT IN (${this.generateErpAdmins()})`;
        }
        if (hideDeactivatedUsers) {
            queryTotalUsersWhereCondition = ` AND ${TablesListAliases.CORE_USER}."valid" = 1`;
        }
        queryTotalUsers += queryTotalUsersWhereCondition;
        const totalUsers = await snowflakeDriver.runQuery(queryTotalUsers);
        const totalUsersResult: number = totalUsers[0]?.totalUsers;
        const enrollmentsResult = await this.getEnrollments(orgChartTree, queryTotalUsersWhereCondition, snowflakeDriver);
        const title = await this.getOrgChartTranslationById(orgChartTree.idorg, snowflakeDriver);
        const hasChildren = await this.hasChildren(orgChartTree, snowflakeDriver);

        return {
            id: orgChartTree.idorg,
            root: orgChartTree.lev === 1,
            title,
            code: orgChartTree.code,
            has_children: hasChildren,
            total_users: totalUsersResult,
            enrolled: enrollmentsResult.enrolled ?? 0,
            completed: enrollmentsResult.completed ?? 0,
            in_progress: enrollmentsResult.in_progress ?? 0,
            subscribed: enrollmentsResult.subscribed ?? 0,
        };
    }

    private getSubscriptionStatusTranslated(translations): string {
        return `
            CASE
                WHEN "status" = '${EnrollmentStatus.SUBSCRIBED}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                WHEN "status" = '${EnrollmentStatus.IN_PROGRESS}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                WHEN "status" = '${EnrollmentStatus.COMPLETED}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                ELSE CAST("status" AS varchar)
            END
        `;
    }

    private calculateTranslationsExportBranchesEnrollments(translations): ExportTranslation[] {
        return [
            {
                column: '"username"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.USERNAME]),
            },
            {
                column: '"fullname"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.FULL_NAME]),
            },
            {
                column: '"course_code"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.COURSE_CODE]),
            },
            {
                column: '"course_name"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.COURSE_NAME]),
            },
            {
                column: '"course_type"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.COURSE_TYPE]),
            },
            {
                column: '"enrollment_date"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.ENROLLMENT_DATE]),
            },
            {
                column: '"completion_date"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.COMPLETION_DATE]),
            },
            {
                column: '"status"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.STATUS]),
                valuesOverride: this.getSubscriptionStatusTranslated(translations),
            },
            {
                column: '"score"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.SCORE]),
            },
            {
                column: '"session_time"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.SESSION_TIME]),
            },
            {
                column: '"credits"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.CREDITS]),
            },
        ];
    }

    private calculateTranslationsExportBranchesList(translations): ExportTranslation[] {
        return [
            {
                column: '"title"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.TITLE]),
            },
            {
                column: '"total_users"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.TOTAL_USERS]),
            },
            {
                column: '"enrolled"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.COURSEUSER_STATUS_ENROLLED]),
            },
            {
                column: '"completed"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED]),
            },
            {
                column: '"in_progress"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS]),
            },
            {
                column: '"subscribed"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED]),
            },
            {
                column: '"overdue"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.OVERDUE]),
            },
            {
                column: '"id"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.BRANCH_ID]),
            },
        ];
    }

    private getWithWebinarLtCourse(): string {
        return `
            WITH ${TablesListAliases.WITH_BRANCH_WEBINARS} AS (
                SELECT DISTINCT ${TablesListAliases.WEBINAR_SESSION}."course_id", ${TablesListAliases.WEBINAR_SESSION_USER}."id_user"
                FROM ${TablesList.WEBINAR_SESSION} AS ${TablesListAliases.WEBINAR_SESSION}
                         JOIN ${TablesList.WEBINAR_SESSION_USER} AS ${TablesListAliases.WEBINAR_SESSION_USER}
                              ON ${TablesListAliases.WEBINAR_SESSION}."id_session" = ${TablesListAliases.WEBINAR_SESSION_USER}."id_session"
            ),  ${TablesListAliases.WITH_BRANCH_LT_COURSES} AS (
                SELECT DISTINCT ${TablesListAliases.LT_COURSE_SESSION}."course_id", ${TablesListAliases.LT_COURSEUSER_SESSION}."id_user"
                FROM ${TablesList.LT_COURSE_SESSION} AS ${TablesListAliases.LT_COURSE_SESSION}
                         JOIN ${TablesList.LT_COURSEUSER_SESSION} AS ${TablesListAliases.LT_COURSEUSER_SESSION}
                              ON ${TablesListAliases.LT_COURSE_SESSION}."id_session" = ${TablesListAliases.LT_COURSEUSER_SESSION}."id_session"
            )`;
    }

    public async getBranchesList(req: Request): Promise<BranchesList> {
        const session: SessionManager = this.session;
        const translations = await this.loadTranslations();
        const snowflakeDriver = session.getSnowflake();
        const queryId = req.query.query_id !== undefined ? req.query.query_id.toString() : undefined;
        let sortAttr = req.query.sort_attr !== undefined ? req.query.sort_attr.toString() : undefined;
        const sortDir = req.query.sort_dir !== undefined ? req.query.sort_dir : 'ASC';
        const page: number = req.query.page !== undefined ? Number(req.query.page) : undefined;
        const pageSize: number = req.query.page_size !== undefined ? Number(req.query.page_size) : undefined;
        const validSortAttr = [ 'title', 'has_children', 'total_users', 'enrolled', 'completed', 'in_progress', 'subscribed', 'overdue'];
        sortAttr = (sortAttr === undefined || sortAttr === '' || !validSortAttr.includes(sortAttr)) ? 'title' : sortAttr;
        let orderBy;
        switch (sortAttr) {
            case 'title':
                orderBy = `ORDER BY LOWER("${sortAttr}") ${sortDir} NULLS LAST`;
                break;
            default:
                orderBy = `ORDER BY "${sortAttr}" ${sortDir} NULLS LAST`;
                break;
        }

        const branchId: number = Number(req.query.branch_id.toString());
        const orgChartTree: OrgChartTree = await this.getOrgChartTree(branchId, snowflakeDriver);
        if (orgChartTree === undefined) {
            throw new NotFoundException('The requested branch does not exist', 1002);
        }
        const branchName = await this.getOrgChartTranslationById(orgChartTree.idorg, snowflakeDriver);
        const translationsExport = this.calculateTranslationsExportBranchesList(translations);
        if (queryId !== undefined && queryId !== '') {
            const result: BranchesList = await snowflakeDriver.paginate(undefined, orderBy, queryId, page, pageSize, DashboardTypes.BRANCHES, JSON.stringify(translationsExport));

            return {
                branch_name: branchName,
                ...result
            };
        }
        const hideDeactivatedUsers: boolean = req.query.hide_deactivated_users !== undefined ? Utils.stringToBoolean(req.query.hide_deactivated_users.toString()) : false;
        let query = `
            ${this.getWithWebinarLtCourse()}
            ${this.generateErpAdminsWith()}
            SELECT ${TablesListAliases.CORE_ORG_CHART_MEMBERS}."idorg" AS "id",
                ANY_VALUE(${TablesListAliases.CORE_ORG_CHART_MEMBERS}."title") AS "title",
                IFF(ANY_VALUE(${TablesListAliases.CORE_ORG_CHART_MEMBERS}."iright") - ANY_VALUE(${TablesListAliases.CORE_ORG_CHART_MEMBERS}."ileft") > 1, 1, 0) AS "has_children",               
                ${this.session.user.isPowerUser() ?
            this.generateCountWithConditionAndResult(
                `${TablesListAliases.CORE_USER_PU}."user_id"`,
                hideDeactivatedUsers) :
            this.generateCountWithConditionAndResult(
                `${TablesListAliases.CORE_ORG_CHART_MEMBERS}."idstmember"`,
                hideDeactivatedUsers)} AS "total_users",
            ${this.generateSumWithCondition(
                `${TablesListAliases.LEARNING_COURSEUSER}."status" = ${LearningCourseuser.STATUS_COMPLETED}
                     OR ${TablesListAliases.LEARNING_COURSEUSER}."status" = ${LearningCourseuser.STATUS_IN_PROGRESS}
                     OR ${TablesListAliases.LEARNING_COURSEUSER}."status" = ${LearningCourseuser.STATUS_SUBSCRIBED}`,
                hideDeactivatedUsers)} AS "enrolled",
            ${this.generateSumWithCondition(
                `${TablesListAliases.LEARNING_COURSEUSER}."status" = ${LearningCourseuser.STATUS_COMPLETED}`,
                hideDeactivatedUsers)} AS "completed",
            ${this.generateSumWithCondition(
                `${TablesListAliases.LEARNING_COURSEUSER}."status" = ${LearningCourseuser.STATUS_IN_PROGRESS}`,
                hideDeactivatedUsers)} AS "in_progress",
            ${this.generateSumWithCondition(
                `${TablesListAliases.LEARNING_COURSEUSER}."status" = ${LearningCourseuser.STATUS_SUBSCRIBED}`,
                hideDeactivatedUsers)} AS "subscribed",
            ${this.generateSumWithCondition(
                `((
                        ${TablesListAliases.LEARNING_COURSEUSER}."date_expire_validity" < CURRENT_TIMESTAMP
                        AND ${TablesListAliases.LEARNING_COURSEUSER}."date_expire_validity" IS NOT NULL
                        ) OR (
                            ${TablesListAliases.LEARNING_COURSE}."valid_time" > 0
                            AND DATEDIFF('second',
                                DATEADD('day',
                                    ${TablesListAliases.LEARNING_COURSE}."valid_time",
                                    IFF(
                                        ${TablesListAliases.LEARNING_COURSE}."valid_time_type" = 0,
                                        ${TablesListAliases.LEARNING_COURSEUSER}."date_first_access",
                                        ${TablesListAliases.LEARNING_COURSEUSER}."date_inscr"
                                    )
                                ),
                                CURRENT_TIMESTAMP()
                            ) >= 0
                        ))
                        AND ${TablesListAliases.LEARNING_COURSEUSER}."status" <> ${LearningCourseuser.STATUS_COMPLETED}`,
            hideDeactivatedUsers)} AS "overdue"
                FROM (${this.generateCoreOrgChartMemberSubQuery(branchId)}) AS ${TablesListAliases.CORE_ORG_CHART_MEMBERS}
                LEFT JOIN ${TablesList.LEARNING_COURSEUSER} AS ${TablesListAliases.LEARNING_COURSEUSER}
                    ON ${TablesListAliases.LEARNING_COURSEUSER}."iduser" = ${TablesListAliases.CORE_ORG_CHART_MEMBERS}."idst"
                LEFT JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE}
                    ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER}."idcourse"`;

        if (this.session.user.isPowerUser()) {
            query += this.generatePuBranchesQuery();
        }
        query += `GROUP BY ${TablesListAliases.CORE_ORG_CHART_MEMBERS}."idorg"`;

        if (pageSize !== undefined) {
            const result: BranchesList = await snowflakeDriver.paginate(query, orderBy, undefined, page, pageSize, DashboardTypes.BRANCHES, JSON.stringify(translationsExport));
            return {
                branch_name: branchName,
                ...result
            };
        } else {
            query += ` ${orderBy}`;
        }

        const items: BranchChildren[] = await snowflakeDriver.runQuery(query);
        return {
            branch_name: branchName,
            items,
            has_more_data: 0,
            current_page: 1,
            current_page_size: items.length,
            total_page_count: 1,
            total_count: items.length,
            query_id: undefined,
        };
    }

    public async getBranchEnrollments(req: Request): Promise<any> {
        const snowflakeDriver = this.session.getSnowflake();
        const translations = await this.loadTranslations();

        const branchId: number = Number(req.query.branch_id.toString());
        const hideDeactivatedUsers: boolean = req.query.hide_deactivated_users !== undefined ? Utils.stringToBoolean(req.query.hide_deactivated_users.toString()) : false;
        const statuses = req.query.status !== undefined ? JSON.parse(JSON.stringify(req.query.status)) : [];
        const orgChartTree: OrgChartTree = await this.getOrgChartTree(branchId, snowflakeDriver);
        if (orgChartTree === undefined) {
            throw new NotFoundException('The requested branch does not exist', 1002);
        }

        // pagination parameters
        const sortDir = req.query.sort_dir;
        const sortAttr = req.query.sort_attr as string;
        const page = req.query.page !== undefined ? Number(req.query.page) : undefined;
        const pageSize = req.query.page_size !== undefined ? Number(req.query.page_size) : undefined;
        const queryId = req.query.query_id !== undefined ? req.query.query_id.toString() : undefined;

        let orderByClause = 'LOWER("username") ASC NULLS LAST, LOWER("course_name") ASC NULLS LAST';
        const validSortAttr = [ 'username', 'fullname', 'course_name', 'course_code', 'course_type', 'enrollment_date', 'completion_date', 'status', 'score', 'session_time', 'credits'];
        if (sortAttr && sortAttr !== '' && validSortAttr.includes(sortAttr)) {
            orderByClause = `LOWER("${sortAttr}") ${sortDir ?? 'ASC'} NULLS LAST, ${orderByClause}`;
        }
        orderByClause = ` ORDER BY ${orderByClause} `;
        const translationsExport = this.calculateTranslationsExportBranchesEnrollments(translations);
        if (queryId !== undefined && queryId !== '') {
            return await snowflakeDriver.paginate(undefined, orderByClause, queryId, page, pageSize, DashboardTypes.BRANCHES_USERS, JSON.stringify(translationsExport));
        }

        let userFilter = `
            SELECT DISTINCT "idstmember" AS "id_user"
            FROM ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS}
            INNER JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER} ON  ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember"
            INNER JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE} ON
                (${TablesListAliases.CORE_ORG_CHART_TREE}."idst_oc" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idst") AND 
                (${TablesListAliases.CORE_ORG_CHART_TREE}."ileft" >= ${orgChartTree.ileft}) AND
                (${TablesListAliases.CORE_ORG_CHART_TREE}."iright" <= ${orgChartTree.iright})
        `;

        if (this.session.user.isPowerUser()) {
            userFilter += `
                INNER JOIN ${TablesList.CORE_USER_PU} AS ${TablesListAliases.CORE_USER_PU} ON 
                (${TablesListAliases.CORE_USER_PU}."puser_id" = ${this.session.user.getIdUser()} AND ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember" = ${TablesListAliases.CORE_USER_PU}."user_id")
            `;
        }

        let queryTotalUsersWhereCondition = ' WHERE TRUE';
        if (!this.session.user.isERPAdmin()) {
            queryTotalUsersWhereCondition = ` AND ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember" NOT IN (${this.generateErpAdmins()})`;
        }

        if (hideDeactivatedUsers) {
            queryTotalUsersWhereCondition += ` AND ${TablesListAliases.CORE_USER}."valid" = 1`;
        }
        userFilter += queryTotalUsersWhereCondition;

        const fullNameExpression = this.session.platform.getShowFirstNameFirst() ?
            `TRIM(CONCAT(ANY_VALUE(${TablesListAliases.CORE_USER}."firstname"), ' ', ANY_VALUE(${TablesListAliases.CORE_USER}."lastname")))` :
            `TRIM(CONCAT(ANY_VALUE(${TablesListAliases.CORE_USER}."lastname"), ' ', ANY_VALUE(${TablesListAliases.CORE_USER}."firstname")))`;

        let enrollmentStatusKeys = Object.keys(ENROLLMENT_STATUSES_MAP).map(Number);
        if (statuses.length > 0) {
            enrollmentStatusKeys = Object.keys(ENROLLMENT_STATUSES_MAP).filter(key => statuses.includes(ENROLLMENT_STATUSES_MAP[key])).map(Number);
        }

        const puCourseJoin = ` JOIN ${TablesList.CORE_USER_PU_COURSE} AS ${TablesListAliases.CORE_USER_PU_COURSE} ON ${TablesListAliases.CORE_USER_PU_COURSE}."course_id" = ${TablesListAliases.LEARNING_COURSEUSER}."idcourse" AND ${TablesListAliases.CORE_USER_PU_COURSE}."puser_id" = ${this.session.user.getIdUser()}`;
        const queryBody = `
            FROM ${TablesList.LEARNING_COURSEUSER} AS ${TablesListAliases.LEARNING_COURSEUSER}
            JOIN (${userFilter}) AS uf ON uf."id_user" = ${TablesListAliases.LEARNING_COURSEUSER}."iduser"
            ${this.session.user.isPowerUser() ? puCourseJoin : ''}
            JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.LEARNING_COURSEUSER}."iduser"
            JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER}."idcourse"
            LEFT JOIN ${TablesList.LEARNING_TRACKSESSION_AGGREGATE} AS ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE} ON ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."iduser" =  ${TablesListAliases.LEARNING_COURSEUSER}."iduser" AND ${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."idcourse" =  ${TablesListAliases.LEARNING_COURSEUSER}."idcourse"
            WHERE ${TablesListAliases.LEARNING_COURSEUSER}."status" IN (${enrollmentStatusKeys.join(',')})
            AND (
                ${TablesListAliases.LEARNING_COURSE}."course_type" = '${CourseTypes.Elearning}'
                    OR (
                        SELECT COUNT(*) FROM ${TablesListAliases.WITH_BRANCH_WEBINARS} 
                            WHERE "id_user" = ${TablesListAliases.LEARNING_COURSEUSER}."iduser"
                                AND "course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse"
                        ) > 0
                    OR (
                        SELECT COUNT(*) FROM ${TablesListAliases.WITH_BRANCH_LT_COURSES} 
                            WHERE "id_user" = ${TablesListAliases.LEARNING_COURSEUSER}."iduser"
                                AND "course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse"
                        ) > 0
            )
            GROUP BY ${TablesListAliases.LEARNING_COURSEUSER}."iduser", ${TablesListAliases.LEARNING_COURSEUSER}."idcourse"
        `;
        // Data query
        const query = `
            ${this.getWithWebinarLtCourse()}
            SELECT
                SUBSTR(ANY_VALUE(${TablesListAliases.CORE_USER}."userid"), 2) AS "username",
                ${fullNameExpression} AS "fullname",
                ANY_VALUE(${TablesListAliases.LEARNING_COURSE}."name") AS "course_name",
                ANY_VALUE(${TablesListAliases.LEARNING_COURSE}."code") AS "course_code",
                CASE
                    WHEN ANY_VALUE(${TablesListAliases.LEARNING_COURSE}."course_type") = '${CourseTypes.Elearning}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_ELEARNING])}
                    WHEN ANY_VALUE(${TablesListAliases.LEARNING_COURSE}."course_type") = '${CourseTypes.Classroom}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_CLASSROOM])}
                    ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_WEBINAR])}
                END AS "course_type",
                ANY_VALUE(${TablesListAliases.LEARNING_COURSEUSER}."date_inscr") AS "enrollment_date",
                ANY_VALUE(${TablesListAliases.LEARNING_COURSEUSER}."date_complete") AS "completion_date",
                CASE
                    WHEN ANY_VALUE(${TablesListAliases.LEARNING_COURSEUSER}."status") = ${EnrollmentStatuses.Subscribed} THEN '${EnrollmentStatus.SUBSCRIBED}'
                    WHEN ANY_VALUE(${TablesListAliases.LEARNING_COURSEUSER}."status") = ${EnrollmentStatuses.InProgress}  THEN '${EnrollmentStatus.IN_PROGRESS}'
                    WHEN ANY_VALUE(${TablesListAliases.LEARNING_COURSEUSER}."status") = ${EnrollmentStatuses.Completed} THEN '${EnrollmentStatus.COMPLETED}'
                    ELSE CAST(ANY_VALUE(${TablesListAliases.LEARNING_COURSEUSER}."status") AS varchar)
                END AS "status",
                ANY_VALUE(${TablesListAliases.LEARNING_COURSEUSER}."score_given") AS "score",
                ANY_VALUE(${TablesListAliases.LEARNING_TRACKSESSION_AGGREGATE}."totaltime") AS "session_time",
                ANY_VALUE(${TablesListAliases.LEARNING_COURSE}."credits") AS "credits"
            ${queryBody}
        `;

        return await snowflakeDriver.paginate(query, orderByClause, undefined, page, pageSize, DashboardTypes.BRANCHES_USERS, JSON.stringify(translationsExport));
    }

    private async getOrgChartTree(branchId: number, snowflakeDriver: Snowflake): Promise<OrgChartTree> {
        const query = `SELECT "idorg", "code", "idparent", "lev", "ileft", "iright"
                                FROM ${TablesList.CORE_ORG_CHART_TREE} WHERE "idorg" = ${branchId}`;

        const result = await snowflakeDriver.runQuery(query);
        return result[0];
    }

    private async getEnrollments(orgChartTree: OrgChartTree, queryTotalUsersWhereCondition: string, snowflakeDriver: Snowflake): Promise<BranchEnrollments> {
        let subQuery = `SELECT DISTINCT ${TablesListAliases.CORE_USER}."idst" AS "id_user"
                                    FROM ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}
                                    INNER JOIN ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS}
                                        ON ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember" = ${TablesListAliases.CORE_USER}."idst"
                                    INNER JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}
                                        ON (${TablesListAliases.CORE_ORG_CHART_TREE}."idst_oc" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idst")
                                        AND (${TablesListAliases.CORE_ORG_CHART_TREE}."ileft" >= ${orgChartTree.ileft})
                                        AND (${TablesListAliases.CORE_ORG_CHART_TREE}."ileft" <= ${orgChartTree.iright})`;
        if (this.session.user.isPowerUser()) {
            subQuery += this.generatePuUserBranchesQuery();
        }
        subQuery += queryTotalUsersWhereCondition;
        let query = `
            ${this.getWithWebinarLtCourse()}
            SELECT SUM(IFF(${TablesListAliases.LEARNING_COURSEUSER}."status" = ${LearningCourseuser.STATUS_COMPLETED}
                           OR ${TablesListAliases.LEARNING_COURSEUSER}."status" = ${LearningCourseuser.STATUS_IN_PROGRESS}
                           OR ${TablesListAliases.LEARNING_COURSEUSER}."status" = ${LearningCourseuser.STATUS_SUBSCRIBED},
                        1, 0)) AS "enrolled",
                   SUM(IFF(${TablesListAliases.LEARNING_COURSEUSER}."status" = ${LearningCourseuser.STATUS_COMPLETED}, 1, 0))   AS "completed",
                   SUM(IFF(${TablesListAliases.LEARNING_COURSEUSER}."status" = ${LearningCourseuser.STATUS_IN_PROGRESS}, 1, 0)) AS "in_progress",
                   SUM(IFF(${TablesListAliases.LEARNING_COURSEUSER}."status" = ${LearningCourseuser.STATUS_SUBSCRIBED}, 1, 0))  AS "subscribed",
                   SUM(IFF(((${TablesListAliases.LEARNING_COURSEUSER}."date_expire_validity" < CURRENT_TIMESTAMP AND ${TablesListAliases.LEARNING_COURSEUSER}."date_expire_validity" IS NOT NULL)
                            OR (${TablesListAliases.LEARNING_COURSE}."valid_time" > 0
                                AND DATEDIFF(second,
                                             DATEADD(day,
                                                    ${TablesListAliases.LEARNING_COURSE}."valid_time",
                                                    IFF(${TablesListAliases.LEARNING_COURSE}."valid_time_type" = 1,
                                                        ${TablesListAliases.LEARNING_COURSEUSER}."date_first_access",
                                                        ${TablesListAliases.LEARNING_COURSEUSER}."date_inscr")
                                                    ),
                                             CURRENT_TIMESTAMP) <= 0
                            )) AND ${TablesListAliases.LEARNING_COURSEUSER}."status" <> ${LearningCourseuser.STATUS_COMPLETED}, 1, 0)
                       ) AS "overdue"
            FROM ${TablesList.LEARNING_COURSEUSER} AS ${TablesListAliases.LEARNING_COURSEUSER}
                INNER JOIN (${subQuery}) AS u ON ${TablesListAliases.LEARNING_COURSEUSER}."iduser" = u."id_user"
                INNER JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE}
                    ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER}."idcourse"`;

        if (this.session.user.isPowerUser()) {
            query += ` INNER JOIN ${TablesList.CORE_USER_PU_COURSE} AS ${TablesListAliases.CORE_USER_PU_COURSE}
                        ON (${TablesListAliases.CORE_USER_PU_COURSE}."course_id" = ${TablesListAliases.LEARNING_COURSEUSER}."idcourse"
                            AND ${TablesListAliases.CORE_USER_PU_COURSE}."puser_id" = ${this.session.user.getIdUser()})`;
        }
        query += ` WHERE (${TablesListAliases.LEARNING_COURSE}."course_type" = 'elearning')
                    OR (
                        SELECT COUNT(*) FROM ${TablesListAliases.WITH_BRANCH_WEBINARS} 
                            WHERE "id_user" = ${TablesListAliases.LEARNING_COURSEUSER}."iduser"
                                AND "course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse"
                        ) > 0
                    OR (
                        SELECT COUNT(*) FROM ${TablesListAliases.WITH_BRANCH_LT_COURSES} 
                            WHERE "id_user" = ${TablesListAliases.LEARNING_COURSEUSER}."iduser"
                                AND "course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse"
                        ) > 0`;

        const result = await snowflakeDriver.runQuery(query);
        return result[0];
    }

    private generateErpAdmins(): string {
        return `SELECT ${TablesListAliases.CORE_USER}."idst"
                    FROM ${TablesList.RBAC_ASSIGNMENT} AS ${TablesListAliases.RBAC_ASSIGNMENT}
                    INNER JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}
                        ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.RBAC_ASSIGNMENT}."user_id"
                    WHERE ${TablesListAliases.RBAC_ASSIGNMENT}."item_name" = '/framework/level/erpadmin'`;
    }

    private generateErpAdminsWith(): string {
        return !this.session.user.isERPAdmin() ? `, with_erp_admins AS (
                    SELECT ${TablesListAliases.CORE_USER}."idst"
                    FROM ${TablesList.RBAC_ASSIGNMENT} AS ${TablesListAliases.RBAC_ASSIGNMENT}
                    INNER JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}
                        ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.RBAC_ASSIGNMENT}."user_id"
                    WHERE ${TablesListAliases.RBAC_ASSIGNMENT}."item_name" = '/framework/level/erpadmin'
        )` : '';
    }

    private generateTotalUsers(orgChartTree: OrgChartTree): string {
        return `
            SELECT COUNT(DISTINCT "idstmember") AS "totalUsers"
            FROM ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS}
                INNER JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}
                    ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember"
                INNER JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}
                    ON (${TablesListAliases.CORE_ORG_CHART_TREE}."idst_oc" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idst")
                AND (${TablesListAliases.CORE_ORG_CHART_TREE}."ileft" >= ${orgChartTree.ileft})
                AND (${TablesListAliases.CORE_ORG_CHART_TREE}."iright" <= ${orgChartTree.iright})`;
    }

    private generatePuUserBranchesQuery(): string {
        return ` INNER JOIN ${TablesList.CORE_USER_PU} AS ${TablesListAliases.CORE_USER_PU}
                        ON (${TablesListAliases.CORE_USER_PU}."puser_id" = ${this.session.user.getIdUser()}
                        AND ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember" = ${TablesListAliases.CORE_USER_PU}."user_id")`;
    }

    private async hasChildren(orgChartTree: OrgChartTree, snowflakeDriver: Snowflake): Promise<boolean> {
        if (this.session.user.isPowerUser()) {
            return await this.getUserHasChildren(orgChartTree, snowflakeDriver);
        }

        return (orgChartTree.iright - orgChartTree.ileft) > 1;
    }

    private async getUserHasChildren(orgChartTree: OrgChartTree, snowflakeDriver: Snowflake): Promise<boolean> {
        const subQuery = `SELECT ${TablesListAliases.CORE_ORG_CHART_TREE}."idorg"
                                    FROM ${TablesList.CORE_USER_PU} AS ${TablesListAliases.CORE_USER_PU}
                                    INNER JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}
                                        ON (${TablesListAliases.CORE_ORG_CHART_TREE}."idst_oc" = ${TablesListAliases.CORE_USER_PU}."user_id")
                                    WHERE ${TablesListAliases.CORE_USER_PU}."puser_id" = ${this.session.user.getIdUser()}`;

        const query = `SELECT count(*) FROM ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}
                                WHERE ${orgChartTree.ileft} < ${TablesListAliases.CORE_ORG_CHART_TREE}."ileft"
                                AND ${orgChartTree.iright} > ${TablesListAliases.CORE_ORG_CHART_TREE}."iright"
                                AND ${TablesListAliases.CORE_ORG_CHART_TREE}."idorg" IN (${subQuery})`;

        const result = await snowflakeDriver.runQuery(query);
        return result[0]['COUNT(*)'] > 0;
    }

    private async getOrgChartTranslationById(idorg: number, snowflakeDriver: Snowflake): Promise<string> {
        const query = `SELECT ${TablesListAliases.CORE_ORG_CHART}."translation"
                                FROM ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}
                                INNER JOIN ${TablesList.CORE_ORG_CHART} AS ${TablesListAliases.CORE_ORG_CHART}
                                    ON (${TablesListAliases.CORE_ORG_CHART_TREE}."idorg" = ${TablesListAliases.CORE_ORG_CHART}."id_dir"
                                        AND (${TablesListAliases.CORE_ORG_CHART}."lang_code" IN (
                                            '${this.session.user.getLang()}', '${this.session.platform.getDefaultLanguage()}', 'english')
                                        ))
                                WHERE ${TablesListAliases.CORE_ORG_CHART_TREE}."idorg" = ${idorg}`;

        const result = await snowflakeDriver.runQuery(query);
        return result[0]?.translation;
    }

    private generateSumWithCondition(additionalCondition: string, hideDeactivatedUsers: boolean): string {
        const queryFilterPU: string = this.session.user.isPowerUser()
            ? `AND ${TablesListAliases.CORE_USER_PU}."user_id" IS NOT NULL AND ${TablesListAliases.CORE_USER_PU_COURSE}."course_id" IS NOT NULL`
            : '';
        const queryFilterDeactivatedUsers: string = hideDeactivatedUsers
            ? `AND ${TablesListAliases.CORE_ORG_CHART_MEMBERS}."valid" = 1`
            : '';
        return `SUM(IFF(
                        (
                            ${TablesListAliases.LEARNING_COURSE}."course_type" = '${CourseTypes.Elearning}'
                            OR (
                                SELECT COUNT(*) FROM ${TablesListAliases.WITH_BRANCH_WEBINARS} 
                                    WHERE "id_user" = ${TablesListAliases.LEARNING_COURSEUSER}."iduser"
                                        AND "course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse"
                                ) > 0
                            OR (
                                SELECT COUNT(*) FROM ${TablesListAliases.WITH_BRANCH_LT_COURSES} 
                                    WHERE "id_user" = ${TablesListAliases.LEARNING_COURSEUSER}."iduser"
                                        AND "course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse"
                                ) > 0
                        )
                        ${queryFilterPU}
                        ${queryFilterDeactivatedUsers}
                        AND (${additionalCondition}),
                        1,
                        0
                    )
                )`;
    }

    private generateCountWithConditionAndResult(result: string, hideDeactivatedUsers: boolean): string {
        return `COUNT(
                    DISTINCT
                        IFF(TRUE ${hideDeactivatedUsers ? `AND ${TablesListAliases.CORE_ORG_CHART_MEMBERS}."valid" = 1` : ''},
                            ${result},
                            NULL)
                )`;
    }

    private generateCoreOrgChartMemberSubQuery(branchId: number): string {
        return `
            SELECT
                ${TablesListAliases.CORE_ORG_CHART_TREE_2}."idorg",
                ANY_VALUE(${TablesListAliases.CORE_ORG_CHART_TREE_2}."ileft") AS "ileft",
                ANY_VALUE(${TablesListAliases.CORE_ORG_CHART_TREE_2}."iright") AS "iright",
                IFF(ANY_VALUE(${TablesListAliases.CORE_ORG_CHART}."translation") IS NOT NULL AND ANY_VALUE(${TablesListAliases.CORE_ORG_CHART}."translation") <> '',
                    ANY_VALUE(${TablesListAliases.CORE_ORG_CHART}."translation"),
                    ANY_VALUE(${TablesListAliases.CORE_ORG_CHART_2}."translation")) AS "title",
                ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember",
                ANY_VALUE(${TablesListAliases.CORE_USER}."idst") AS "idst",
                ANY_VALUE(${TablesListAliases.CORE_USER}."valid") = 1 AS "valid"
            FROM ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}
            JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE_2}
                ON ${TablesListAliases.CORE_ORG_CHART_TREE}."idorg" = ${branchId}
                AND ${TablesListAliases.CORE_ORG_CHART_TREE_2}."ileft" >= ${TablesListAliases.CORE_ORG_CHART_TREE}."ileft"
                AND ${TablesListAliases.CORE_ORG_CHART_TREE_2}."iright" <= ${TablesListAliases.CORE_ORG_CHART_TREE}."iright"
                AND ${TablesListAliases.CORE_ORG_CHART_TREE_2}."lev" = (${TablesListAliases.CORE_ORG_CHART_TREE}."lev" + 1)
            LEFT JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE_3}
                ON ${TablesListAliases.CORE_ORG_CHART_TREE_3}."ileft" >= ${TablesListAliases.CORE_ORG_CHART_TREE_2}."ileft"
                AND ${TablesListAliases.CORE_ORG_CHART_TREE_3}."iright" <= ${TablesListAliases.CORE_ORG_CHART_TREE_2}."iright"
            LEFT JOIN ${TablesList.CORE_ORG_CHART} as ${TablesListAliases.CORE_ORG_CHART}
                ON ${TablesListAliases.CORE_ORG_CHART}."id_dir" = ${TablesListAliases.CORE_ORG_CHART_TREE_2}."idorg"
                AND ${TablesListAliases.CORE_ORG_CHART}."lang_code" = '${this.session.user.getLang()}'
            LEFT JOIN ${TablesList.CORE_ORG_CHART} as ${TablesListAliases.CORE_ORG_CHART_2}
                ON ${TablesListAliases.CORE_ORG_CHART_2}."id_dir" = ${TablesListAliases.CORE_ORG_CHART_TREE_2}."idorg"
                AND ${TablesListAliases.CORE_ORG_CHART_2}."lang_code" = '${this.session.platform.getDefaultLanguage()}'
            LEFT JOIN ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS}
                ON ${TablesListAliases.CORE_ORG_CHART_TREE_3}."idst_oc" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idst"
            LEFT JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}
                ON ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember" = ${TablesListAliases.CORE_USER}."idst" ${!this.session.user.isERPAdmin() ? ` AND ${TablesListAliases.CORE_USER}."idst" NOT IN (SELECT * FROM with_erp_admins)` : ''}
            GROUP BY ${TablesListAliases.CORE_ORG_CHART_TREE_2}."idorg", ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember"`;
    }

    private generatePuBranchesQuery(): string {
        const subQuery = `SELECT DISTINCT(${TablesListAliases.CORE_ORG_CHART_TREE_2}."idorg")
                                    FROM ${TablesList.CORE_USER_PU} AS ${TablesListAliases.CORE_USER_PU}
                                    INNER JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}
                                        ON ${TablesListAliases.CORE_ORG_CHART_TREE}."idst_oc" = ${TablesListAliases.CORE_USER_PU}."user_id"
                                        AND ${TablesListAliases.CORE_USER_PU}."puser_id" = ${this.session.user.getIdUser()}
                                    INNER JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE_2}
                                        ON ${TablesListAliases.CORE_ORG_CHART_TREE_2}."ileft" <= ${TablesListAliases.CORE_ORG_CHART_TREE}."ileft"
                                        AND ${TablesListAliases.CORE_ORG_CHART_TREE_2}."iright" >= ${TablesListAliases.CORE_ORG_CHART_TREE}."iright"
                                    WHERE ${TablesListAliases.CORE_USER_PU}."puser_id" = ${this.session.user.getIdUser()}`;

        return `LEFT JOIN ${TablesList.CORE_USER_PU} AS ${TablesListAliases.CORE_USER_PU}
                    ON ${TablesListAliases.CORE_USER_PU}."user_id" = ${TablesListAliases.CORE_ORG_CHART_MEMBERS}."idst"
                    AND ${TablesListAliases.CORE_USER_PU}."puser_id" = ${this.session.user.getIdUser()}
                LEFT JOIN ${TablesList.CORE_USER_PU_COURSE} AS ${TablesListAliases.CORE_USER_PU_COURSE}
                    ON ${TablesListAliases.CORE_USER_PU_COURSE}."course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse"
                    AND ${TablesListAliases.CORE_USER_PU_COURSE}."puser_id" = ${this.session.user.getIdUser()}
                WHERE ${TablesListAliases.CORE_ORG_CHART_MEMBERS}."idorg" IN (${subQuery})`;
    }
}
