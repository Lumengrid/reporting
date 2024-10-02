import { BaseDashboardManager } from '../manager/base-dashboard-manager';
import SessionManager from '../../services/session/session-manager.session';
import { TablesList, TablesListAliases } from '../../models/report-manager';
import { Request } from 'express';
import {
    Course,
    CourseCompletion,
    CoursesEnrollments,
    CourseSummary,
    ExportTranslation,
    TopCourseByEnrollment,
    UserEnrollmentsByCourse,
} from '../interfaces/dashboard.interface';
import { CourseTypes, EnrollmentStatuses } from '../../models/base';
import { Utils } from '../../reports/utils';
import { DashboardTypes, FieldTranslation, TimeFrame } from '../constants/dashboard-types';
import { NotFoundException } from '../../exceptions';

export class DashboardCourses extends BaseDashboardManager {
    translatableFields = [
            FieldTranslation.COMPLETION_DATE,
            FieldTranslation.COURSEUSER_STATUS_COMPLETED,
            FieldTranslation.COURSEUSER_STATUS_CONFIRMED,
            FieldTranslation.COURSEUSER_STATUS_ENROLLED,
            FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS,
            FieldTranslation.COURSEUSER_STATUS_NOT_STARTED,
            FieldTranslation.COURSEUSER_STATUS_OVERBOOKING,
            FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED,
            FieldTranslation.COURSEUSER_STATUS_SUSPENDED,
            FieldTranslation.COURSEUSER_STATUS_WAITING_LIST,
            FieldTranslation.COURSE_CODE,
            FieldTranslation.COURSE_NAME,
            FieldTranslation.COURSE_TYPE,
            FieldTranslation.COURSE_TYPE_ELEARNING,
            FieldTranslation.COURSE_TYPE_WEBINAR,
            FieldTranslation.ENROLLMENT_DATE,
            FieldTranslation.FIRST_NAME,
            FieldTranslation.HAS_ESIGNATURE_ENABLED,
            FieldTranslation.IDCOURSE,
            FieldTranslation.LAST_ACCESS,
            FieldTranslation.LAST_NAME,
            FieldTranslation.OTHER_COURSES,
            FieldTranslation.SCORE,
            FieldTranslation.STATUS,
            FieldTranslation.TIME_IN_COURSE,
            FieldTranslation.USERNAME,
            FieldTranslation.YES,
            FieldTranslation.COURSE_TYPE_CLASSROOM,
        ];

    private getSubscriptionStatusTranslated(translations): string {
        return `
            CASE
                WHEN "status" = '${EnrollmentStatuses.Subscribed}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED])}
                WHEN "status" = '${EnrollmentStatuses.InProgress}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS])}
                WHEN "status" = '${EnrollmentStatuses.Completed}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_COMPLETED])}
                WHEN "status" = '${EnrollmentStatuses.WaitingList}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_WAITING_LIST])}
                WHEN "status" = '${EnrollmentStatuses.Confirmed}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_CONFIRMED])}
                WHEN "status" = '${EnrollmentStatuses.Suspend}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_SUSPENDED])}
                WHEN "status" = '${EnrollmentStatuses.Overbooking}' THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEUSER_STATUS_OVERBOOKING])}
                ELSE CAST("status" AS varchar)
            END
        `;
    }

    private getTypesTranslated(translations): string {
        return `
            CASE
                WHEN "type" = ${this.renderStringInQueryCase(CourseTypes.Elearning)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_ELEARNING])}
                WHEN "type" = ${this.renderStringInQueryCase(CourseTypes.Classroom)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_CLASSROOM])}
                WHEN "type" = ${this.renderStringInQueryCase(CourseTypes.Webinar)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSE_TYPE_WEBINAR])}
                ELSE CAST("type" AS varchar)
            END
        `;
    }

    private generateHideDeactivatedUsersQuery(hide: boolean, field = TablesListAliases.LEARNING_COURSEUSER + '."iduser"', join = true): string {
        if (!hide) return '';
        return (join
                    ? `INNER JOIN ${TablesList.CORE_USER} as ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${field}`
                    : ''
                ) + ` AND ${TablesListAliases.CORE_USER}."valid" = 1`;
    }

    private generateSearchTextQuery(searchText?: string, field = TablesListAliases.CORE_USER + '."userid"'): string {
        return searchText !== undefined && searchText !== '' ?
            ` AND ${field} LIKE '${searchText}'` : '';
    }

    private generateUsersInBranchQuery(branchId?: number, field = TablesListAliases.LEARNING_COURSEUSER + '."iduser"'): string {
        if (!branchId) {
            return '';
        }
        return ` AND ${field} IN (SELECT "idstmember" FROM ${TablesListAliases.WITH_BRACH_MEMBERS})`;
    }

    private generateUsersInBranchWithQuery(branchId?: number): string {
        if (!branchId) {
            return '';
        }
        return ` WITH ${TablesListAliases.WITH_BRACH_MEMBERS} AS (
                    SELECT DISTINCT ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember"
                        FROM ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}
                        JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}s 
                            ON ${TablesListAliases.CORE_ORG_CHART_TREE}."idorg" = ${branchId}
                                AND ${TablesListAliases.CORE_ORG_CHART_TREE}s."ileft" >= ${TablesListAliases.CORE_ORG_CHART_TREE}."ileft"
                                AND ${TablesListAliases.CORE_ORG_CHART_TREE}s."iright" <= ${TablesListAliases.CORE_ORG_CHART_TREE}."iright"
                        JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}d 
                            ON ${TablesListAliases.CORE_ORG_CHART_TREE}d."ileft" >= ${TablesListAliases.CORE_ORG_CHART_TREE}s."ileft"
                                AND ${TablesListAliases.CORE_ORG_CHART_TREE}d."iright" <= ${TablesListAliases.CORE_ORG_CHART_TREE}s."iright"
                        JOIN ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS} 
                            ON ${TablesListAliases.CORE_ORG_CHART_TREE}d."idst_oc" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idst"
                )`;
    }

    private generateTimeframeQuery(filter?: string, startDate?: string, endDate?: string, field = TablesListAliases.LEARNING_COURSEUSER + '."date_inscr"'): string {
        let timeframeQuery = '';
        switch (filter) {
            case TimeFrame.THIS_WEEK:
                timeframeQuery = ` AND ${field} > DATEADD(day, -7,current_date) `;
                break;
            case TimeFrame.THIS_MONTH:
                timeframeQuery = ` AND ${field} > DATEADD(month, -1,current_date) `;
                break;
            case TimeFrame.THIS_YEAR:
                timeframeQuery = ` AND ${field} > DATEADD(year, -1,current_date) `;
                break;
            case TimeFrame.CUSTOM:
                const hasStartDate = startDate !== undefined && startDate.toString().length >= 10;
                const hasEndDate = endDate !== undefined && endDate.toString().length >= 10;
                startDate = hasStartDate ? startDate.toString().substring(0, 10) + ' 00:00:00' : '';
                endDate = hasEndDate ? endDate.toString().substring(0, 10) + ' 23:59:59' : '';
                // prepare condition based on passed dates, if any
                if (hasStartDate && hasEndDate) {
                    timeframeQuery = ` AND ${field} BETWEEN TIMESTAMP '${startDate}' AND TIMESTAMP '${endDate}' `;
                } else {
                    if (hasStartDate) {
                        timeframeQuery = ` AND ${field} > TIMESTAMP '${startDate}' `;
                    } else if (hasEndDate) {
                        timeframeQuery = ` AND ${field} < TIMESTAMP '${endDate}' `;
                    }
                }
                break;
        }
        return timeframeQuery;
    }

    private generateNotErpUserQuery(field =  TablesListAliases.LEARNING_COURSEUSER + '."iduser"'): string {
        return !this.session.user.isERPAdmin() ?
            ` AND ${field} NOT IN (
                    SELECT cast("user_id" as integer)
                        FROM ${TablesList.RBAC_ASSIGNMENT}
                    WHERE "item_name" = '/framework/level/erpadmin')` : '';
    }

    private async generateCourseQuery(courseId?: number, table = TablesListAliases.LEARNING_COURSE): Promise<string> {
        if (!courseId) return '';
        const course = await this.getCourseById(courseId);
        if (course === undefined) {
            throw new NotFoundException('Invalid Course', 1002);
        }
        return ` AND ${table}."idcourse" = ${courseId}`;
    }

    private generatePuUserQuery(): string {
        if (this.session.user.isPowerUser() && !this.session.user.isGodAdmin()) {
            return ` JOIN ${TablesList.CORE_USER_PU} as ${TablesListAliases.CORE_USER_PU}
                        ON  ${TablesListAliases.CORE_USER_PU}."puser_id" = ${this.session.user.getIdUser()} AND
                            ${TablesListAliases.CORE_USER_PU}."user_id" = ${TablesListAliases.LEARNING_COURSEUSER}."iduser" `;
        }
        return '';
    }

    private generatePuUserCourseQuery(): string {
        let query = '';
        if (this.session.user.isPowerUser() && !this.session.user.isGodAdmin()) {
            query = ` INNER JOIN ${TablesList.CORE_USER_PU} as ${TablesListAliases.CORE_USER_PU} 
                        ON ${TablesListAliases.CORE_USER_PU}."puser_id" = ${this.session.user.getIdUser()} AND 
                           ${TablesListAliases.CORE_USER_PU}."user_id" = ${TablesListAliases.LEARNING_COURSEUSER}."iduser"` ;
            query += ` INNER JOIN ${TablesList.CORE_USER_PU_COURSE} as ${TablesListAliases.CORE_USER_PU_COURSE} 
                        ON ${TablesListAliases.CORE_USER_PU_COURSE}."puser_id" = ${this.session.user.getIdUser()} AND 
                           ${TablesListAliases.CORE_USER_PU_COURSE}."course_id" = ${TablesListAliases.LEARNING_COURSEUSER}."idcourse"`;
        }
        return query;
    }

    private generateCourseSummaryQuery(): string {
        return `
                sum(case when ${TablesListAliases.LEARNING_COURSEUSER}."status" > ${EnrollmentStatuses.Confirmed} then 1 else 0 end) as "enrolled",
                sum(case when ${TablesListAliases.LEARNING_COURSEUSER}."status" = ${EnrollmentStatuses.Completed} then 1 else 0 end) as "completed",
                sum(case when ${TablesListAliases.LEARNING_COURSEUSER}."status" = ${EnrollmentStatuses.InProgress} then 1 else 0 end) as "in_progress",
                sum(case when ${TablesListAliases.LEARNING_COURSEUSER}."status" = ${EnrollmentStatuses.Subscribed} then 1 else 0 end) as "not_started"`;
    }

    public async getCourseById(courseId: number): Promise<Course | undefined> {
        const session: SessionManager = this.session;
        const snowflakeDriver = session.getSnowflake();
        const query = `SELECT TOP 1 "name" as "title", "code", "description" FROM ${TablesList.LEARNING_COURSE} WHERE "idcourse" = ${courseId}`;

        const result = await snowflakeDriver.runQuery(query);
        return Array.isArray(result) && result.length > 0 ? result[0] : undefined;
    }

    private calculateTranslationsExportUserEnrollmentsByCourse(translations): ExportTranslation[] {
        return [
            {
                column: '"username"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.USERNAME]),
            },
            {
                column: '"first_name"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.FIRST_NAME]),
            },
            {
                column: '"last_name"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.LAST_NAME]),
            },
            {
                column: '"status"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.STATUS]),
                valuesOverride: this.getSubscriptionStatusTranslated(translations),
            },
            {
                column: '"enrollment_date"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.ENROLLMENT_DATE]),
            },
            {
                column: '"time_in_course"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.TIME_IN_COURSE]),
            },
            {
                column: '"last_access"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.LAST_ACCESS]),
            },
            {
                column: '"completion_date"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.COMPLETION_DATE]),
            },
            {
                column: '"score"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.SCORE]),
            },
        ];
    }

    private calculateTranslationsExportCoursesEnrollments(translations): ExportTranslation[] {
        const data: ExportTranslation[] = [
            {
                column: '"idcourse"', // no fe field
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.IDCOURSE]),
            },
            {
                column: '"name"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.COURSE_NAME]),
            },
            {
                column: '"code"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.COURSE_CODE]),
            },
            {
                column: '"type"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.COURSE_TYPE]),
                valuesOverride: this.getTypesTranslated(translations),
            },
        ];
        if (this.session.platform.checkPluginESignatureEnabled()) {
            data.push({
                column: '"has_esignature_enabled"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.HAS_ESIGNATURE_ENABLED]),
            });
        }
        data.push(...[
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
                column: '"not_started"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.COURSEUSER_STATUS_NOT_STARTED]),
            },
        ]);
        return data;
    }

    public async getReportUserEnrollmentsByCourse (req: Request): Promise<UserEnrollmentsByCourse> {
        const session: SessionManager = this.session;
        const snowflakeDriver = session.getSnowflake();
        const queryId = req.query.query_id !== undefined ? req.query.query_id.toString() : undefined;
        let sortAttr = req.query.sort_attr !== undefined ? req.query.sort_attr.toString() : undefined;
        const sortDir = req.query.sort_dir !== undefined ? req.query.sort_dir : 'ASC';
        const page: number = req.query.page !== undefined ? Number(req.query.page) : undefined;
        const pageSize: number = req.query.page_size !== undefined ? Number(req.query.page_size) : undefined;
        const validSortAttr = [ 'username', 'first_name', 'last_name'];
        sortAttr = (sortAttr === undefined || sortAttr === '' || !validSortAttr.includes(sortAttr) ? 'username' : sortAttr);
        const orderBy = ` ORDER BY LOWER("${sortAttr}") ${sortDir} NULLS LAST`;
        const translations = await this.loadTranslations();
        const translationsExport = this.calculateTranslationsExportUserEnrollmentsByCourse(translations);
        if (queryId !== undefined && queryId !== '') {
            return await snowflakeDriver.paginate(undefined, orderBy, queryId, page, pageSize, DashboardTypes.COURSES, JSON.stringify(translationsExport));
        }
        const branchId: number | undefined = req.query.branch_id !== undefined ? Number(req.query.branch_id.toString()) : undefined;
        const courseId: number | undefined = req.query.course_id !== undefined ? Number(req.query.course_id.toString()) : undefined;
        const searchText = req.query.search_text !== undefined ? req.query.search_text.toString() : undefined;
        const hideDeactivatedUsers = req.query.hide_deactivated_users !== undefined ? Utils.stringToBoolean(req.query.hide_deactivated_users.toString()) : false;
        const hideDeactivatedUsersQuery = this.generateHideDeactivatedUsersQuery(hideDeactivatedUsers, TablesListAliases.LEARNING_COURSEUSER + '."iduser"', false);
        const puUsersQuery = this.generatePuUserQuery();
        const notErpUserQuery = this.generateNotErpUserQuery(TablesListAliases.LEARNING_COURSEUSER + '."iduser"');
        const usersInBranchQuery = this.generateUsersInBranchQuery(branchId, TablesListAliases.LEARNING_COURSEUSER + '."iduser"');
        const usersInBranchWithQuery = this.generateUsersInBranchWithQuery(branchId);
        const timeframe = req.query.timeframe !== undefined ? req.query.timeframe.toString() : undefined;
        const timeframeCompletion = req.query.timeframe_completion !== undefined ? req.query.timeframe_completion.toString() : undefined;
        const startDate = req.query.startDate !== undefined ? req.query.startDate.toString() : undefined;
        const endDate = req.query.endDate !== undefined ? req.query.endDate.toString() : undefined;
        const startDateCompletion = req.query.startDate_completion !== undefined ? req.query.startDate_completion.toString() : undefined;
        const endDateCompletion = req.query.endDate_completion !== undefined ? req.query.endDate_completion.toString() : undefined;
        const timeframeDateInscrQuery = this.generateTimeframeQuery(timeframe, startDate, endDate, TablesListAliases.LEARNING_COURSEUSER + '."date_inscr"');
        const timeframeDateCompleteQuery = this.generateTimeframeQuery(timeframeCompletion, startDateCompletion, endDateCompletion, TablesListAliases.LEARNING_COURSEUSER + '."date_complete"');
        const courseQuery = await this.generateCourseQuery(courseId, TablesListAliases.LEARNING_COURSEUSER);
        const searchTextQuery = this.generateSearchTextQuery(searchText);
        const query = `
                ${usersInBranchWithQuery}
                SELECT
                    SUBSTR(${TablesListAliases.CORE_USER}."userid", 2) as "username",
                    ${TablesListAliases.CORE_USER}."firstname" as "first_name",
                    ${TablesListAliases.CORE_USER}."lastname" as "last_name",
                    CAST(${TablesListAliases.LEARNING_COURSEUSER}."status" AS varchar) as "status",
                    MAX(${TablesListAliases.LEARNING_COURSEUSER}."date_inscr") as "enrollment_date",
                    SUM(DATEDIFF('second', ${TablesListAliases.LEARNING_TRACKSESSION}."entertime",${TablesListAliases.LEARNING_TRACKSESSION}."lasttime")) as "time_in_course",
                    MAX(${TablesListAliases.LEARNING_COURSEUSER}."date_last_access") as "last_access",
                    MAX(${TablesListAliases.LEARNING_COURSEUSER}."date_complete") as "completion_date",
                    MAX(${TablesListAliases.LEARNING_COURSEUSER}."score_given") as "score"
                FROM ${TablesList.LEARNING_COURSEUSER} as ${TablesListAliases.LEARNING_COURSEUSER}
                    INNER JOIN ${TablesList.CORE_USER} as ${TablesListAliases.CORE_USER} 
                    ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.LEARNING_COURSEUSER}."iduser"
                    ${hideDeactivatedUsersQuery} ${puUsersQuery}
                LEFT JOIN  ${TablesList.LEARNING_TRACKSESSION} as ${TablesListAliases.LEARNING_TRACKSESSION} 
                    ON ${TablesListAliases.LEARNING_TRACKSESSION}."iduser" = ${TablesListAliases.CORE_USER}."idst" 
                        AND ${TablesListAliases.LEARNING_TRACKSESSION}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER}."idcourse"
                WHERE TRUE
                    ${notErpUserQuery} ${usersInBranchQuery} ${timeframeDateInscrQuery} ${timeframeDateCompleteQuery} ${courseQuery} ${searchTextQuery}
                GROUP BY ${TablesListAliases.LEARNING_COURSEUSER}."iduser", ${TablesListAliases.CORE_USER}."userid",
                         ${TablesListAliases.CORE_USER}."firstname", ${TablesListAliases.CORE_USER}."lastname", 
                         ${TablesListAliases.LEARNING_COURSEUSER}."status" ${orderBy}`;

        return await snowflakeDriver.paginate(query, orderBy, undefined, page, pageSize, DashboardTypes.COURSES, JSON.stringify(translationsExport));
    }

    public async getCoursesCompletion(req: Request): Promise<CourseCompletion[]> {
        const session: SessionManager = this.session;
        const snowflakeDriver = session.getSnowflake();
        const hideDeactivatedUsers = req.query.hide_deactivated_users !== undefined ? Utils.stringToBoolean(req.query.hide_deactivated_users.toString()) : false;
        const branchId: number | undefined = req.query.branch_id !== undefined ? Number(req.query.branch_id.toString()) : undefined;
        const courseId: number | undefined = req.query.course_id !== undefined ? Number(req.query.course_id.toString()) : undefined;
        const notErpUserQuery = this.generateNotErpUserQuery();
        const hideDeactivatedUsersQuery = this.generateHideDeactivatedUsersQuery(hideDeactivatedUsers);
        const puUsersQuery = this.generatePuUserCourseQuery();
        const usersInBranchQuery = this.generateUsersInBranchQuery(branchId);
        const usersInBranchWithQuery = this.generateUsersInBranchWithQuery(branchId);
        const timeframe = req.query.timeframe !== undefined ? req.query.timeframe.toString() : undefined;
        const endDate = req.query.endDate !== undefined ? req.query.endDate.toString() : undefined;
        const startDate = req.query.startDate !== undefined ? req.query.startDate.toString() : undefined;
        const timeframeQuery = this.generateTimeframeQuery(timeframe, startDate, endDate);
        const courseQuery = await this.generateCourseQuery(courseId);
        const query = `
            ${usersInBranchWithQuery}
            SELECT 
                YEAR(${TablesListAliases.LEARNING_COURSEUSER}."date_inscr") as "year",
                MONTH(${TablesListAliases.LEARNING_COURSEUSER}."date_inscr") as "month",
                MAX(${TablesListAliases.LEARNING_COURSEUSER}."date_inscr") as "date",
                ${this.generateCourseSummaryQuery()}
            FROM ${TablesList.LEARNING_COURSE} as ${TablesListAliases.LEARNING_COURSE}
                INNER JOIN ${TablesList.LEARNING_COURSEUSER} as ${TablesListAliases.LEARNING_COURSEUSER} 
                    ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER}."idcourse"
                    ${hideDeactivatedUsersQuery} ${puUsersQuery}
            WHERE ${TablesListAliases.LEARNING_COURSEUSER}."status" > -1
                    ${notErpUserQuery} ${usersInBranchQuery} ${timeframeQuery} ${courseQuery}
            GROUP BY 
                    YEAR(${TablesListAliases.LEARNING_COURSEUSER}."date_inscr"), 
                    MONTH(${TablesListAliases.LEARNING_COURSEUSER}."date_inscr")
            HAVING YEAR(${TablesListAliases.LEARNING_COURSEUSER}."date_inscr") > 0
            ORDER BY MAX(${TablesListAliases.LEARNING_COURSEUSER}."date_inscr") ASC NULLS LAST`;

        const result = await snowflakeDriver.runQuery(query);

        return result;
    }

    public async getTopCoursesByEnrollments(req: Request): Promise<TopCourseByEnrollment[]> {
        const session: SessionManager = this.session;
        const snowflakeDriver = session.getSnowflake();
        const hideDeactivatedUsers = req.query.hide_deactivated_users !== undefined ? Utils.stringToBoolean(req.query.hide_deactivated_users.toString()) : false;
        const branchId: number | undefined = req.query.branch_id !== undefined ? Number(req.query.branch_id.toString()) : undefined;
        const notErpUserQuery = this.generateNotErpUserQuery();
        const hideDeactivatedUsersQuery = this.generateHideDeactivatedUsersQuery(hideDeactivatedUsers);
        const puUsersQuery = this.generatePuUserCourseQuery();
        const usersInBranchQuery = this.generateUsersInBranchQuery(branchId);
        const usersInBranchWithQuery = this.generateUsersInBranchWithQuery(branchId);
        const timeframe = req.query.timeframe !== undefined ? req.query.timeframe.toString() : undefined;
        const endDate = req.query.endDate !== undefined ? req.query.endDate.toString() : undefined;
        const startDate = req.query.startDate !== undefined ? req.query.startDate.toString() : undefined;
        const timeframeQuery = this.generateTimeframeQuery(timeframe, startDate, endDate);
        const translations = await this.loadTranslations();
        const withTopCourseQuery = `
            top15 AS (SELECT 
                ${TablesListAliases.LEARNING_COURSE}."idcourse" as "idcourse",
                ${TablesListAliases.LEARNING_COURSE}."code" as "code",
                ${TablesListAliases.LEARNING_COURSE}."name" as "name",
                ${this.generateCourseSummaryQuery()}
            FROM ${TablesList.LEARNING_COURSE} as ${TablesListAliases.LEARNING_COURSE}
            INNER JOIN ${TablesList.LEARNING_COURSEUSER} as ${TablesListAliases.LEARNING_COURSEUSER} 
                ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER}."idcourse"
                ${hideDeactivatedUsersQuery} ${puUsersQuery}
            WHERE ${TablesListAliases.LEARNING_COURSEUSER}."status" > -1 
                ${notErpUserQuery} ${usersInBranchQuery} ${timeframeQuery}
            GROUP BY ${TablesListAliases.LEARNING_COURSE}."idcourse", ${TablesListAliases.LEARNING_COURSE}."code", ${TablesListAliases.LEARNING_COURSE}."name" 
            ORDER BY "enrolled" DESC NULLS LAST LIMIT 15) `;
        const otherCoursesQuery = `
            SELECT -1 as "idcourse",
                null as "code",
                ${this.renderStringInQueryCase(translations[FieldTranslation.OTHER_COURSES])} as "name",
                ${this.generateCourseSummaryQuery()}
            FROM ${TablesList.LEARNING_COURSE} as ${TablesListAliases.LEARNING_COURSE}
            INNER JOIN ${TablesList.LEARNING_COURSEUSER} as ${TablesListAliases.LEARNING_COURSEUSER} 
                ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER}."idcourse"
            WHERE ${TablesListAliases.LEARNING_COURSEUSER}."status" > -1 
                ${notErpUserQuery} ${usersInBranchQuery} ${timeframeQuery}
                AND ${TablesListAliases.LEARNING_COURSE}."idcourse" not in (SELECT "idcourse" FROM top15)`;
        let query = usersInBranchWithQuery + (usersInBranchWithQuery === '' ? 'WITH ' : ', ') + withTopCourseQuery;
        query += `(SELECT * FROM top15) UNION ALL (${otherCoursesQuery})`;

        const result: TopCourseByEnrollment[] = await snowflakeDriver.runQuery(query);
        // if the last item of the array result ("other courses") is empty => there aren't other courses
        // so we remove the element from the result list
        if (Array.isArray(result) && result.length > 0 && (result[result.length - 1]?.enrolled === undefined ||  result[result.length - 1]?.enrolled === null)) {
            result.pop(); // removes last
        }
        return result;
    }

    public async getCoursesSummary(req: Request): Promise<CourseSummary> {
        const session: SessionManager = this.session;
        const snowflakeDriver = session.getSnowflake();
        const hideDeactivatedUsers = req.query.hide_deactivated_users !== undefined ? Utils.stringToBoolean(req.query.hide_deactivated_users.toString()) : false;
        const branchId: number | undefined = req.query.branch_id !== undefined ? Number(req.query.branch_id.toString()) : undefined;
        const courseId: number | undefined = req.query.course_id !== undefined ? Number(req.query.course_id.toString()) : undefined;
        const notErpUserQuery = this.generateNotErpUserQuery();
        const hideDeactivatedUsersQuery = this.generateHideDeactivatedUsersQuery(hideDeactivatedUsers);
        const puUsersQuery = this.generatePuUserCourseQuery();
        const usersInBranchQuery = this.generateUsersInBranchQuery(branchId);
        const usersInBranchWithQuery = this.generateUsersInBranchWithQuery(branchId);
        const timeframe = req.query.timeframe !== undefined ? req.query.timeframe.toString() : undefined;
        const endDate = req.query.endDate !== undefined ? req.query.endDate.toString() : undefined;
        const startDate = req.query.startDate !== undefined ? req.query.startDate.toString() : undefined;
        const timeframeQuery = this.generateTimeframeQuery(timeframe, startDate, endDate);
        const courseQuery = await this.generateCourseQuery(courseId);
        const query = `
            ${usersInBranchWithQuery}
            SELECT ${this.generateCourseSummaryQuery()}
            FROM ${TablesList.LEARNING_COURSE} as ${TablesListAliases.LEARNING_COURSE}
                INNER JOIN ${TablesList.LEARNING_COURSEUSER} as ${TablesListAliases.LEARNING_COURSEUSER} 
                    ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER}."idcourse"
                    ${hideDeactivatedUsersQuery} ${puUsersQuery}
            WHERE ${TablesListAliases.LEARNING_COURSEUSER}."status" > -1 ${notErpUserQuery} ${usersInBranchQuery} ${timeframeQuery} ${courseQuery}`;

        const result = await snowflakeDriver.runQuery(query);

        return result;
    }

    public async getReportCoursesEnrollments(req: Request): Promise<CoursesEnrollments> {
        const session: SessionManager = this.session;
        const snowflakeDriver = session.getSnowflake();
        const queryId = req.query.query_id !== undefined ? req.query.query_id.toString() : undefined;
        let sortAttr = req.query.sort_attr !== undefined ? req.query.sort_attr.toString() : undefined;
        const sortDir = req.query.sort_dir !== undefined ? req.query.sort_dir : 'ASC';
        const page: number = req.query.page !== undefined ? Number(req.query.page) : undefined;
        const pageSize: number = req.query.page_size !== undefined ? Number(req.query.page_size) : undefined;
        const validSortAttr = [ 'code', 'type', 'has_esignature_enabled', 'enrolled', 'completed', 'in_progress', 'not_started', 'name'];
        sortAttr = (sortAttr === undefined || sortAttr === '' || !validSortAttr.includes(sortAttr) ? '"enrolled"' : sortAttr === 'name' ? `LOWER ("name")` : `"${sortAttr}"`);
        const orderBy = ` ORDER BY LOWER(${sortAttr}) ${sortDir} NULLS LAST`;
        const translations = await this.loadTranslations();
        const translationsExport = this.calculateTranslationsExportCoursesEnrollments(translations);
        if (queryId !== undefined && queryId !== '') {
            return await snowflakeDriver.paginate(undefined, orderBy, queryId, page, pageSize, DashboardTypes.COURSES, JSON.stringify(translationsExport));
        }
        const branchId: number | undefined = req.query.branch_id !== undefined ? Number(req.query.branch_id.toString()) : undefined;
        const searchText = req.query.search_text !== undefined ? req.query.search_text.toString() : undefined;
        const hideDeactivatedUsers = req.query.hide_deactivated_users !== undefined ? Utils.stringToBoolean(req.query.hide_deactivated_users.toString()) : false;
        const notErpUserQuery = this.generateNotErpUserQuery();
        const hideDeactivatedUsersQuery = this.generateHideDeactivatedUsersQuery(hideDeactivatedUsers, TablesListAliases.LEARNING_COURSEUSER + '."iduser"');
        const puUsersQuery = this.generatePuUserCourseQuery();
        const usersInBranchQuery = this.generateUsersInBranchQuery(branchId);
        const usersInBranchWithQuery = this.generateUsersInBranchWithQuery(branchId);
        const timeframe = req.query.timeframe !== undefined ? req.query.timeframe.toString() : undefined;
        const timeframeCompletion = req.query.timeframe_completion !== undefined ? req.query.timeframe_completion.toString() : undefined;
        const startDate = req.query.startDate !== undefined ? req.query.startDate.toString() : undefined;
        const endDate = req.query.endDate !== undefined ? req.query.endDate.toString() : undefined;
        const startDateCompletion = req.query.startDate_completion !== undefined ? req.query.startDate_completion.toString() : undefined;
        const endDateCompletion = req.query.endDate_completion !== undefined ? req.query.endDate_completion.toString() : undefined;
        const timeframeDateInscrQuery = this.generateTimeframeQuery(timeframe, startDate, endDate, TablesListAliases.LEARNING_COURSEUSER + '."date_inscr"');
        const timeframeDateCompleteQuery = this.generateTimeframeQuery(timeframeCompletion, startDateCompletion, endDateCompletion, TablesListAliases.LEARNING_COURSEUSER + '."date_complete"');
        const searchTextQuery = this.generateSearchTextQuery(searchText, TablesListAliases.LEARNING_COURSE + '."name"');
        const selectHasSignatureEnabled =
            this.session.platform.checkPluginESignatureEnabled() ?
                `CASE WHEN ${TablesListAliases.LEARNING_COURSE}."has_esignature_enabled" = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.YES])} ELSE '' END as "has_esignature_enabled",`
                : '';
        const query = `
            ${usersInBranchWithQuery}
            SELECT  ${TablesListAliases.LEARNING_COURSE}."idcourse" as "idcourse",
                    ${TablesListAliases.LEARNING_COURSE}."name" as "name",
                    ${TablesListAliases.LEARNING_COURSE}."code" as "code",
                    ${TablesListAliases.LEARNING_COURSE}."course_type" as "type",
                    ${selectHasSignatureEnabled}
                    ${this.generateCourseSummaryQuery()}
            FROM ${TablesList.LEARNING_COURSE} as ${TablesListAliases.LEARNING_COURSE}
                    INNER JOIN ${TablesList.LEARNING_COURSEUSER} as ${TablesListAliases.LEARNING_COURSEUSER} 
                        ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.LEARNING_COURSEUSER}."idcourse"
                        ${hideDeactivatedUsersQuery} ${puUsersQuery}
            WHERE TRUE ${notErpUserQuery} ${usersInBranchQuery} ${timeframeDateInscrQuery} 
                       ${timeframeDateCompleteQuery} ${searchTextQuery}
            GROUP BY ${TablesListAliases.LEARNING_COURSE}."idcourse", ${TablesListAliases.LEARNING_COURSE}."code",
                     ${TablesListAliases.LEARNING_COURSE}."name", ${TablesListAliases.LEARNING_COURSE}."course_type" 
                     ${selectHasSignatureEnabled !== '' ? ',' + TablesListAliases.LEARNING_COURSE + '."has_esignature_enabled"' : ''} 
                     ${orderBy}`;

        return await snowflakeDriver.paginate(query, orderBy, undefined, page, pageSize, DashboardTypes.COURSES, JSON.stringify(translationsExport));
    }
}
