import {
    FieldsList,
    ReportAvailablesFields,
    ReportManagerInfo,
    ReportManagerInfoResponse,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility,
    ReportManagerLearningPlansFilter, TablesList, TablesListAliases
} from './report-manager';
import { DateOptions, SortingOptions, TimeFrameOptions, VisibilityTypes } from './custom-report';
import { LegacyReport, VisibilityRule } from './migration-component';
import SessionManager from '../services/session/session-manager.session';
import { v4 } from 'uuid';
import { ReportsTypes } from '../reports/constants/report-types';
import { BaseReportManager } from './base-report-manager';
import { Exception } from '../exceptions/exception';
import { UserLevels } from '../services/session/user-manager.session';
import { EnrollmentStatuses } from './base';

export class LearningplansUsersStatisticsManager extends BaseReportManager {
    reportType = ReportsTypes.LP_USERS_STATISTICS;
    allFields = {
        lp: [
            FieldsList.LP_NAME,
            FieldsList.LP_CODE,
            FieldsList.LP_CREDITS,
            FieldsList.LP_UUID,
            FieldsList.LP_LAST_EDIT,
            FieldsList.LP_CREATION_DATE,
            FieldsList.LP_DESCRIPTION,
            FieldsList.LP_ASSOCIATED_COURSES,
            FieldsList.LP_MANDATORY_ASSOCIATED_COURSES,
            FieldsList.LP_STATUS,
            FieldsList.LP_LANGUAGE,
        ],
        learningPlansStatistics: [
            FieldsList.STATS_PATH_COMPLETED_USERS,
            FieldsList.STATS_PATH_COMPLETED_USERS_PERCENTAGE,
            FieldsList.STATS_PATH_IN_PROGRESS_USERS,
            FieldsList.STATS_PATH_IN_PROGRESS_USERS_PERCENTAGE,
            FieldsList.STATS_PATH_NOT_STARTED_USERS,
            FieldsList.STATS_PATH_NOT_STARTED_USERS_PERCENTAGE,
            FieldsList.STATS_PATH_ENROLLED_USERS,
        ],
    };
    mandatoryFields = {
        lp: [
            FieldsList.LP_NAME
        ],
    };

    public constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap | undefined) {
        super(session, reportDetails);
    }

    async getAvailablesFields(): Promise<ReportAvailablesFields> {
        const result: ReportAvailablesFields = await this.getBaseAvailableFields();
        const lpExtraFields = await this.getAvailableLearningPlanExtraFields();
        result.learningPlans.push(...lpExtraFields);

        return result;
    }

    public async getBaseAvailableFields(): Promise<ReportAvailablesFields> {
        const result: ReportAvailablesFields = {};
        const translations = await this.loadTranslations(true);
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
        // Recover Learning Plans Statistics fields
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

        return result;
    }

    public async getQuery(limit = 0, isPreview: boolean, checkPuVisibility = true): Promise<string> {
        throw new Error('Method not implemented.');
    }

    public async getQuerySnowflake(limit = 0, isPreview: boolean, checkPuVisibility = true, fromSchedule = false): Promise<string> {
        const translations = await this.loadTranslations();
        const queryHelper = {
            select: [],
            from: [],
            join: [],
            cte: [],
            groupBy: [`${TablesListAliases.LEARNING_COURSEPATH}."id_path"`],
            lpAdditionalFieldsSelect: [],
            lpAdditionalFieldsFrom: [],
            lpAdditionalFieldsIds: [],
            translations,
        };
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
            allLPs = false;
            lpInCondition = this.getLPSubQuery(this.session.user.getIdUser(), fullLPs);
        }
        const filterPath = !allLPs && lpInCondition !== '' ? ` AND "id_path" IN (${lpInCondition})` : '';
        const filterUsers = fullUsers !== '' ? ` AND "iduser" IN (${fullUsers})` : '';
        let table = `SELECT * FROM ${TablesList.LEARNING_COURSEPATH} WHERE TRUE ${filterPath}`;
        queryHelper.from.push(`(${table}) AS ${TablesListAliases.LEARNING_COURSEPATH}`);
        table = `SELECT * FROM ${TablesList.LEARNING_COURSEPATH_USER} WHERE TRUE ${filterPath} ${filterUsers}`;
        // LP Enrollment
        table += this.composeDateOptionsFilter('date_assign', '');
        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.LEARNING_COURSEPATH_USER} ON ${TablesListAliases.LEARNING_COURSEPATH}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
        // JOIN CORE USER
        table = `SELECT "idst" FROM ${TablesList.CORE_USER} WHERE `;
        table += `"userid" <> '/Anonymous'`;
        table += hideDeactivated ? ` AND "valid" = 1 ` : '';
        if (hideExpiredUsers) {
            table += ` AND ("expiration" IS NULL OR "expiration" > current_timestamp())`;
        }
        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser"`);
        this.filterByCompletionDateMandatoryCourses(queryHelper, allLPs, lpInCondition, fullUsers);
        // User additional field filter
        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            this.getAdditionalUsersFieldsFiltersSnowflake(queryHelper, userExtraFields);
        }
        const lpExtraFields = await this.session.getHydra().getLearningPlanExtraFields();
        this.updateExtraFieldsDuplicated(lpExtraFields.data.items, translations, 'lp');
        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged =
                    this.querySelectLPFields(field, queryHelper) ||
                    this.querySelectLpUsageStatisticsFields(field, queryHelper) ||
                    this.queryWithLpAdditionalFields(field, queryHelper, lpExtraFields);
            }
        }
        // Add Query with when LP additional fields are in the view options
        if (queryHelper.lpAdditionalFieldsIds.length > 0) {
            queryHelper.cte.push(this.additionalLpFieldQueryWith(queryHelper.lpAdditionalFieldsIds));
            queryHelper.cte.push(this.additionalFieldQueryWith(queryHelper.lpAdditionalFieldsFrom, queryHelper.lpAdditionalFieldsSelect, queryHelper.lpAdditionalFieldsIds, 'id_path', TablesList.LEARNING_COURSEPATH_FIELD_VALUE_WITH, TablesList.LEARNING_PLAN_ADDITIONAL_FIELDS_TRANSLATIONS));
        }
        let query = `
            ${queryHelper.cte.length > 0 ? `WITH ${queryHelper.cte.join(', ')}` : ''}
            SELECT ${queryHelper.select.join(', ')}
            FROM ${queryHelper.from.join(' ')}
            GROUP BY ${queryHelper.groupBy.join(', ')}`;
        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(queryHelper.select, translations, {user: [], course: [], userCourse: [], webinar: [], classroom: [], learningPlan: lpExtraFields.data.items}, fromSchedule);
        }
        // get the sql like LIMIT for the query
        query += this.getQueryExportLimit(limit);

        return query;
    }

    private filterByCompletionDateMandatoryCourses(queryHelper: any, allLPs: boolean, lpInCondition: string, fullUsers: string): void {
        if (!this.info.completionDate) {
            return;
        }
        const completionDateFilter = this.buildDateFilter(`MAX(${TablesListAliases.LEARNING_COURSEUSER}."date_complete")`, this.info.completionDate, 'AND', true);
        if (completionDateFilter === '') {
            return;
        }
        const {from, cte} = queryHelper;
        cte.push(`${TablesList.LEARNING_COURSEPATH_COURSESUSER_MANDATORY_COMPLETE_WITH} AS (
            SELECT
                ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path" AS "id_path",
                ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser" AS "iduser"
            FROM ${TablesList.LEARNING_COURSEPATH_USER} AS ${TablesListAliases.LEARNING_COURSEPATH_USER}
                JOIN ${TablesList.LEARNING_COURSEPATH_COURSES} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES}
                    ON ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_path"
                JOIN ${TablesList.LEARNING_COURSEUSER} AS ${TablesListAliases.LEARNING_COURSEUSER}
                    ON ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser" = ${TablesListAliases.LEARNING_COURSEUSER}."iduser"
                           AND ${TablesListAliases.LEARNING_COURSEUSER}."idcourse" = ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_item"
                           AND ${TablesListAliases.LEARNING_COURSEUSER}."status" = ${EnrollmentStatuses.Completed}
                JOIN ${TablesList.LEARNING_COURSEPATH_COURSES_COUNT} AS ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}
                    ON ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."id_path"
            WHERE (${TablesListAliases.LEARNING_COURSEPATH_COURSES}."is_required" = 1 OR ${TablesListAliases.LEARNING_COURSEPATH_COURSES}."is_required" IS NULL)
                    ${!allLPs && lpInCondition !== '' ? ` AND ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path" IN (${lpInCondition})` : ''}
                    ${fullUsers !== '' ? ` AND ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser" IN (${fullUsers})` : ''}
            GROUP BY ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser",
                     ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path",
                     ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."coursesMandatory"
            HAVING ${TablesListAliases.LEARNING_COURSEPATH_COURSES_COUNT}."coursesMandatory" = COUNT(DISTINCT ${TablesListAliases.LEARNING_COURSEUSER}."idcourse")
                ${completionDateFilter})`);
        from.push(`JOIN (${TablesList.LEARNING_COURSEPATH_COURSESUSER_MANDATORY_COMPLETE_WITH}) AS ${TablesListAliases.LEARNING_COURSEPATH_COURSESUSER_MANDATORY_COMPLETE_WITH}
            ON ${TablesListAliases.LEARNING_COURSEPATH_COURSESUSER_MANDATORY_COMPLETE_WITH}."iduser" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."iduser"
               AND ${TablesListAliases.LEARNING_COURSEPATH_COURSESUSER_MANDATORY_COMPLETE_WITH}."id_path" = ${TablesListAliases.LEARNING_COURSEPATH_USER}."id_path"`);
    }

    protected getReportDefaultStructure(report: ReportManagerInfo, title: string, platform: string, idUser: number, description: string): ReportManagerInfo {
        const id = v4();
        const date = new Date();
        const tmpFields: string[] = [];
        this.mandatoryFields.lp.forEach(element => {
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

        return report;
    }

    protected getSortingOptions(): SortingOptions {
        return {
            selector: 'default',
            selectedField: FieldsList.LP_NAME,
            orderBy: 'asc',
        };
    }

    parseLegacyReport(legacyReport: LegacyReport, platform: string, visibilityRules: VisibilityRule[]): ReportManagerInfo {
        throw new Exception('Function not implemented');
    }

    setSortingOptions(item: SortingOptions): void {
        this.info.sortingOptions = {
            selector: item && item.selector ? item.selector : 'default',
            selectedField: item && item.selectedField ? item.selectedField : FieldsList.LP_NAME,
            orderBy: item && item.orderBy ? item.orderBy : 'asc'
        };
    }
}
