import { v4 } from 'uuid';
import { ReportsTypes } from '../reports/constants/report-types';
import SessionManager from '../services/session/session-manager.session';
import { SortingOptions, TextFilterOptions, VisibilityTypes } from './custom-report';
import { LegacyReport, VisibilityRule } from './migration-component';
import {
    FieldsList,
    ReportAvailablesFields,
    ReportManagerInfo,
    ReportManagerInfoVisibility,
    TablesList,
    TablesListAliases,
} from './report-manager';
import {
    CustomReportType,
    JsonCourseFilter,
    JsonDateFilter,
    JsonFilter,
    JsonTextFilter,
    JsonUserFilter
} from '../query-builder/interfaces/query-builder.interface';
import { Exception } from '../exceptions/exception';
import {
    QUERY_BUILDER_FILTER_TYPE_COURSES,
    QUERY_BUILDER_FILTER_TYPE_DATE,
    QUERY_BUILDER_FILTER_TYPE_TEXT,
    QUERY_BUILDER_FILTER_TYPE_USERS
} from '../query-builder/models/query-builder';
import { UserLevels } from '../services/session/user-manager.session';
import { Utils } from '../reports/utils';
import { BaseReportManager } from './base-report-manager';


export class QueryBuilderManager extends BaseReportManager {
    reportType = ReportsTypes.QUERY_BUILDER_DETAIL;

    private customReportType: CustomReportType | undefined;
    public constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap | undefined) {
        super(session, reportDetails);
    }

    public async loadQueryBuilder(session: SessionManager, queryBuilderId: string) {
        const queryBuilder = await session.getDynamo().getCustomReportTypesById(queryBuilderId);
        this.customReportType = queryBuilder as CustomReportType;
        this.info.fields = this.getColumnBySql(queryBuilder.sql);
    }

    protected getReportDefaultStructure(report: ReportManagerInfo, title: string, platform: string, idUser: number, description: string, queryBuilder?: CustomReportType): ReportManagerInfo {
        const id = v4();
        const date = new Date();

        /**
         * Report's infos
         */
        report.idReport = id;
        report.queryBuilderId = queryBuilder.id;
        report.queryBuilderName = queryBuilder.name;
        report.author = idUser;
        report.creationDate = this.convertDateObjectToDatetime(date);
        report.platform = platform;
        report.standard = false;
        report.fields = this.getColumnBySql(queryBuilder.sql);

        /**
         * Filters Tab
         */
        report.queryBuilderFilters = {};

        if (queryBuilder.json) {
            const jsonObject = JSON.parse(queryBuilder.json);
            Object.keys(jsonObject).forEach(filterName => {
                const filter = jsonObject[filterName] as JsonFilter;
                if (filter.type === QUERY_BUILDER_FILTER_TYPE_USERS) {
                    report.queryBuilderFilters[filterName] = new JsonUserFilter();
                } else if (filter.type === QUERY_BUILDER_FILTER_TYPE_COURSES) {
                    report.queryBuilderFilters[filterName] = new JsonCourseFilter();
                } else if (filter.type === QUERY_BUILDER_FILTER_TYPE_DATE) {
                    report.queryBuilderFilters[filterName] = new JsonDateFilter(filter.description);
                } else if (filter.type === QUERY_BUILDER_FILTER_TYPE_TEXT) {
                    report.queryBuilderFilters[filterName] = new JsonTextFilter(filter.description);
                }
            });
        }

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

        const sql = this.customReportType?.sql as string;

        result.customFields = [];

        const arrayColumn = this.getColumnBySql(sql);
        arrayColumn.forEach((element: string) => {
            result.customFields?.push({
                field: element,
                idLabel: element,
                mandatory: true,
                isAdditionalField: false,
                translation: element
            });
        });


        return result;
    }

    public getBaseAvailableFields(): Promise<ReportAvailablesFields> {
        throw new Error('Method not implemented.');
    }

    /**
     * Parse the sql in order to retrieve the columns name
     * @param sql
     * @private
     */
    private getColumnBySql(sql: string) {
        // sanitize the sql string
        sql = sql.replace(/\r?\n|\r/g, ' ');

        const regex = new RegExp('select[\t ](.*?)[\t ]from[\t ].*', 'gmi');
        const result: string[] = [];
        const resultRegexSql = regex.exec(<string>sql);
        let arrayColumn: string[] = [];
        if (resultRegexSql) {
            arrayColumn = resultRegexSql[1].split(',');
        }

        arrayColumn.forEach((element: string, index: number) => {
            // if not include " or the row length is lower than 4 characters skip the line
            if (!element.toUpperCase().includes('"') || element.trim().length < 4) {
                return; // skip
            }
            let column = '';
            if (element.toUpperCase().includes('AS "')) {
                const regex = new RegExp('as "(.*?)"', 'gmi');
                const resultRegexSql = regex.exec(element);

                if (resultRegexSql) {
                    column = resultRegexSql[1];
                }
            } else if (element.toUpperCase().includes('ARBITRARY') || element.toUpperCase().includes('CAST')) {
                column = '_col' + index;
            } else if (element.includes('.')) {
                column = element.split('.')[1];
            } else {
                column = element;
            }
            result.push(column.trim());
        });

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

    public async getQuery(limit = 0, isPreview: boolean): Promise<string> {
        let sqlString = this.customReportType.sql;
        const userExtraFields = await this.session.getHydra().getUserExtraFields();

        if (this.customReportType.json) {
            const jsonObject = JSON.parse(this.customReportType.json);
            for (const filterName of Object.keys(jsonObject)) {
                const filter = jsonObject[filterName] as JsonFilter;
                if (filter.type === QUERY_BUILDER_FILTER_TYPE_USERS) {
                    const userFilter = this.info.queryBuilderFilters[`${filterName}`] as JsonUserFilter;
                    const userSubQuery = await this.getSubQueryByUserFiler(userFilter);

                    sqlString = sqlString.replace(`{${filterName}}`, `${filter.field} in (${userSubQuery})`);
                } else if (filter.type === QUERY_BUILDER_FILTER_TYPE_COURSES) {
                    const jsonCourseFilter = this.info.queryBuilderFilters[`${filterName}`] as JsonCourseFilter;
                    const courseSubQuery = await this.calculateCourseFilter(false, false, true, jsonCourseFilter);

                   sqlString = sqlString.replace(`{${filterName}}`, `${filter.field} IN (${courseSubQuery})`);
                } else if (filter.type === QUERY_BUILDER_FILTER_TYPE_DATE) {
                    const jsonDateFilter = this.info.queryBuilderFilters[`${filterName}`] as JsonDateFilter;

                    let dateQuery = this.buildDateFilter(filter.field, jsonDateFilter.filterConfiguration, '', true).trim();

                    if (dateQuery === '') { // no filter selected
                        dateQuery = 'TRUE';
                    }

                    sqlString = sqlString.replace(`{${filterName}}`, `(${dateQuery})`);
                } else if (filter.type === QUERY_BUILDER_FILTER_TYPE_TEXT) {
                    const jsonTextFilter = this.info.queryBuilderFilters[`${filterName}`] as JsonTextFilter;
                    // if the caseInsensitive property is not defined is insensitive by default
                    if (typeof filter.caseInsensitive === 'undefined') {
                        filter.caseInsensitive = true;
                    }
                    // The user can define the caseInsensitive property as "true", true, 1 ...
                    const textQuery = this.buildTextFilter(jsonTextFilter, filter.field, Utils.stringToBoolean(filter.caseInsensitive.toString()));

                    sqlString = sqlString.replace(`{${filterName}}`, `(${textQuery})`);
                }
            }
        }

        if (sqlString && limit > 0) {
            sqlString = 'SELECT * FROM (' + sqlString + ') LIMIT ' + limit;
        }
        return sqlString;
    }


    public async getQuerySnowflake(limit = 0, isPreview: boolean, checkPuVisibility = true, fromSchedule = false): Promise<string> {
        let sqlString = this.customReportType.sql;
        const queryHelper = {
            from: [],
            join: [],
            userAdditionalFieldsSelect: [],
            userAdditionalFieldsFrom: [],
            userAdditionalFieldsId: [],
        };
        if (this.customReportType.json) {
            const jsonObject = JSON.parse(this.customReportType.json);
            for (const filterName of Object.keys(jsonObject)) {
                const filter = jsonObject[filterName] as JsonFilter;
                let replaceValue = '';
                const field = this.convertToDatalakeV3(filter.field);
                switch (filter.type) {
                    case QUERY_BUILDER_FILTER_TYPE_USERS:
                        const userFilter = this.info.queryBuilderFilters[`${filterName}`] as JsonUserFilter;
                        const userSubQuery = await this.getSubQueryByUserFilerSnowflake(queryHelper, userFilter);
                        replaceValue = `${field} in (${userSubQuery})`;
                        break;
                    case QUERY_BUILDER_FILTER_TYPE_COURSES:
                        const jsonCourseFilter = this.info.queryBuilderFilters[`${filterName}`] as JsonCourseFilter;
                        const courseSubQuery = await this.calculateCourseFilterSnowflake(false, false, true, jsonCourseFilter);
                        replaceValue = `${field} in (${courseSubQuery})`;
                        break;
                    case QUERY_BUILDER_FILTER_TYPE_DATE:
                        const jsonDateFilter = this.info.queryBuilderFilters[`${filterName}`] as JsonDateFilter;
                        const dateQuery = this.buildDateFilter(field, jsonDateFilter.filterConfiguration, '', true).trim();
                        replaceValue = (dateQuery === '' ? 'TRUE' : dateQuery);
                        break;
                    case QUERY_BUILDER_FILTER_TYPE_TEXT:
                        const jsonTextFilter = this.info.queryBuilderFilters[`${filterName}`] as JsonTextFilter;
                        // if the caseInsensitive property is not defined is insensitive by default
                        if (typeof filter.caseInsensitive === 'undefined') {
                            filter.caseInsensitive = true;
                        }
                        // The user can define the caseInsensitive property as "true", true, 1 ...
                        replaceValue = this.buildTextFilter(jsonTextFilter, field, Utils.stringToBoolean(filter.caseInsensitive.toString()));
                        break;
                }
                sqlString = sqlString.replace(`{${filterName}}`, `(${replaceValue})`);
            }
        }
        if (sqlString && limit > 0) {
            sqlString = `SELECT * FROM (${sqlString}) LIMIT ${limit}`;
        }
        return (sqlString && queryHelper.userAdditionalFieldsId.length > 0)
            ? `WITH ${this.additionalFieldQueryWith(queryHelper.userAdditionalFieldsFrom, queryHelper.userAdditionalFieldsSelect, queryHelper.userAdditionalFieldsId, 'id_user', TablesList.CORE_USER_FIELD_VALUE_WITH, TablesList.USERS_ADDITIONAL_FIELDS_TRANSLATIONS)} ${sqlString}`
            : sqlString;
    }

    buildTextFilter (jsonTextFilter: JsonTextFilter, field: string, insensitive: boolean): string {
        if (jsonTextFilter.any === true) {
            return 'TRUE';
        }

        switch (jsonTextFilter.operator) {
            case TextFilterOptions.like:
                const value = `%${jsonTextFilter.value}%`;
                if (insensitive === true) {
                    return `UPPER(${field}) LIKE UPPER(${this.renderStringInQueryCase(value)})`;
                } else {
                    return `${field} LIKE ${this.renderStringInQueryCase(value)}`;
                }
            case TextFilterOptions.equals:
                return `${field} = ${this.renderStringInQueryCase(jsonTextFilter.value)}`;
            case TextFilterOptions.notEquals:
                return `${field} != ${this.renderStringInQueryCase(jsonTextFilter.value)}`;
            case TextFilterOptions.isEmpty:
                return `${field} = '' OR ${field} IS NULL`;
            default:
                throw new Error(`Operator '${jsonTextFilter.operator}' not supported. Report Id: ${this.info.idReport}`);
        }
    }

    async getSubQueryByUserFiler(userFilter: JsonUserFilter): Promise<string> {
        const userExtraFields = await this.session.getHydra().getUserExtraFields();

        const allUsers = userFilter.all;
        const hideDeactivated = userFilter.hideDeactivated;
        const hideExpiredUsers = userFilter.hideExpiredUsers;
        let fullUsers = '';

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilter(true, userFilter);
        }

        let table = `SELECT idst
                     FROM ${TablesList.CORE_USER} `;
        let where = hideDeactivated ? `valid ${this.getCheckIsValidFieldClause()}` : 'true';

        if (fullUsers !== '') {
            where += ` AND idst IN (${fullUsers})`;
        }

        if (hideExpiredUsers) {
            where += ` AND (expiration IS NULL OR expiration > NOW())`;
        }

        if (userFilter.userAdditionalFieldsFilter && userFilter.isUserAddFields) {
            const tmp = await this.getAdditionalFieldsFilters(userExtraFields, userFilter);
            if (tmp.length) {
                table += `LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = idst`;
                where += ' ' + tmp.join(' ');
            }
        }

        // return the sub-query for the user filter
        return `${table} WHERE ${where}`;
    }

    async getSubQueryByUserFilerSnowflake(queryHelper: any, userFilter: JsonUserFilter): Promise<string> {
        const allUsers = userFilter.all;
        const hideDeactivated = userFilter.hideDeactivated;
        const hideExpiredUsers = userFilter.hideExpiredUsers;
        let fullUsers = '';
        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilterSnowflake(true, userFilter);
        }
        let table = `SELECT "idst" FROM ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER} `;
        let where = hideDeactivated ? `"valid" = 1` : 'true';
        if (fullUsers !== '') {
            where += ` AND "idst" IN (${fullUsers})`;
        }
        if (hideExpiredUsers) {
            where += ` AND ("expiration" IS NULL OR "expiration" > current_timestamp())`;
        }
        if (userFilter.userAdditionalFieldsFilter && userFilter.isUserAddFields) {
            const userExtraFields = await this.session.getHydra().getUserExtraFields();
            const tmp = this.getAdditionalUsersFieldsFiltersSnowflakeQueryBuilder(queryHelper, userExtraFields, userFilter);
            if (tmp.length) {
                table += `${queryHelper.from.join('')}`;
                where += ' ' + tmp.join(' ');
            }
        }
        // return the sub-query for the user filter
        return `${table} WHERE ${where}`;
    }

    parseLegacyReport(legacyReport: LegacyReport, platform: string, visibilityRules: VisibilityRule[]): ReportManagerInfo {
       throw new Exception('Function not implemented');
    }
}
