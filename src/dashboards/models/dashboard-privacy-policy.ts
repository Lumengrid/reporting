import { BaseDashboardManager } from '../manager/base-dashboard-manager';
import SessionManager from '../../services/session/session-manager.session';
import { TablesList, TablesListAliases } from '../../models/report-manager';
import { Request } from 'express';
import {
    ExportTranslation,
    PrivacyBarChart,
    PrivacyCharts,
    PrivacyFilter,
    PrivacyFilters
} from '../interfaces/dashboard.interface';
import { DashboardTypes, FieldTranslation, FilterOperation } from '../constants/dashboard-types';
import { Utils } from '../../reports/utils';


export class DashboardPrivacyPolicy extends BaseDashboardManager {
    translatableFields = [
        FieldTranslation.ACCEPTANCE_DATE,
        FieldTranslation.DASHBOARDS_CURRENT_VERSION,
        FieldTranslation.DOMAIN,
        FieldTranslation.EMAIL,
        FieldTranslation.FIRST_NAME,
        FieldTranslation.LAST_LOGIN,
        FieldTranslation.LAST_NAME,
        FieldTranslation.NO,
        FieldTranslation.NO_ANSWER,
        FieldTranslation.POLICY_ACCEPTED,
        FieldTranslation.POLICY_NAME,
        FieldTranslation.TRACK_ID,
        FieldTranslation.USER_ID,
        FieldTranslation.USERNAME,
        FieldTranslation.VERSION,
        FieldTranslation.VERSION_ID,
        FieldTranslation.YES,
    ];
    YES = 'yes';
    NO = 'no';
    NO_ANSWER = 'no answer';

    private defineActionByCondition(column: string, filter: PrivacyFilter): string {
        if (filter.value === undefined) {
            return '';
        }
        const value = filter.value;
        switch (filter.option) {
            case FilterOperation.CONTAINS:
                return `${column} LIKE '%${value}%'`;
            case FilterOperation.LIKE:
                return `${column} LIKE '${value}'`;
            case FilterOperation.EQUAL:
                if (typeof value === 'string' || value instanceof String) {
                    return `${column} = '${value}'`;
                } else {
                    return `${column} = ${value}`;
                }
            case FilterOperation.NOT_EQUAL:
            case FilterOperation.NOT_EQUAL_V2:
                if (typeof value === 'string' || value instanceof String) {
                    return `(${column} <> '${value}' OR ${column} IS NULL)`;
                } else {
                    return `(${column} <> ${value} OR ${column} IS NULL)`;
                }
            case FilterOperation.ENDS_WITH:
                if (value !== '') {
                    return `${column} LIKE '%${value}'`;
                }
                break;
            case FilterOperation.STARTS_WITH:
                if (value !== '') {
                    return `${column} LIKE '${value}%'`;
                }
                break;
            case FilterOperation.IS_EMPTY:
                return `(TRIM(${column}) LIKE '' OR ${column} IS NULL)`;
            case FilterOperation.NOT_EMPTY:
                return `(TRIM(${column}) NOT LIKE '')`;
            case FilterOperation.NOT_START_WITH:
                if (value !== '') {
                    return `(${column} NOT LIKE '${value}%' OR ${column} IS NULL)`;
                }
                break;
            case FilterOperation.NOT_END_WITH:
                if (value !== '') {
                    return `(${column} NOT LIKE '%${value}' OR ${column} IS NULL)`;
                }
                break;
            case FilterOperation.NOT_CONTAINS:
                if (value !== '' || value === 0) {
                    return `(${column} NOT LIKE '%${value}%' OR ${column} IS NULL)`;
                }
                break;
            case FilterOperation.GREATER:
                return `${column} > ${value}`;
            case FilterOperation.GREATER_EQUAL:
                return `${column} >= ${value}`;
            case FilterOperation.LESSER:
                return `${column} < ${value}`;
            case FilterOperation.LESSER_EQUAL:
                return `${column} <= ${value}`;
            default:
                return '';
        }
    }


    private getPrivacyClauseByFilter (filters: PrivacyFilters[]): string {
        const actions = [];
        let conditions = [];
        let column = '';
        const policiesAccepted = filters.filter((a) => a.policy_accepted !== undefined);
        const policiesNames = filters.filter((a) => a.policy_name !== undefined);
        const usernames = filters.filter((a) => a.username !== undefined);
        const versions = filters.filter((a) => a.version !== undefined);
        if (policiesAccepted !== undefined && policiesAccepted.length > 0) {
            column = `IFF (${TablesListAliases.TC_POLICY_TRACK}."id" IS NULL AND ${TablesListAliases.TC_POLICY_VERSIONS}."id_policy" = 1 
                    AND ${TablesListAliases.TC_POLICY_VERSIONS}."id" = 1 AND ${TablesListAliases.CORE_USER}."register_date" < ${TablesListAliases.TC_POLICY_VERSIONS}."creation_date",
                        'yes',
                        CASE ${TablesListAliases.TC_POLICY_TRACK}."answer"
                            WHEN 1 THEN '${this.YES}' 
                            WHEN 0 THEN '${this.NO}'
                            ELSE '${this.NO_ANSWER}'
                        END
                        )`;
            policiesAccepted.forEach(item => {
                conditions.push(this.defineActionByCondition(column, item.policy_accepted));
            });
            if (conditions.filter((c) => c !== '').length > 0) {
                actions.push('(' + conditions.filter((c) => c !== '').map((c) => `(${c})`).join(' OR ') + ')');
            }
        }
        if (policiesNames !== undefined && policiesNames.length > 0) {
            conditions = [];
            column = `IFF(
                        JSON_EXTRACT_PATH_TEXT(${TablesListAliases.TC_POLICIES}."name", '"${this.session.user.getLangCode()}"') IS NULL, 
                        JSON_EXTRACT_PATH_TEXT(${TablesListAliases.TC_POLICIES}."name", '"${this.session.platform.getDefaultLanguageCode()}"'), 
                        JSON_EXTRACT_PATH_TEXT(${TablesListAliases.TC_POLICIES}."name", '"${this.session.user.getLangCode()}"')
                      )`;
            policiesNames.forEach(item => {
                conditions.push(this.defineActionByCondition(column, item.policy_name));
            });
            if (conditions.filter((c) => c !== '').length > 0) {
                actions.push('(' + conditions.filter((c) => c !== '').map((c) => `(${c})`).join(' OR ') + ')');
            }
        }
        if (usernames !== undefined && usernames.length > 0) {
            conditions = [];
            column = `SUBSTR((${TablesListAliases.CORE_USER}."userid"), 2)`;
            usernames.forEach(item => {
                conditions.push(this.defineActionByCondition(column, item.username));
            });
            if (conditions.filter((c) => c !== '').length > 0) {
                actions.push('(' + conditions.filter((c) => c !== '').map((c) => `(${c})`).join(' OR ') + ')');
            }
        }
        if (versions !== undefined && versions.length > 0) {
            conditions = [];
            versions.forEach(item => {
                column = item.version.value !== undefined && item.version.value !== '' ? `CAST (${TablesListAliases.TC_POLICY_VERSIONS}."id" as VARCHAR)` : `${TablesListAliases.TC_POLICY_VERSIONS}."id"`;
                conditions.push(this.defineActionByCondition(column, item.version));
            });
            if (conditions.filter((c) => c !== '').length > 0) {
                actions.push('(' + conditions.filter((c) => c !== '').map((c) => `(${c})`).join(' OR ') + ')');
            }
        }
        return actions.filter(action => action !== '').map(action => ' AND ' + action).join('');
    }

    private getPolicyAcceptedTranslated(translations): string {
        return `
            CASE
                WHEN "policy_accepted" = ${this.renderStringInQueryCase(this.YES)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}
                WHEN "policy_accepted" = ${this.renderStringInQueryCase(this.NO)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.NO])}
                WHEN "policy_accepted" = ${this.renderStringInQueryCase(this.NO_ANSWER)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.NO_ANSWER])}
                ELSE CAST("policy_accepted" AS varchar)
            END
        `;
    }

    private calculateTranslationsExportUsers(translations, allFields: boolean): ExportTranslation[] {
        const data: ExportTranslation[] = [
            {
                column: '"username"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.USERNAME]),
            },
            {
                column: '"policy_name"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.POLICY_NAME]),
            },
            {
                column: '"version"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.VERSION]),
            },
            {
                column: '"last_login"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.LAST_LOGIN]),
            },
            {
                column: '"domain"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.DOMAIN]),
            },
            {
                column: '"policy_accepted"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.POLICY_ACCEPTED]),
                valuesOverride: this.getPolicyAcceptedTranslated(translations),
            },
            {
                column: '"acceptance_date"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.ACCEPTANCE_DATE]),
            },
        ];
        if (allFields) {
            data.push(...[
                {
                    column: '"answer_sub_policy_1"',
                },
                {
                    column: '"answer_sub_policy_2"',
                },
                {
                    column: '"answer_sub_policy_3"',
                }]);
        }
        // no fe fields
        data.push(...[
            {
                column: '"user_id"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.USER_ID]),
            },
            {
                column: '"firstname"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.FIRST_NAME]),
            },
            {
                column: '"lastname"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.LAST_NAME]),
            },
            {
                column: '"email"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.EMAIL]),
            },
            {
                column: '"track_id"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.TRACK_ID]),
            },

            {
                column: '"version_id"',
                translation: this.renderStringInQuerySelect(translations[FieldTranslation.VERSION_ID]),
            }]);
        return data;
    }

    public async getUsers (req: Request, countOnly = false): Promise<any> {
        const session: SessionManager = this.session;
        const snowflakeDriver = session.getSnowflake();
        const translations = await this.loadTranslations();

        const currentVersionOnly: boolean = req.query.current_version_only !== undefined ? Utils.stringToBoolean(req.query.current_version_only.toString()) : false;
        const allFields: boolean = req.query.all_fields !== undefined ? Utils.stringToBoolean(req.query.all_fields.toString()) : false;
        const hideDeactivatedUser: boolean = req.query.hide_deactivated_user !== undefined ? Utils.stringToBoolean(req.query.hide_deactivated_user.toString()) : false;
        const branchId: number = req.query.branch_id !== undefined ? Number(req.query.branch_id) : undefined;
        const selectionStatus: number = req.query.selection_status !== undefined ? Number(req.query.selection_status) : 1;
        const filters: PrivacyFilters[] = req.query.filters !== undefined ? JSON.parse(req.query.filters.toString()) : undefined;
        const userIds = req.query.user_ids !== undefined ? [req.query.user_ids.toString()] : undefined;
        const multiDomainIds = req.query.multidomain_ids !== undefined ? [req.query.multidomain_ids.toString()] : undefined;
        const isMultiDomainActive: boolean = session.platform.checkPluginMultiDomainEnabled();
        const url: string = req.hostname;
        // pagination parameters
        const sortDir = req.query.sort_dir;
        const sortAttr = req.query.sort_attr;
        const page = req.query.page !== undefined ? Number(req.query.page) : undefined;
        const pageSize = req.query.page_size !== undefined ? Number(req.query.page_size) : undefined;
        const queryId = req.query.query_id !== undefined ? req.query.query_id.toString() : undefined;
        const translationsExport = this.calculateTranslationsExportUsers(translations, allFields);
        let orderByClause = '"version" DESC, "user_id" DESC';
        switch (sortAttr) {
            case 'user_id':
            case 'lastname':
            case 'firstname':
            case 'email':
            case 'last_login':
            case 'policy_accepted':
            case 'acceptance_date':
            case 'domain':
            case 'version':
            case 'policy_name':
            case 'username':
                orderByClause = `LOWER(${this.renderStringInQuerySelect(sortAttr)}) ${sortDir ?? 'asc'}, ${orderByClause}`;
        }

        orderByClause = ` ORDER BY ${orderByClause} `;


        if (queryId !== undefined && !countOnly) {
            return await snowflakeDriver.paginate(undefined, orderByClause, queryId, page, pageSize, DashboardTypes.PRIVACY_POLICIES, JSON.stringify(translationsExport));
        }

        const selectVersionQuery = `
        CASE
            WHEN ${TablesListAliases.TC_POLICY_VERSIONS}."id" = (SELECT MAX("id") FROM ${TablesList.TC_POLICY_VERSIONS} AS ${TablesListAliases.TC_POLICY_VERSIONS}_2 WHERE ${TablesListAliases.TC_POLICY_VERSIONS}_2."id_policy" = ${TablesListAliases.TC_POLICY_VERSIONS}."id_policy")
            THEN CONCAT(CAST(ANY_VALUE(${TablesListAliases.TC_POLICY_VERSIONS}."version") AS VARCHAR), ' ', ${this.renderStringInQueryCase(translations[FieldTranslation.DASHBOARDS_CURRENT_VERSION])})
            ELSE CAST(ANY_VALUE(${TablesListAliases.TC_POLICY_VERSIONS}."version") AS VARCHAR)
        END AS "version"`;

        const selectPolicyNameQuery =
            `IFF(JSON_EXTRACT_PATH_TEXT(ANY_VALUE(${TablesListAliases.TC_POLICIES}."name"), '"${session.user.getLangCode()}"') IS NULL, JSON_EXTRACT_PATH_TEXT(ANY_VALUE(${TablesListAliases.TC_POLICIES}."name"), '"${session.platform.getDefaultLanguageCode()}"'), JSON_EXTRACT_PATH_TEXT(ANY_VALUE(${TablesListAliases.TC_POLICIES}."name"), '"${session.user.getLangCode()}"')) AS "policy_name"`;
        const selectUsernameQuery = `SUBSTR(ANY_VALUE(${TablesListAliases.CORE_USER}."userid"), 2) as "username"`;

        const policyAcceptedQuery = `
        IFF(ANY_VALUE(${TablesListAliases.TC_POLICY_TRACK}."id") IS NULL AND ANY_VALUE(${TablesListAliases.TC_POLICY_VERSIONS}."id_policy") = 1 AND ${TablesListAliases.TC_POLICY_VERSIONS}."id" = 1 AND ANY_VALUE(${TablesListAliases.CORE_USER}."register_date") < ANY_VALUE(${TablesListAliases.TC_POLICY_VERSIONS}."creation_date"), '${this.YES}',
        (CASE
            WHEN ANY_VALUE(${TablesListAliases.TC_POLICY_TRACK}."answer") = 1 THEN '${this.YES}'
            WHEN ANY_VALUE(${TablesListAliases.TC_POLICY_TRACK}."answer") = 0 THEN '${this.NO}'
            ELSE '${this.NO_ANSWER}'
        END)
        ) as "policy_accepted"`;

        const acceptanceDateQuery = `(
        IFF(
            ANY_VALUE(${TablesListAliases.TC_POLICY_TRACK}."id") IS NULL AND ${TablesListAliases.TC_POLICY_VERSIONS}."id_policy" = 1 AND ${TablesListAliases.TC_POLICY_VERSIONS}."id" = 1 AND ANY_VALUE(${TablesListAliases.CORE_USER}."register_date") < ANY_VALUE(${TablesListAliases.TC_POLICY_VERSIONS}."creation_date"),
            ANY_VALUE(${TablesListAliases.CORE_USER}."register_date"),
            ANY_VALUE(${TablesListAliases.TC_POLICY_TRACK}."date_track")
        )) as "acceptance_date"`;

        const selectRegisterDateQuery = `ANY_VALUE(${TablesListAliases.CORE_USER}."register_date") as "register_date"`;

        let query = `
        SELECT ${TablesListAliases.CORE_USER}."idst" AS "user_id", 
        ANY_VALUE(${TablesListAliases.CORE_USER}."firstname") AS "firstname",
        ANY_VALUE(${TablesListAliases.CORE_USER}."lastname") AS "lastname",
        ANY_VALUE(${TablesListAliases.CORE_USER}."email") AS "email",
        ANY_VALUE(${TablesListAliases.CORE_USER}."lastenter") AS "last_login",
        ANY_VALUE(${TablesListAliases.TC_POLICY_TRACK}."id") as "track_id",
        CASE
            WHEN ANY_VALUE(${TablesListAliases.CORE_MULTI_DOMAIN}."domain_type") IS NULL THEN '${url}'
            WHEN ANY_VALUE(${TablesListAliases.CORE_MULTI_DOMAIN}."domain_type") = 'subfolder' THEN CONCAT('${url}', '/', ANY_VALUE(${TablesListAliases.CORE_MULTI_DOMAIN}."domain"))
            ELSE ANY_VALUE(${TablesListAliases.CORE_MULTI_DOMAIN}."domain")
        END AS "domain",
    ${TablesListAliases.TC_POLICY_VERSIONS}."id" as "version_id", ${policyAcceptedQuery}, ${acceptanceDateQuery}`;

        if (filters !== undefined && filters.length > 0) {
            const versions = filters.filter((a) => a.version !== undefined);
            if ((versions !== undefined && versions.length > 0) || !countOnly) {
                query += ', ' + selectVersionQuery;
            }
        } else {
            if (!countOnly) {
                query += ', ' + selectVersionQuery;
            }
        }

        if (countOnly) {
            query += ', ' + selectRegisterDateQuery;
        } else {
            query += ', ' + selectUsernameQuery + ', ' + selectPolicyNameQuery;
        }

        if (allFields) {
            query += `, 
            CASE
                WHEN ANY_VALUE(${TablesListAliases.TC_SUB_POLICIES_TRACK}_1."answer") = 1 THEN '${this.YES}'
                WHEN ANY_VALUE(${TablesListAliases.TC_SUB_POLICIES_TRACK}_1."answer") = 0 THEN '${this.NO}'
                ELSE '${this.NO_ANSWER}'
            END as "answer_sub_policy_1",
            CASE 
                WHEN ANY_VALUE(${TablesListAliases.TC_SUB_POLICIES_TRACK}_2."answer") = 1 THEN '${this.YES}'
                WHEN ANY_VALUE(${TablesListAliases.TC_SUB_POLICIES_TRACK}_2."answer") = 0 THEN '${this.NO}'
                ELSE '${this.NO_ANSWER}'
            END as "answer_sub_policy_2",
            CASE 
                WHEN ANY_VALUE(${TablesListAliases.TC_SUB_POLICIES_TRACK}_3."answer") = 1 THEN '${this.YES}'
                WHEN ANY_VALUE(${TablesListAliases.TC_SUB_POLICIES_TRACK}_3."answer") = 0 THEN '${this.NO}'
                ELSE '${this.NO_ANSWER}'
            END as "answer_sub_policy_3"`;
        }
        let coreUserTable = TablesList.CORE_USER.toString();
        let coreGroupMembersFilter = '';
        // Filter by User ID(s)
        if (userIds !== undefined && userIds.length > 0) {
            coreUserTable = `(SELECT * FROM  ${TablesList.CORE_USER} WHERE "idst" IN (${userIds.join(',')}))`;
            coreGroupMembersFilter = ` WHERE "idst" IN (${userIds.join(',')})`;
        }
        // Return all the users of the root folder (aka main domain)
        if (multiDomainIds === undefined || multiDomainIds.length === 0 || !isMultiDomainActive) {
            query += ` 
            FROM ${coreUserTable} AS ${TablesListAliases.CORE_USER}
            INNER JOIN ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS} ON ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember" = ${TablesListAliases.CORE_USER}."idst"
            INNER JOIN ${TablesListAliases.TC_POLICY_TREE} ON ${TablesListAliases.TC_POLICY_TREE}."idst" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idst"
            LEFT JOIN ${TablesList.CORE_MULTI_DOMAIN} AS ${TablesListAliases.CORE_MULTI_DOMAIN} ON ${TablesListAliases.CORE_MULTI_DOMAIN}."org_chart" = ${TablesListAliases.TC_POLICY_TREE}."idorg"`;
        }

        if (isMultiDomainActive && multiDomainIds !== undefined && multiDomainIds.length > 0) {
            const multiDomainWhereClause = `${TablesListAliases.CORE_MULTI_DOMAIN}."id" IN (${multiDomainIds.join(',')})`;
            query += `
            FROM (
                SELECT ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember" AS "idst", ANY_VALUE(${TablesListAliases.CORE_MULTI_DOMAIN}."id_policy") AS "id_policy", ANY_VALUE(${TablesListAliases.CORE_MULTI_DOMAIN}."domain_type") AS "domain_type", ANY_VALUE(${TablesListAliases.CORE_MULTI_DOMAIN}."domain") AS "domain"
                    FROM ${TablesList.CORE_MULTI_DOMAIN} AS ${TablesListAliases.CORE_MULTI_DOMAIN}
                    INNER JOIN ${TablesListAliases.TC_POLICY_TREE} ON ${TablesListAliases.CORE_MULTI_DOMAIN}."org_chart" = ${TablesListAliases.TC_POLICY_TREE}."idorg"
                    INNER JOIN ${TablesList.CORE_GROUP_MEMBERS} AS ${TablesListAliases.CORE_GROUP_MEMBERS} ON (${TablesListAliases.CORE_GROUP_MEMBERS}."idst" = ${TablesListAliases.TC_POLICY_TREE}."idst")
                    WHERE ${multiDomainWhereClause}
                    GROUP BY ${TablesListAliases.CORE_GROUP_MEMBERS}."idstmember") AS ${TablesListAliases.CORE_MULTI_DOMAIN}
            INNER JOIN ${coreUserTable} AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.CORE_MULTI_DOMAIN}."idst"`;
        }
        const withTree = `
        WITH ${TablesListAliases.TC_POLICY_TREE} as (
            SELECT DISTINCT ${TablesListAliases.CORE_GROUP_MEMBERS}."idst", ${TablesListAliases.CORE_ORG_CHART_TREE}d."idorg"
                FROM (
                        SELECT DISTINCT "idst"
                            FROM ${TablesList.CORE_GROUP_MEMBERS} ${coreGroupMembersFilter}) AS ${TablesListAliases.CORE_GROUP_MEMBERS}
                            INNER JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE} 
                                ON ${TablesListAliases.CORE_ORG_CHART_TREE}."idst_oc" = ${TablesListAliases.CORE_GROUP_MEMBERS}."idst"
                            INNER JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}d ON 
                                (
                                    ${TablesListAliases.CORE_ORG_CHART_TREE}d."ileft" <= ${TablesListAliases.CORE_ORG_CHART_TREE}."ileft" 
                                    AND 
                                    ${TablesListAliases.CORE_ORG_CHART_TREE}d."iright" >= ${TablesListAliases.CORE_ORG_CHART_TREE}."iright"
                                )
                    ) `;

        const defaultPolicyId = `(SELECT "id" FROM ${TablesList.TC_POLICIES} WHERE "default" = 1 LIMIT 1)`;

        query += `
        INNER JOIN ${TablesList.TC_POLICY_VERSIONS} AS ${TablesListAliases.TC_POLICY_VERSIONS} ON (IFF(${TablesListAliases.CORE_MULTI_DOMAIN}."id_policy" IS NOT NULL, ${TablesListAliases.CORE_MULTI_DOMAIN}."id_policy", ${defaultPolicyId}) ) = ${TablesListAliases.TC_POLICY_VERSIONS}."id_policy" `;

        if (currentVersionOnly) {
            query = withTree + query;
            query += `LEFT JOIN ${TablesList.TC_POLICIES_TRACK} AS ${TablesListAliases.TC_POLICY_TRACK} ON ${TablesListAliases.TC_POLICY_TRACK}."id_policy_version" = ${TablesListAliases.TC_POLICY_VERSIONS}."id" AND ${TablesListAliases.TC_POLICY_TRACK}."id_user" = ${TablesListAliases.CORE_USER}."idst"`;
        } else {
            const withTrack = `
            , ${TablesListAliases.TC_POLICY_TRACK} AS (
                SELECT NULL AS "id", ${TablesListAliases.CORE_USER}."idst" AS "id_user", MAX(${TablesListAliases.TC_POLICY_VERSIONS}."id") AS "id_policy_version", NULL AS "date_track", NULL AS "answer"
                FROM ${TablesList.TC_POLICIES} AS ${TablesListAliases.TC_POLICIES} 
                    JOIN ${TablesList.TC_POLICY_VERSIONS} AS ${TablesListAliases.TC_POLICY_VERSIONS} 
                        ON ${TablesListAliases.TC_POLICIES}."id" = ${TablesListAliases.TC_POLICY_VERSIONS}."id_policy"
                    JOIN ${coreUserTable} AS ${TablesListAliases.CORE_USER} 
                        ON ${TablesListAliases.CORE_USER}."userid" <> '/Anonymous'
                WHERE (${TablesListAliases.CORE_USER}."idst", ${TablesListAliases.TC_POLICIES}."id") NOT IN 
                    (
                        SELECT "id_user", ${TablesListAliases.TC_POLICY_VERSIONS}."id_policy" 
                        FROM ${TablesList.TC_POLICIES_TRACK} AS ${TablesListAliases.TC_POLICY_TRACK} 
                            JOIN ${TablesList.TC_POLICY_VERSIONS} AS ${TablesListAliases.TC_POLICY_VERSIONS} ON 
                                ${TablesListAliases.TC_POLICY_TRACK}."id_policy_version" = ${TablesListAliases.TC_POLICY_VERSIONS}."id"
                    )
                GROUP BY ${TablesListAliases.CORE_USER}."idst", ${TablesListAliases.TC_POLICIES} ."id"
                UNION
                SELECT MAX(${TablesListAliases.TC_POLICY_TRACK}."id"), "id_user", "id_policy_version", MAX("date_track"), MAX("answer")
                FROM ${TablesList.TC_POLICIES_TRACK} AS ${TablesListAliases.TC_POLICY_TRACK} 
                    JOIN ${TablesList.TC_POLICY_VERSIONS} AS ${TablesListAliases.TC_POLICY_VERSIONS} 
                        ON ${TablesListAliases.TC_POLICY_TRACK}."id_policy_version" =  ${TablesListAliases.TC_POLICY_VERSIONS}."id" 
                WHERE ("id_user", ${TablesListAliases.TC_POLICY_VERSIONS}."id_policy", "date_track") IN 
                    (
                        SELECT "id_user", ${TablesListAliases.TC_POLICY_VERSIONS}."id_policy", MAX("date_track") 
                        FROM ${TablesList.TC_POLICY_VERSIONS} AS ${TablesListAliases.TC_POLICY_VERSIONS}
                            JOIN ${TablesList.TC_POLICIES} AS ${TablesListAliases.TC_POLICIES} 
                                ON ${TablesListAliases.TC_POLICIES}."id" = ${TablesListAliases.TC_POLICY_VERSIONS}."id_policy"
                            JOIN ${coreUserTable} AS ${TablesListAliases.CORE_USER} 
                                ON ${TablesListAliases.CORE_USER}."userid" <> '/Anonymous'
                            JOIN ${TablesList.TC_POLICIES_TRACK} AS ${TablesListAliases.TC_POLICY_TRACK} 
                                ON ${TablesListAliases.TC_POLICY_VERSIONS}."id" = ${TablesListAliases.TC_POLICY_TRACK}."id_policy_version" 
                                AND ${TablesListAliases.TC_POLICY_TRACK}."id_user" = ${TablesListAliases.CORE_USER}."idst"
                        GROUP BY "id_user",${TablesListAliases.TC_POLICY_VERSIONS}."id_policy"
                    ) 
                GROUP BY "id_user", "id_policy_version"
             ) `;
            query = withTree + withTrack + query;
            query += ` JOIN ${TablesListAliases.TC_POLICY_TRACK} ON ${TablesListAliases.TC_POLICY_TRACK}."id_policy_version" = ${TablesListAliases.TC_POLICY_VERSIONS}."id" AND ${TablesListAliases.TC_POLICY_TRACK}."id_user" = ${TablesListAliases.CORE_USER}."idst"`;
        }

        query += `INNER JOIN ${TablesList.TC_POLICIES} AS ${TablesListAliases.TC_POLICIES} ON (${TablesListAliases.TC_POLICIES}."id" = ${TablesListAliases.TC_POLICY_VERSIONS}."id_policy") `;

        if (allFields) {
            const countSubPolTrackSql = `SELECT count(*) FROM ${TablesList.TC_SUB_POLICIES_TRACK} AS count_${TablesListAliases.TC_SUB_POLICIES_TRACK} WHERE count_${TablesListAliases.TC_SUB_POLICIES_TRACK}."id_track" = ${TablesListAliases.TC_POLICY_TRACK}."id"`;
            const minSubPolIdSql = `SELECT MIN(min_${TablesListAliases.TC_SUB_POLICIES_TRACK}."id_sub_policy") FROM ${TablesList.TC_SUB_POLICIES_TRACK} AS min_${TablesListAliases.TC_SUB_POLICIES_TRACK} WHERE min_${TablesListAliases.TC_SUB_POLICIES_TRACK}."id_track" IN (SELECT "id" FROM ${TablesListAliases.TC_POLICY_TRACK})`;
            const maxSubPolIdSql = `SELECT MAX(max_${TablesListAliases.TC_SUB_POLICIES_TRACK}."id_sub_policy") FROM ${TablesList.TC_SUB_POLICIES_TRACK} AS max_${TablesListAliases.TC_SUB_POLICIES_TRACK} WHERE max_${TablesListAliases.TC_SUB_POLICIES_TRACK}."id_track" IN (SELECT "id" FROM ${TablesListAliases.TC_POLICY_TRACK})`;

            const middleSubPolIdSql = `
            SELECT mid_${TablesListAliases.TC_SUB_POLICIES_TRACK}."id_sub_policy"
            FROM ${TablesList.TC_SUB_POLICIES_TRACK} AS mid_${TablesListAliases.TC_SUB_POLICIES_TRACK}
            WHERE mid_${TablesListAliases.TC_SUB_POLICIES_TRACK}."id_track" IN (SELECT "id" FROM ${TablesListAliases.TC_POLICY_TRACK})
            AND (
                mid_${TablesListAliases.TC_SUB_POLICIES_TRACK}."id_sub_policy" <> (${minSubPolIdSql}) AND mid_${TablesListAliases.TC_SUB_POLICIES_TRACK}."id_sub_policy" <> (${maxSubPolIdSql})
            )`;

            query += `
            LEFT JOIN ${TablesList.TC_SUB_POLICIES_TRACK} AS ${TablesListAliases.TC_SUB_POLICIES_TRACK}_1 ON ${TablesListAliases.TC_SUB_POLICIES_TRACK}_1."id_track" = ${TablesListAliases.TC_POLICY_TRACK}."id" AND
                ${TablesListAliases.TC_SUB_POLICIES_TRACK}_1."id_sub_policy" = (
                    CASE
                        WHEN (${countSubPolTrackSql}) > 0 THEN (${minSubPolIdSql})
                        ELSE NULL
                    END
                )
            LEFT JOIN ${TablesList.TC_SUB_POLICIES_TRACK} AS ${TablesListAliases.TC_SUB_POLICIES_TRACK}_2 ON ${TablesListAliases.TC_SUB_POLICIES_TRACK}_2."id_track" = ${TablesListAliases.TC_POLICY_TRACK}."id" AND
                ${TablesListAliases.TC_SUB_POLICIES_TRACK}_2."id_sub_policy" = (
                    CASE
                        WHEN (${countSubPolTrackSql}) < 2 THEN NULL
                        WHEN (${countSubPolTrackSql}) = 2 THEN (${maxSubPolIdSql})
                        WHEN (${countSubPolTrackSql}) > 2 THEN (${middleSubPolIdSql})
                        ELSE NULL
                    END
                )
            LEFT JOIN ${TablesList.TC_SUB_POLICIES_TRACK} AS ${TablesListAliases.TC_SUB_POLICIES_TRACK}_3 ON
            ${TablesListAliases.TC_SUB_POLICIES_TRACK}_3."id_track" = ${TablesListAliases.TC_POLICY_TRACK}."id" AND
                ${TablesListAliases.TC_SUB_POLICIES_TRACK}_3."id_sub_policy" = (
                CASE
                    WHEN (${countSubPolTrackSql}) < 3 THEN NULL
                    ELSE (${maxSubPolIdSql})
                END
            ) `;
        }

        query += ` WHERE ${TablesListAliases.CORE_USER}."userid" <> '/Anonymous' AND (${TablesListAliases.CORE_MULTI_DOMAIN}."id_policy" > 0 OR ${TablesListAliases.CORE_MULTI_DOMAIN}."id_policy" IS NULL)`;

        if (branchId !== undefined) {
            const subQueryBranch = `SELECT "idorg" FROM ${TablesList.CORE_ORG_CHART_TREE} where "idorg" = ${branchId}`;
            query += `
            AND ${TablesListAliases.CORE_ORG_CHART_TREE}."idorg" IN (
                IFF(
                    2 = ${selectionStatus} AND EXISTS(${subQueryBranch}),
                    (SELECT ${TablesListAliases.CORE_ORG_CHART_TREE}s."idorg"
                    FROM (SELECT * FROM ${TablesList.CORE_ORG_CHART_TREE} where "idorg" = ${branchId}) AS ${TablesListAliases.CORE_ORG_CHART_TREE}
                    JOIN ${TablesList.CORE_ORG_CHART_TREE} AS ${TablesListAliases.CORE_ORG_CHART_TREE}s ON
                    ${TablesListAliases.CORE_ORG_CHART_TREE}s."ileft" >= ${TablesListAliases.CORE_ORG_CHART_TREE}."ileft"
                    AND ${TablesListAliases.CORE_ORG_CHART_TREE}s."iright" <= ${TablesListAliases.CORE_ORG_CHART_TREE}."iright"
                    AND (${TablesListAliases.CORE_ORG_CHART_TREE}s."lev" >= (${TablesListAliases.CORE_ORG_CHART_TREE}."lev" + 1) OR ${TablesListAliases.CORE_ORG_CHART_TREE}s."lev" = ${TablesListAliases.CORE_ORG_CHART_TREE}."lev")),
                    (${subQueryBranch})
                )
            )`;
        }

        if (currentVersionOnly) {
            query += ` AND (${TablesListAliases.TC_POLICY_VERSIONS}."id" = (SELECT MAX("id") FROM ${TablesList.TC_POLICY_VERSIONS} AS ${TablesListAliases.TC_POLICY_VERSIONS}_2 WHERE ${TablesListAliases.TC_POLICY_VERSIONS}_2."id_policy" = ${TablesListAliases.TC_POLICY_VERSIONS}."id_policy" ))`;
        }

        // Filter 'deactivated' users if it's needed
        if (hideDeactivatedUser) {
            query += ` AND ${TablesListAliases.CORE_USER}."valid" = 1`;
        }

        if (filters !== undefined && filters.length > 0) {
            query += this.getPrivacyClauseByFilter(filters);
        }

        query += ` GROUP BY ${TablesListAliases.TC_POLICY_VERSIONS}."id", ${TablesListAliases.TC_POLICY_VERSIONS}."id_policy", ${TablesListAliases.CORE_USER}."idst", ${TablesListAliases.TC_POLICY_TRACK}."date_track"`;

        if (countOnly) {
            const queryCountOnly = `
            SELECT count("user_id") AS "countAll",
            SUM(CASE WHEN t."policy_accepted"= '${this.YES}' then 1 end) AS "countYes", 
            SUM(CASE WHEN t."policy_accepted"= '${this.NO}' then 1 end) AS "countNo", 
            SUM(CASE WHEN t."policy_accepted"= '${this.NO_ANSWER}' then 1 end) AS "countNoAnswer",
            SUM(CASE WHEN (t."policy_accepted"= '${this.YES}' AND t."acceptance_date" <= DATEADD(year, -1, current_date)) OR t."register_date" IS NULL then 1 end) AS "countYesMoreThanYear", 
            SUM(CASE WHEN t."policy_accepted"= '${this.NO}' AND t."acceptance_date" <= DATEADD(year, -1, current_date) then 1 end) AS "countNoMoreThanYear",
            SUM(CASE WHEN t."policy_accepted"= '${this.YES}' AND t."acceptance_date" >= DATEADD(day, -1, current_date)  then 1 end) AS "countYesDay", 
            SUM(CASE WHEN t."policy_accepted"= '${this.NO}' AND t."acceptance_date" >= DATEADD(day, -1, current_date)  then 1 end) AS "countNoDay",
            SUM(CASE WHEN t."policy_accepted"= '${this.YES}' AND t."acceptance_date" >= DATEADD(day, -7, current_date)  then 1 end) AS "countYesWeek", 
            SUM(CASE WHEN t."policy_accepted"= '${this.NO}' AND t."acceptance_date" >= DATEADD(day, -7, current_date)  then 1 end) AS "countNoWeek",
            SUM(CASE WHEN t."policy_accepted"= '${this.YES}' AND t."acceptance_date" >= DATEADD(month, -1, current_date) then 1 end) AS "countYesMonth", 
            SUM(CASE WHEN t."policy_accepted"= '${this.NO}' AND t."acceptance_date" >= DATEADD(month, -1, current_date) then 1 end) AS "countNoMonth",
            SUM(CASE WHEN t."policy_accepted"= '${this.YES}' AND t."acceptance_date" >= DATEADD(year, -1, current_date) then 1 end) AS "countYesYear", 
            SUM(CASE WHEN t."policy_accepted"= '${this.NO}' AND t."acceptance_date" >= DATEADD(year, -1, current_date) then 1 end) AS "countNoYear"
            FROM (${query}) AS t`;

            return await snowflakeDriver.runQuery(queryCountOnly);
        }

        return await snowflakeDriver.paginate(query, orderByClause, queryId, page, pageSize, DashboardTypes.PRIVACY_POLICIES, JSON.stringify(translationsExport));
    }

    public async getCharts(req: Request): Promise<PrivacyCharts> {
        const results = await this.getUsers(req, true);
        if (!results || results.length === 0) {
            throw new Error('No Privacy Policy Charts data');
        }
        const data = results[0];
        Object.keys(data).forEach((key: string) => {
            data[key] = data[key] ?? 0;
        });
        const all = data.countAll;
        const barChart: PrivacyBarChart = {
            day: {
                accepted: data.countYesDay,
                rejected: data.countNoDay ,
                no_answer: all - data.countYesDay - data.countNoDay,
            },
            week: {
                accepted: data.countYesWeek ,
                rejected: data.countNoWeek,
                no_answer: all - data.countYesWeek - data.countNoWeek,
            },
            month: {
                accepted: data.countYesMonth,
                rejected: data.countNoMonth,
                no_answer: all - data.countYesMonth - data.countNoMonth,
            },
            year: {
                accepted: data.countYesYear,
                rejected: data.countNoYear,
                no_answer: all - data.countYesYear - data.countNoYear,
            },
            more_than_year: {
                accepted: data.countYesMoreThanYear,
                rejected: data.countNoMoreThanYear,
                no_answer: all - data.countYesMoreThanYear - data.countNoMoreThanYear,
            },
        };

        return {
            bar_charts_data: barChart,
            donut_chart_data: {
                accepted: data.countYes ,
                rejected: data.countNo,
                no_answer: all - data.countYes - data.countNo,
            },
        };
    }
}
