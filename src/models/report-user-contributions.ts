import { v4 } from 'uuid';

import { ReportsTypes } from '../reports/constants/report-types';
import { Utils } from '../reports/utils';
import SessionManager from '../services/session/session-manager.session';
import { UserLevels } from '../services/session/user-manager.session';
import {
    AdditionalFieldsTypes,
    UserLevelsGroups
} from './base';
import {
    DateOptionsValueDescriptor,
    SortingOptions,
    VisibilityTypes
} from './custom-report';
import { LegacyOrderByParsed, LegacyReport, VisibilityRule } from './migration-component';
import {
    FieldsList,
    FieldTranslation,
    ReportAvailablesFields,
    ReportManagerInfo,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility,
    TablesList,
    TablesListAliases,
} from './report-manager';
import { BaseReportManager } from './base-report-manager';

// NOTE: THIS REPORT WAS RENAMED IN USER - ASSETS. We keep for now the old name (for constant and translation key)
// because is used as ID both in legacy reports (MySQL) and in the new ones (DynamoDB)
export class UserContributionsManager extends BaseReportManager {
    reportType = ReportsTypes.USER_CONTRIBUTIONS;

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
        assets: [
            FieldsList.INVOLVED_CHANNELS,
            FieldsList.PUBLISHED_ASSETS,
            FieldsList.UNPUBLISHED_ASSETS,
            FieldsList.PRIVATE_ASSETS,
            FieldsList.UPLOADED_ASSETS
        ],
    };
    mandatoryFields = {
        user : [
            FieldsList.USER_USERID
        ]
    };

    public constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap | undefined) {
        super(session, reportDetails);
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

        // Date options
        report.contributionDate = this.getDefaultDateOptions();

        /**
         * View Options Tab
         */
        const tmpFields: string[] = [];

        this.mandatoryFields.user.forEach(element => {
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

        // Recover assets fields
        result.assets = [];
        for (const field of this.allFields.assets) {
            result.assets.push({
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
        const translations = await this.loadTranslations();
        const athena = this.session.getAthena();

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        let fullUsers = '';

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilter();
        }

        const select = [];
        const from = [];

        let table = hideDeactivated ? `SELECT * FROM ${TablesList.CORE_USER} WHERE valid ${this.getCheckIsValidFieldClause()}` : `SELECT * FROM ${TablesList.CORE_USER} WHERE true`;

        if (fullUsers !== '') {
            table += ` AND idst IN (${fullUsers})`;
        }

        // Remove expired users
        if (this.info.users && this.info.users.hideExpiredUsers) {
            table += ` AND (expiration IS NULL OR expiration > NOW())`;
        }

        from.push(`(${table}) AS ${TablesListAliases.CORE_USER}`);
        from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT} AS ${TablesListAliases.APP7020_CONTENT} ON ${TablesListAliases.APP7020_CONTENT}.userid = ${TablesListAliases.CORE_USER}.idst`);

        let where = [
            `AND ${TablesListAliases.CORE_USER}.userid <> '/Anonymous'`
        ];

        // Filter for asset creation date
        if (this.info.contributionDate && !this.info.contributionDate.any) {
            const contributionDateFilter = this.buildDateFilter(`${TablesListAliases.APP7020_CONTENT}.created`, this.info.contributionDate, '', true);
            where.push(`AND ((${contributionDateFilter}) OR ${TablesListAliases.APP7020_CONTENT}.created IS NULL)`);
        }

        // Variables to check if the specified table was already joined in the query
        let joinCoreUserFieldValue = false;

        // Variables to check if the specified materialized tables was already joined in the query
        let joinCoreUserLevels = false;
        let joinCoreUserBranches = false;
        let joinApp7020InvolvedChannelsAggregate = false;
        let joinSkillManagersValue = false;

        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');

        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            const tmp = await this.getAdditionalFieldsFilters(userExtraFields);
            if (tmp.length) {
                if (!joinCoreUserFieldValue) {
                    joinCoreUserFieldValue = true;
                    from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = ${TablesListAliases.CORE_USER}.idst`);
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
                            select.push(`ARBITRARY(CONCAT(${TablesListAliases.CORE_USER}.firstname, ' ', ${TablesListAliases.CORE_USER}.lastname)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                        } else {
                            select.push(`ARBITRARY(CONCAT(${TablesListAliases.CORE_USER}.lastname, ' ', ${TablesListAliases.CORE_USER}.firstname)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                        }
                        break;
                    case FieldsList.USER_EMAIL:
                        select.push(`ARBITRARY(${TablesListAliases.CORE_USER}.email) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL])}`);
                        break;
                    case FieldsList.USER_EMAIL_VALIDATION_STATUS:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.CORE_USER}.email_status) = 0 THEN ARBITRARY(${athena.renderStringInQueryCase(translations[FieldTranslation.NO])})
                                ELSE ARBITRARY(${athena.renderStringInQueryCase(translations[FieldTranslation.YES])})
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL_VALIDATION_STATUS])}`);
                        break;
                    case FieldsList.USER_LEVEL:
                        if (!joinCoreUserLevels) {
                            joinCoreUserLevels = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_USER_LEVELS} AS ${TablesListAliases.CORE_USER_LEVELS} ON ${TablesListAliases.CORE_USER_LEVELS}.idUser = ${TablesListAliases.CORE_USER}.idst`);
                        }
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.CORE_USER_LEVELS}.level) = ARBITRARY(${athena.renderStringInQueryCase(UserLevelsGroups.GodAdmin)}) THEN ARBITRARY(${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_GODADMIN])})
                                WHEN ARBITRARY(${TablesListAliases.CORE_USER_LEVELS}.level) = ARBITRARY(${athena.renderStringInQueryCase(UserLevelsGroups.PowerUser)}) THEN ARBITRARY(${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_POWERUSER])})
                                ELSE ARBITRARY(${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_USER])})
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LEVEL])}`);
                        break;

                    case FieldsList.USER_DEACTIVATED:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.CORE_USER}.valid) ${this.getCheckIsValidFieldClause()} THEN ARBITRARY(${athena.renderStringInQueryCase(translations[FieldTranslation.NO])})
                                ELSE ARBITRARY(${athena.renderStringInQueryCase(translations[FieldTranslation.YES])})
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
                            if (this.session.user.getLevel() === UserLevels.POWER_USER) {
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
                            if (this.session.user.getLevel() === UserLevels.POWER_USER) {
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
                    // Assets Fields
                    case FieldsList.UPLOADED_ASSETS:
                        select.push(`SUM(CASE WHEN ${TablesListAliases.APP7020_CONTENT}.conversion_status >= 3 THEN 1 ELSE 0 END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.UPLOADED_ASSETS])}`);
                        break;
                    case FieldsList.PUBLISHED_ASSETS:
                        select.push(`SUM(CASE WHEN ${TablesListAliases.APP7020_CONTENT}.conversion_status = 20 THEN 1 ELSE 0 END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.PUBLISHED_ASSETS])}`);
                        break;
                    case FieldsList.UNPUBLISHED_ASSETS:
                        select.push(`SUM(CASE WHEN ${TablesListAliases.APP7020_CONTENT}.conversion_status = 18 THEN 1 ELSE 0 END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.UNPUBLISHED_ASSETS])}`);
                        break;
                    case FieldsList.PRIVATE_ASSETS:
                        select.push(`SUM(CASE WHEN ${TablesListAliases.APP7020_CONTENT}.is_private > 0 THEN 1 ELSE 0 END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.PRIVATE_ASSETS])}`);
                        break;
                    case FieldsList.INVOLVED_CHANNELS:
                        if (!joinApp7020InvolvedChannelsAggregate) {
                            joinApp7020InvolvedChannelsAggregate = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_INVOLVED_CHANNELS_AGGREGATE} AS ${TablesListAliases.APP7020_INVOLVED_CHANNELS_AGGREGATE} ON ${TablesListAliases.APP7020_INVOLVED_CHANNELS_AGGREGATE}.idst = ${TablesListAliases.CORE_USER}.idst AND ${TablesListAliases.APP7020_INVOLVED_CHANNELS_AGGREGATE}.lang = '${this.session.user.getLangCode()}'`);
                        }
                        select.push(`ARBITRARY(${TablesListAliases.APP7020_INVOLVED_CHANNELS_AGGREGATE}.involvedChannels) AS ${athena.renderStringInQuerySelect(translations[FieldsList.INVOLVED_CHANNELS])}`);
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
                                            from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = ${TablesListAliases.CORE_USER}.idst`);
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
                        }
                        break;
                }
            }
        }


        let query = `SELECT ${select.join(', ')}
        FROM ${from.join(' ')}
        ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}
        GROUP BY ${TablesListAliases.CORE_USER}.idst`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(select, translations, {user: userExtraFields.data.items, course: [], userCourse: [], webinar: [], classroom: []});
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

        // Needed to save some info for the select switch statement
        const queryHelper = {
            select: [],
            archivedSelect: [],
            from: [],
            join: [],
            cte: [],
            groupBy: [`${TablesListAliases.CORE_USER}."idst"`],
            archivedGroupBy: [],
            userAdditionalFieldsSelect: [],
            userAdditionalFieldsFrom: [],
            userAdditionalFieldsId: [],
            translations,
            checkPuVisibility
        };

        let table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `"valid" = 1 ` : 'true';
        if (fullUsers !== '') {
            table += ` AND "idst" IN (${fullUsers})`;
        }

        if (hideExpiredUsers) {
            table += ` AND ("expiration" IS NULL OR "expiration" > current_timestamp())`;
        }
        queryHelper.from.push(`(${table}) AS ${TablesListAliases.CORE_USER}`);
        queryHelper.from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT} AS ${TablesListAliases.APP7020_CONTENT} ON ${TablesListAliases.APP7020_CONTENT}."userid" = ${TablesListAliases.CORE_USER}."idst"`);

        const where = [
            `AND ${TablesListAliases.CORE_USER}."userid" <> '/Anonymous'`
        ];

        // Filter for asset creation date
        if (this.info.contributionDate && !this.info.contributionDate.any) {
            const contributionDateFilter = this.buildDateFilter(`${TablesListAliases.APP7020_CONTENT}.created`, this.info.contributionDate, '', true);
            where.push(`AND ((${contributionDateFilter}) OR ${TablesListAliases.APP7020_CONTENT}."created" IS NULL)`);
        }

        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');
        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            this.getAdditionalUsersFieldsFiltersSnowflake(queryHelper, userExtraFields);
        }

        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged = this.querySelectUserFields(field, queryHelper) ||
                    this.querySelectAssetFields(field, queryHelper) ||
                    this.queryWithUserAdditionalFields(field, queryHelper, userExtraFields);
            }
        }

        if (queryHelper.userAdditionalFieldsId.length > 0) {
            queryHelper.cte.push(this.additionalUserFieldQueryWith(queryHelper.userAdditionalFieldsId));
            queryHelper.cte.push(this.additionalFieldQueryWith(queryHelper.userAdditionalFieldsFrom, queryHelper.userAdditionalFieldsSelect, queryHelper.userAdditionalFieldsId, 'id_user', TablesList.CORE_USER_FIELD_VALUE_WITH, TablesList.USERS_ADDITIONAL_FIELDS_TRANSLATIONS));
        }

        let query = `
            ${queryHelper.cte.length > 0 ? `WITH ${queryHelper.cte.join(', ')}` : ''}
            SELECT ${queryHelper.select.join(', ')}
            FROM ${queryHelper.from.join(' ')}
            ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}
            GROUP BY ${[...new Set(queryHelper.groupBy)].join(', ')}`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(
                queryHelper.select,
                queryHelper.translations,
                {
                    user: userExtraFields.data.items,
                    course: [],
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

        // check filters section validity
        if (!filterData.filters) {
            this.logger.error(`No legacy filters section for id report: ${legacyReport.id_filter}`);
            throw new Error('No legacy filters section');
        }

        // Contribution Date
        if (filterData.filters.start_date.type !== 'any' && report.contributionDate) {
            report.contributionDate = utils.parseLegacyFilterDate(report.contributionDate as DateOptionsValueDescriptor, filterData.filters.start_date);
        }

        // map selected fields and manage order by
        if (filterData.fields) {
            let legacyOrderField: LegacyOrderByParsed | undefined;
            const userMandatoryFieldsMap = this.mandatoryFields.user.reduce((previousValue: { [key: string]: string }, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});

            // user fields and order by
            const userFieldsDescriptor = this.mapUserSelectedFields(filterData.fields.us, filterData.order, userMandatoryFieldsMap, this.reportType);
            report.fields.push(...userFieldsDescriptor.fields);
            if (userFieldsDescriptor.orderByDescriptor) legacyOrderField = userFieldsDescriptor.orderByDescriptor;

            const contributionFieldsDescriptor = this.mapContributionSelectedFields(filterData.fields.as, filterData.order);
            report.fields.push(...contributionFieldsDescriptor.fields);
            if (contributionFieldsDescriptor.orderByDescriptor) legacyOrderField = contributionFieldsDescriptor.orderByDescriptor;

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

}
