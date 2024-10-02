import { v4 } from 'uuid';

import { ReportsTypes } from '../reports/constants/report-types';
import SessionManager from '../services/session/session-manager.session';
import { SortingOptions, VisibilityTypes } from './custom-report';
import { LegacyReport, VisibilityRule } from './migration-component';
import {
    FieldsList,
    FieldTranslation,
    ReportAvailablesFields,
    ReportManagerAssetsFilter,
    ReportManagerInfo,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility,
    TablesList,
    TablesListAliases,
} from './report-manager';
import { Exception } from '../exceptions/exception';
import { UserLevels } from '../services/session/user-manager.session';
import { AdditionalFieldsTypes, UserLevelsGroups } from './base';
import { BaseReportManager } from './base-report-manager';

export class UsersAssets extends BaseReportManager {
    // Report - Viewer - Asset Details
    reportType = ReportsTypes.VIEWER_ASSET_DETAILS;

    // View Options Fields
    allFields = {
        user: [
            FieldsList.USER_USERID,
            FieldsList.USER_ID,
            FieldsList.USER_FIRSTNAME,
            FieldsList.USER_LASTNAME,
            FieldsList.USER_FULLNAME,
            FieldsList.USER_EMAIL,
            FieldsList.USER_EMAIL_VALIDATION_STATUS,
            FieldsList.USER_LEVEL,
            FieldsList.USER_DEACTIVATED,
            FieldsList.USER_EXPIRATION,
            FieldsList.USER_SUSPEND_DATE,
            FieldsList.USER_REGISTER_DATE,
            FieldsList.USER_LAST_ACCESS_DATE,
            FieldsList.USER_BRANCH_NAME,
            FieldsList.USER_BRANCH_PATH,
            FieldsList.USER_BRANCHES_CODES
        ],
        assets: [
            FieldsList.ASSET_NAME,
            FieldsList.CHANNELS,
            FieldsList.PUBLISHED_BY,
            FieldsList.PUBLISHED_ON,
            FieldsList.LAST_EDIT_BY,
            FieldsList.ASSET_TYPE,
            FieldsList.ASSET_AVERAGE_REVIEW,
            FieldsList.ASSET_DESCRIPTION,
            FieldsList.ASSET_TAG,
            FieldsList.ASSET_SKILL,
        ],
        assetsStatusFields: [
            FieldsList.ASSET_LAST_ACCESS,
            FieldsList.ASSET_FIRST_ACCESS,
            FieldsList.ASSET_NUMBER_ACCESS,
        ]
    };
    mandatoryFields = {
        user : [
            FieldsList.USER_USERID
        ],
        assets: [
            FieldsList.ASSET_NAME
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

        // Assets
        report.assets = new ReportManagerAssetsFilter();
        report.assets.all = true;

        // Date options
        report.publishedDate = this.getDefaultDateOptions();

        // Publish status
        report.publishStatus = this.getDefaultPublishStatus();

        /**
         * View Options Tab
         */
        const tmpFields: string[] = [];

        this.mandatoryFields.user.forEach(element => {
            tmpFields.push(element);
        });

        this.mandatoryFields.assets.forEach(element => {
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

        // Assets
        result.assets = [];
        for (const field of this.allFields.assets) {
            result.assets.push({
                field,
                idLabel: field,
                mandatory: this.mandatoryFields.assets.includes(field),
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        // Assets Status Fields
        result.assetsStatusFields = [];
        for (const field of this.allFields.assetsStatusFields) {
            result.assetsStatusFields.push({
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

        // Variables to check if the specified table was already joined in the query
        let joinCoreUserLevels = false;
        let joinCoreUserBranches = false;
        let joinApp7020ChannelTranslation = false;
        let joinApp7020ChannelAsset = false;
        let joinApp7020ContentRating = false;
        let joinApp7020TagLink = false;
        let joinApp7020Tag = false;
        let joinSkillObject = false;
        let joinSkill = false;
        let joinCoureUserPublish = false;
        let joinCoureUserEdit = false;
        let joinApp7020ContentHistory = false;
        let joinCoreUserFieldValue = false;

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;
        let fullUsers = '';

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilter();
        }

        const allAssets = this.info.assets ? this.info.assets.all : false;
        let fullAssets: number[] = [];

        if (!allAssets) {
            fullAssets = await this.calculateAssetFilter();
        }
        const select = [];
        const from = [];

        let table = `SELECT ${TablesListAliases.APP7020_CONTENT}.*, ${TablesListAliases.APP7020_CONTENT_HISTORY}.idUser as "userIdView"
                     FROM ${TablesList.APP7020_CONTENT} as ${TablesListAliases.APP7020_CONTENT}
                     JOIN ${TablesList.APP7020_CONTENT_HISTORY} as ${TablesListAliases.APP7020_CONTENT_HISTORY} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_CONTENT_HISTORY}.idContent
                     WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND coh.idUser IN (${fullUsers})`;
        }
        if (!allAssets) {
            table += fullAssets.length > 0 ? ` AND ${TablesListAliases.APP7020_CONTENT}.id IN (${fullAssets.join(',')})` : ' AND FALSE';
        }
        from.push(`(${table}) AS ${TablesListAliases.APP7020_CONTENT}`);


        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `valid ${this.getCheckIsValidFieldClause()}` : 'true';

        if (hideExpiredUsers) {
            table += ` AND (expiration IS NULL OR expiration > NOW())`;
        }
        from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.APP7020_CONTENT}.userIdView`);

        from.push(`JOIN ${TablesList.APP7020_CONTENT_PUBLISHED_AGGREGATE} AS ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE}.idContent`);

        let where = [
            `AND ${TablesListAliases.APP7020_CONTENT}.userIdView NOT IN
            (
                SELECT cast(user_id as integer)
                FROM rbac_assignment
                WHERE item_name = '/framework/level/erpadmin'
            )`
        ];

        const userExtraFields = await this.session.getHydra().getUserExtraFields();

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
        // Publish date
        if (this.info.publishedDate && !this.info.publishedDate.any) {
            const pusblishDateFilter = this.buildDateFilter(`${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE}.lastPublishDate`, this.info.publishedDate, 'AND', true);
            where.push(`${pusblishDateFilter}`);
        }

        if (this.info.publishStatus && (this.info.publishStatus.published || this.info.publishStatus.unpublished)) {
            const allValuesAreTrue = Object.keys(this.info.publishStatus).every((k) => this.info.publishStatus[k]);
            if (!allValuesAreTrue) {
                if (this.info.publishStatus.published) {
                    const assetPublished = `${TablesListAliases.APP7020_CONTENT}.conversion_status = 20`;
                    where.push(`AND ${assetPublished}`);
                } else {
                    const assetNotPublished = `${TablesListAliases.APP7020_CONTENT}.conversion_status != 20`;
                    where.push(`AND ${assetNotPublished}`);
                }
            }

        }

        if (this.info.fields) {
            for (const field of this.info.fields) {
                switch (field) {
                    // User fields
                    case FieldsList.USER_ID:
                        select.push(`${TablesListAliases.CORE_USER}.idst AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_ID])}`);
                        break;
                    case FieldsList.USER_USERID:
                        select.push(`SUBSTR(ARBITRARY(${TablesListAliases.CORE_USER}.userid), 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_USERID])}`);
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
                        if (!joinCoreUserLevels) {
                            joinCoreUserLevels = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_USER_LEVELS} AS ${TablesListAliases.CORE_USER_LEVELS} ON ${TablesListAliases.CORE_USER_LEVELS}.idUser = ${TablesListAliases.CORE_USER}.idst`);
                        }

                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.CORE_USER_LEVELS}.level) = ${athena.renderStringInQueryCase(UserLevelsGroups.GodAdmin)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_GODADMIN])}
                                WHEN ARBITRARY(${TablesListAliases.CORE_USER_LEVELS}.level) = ${athena.renderStringInQueryCase(UserLevelsGroups.PowerUser)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_POWERUSER])}
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
                    // Assets fields
                    case FieldsList.ASSET_NAME:
                        select.push(`ARBITRARY(${TablesListAliases.APP7020_CONTENT}.title) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ASSET_NAME])}`);
                        break;
                    case FieldsList.PUBLISHED_ON:
                        select.push(`IF (ARBITRARY(${TablesListAliases.APP7020_CONTENT}.conversion_status = 20), DATE_FORMAT(ARBITRARY(${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE}.lastPublishDate) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s'), '') AS ${athena.renderStringInQuerySelect(translations[FieldsList.PUBLISHED_ON])}`);
                        break;
                    case FieldsList.CHANNELS:
                        if (!joinApp7020ChannelAsset) {
                            joinApp7020ChannelAsset = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_CHANNEL_ASSETS} AS ${TablesListAliases.APP7020_CHANNEL_ASSETS} ON ${TablesListAliases.APP7020_CHANNEL_ASSETS}.idAsset = ${TablesListAliases.APP7020_CONTENT}.id AND ${TablesListAliases.APP7020_CHANNEL_ASSETS}.asset_type = 1`);
                        }
                        if (!joinApp7020ChannelTranslation) {
                            joinApp7020ChannelTranslation = true;
                            // Workaround to return a translated name for the channel. Check first if the channel name translation exists in the user platofrom language.
                            // If not get the Default Language for the channel naame (must alwasy be set).
                            //  As final fallback (edge cases) return the first translation of the channel name
                            const subQueryForLanguage = `
                                SELECT
                                    cha.idChannel as "idChannel",
                                    ARBITRARY(
                                        CASE
                                            WHEN ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}.name IS NOT NULL THEN ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}.name
                                            WHEN ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}DefaultLang.name IS NOT NULL THEN ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}DefaultLang.name
                                            ELSE ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}Fallback.name
                                        END
                                    ) as "name"
                                    FROM ${TablesList.APP7020_CHANNEL_ASSETS} AS ${TablesListAliases.APP7020_CHANNEL_ASSETS}
                                    LEFT JOIN ${TablesList.APP7020_CHANNEL_TRANSLATION} AS ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}
                                        ON ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}.idChannel = ${TablesListAliases.APP7020_CHANNEL_ASSETS}.idChannel
                                        AND ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}.lang = '${this.session.user.getLangCode()}'

                                    LEFT JOIN ${TablesList.APP7020_CHANNEL_TRANSLATION} AS ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}DefaultLang
                                        ON ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}DefaultLang.idChannel = ${TablesListAliases.APP7020_CHANNEL_ASSETS}.idChannel
                                        AND ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}DefaultLang.lang = '${this.session.platform.getDefaultLanguageCode()}'

                                    LEFT JOIN ${TablesList.APP7020_CHANNEL_TRANSLATION} AS ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}Fallback
                                        ON ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}Fallback.idChannel = ${TablesListAliases.APP7020_CHANNEL_ASSETS}.idChannel
                                        AND ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}Fallback.lang IS NOT NULL
                                GROUP BY ${TablesListAliases.APP7020_CHANNEL_ASSETS}.idChannel
                            `;

                            from.push(`LEFT JOIN (${subQueryForLanguage}) AS ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}
                                ON ${TablesListAliases.APP7020_CHANNEL_TRANSLATION}.idChannel = ${TablesListAliases.APP7020_CHANNEL_ASSETS}.idChannel`);
                        }
                        select.push(`ARRAY_JOIN(ARRAY_SORT(ARRAY_AGG(DISTINCT(${TablesListAliases.APP7020_CHANNEL_TRANSLATION}.name))), ', ') AS ${athena.renderStringInQuerySelect(translations[FieldsList.CHANNELS])}`);
                        break;
                    case FieldsList.PUBLISHED_BY:
                        if (!joinCoureUserPublish) {
                            joinCoureUserPublish = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}_publish ON ${TablesListAliases.CORE_USER}_publish.idst = ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE}.lastPublishedBy`);
                        }

                        select.push(`IF (ARBITRARY(${TablesListAliases.APP7020_CONTENT}.conversion_status) = 20, SUBSTR(ARBITRARY(${TablesListAliases.CORE_USER}_publish.userid), 2), '') AS ${athena.renderStringInQuerySelect(translations[FieldsList.PUBLISHED_BY])}`);
                        break;
                    case FieldsList.LAST_EDIT_BY:
                        if (!joinCoureUserEdit) {
                            joinCoureUserEdit = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}_modify ON ${TablesListAliases.CORE_USER}_modify.idst = ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE}.lastEditBy`);
                        }

                        select.push(`SUBSTR(ARBITRARY(${TablesListAliases.CORE_USER}_modify.userid), 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.LAST_EDIT_BY])}`);
                        break;
                    case FieldsList.ASSET_TYPE:
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.VIDEO])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.DOC])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 3 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.EXCEL])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 4 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.PPT])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 5 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.PDF])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 6 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.TEXT])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 7 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.IMAGE])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 8 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.QUESTION])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 9 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.RESPONSE])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 10 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.OTHER])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 11 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.DEFAULT_OTHER])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 12 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.DEFAULT_MUSIC])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 13 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.DEFAULT_ARCHIVE])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 15 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.LINKS])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 16 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.GOOGLE_DRIVE_DOCS])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 17 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.GOOGLE_DRIVE_SHEETS])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 18 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.GOOGLE_DRIVE_SLIDES])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 19 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.PLAYLIST])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 20 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YOUTUBE])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 21 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.VIMEO])}
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) = 22 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.WISTIA])}
                                ELSE CAST (ARBITRARY(${TablesListAliases.APP7020_CONTENT}.contentType) as varchar)
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ASSET_TYPE])}`);
                        break;
                    case FieldsList.ASSET_AVERAGE_REVIEW:
                        if (!joinApp7020ContentRating) {
                            joinApp7020ContentRating = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_RATING} AS ${TablesListAliases.APP7020_CONTENT_RATING} ON ${TablesListAliases.APP7020_CONTENT_RATING}.idContent = ${TablesListAliases.APP7020_CONTENT}.id`);
                        }
                        select.push(`AVG(${TablesListAliases.APP7020_CONTENT_RATING}.rating) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ASSET_AVERAGE_REVIEW])}`);
                        break;
                    case FieldsList.ASSET_DESCRIPTION:
                        select.push(`ARBITRARY(${TablesListAliases.APP7020_CONTENT}.description) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ASSET_DESCRIPTION])}`);
                        break;
                    case FieldsList.ASSET_TAG:
                        if (!joinApp7020TagLink) {
                            joinApp7020TagLink = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_TAG_LINK} AS ${TablesListAliases.APP7020_TAG_LINK} ON ${TablesListAliases.APP7020_TAG_LINK}.idContent = ${TablesListAliases.APP7020_CONTENT}.id`);
                        }
                        if (!joinApp7020Tag) {
                            joinApp7020Tag = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_TAG} AS ${TablesListAliases.APP7020_TAG} ON ${TablesListAliases.APP7020_TAG}.id = ${TablesListAliases.APP7020_TAG_LINK}.idTag`);
                        }
                        select.push(`ARRAY_JOIN(ARRAY_AGG(DISTINCT(${TablesListAliases.APP7020_TAG}.tagText)), ', ') AS ${athena.renderStringInQuerySelect(translations[FieldsList.ASSET_TAG])}`);
                        break;
                    case FieldsList.ASSET_SKILL:
                        if (!joinSkillObject) {
                            joinSkillObject = true;
                            from.push(`LEFT JOIN ${TablesList.SKILL_SKILLS_OBJECTS} AS ${TablesListAliases.SKILL_SKILLS_OBJECTS} ON ${TablesListAliases.SKILL_SKILLS_OBJECTS}.idObject = ${TablesListAliases.APP7020_CONTENT}.id AND ${TablesListAliases.SKILL_SKILLS_OBJECTS}.objectType = 3`);
                        }
                        if (!joinSkill) {
                            joinSkill = true;
                            from.push(`LEFT JOIN ${TablesList.SKILL_SKILLS} AS ${TablesListAliases.SKILL_SKILLS} ON ${TablesListAliases.SKILL_SKILLS}.id = ${TablesListAliases.SKILL_SKILLS_OBJECTS}.idSkill`);
                        }
                        select.push(`ARRAY_JOIN(ARRAY_AGG(DISTINCT(${TablesListAliases.SKILL_SKILLS}.title)), ', ') AS ${athena.renderStringInQuerySelect(translations[FieldsList.ASSET_SKILL])}`);
                        break;
                    case FieldsList.ASSET_LAST_ACCESS:
                        if (!joinApp7020ContentHistory) {
                            joinApp7020ContentHistory = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY} AS ${TablesListAliases.APP7020_CONTENT_HISTORY} ON ${TablesListAliases.APP7020_CONTENT_HISTORY}.idContent = ${TablesListAliases.APP7020_CONTENT}.id AND ${TablesListAliases.APP7020_CONTENT_HISTORY}.idUser = ${TablesListAliases.APP7020_CONTENT}.userIdView`);
                        }
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.APP7020_CONTENT_HISTORY}.viewed) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.ASSET_LAST_ACCESS])}`);
                        break;
                    case FieldsList.ASSET_FIRST_ACCESS:
                        if (!joinApp7020ContentHistory) {
                            joinApp7020ContentHistory = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY} AS ${TablesListAliases.APP7020_CONTENT_HISTORY} ON ${TablesListAliases.APP7020_CONTENT_HISTORY}.idContent = ${TablesListAliases.APP7020_CONTENT}.id AND ${TablesListAliases.APP7020_CONTENT_HISTORY}.idUser = ${TablesListAliases.APP7020_CONTENT}.userIdView`);
                        }
                        select.push(`DATE_FORMAT(MIN(${TablesListAliases.APP7020_CONTENT_HISTORY}.viewed) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.ASSET_FIRST_ACCESS])}`);
                        break;
                    case FieldsList.ASSET_NUMBER_ACCESS:
                        if (!joinApp7020ContentHistory) {
                            joinApp7020ContentHistory = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY} AS ${TablesListAliases.APP7020_CONTENT_HISTORY} ON ${TablesListAliases.APP7020_CONTENT_HISTORY}.idContent = ${TablesListAliases.APP7020_CONTENT}.id AND ${TablesListAliases.APP7020_CONTENT_HISTORY}.idUser = ${TablesListAliases.APP7020_CONTENT}.userIdView`);
                        }
                        select.push(`COUNT(DISTINCT(${TablesListAliases.APP7020_CONTENT_HISTORY}.id)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ASSET_NUMBER_ACCESS])}`);
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
                     FROM ${from.join(' ')} ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}
       GROUP BY ${TablesListAliases.CORE_USER}.idst, ${TablesListAliases.APP7020_CONTENT}.id`;


        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(select, translations, {
                user: userExtraFields.data.items,
                course: [],
                userCourse: [],
                webinar: [],
                classroom: []
            });
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

        const queryHelper = {
            select: [],
            archivedSelect: [],
            from: [],
            join: [],
            cte: [],
            groupBy: [`${TablesListAliases.CORE_USER}."idst"`, `${TablesListAliases.APP7020_CONTENT}."id"`],
            archivedGroupBy: [],
            userAdditionalFieldsSelect: [],
            userAdditionalFieldsFrom: [],
            userAdditionalFieldsId: [],
            courseAdditionalFieldsSelect: [],
            courseAdditionalFieldsFrom: [],
            courseAdditionalFieldsId: [],
            translations,
            checkPuVisibility
        };

        let table = `SELECT ${TablesListAliases.APP7020_CONTENT}.*, ${TablesListAliases.APP7020_CONTENT_HISTORY}."iduser" as "useridview"
                     FROM ${TablesList.APP7020_CONTENT} as ${TablesListAliases.APP7020_CONTENT}
                     JOIN ${TablesList.APP7020_CONTENT_HISTORY} as ${TablesListAliases.APP7020_CONTENT_HISTORY} ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_CONTENT_HISTORY}."idcontent"
                     WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND coh."iduser" IN (${fullUsers})`;
        }
        table += this.getAssetsFiltersSnowflake();
        queryHelper.from.push(`(${table}) AS ${TablesListAliases.APP7020_CONTENT}`);

        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `"valid" = 1` : 'true';

        if (hideExpiredUsers) {
            table += ` AND ("expiration" IS NULL OR "expiration" > current_timestamp())`;
        }
        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.APP7020_CONTENT}."useridview"`);

        queryHelper.from.push(`JOIN (SELECT "idcontent", max("lastpublishdate") as "lastpublishdate", max("lasteditdate") as "lasteditdate" FROM ${TablesList.APP7020_CONTENT_PUBLISHED_AGGREGATE} GROUP BY "idcontent") 
            AS ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_MAX} ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_MAX}."idcontent"`);

        const where = [
            `AND ${TablesListAliases.APP7020_CONTENT}."useridview" NOT IN
            (
                SELECT cast("user_id" as integer)
                FROM rbac_assignment
                WHERE "item_name" = '/framework/level/erpadmin'
            )`
        ];
        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');

        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            this.getAdditionalUsersFieldsFiltersSnowflake(queryHelper, userExtraFields);
        }

        // Publish date
        if (this.info.publishedDate && !this.info.publishedDate.any) {
            const pusblishDateFilter = this.buildDateFilter(`${TablesListAliases.APP7020_CONTENT_PUBLISHED_AGGREGATE_MAX}.lastPublishDate`, this.info.publishedDate, 'AND', true);
            where.push(`${pusblishDateFilter}`);
        }

        if (this.info.publishStatus && (this.info.publishStatus.published || this.info.publishStatus.unpublished)) {
            const allValuesAreTrue = Object.keys(this.info.publishStatus).every((k) => this.info.publishStatus[k]);
            if (!allValuesAreTrue) {
                if (this.info.publishStatus.published) {
                    const assetPublished = `${TablesListAliases.APP7020_CONTENT}."conversion_status" = 20`;
                    where.push(`AND ${assetPublished}`);
                } else {
                    const assetNotPublished = `${TablesListAliases.APP7020_CONTENT}."conversion_status" != 20`;
                    where.push(`AND ${assetNotPublished}`);
                }
            }

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

        let query = '';
        query += `
            ${queryHelper.cte.length > 0 ? `WITH ${queryHelper.cte.join(', ')}` : ''}
            SELECT ${queryHelper.select.join(', ')}
            FROM ${queryHelper.from.join(' ')}
            ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}
            GROUP BY ${[...new Set(queryHelper.groupBy)].join(', ')}`;

        // custom columns sorting
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
        throw new Exception('Function not implemented');
    }
}
