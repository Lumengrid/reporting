import { v4 } from 'uuid';

import { ReportsTypes } from '../reports/constants/report-types';
import SessionManager from '../services/session/session-manager.session';
import { UserLevels } from '../services/session/user-manager.session';
import { AdditionalFieldsTypes, CourseTypes, EcommItemTypes, joinedTables, UserLevelsGroups } from './base';
import { SortingOptions, VisibilityTypes } from './custom-report';
import { LegacyOrderByParsed, LegacyReport, VisibilityRule } from './migration-component';
import {
    FieldsList,
    FieldTranslation,
    ReportAvailablesFields,
    ReportManagerInfo,
    ReportManagerInfoCoursesFilter,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility,
    ReportManagerLearningPlansFilter,
    TablesList,
    TablesListAliases,
} from './report-manager';
import { BaseReportManager } from './base-report-manager';

export class EcommerceTransactionsManager extends BaseReportManager {
    reportType = ReportsTypes.ECOMMERCE_TRANSACTION;

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
        ecommerceTransaction: [
            FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_1,
            FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_2,
            FieldsList.ECOMMERCE_TRANSACTION_CITY,
            FieldsList.ECOMMERCE_TRANSACTION_COMPANY_NAME,
            FieldsList.ECOMMERCE_TRANSACTION_COUPON_CODE,
            FieldsList.ECOMMERCE_TRANSACTION_COUPON_DESCRIPTION,
            FieldsList.ECOMMERCE_TRANSACTION_DISCOUNT,
            FieldsList.ECOMMERCE_TRANSACTION_EXTERNAL_TRANSACTION_ID,
            FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_DATE,
            FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_METHOD,
            FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_STATUS,
            FieldsList.ECOMMERCE_TRANSACTION_QUANTITY,
            FieldsList.ECOMMERCE_TRANSACTION_STATE,
            FieldsList.ECOMMERCE_TRANSACTION_SUBTOTAL_PRICE,
            FieldsList.ECOMMERCE_TRANSACTION_TOTAL_PRICE,
            FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_CREATION_DATE,
            FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_ID,
            FieldsList.ECOMMERCE_TRANSACTION_VAT_NUMBER,
            FieldsList.ECOMMERCE_TRANSACTION_ZIP_CODE,
        ],
        ecommerceTransactionItem: [
            FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_CODE,
            FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_NAME,
            FieldsList.ECOMMERCE_TRANSACTION_ITEM_START_DATE,
            FieldsList.ECOMMERCE_TRANSACTION_ITEM_END_DATE,
            FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_WEBINAR_SESSION_NAME,
            FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_LOCATION,
            FieldsList.ECOMMERCE_TRANSACTION_ITEM_TYPE,
            FieldsList.ECOMMERCE_TRANSACTION_PRICE,
        ],
        contentPartners: [
            FieldsList.CONTENT_PARTNERS_AFFILIATE,
            FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_CODE,
            FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_SOURCE
        ]
    };
    mandatoryFields = {
        user : [
            FieldsList.USER_USERID
        ],
        ecommerceTransaction: [
            FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_ID,
        ],
    };

    public constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap | undefined) {
        super(session, reportDetails);

        if (!this.session.platform.checkPlugincontentPartnersEnabled()) {
            this.allFields.contentPartners = [];
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

        // Courses
        report.courses = new ReportManagerInfoCoursesFilter();
        report.courses.all = true;

        report.learningPlans = new ReportManagerLearningPlansFilter();
        report.learningPlans.all = true;


        /**
         * View Options Tab
         */
        const tmpFields: string[] = [];

        this.mandatoryFields.user.forEach(element => {
            tmpFields.push(element);
        });

        this.mandatoryFields.ecommerceTransaction.forEach(element => {
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

        // Recover ecommerce transaction fields
        result.ecommerceTransaction = [];
        for (const field of this.allFields.ecommerceTransaction) {
            result.ecommerceTransaction.push({
                field,
                idLabel: field,
                mandatory: this.mandatoryFields.ecommerceTransaction.includes(field),
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        result.ecommerceTransactionItem = [];
        for (const field of this.allFields.ecommerceTransactionItem) {
            result.ecommerceTransactionItem.push({
                field,
                idLabel: field,
                mandatory: false,
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        if (this.session.platform.checkPlugincontentPartnersEnabled()) {
            result.contentPartners = [];
            for (const field of this.allFields.contentPartners) {
                result.contentPartners.push({
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
        const hydra = this.session.getHydra();
        const athena = this.session.getAthena();

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;
        let fullUsers = '';

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilter();
        }

        const fullCourses = await this.calculateCourseFilter(true);

        let allCourses = this.info.courses ? this.info.courses.all : false;
        let fullLPs: number[] = [];

        if (!allCourses) {
            fullLPs = this.info.learningPlans ? this.info.learningPlans.learningPlans.map(a => parseInt(a.id as string, 10)) : [];
        }

        if (this.session.user.getLevel() === UserLevels.POWER_USER) {
            const puCourses = await hydra.getPuLPs();
            if (allCourses) {
                allCourses = false;
                fullLPs = puCourses.data;
            } else {
                fullLPs = fullLPs.filter(x => puCourses.data.includes(x));
            }
        }

        const select = [];
        const from = [];
        let table = `SELECT * FROM ${TablesList.ECOMMERCE_TRANSACTION} WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND id_user IN (${fullUsers})`;
        }

        from.push(`(${table}) AS ${TablesListAliases.ECOMMERCE_TRANSACTION}`);


        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `valid ${this.getCheckIsValidFieldClause()}` : 'true';

        if (hideExpiredUsers) {
            table += ` AND (expiration IS NULL OR expiration > NOW())`;
        }
        from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.ECOMMERCE_TRANSACTION}.id_user`);

        table = `SELECT * FROM ${TablesList.ECOMMERCE_TRANSACTION_INFO} WHERE TRUE`;

        if (!allCourses && fullCourses !== '' && fullLPs.length > 0) {
            table += ` AND (id_course IN (${fullCourses}) OR id_path IN (${fullLPs.join(',')}))`;
        } else if (fullCourses !== '') {
            table += ` AND id_course IN (${fullCourses})`;
        } else if (!allCourses && fullLPs.length > 0) {
            table += ` AND id_path IN (${fullLPs.join(',')})`;
        }

        from.push(`JOIN (${table}) AS ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO} ON ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_trans = ${TablesListAliases.ECOMMERCE_TRANSACTION}.id_trans`);

        from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_course AND ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.item_type IN ('course', 'courseseats')`);
        from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH} AS ${TablesListAliases.LEARNING_COURSEPATH} ON ${TablesListAliases.LEARNING_COURSEPATH}.id_path = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_path AND ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.item_type IN ('coursepath')`);

        let where = [
            `AND ${TablesListAliases.CORE_USER}.userid <> '/Anonymous'`
        ];

        // Variables to check if the specified table was already joined in the query
        let joinCoreUserFieldValue = false;
        let joinEcommerceCouponFieldValue = false;
        let joinLtCourseSessionValue = false;
        let joinWebinarSessionValue = false;
        let joinLtLocationValue = false;
        let joinContentPartners = false;
        let joinSkillManagersValue = false;
        let joinTransactionInfoAggregate = false;

        // Variables to check if the specified materialized tables was already joined in the query
        let joinCoreUserLevels = false;
        let joinCoreUserBranches = false;
        let joinCoreUserBilling = false;

        // This view is necessary! We grouping by transaction_id in and we avoid duplicate
        const contentPartnersView = `
            SELECT ARBITRARY(${TablesListAliases.CONTENT_PARTNERS}.name) AS "name", ${TablesListAliases.CONTENT_PARTNERS_REFERRAL_LOG}.transaction_id
            FROM ${TablesList.CONTENT_PARTNERS_REFERRAL_LOG} AS ${TablesListAliases.CONTENT_PARTNERS_REFERRAL_LOG}
                  LEFT JOIN ${TablesList.ECOMMERCE_TRANSACTION_INFO} AS ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO} on ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_trans = ${TablesListAliases.CONTENT_PARTNERS_REFERRAL_LOG}.transaction_id
                  LEFT JOIN ${TablesList.CONTENT_PARTNERS_AFFILIATES} AS ${TablesListAliases.CONTENT_PARTNERS_AFFILIATES}
                            ON ${TablesListAliases.CONTENT_PARTNERS_AFFILIATES}.id_partner = ${TablesListAliases.CONTENT_PARTNERS_REFERRAL_LOG}.partner_id
                                OR JSON_EXTRACT_SCALAR(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.item_data_json, '$.position_snapshot.is_affiliate') = '1'
                  LEFT JOIN ${TablesList.CONTENT_PARTNERS} AS ${TablesListAliases.CONTENT_PARTNERS}
                            ON ${TablesListAliases.CONTENT_PARTNERS}.id = CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.item_data_json, '$.position_snapshot.id_partner') AS INTEGER)
            GROUP BY ${TablesListAliases.CONTENT_PARTNERS_REFERRAL_LOG}.transaction_id`;

        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');

        // subquery
        const transactionInfoAggregateQuery = `SELECT id_trans, SUM(CAST(price as DOUBLE)) AS total_price FROM ${TablesList.ECOMMERCE_TRANSACTION_INFO} GROUP BY id_trans`;

        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            const tmp = await this.getAdditionalFieldsFilters(userExtraFields);
            if (tmp.length) {
                if (!joinCoreUserFieldValue) {
                    joinCoreUserFieldValue = true;
                    from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEUSER_AGGREGATE} AS ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                    from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
                }
                where = where.concat(tmp);
            }
        }

        // Query costants for calculations
        const seats = `JSON_EXTRACT_SCALAR(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.item_data_json, '$.seats')`;
        const price = `${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.price`;
        const transactionDiscount = `CAST(${TablesListAliases.ECOMMERCE_TRANSACTION}.discount AS DOUBLE)`;

        if (this.info.fields) {
            for (const field of this.info.fields) {
                switch (field) {
                    // User fields
                    case FieldsList.USER_ID:
                        select.push(`${TablesListAliases.CORE_USER}.idst AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_ID])}`);
                        break;
                    case FieldsList.USER_USERID:
                        select.push(`SUBSTR(${TablesListAliases.CORE_USER}.userid, 2) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_USERID])}`);
                        break;
                    case FieldsList.USER_FIRSTNAME:
                        select.push(`${TablesListAliases.CORE_USER}.firstname AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FIRSTNAME])}`);
                        break;
                    case FieldsList.USER_LASTNAME:
                        select.push(`${TablesListAliases.CORE_USER}.lastname AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LASTNAME])}`);
                        break;
                    case FieldsList.USER_FULLNAME:
                        if (this.session.platform.getShowFirstNameFirst()) {
                            select.push(`CONCAT(${TablesListAliases.CORE_USER}.firstname, ' ', ${TablesListAliases.CORE_USER}.lastname) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                        } else {
                            select.push(`CONCAT(${TablesListAliases.CORE_USER}.lastname, ' ', ${TablesListAliases.CORE_USER}.firstname) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_FULLNAME])}`);
                        }
                        break;
                    case FieldsList.USER_EMAIL:
                        select.push(`${TablesListAliases.CORE_USER}.email AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EMAIL])}`);
                        break;
                    case FieldsList.USER_EMAIL_VALIDATION_STATUS:
                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.CORE_USER}.email_status = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
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
                                WHEN ${TablesListAliases.CORE_USER_LEVELS}.level = ${athena.renderStringInQueryCase(UserLevelsGroups.GodAdmin)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_GODADMIN])}
                                WHEN ${TablesListAliases.CORE_USER_LEVELS}.level = ${athena.renderStringInQueryCase(UserLevelsGroups.PowerUser)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_POWERUSER])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.USER_LEVEL_USER])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LEVEL])}`);
                        break;
                    case FieldsList.USER_DEACTIVATED:
                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.CORE_USER}.valid ${this.getCheckIsValidFieldClause()} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_DEACTIVATED])}`);
                        break;
                    case FieldsList.USER_EXPIRATION:
                        select.push(`${TablesListAliases.CORE_USER}.expiration AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_EXPIRATION])}`);
                        break;
                    case FieldsList.USER_SUSPEND_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.CORE_USER}.suspend_date AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_SUSPEND_DATE])}`);
                        break;
                    case FieldsList.USER_REGISTER_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.CORE_USER}.register_date AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_REGISTER_DATE])}`);
                        break;
                    case FieldsList.USER_LAST_ACCESS_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.CORE_USER}.lastenter AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_LAST_ACCESS_DATE])}`);
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
                        select.push(`${TablesListAliases.CORE_USER_BRANCHES_NAMES}.${FieldsList.USER_BRANCH_NAME} AS ${this.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_NAME])}`);
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
                        select.push(`${TablesListAliases.CORE_USER_BRANCHES}.branches AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_BRANCH_PATH])}`);
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
                        select.push(`${TablesListAliases.CORE_USER_BRANCHES}.codes AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_BRANCHES_CODES])}`);
                        break;
                    case FieldsList.USER_DIRECT_MANAGER:
                        if (!joinSkillManagersValue) {
                            joinSkillManagersValue = true;
                            from.push(`LEFT JOIN ${TablesList.SKILL_MANAGERS} AS ${TablesListAliases.SKILL_MANAGERS} ON ${TablesListAliases.SKILL_MANAGERS}.idEmployee = ${TablesListAliases.CORE_USER}.idst AND ${TablesListAliases.SKILL_MANAGERS}.type = 1`);
                            from.push(`LEFT JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER}s ON ${TablesListAliases.CORE_USER}s.idst = ${TablesListAliases.SKILL_MANAGERS}.idManager`);
                        }
                        let directManagerFullName = '';
                        if (this.session.platform.getShowFirstNameFirst()) {
                            directManagerFullName = `CONCAT(${TablesListAliases.CORE_USER}s.firstname, ' ', ${TablesListAliases.CORE_USER}s.lastname)`;
                        } else {
                            directManagerFullName = `CONCAT(${TablesListAliases.CORE_USER}s.lastname, ' ', ${TablesListAliases.CORE_USER}s.firstname)`;
                        }
                        select.push(`IF(${directManagerFullName} = ' ', SUBSTR(${TablesListAliases.CORE_USER}s.userid, 2), ${directManagerFullName}) AS ${athena.renderStringInQuerySelect(translations[FieldsList.USER_DIRECT_MANAGER])}`);
                        break;

                    // Ecommerce Transaction fields
                    case FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_1:
                        if (!joinCoreUserBilling) {
                            joinCoreUserBilling = true;
                            from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}.id = ${TablesListAliases.ECOMMERCE_TRANSACTION}.billing_info_id`);
                        }
                        select.push(`${TablesListAliases.CORE_USER_BILLING}.bill_address1 AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_1])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_2:
                        if (!joinCoreUserBilling) {
                            joinCoreUserBilling = true;
                            from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}.id = ${TablesListAliases.ECOMMERCE_TRANSACTION}.billing_info_id`);
                        }
                        select.push(`${TablesListAliases.CORE_USER_BILLING}.bill_address2 AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_2])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_CITY:
                        if (!joinCoreUserBilling) {
                            joinCoreUserBilling = true;
                            from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}.id = ${TablesListAliases.ECOMMERCE_TRANSACTION}.billing_info_id`);
                        }
                        select.push(`${TablesListAliases.CORE_USER_BILLING}.bill_city AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_CITY])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_STATE:
                        if (!joinCoreUserBilling) {
                            joinCoreUserBilling = true;
                            from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}.id = ${TablesListAliases.ECOMMERCE_TRANSACTION}.billing_info_id`);
                        }
                        select.push(`${TablesListAliases.CORE_USER_BILLING}.bill_state AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_STATE])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_ZIP_CODE:
                        if (!joinCoreUserBilling) {
                            joinCoreUserBilling = true;
                            from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}.id = ${TablesListAliases.ECOMMERCE_TRANSACTION}.billing_info_id`);
                        }
                        select.push(`${TablesListAliases.CORE_USER_BILLING}.bill_zip AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ZIP_CODE])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_COMPANY_NAME:
                        if (!joinCoreUserBilling) {
                            joinCoreUserBilling = true;
                            from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}.id = ${TablesListAliases.ECOMMERCE_TRANSACTION}.billing_info_id`);
                        }
                        select.push(`${TablesListAliases.CORE_USER_BILLING}.bill_company_name AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_COMPANY_NAME])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_VAT_NUMBER:
                        if (!joinCoreUserBilling) {
                            joinCoreUserBilling = true;
                            from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}.id = ${TablesListAliases.ECOMMERCE_TRANSACTION}.billing_info_id`);
                        }
                        select.push(`${TablesListAliases.CORE_USER_BILLING}.bill_vat_number AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_VAT_NUMBER])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_STATUS:
                        select.push(`
                            CASE
                                WHEN CAST(${TablesListAliases.ECOMMERCE_TRANSACTION}.cancelled AS INTEGER) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.PAYMENT_STATUS_CANCELED])}
                                WHEN CAST(${TablesListAliases.ECOMMERCE_TRANSACTION}.paid AS INTEGER) = 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.PAYMENT_STATUS_PENDING])}
                                WHEN CAST(${TablesListAliases.ECOMMERCE_TRANSACTION}.paid AS INTEGER) = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.PAYMENT_STATUS_SUCCESSFUL])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.PAYMENT_STATUS_FAILED])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_STATUS])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_METHOD:
                        if (!joinTransactionInfoAggregate) {
                            joinTransactionInfoAggregate = true;
                            from.push(`LEFT JOIN (${transactionInfoAggregateQuery}) AS ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE} ON ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE}.id_trans = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_trans`);
                        }

                        const final = `
                            CASE
                                WHEN
                                    ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE}.total_price - ${transactionDiscount} <= 0
                                    THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.FREE_PURCHASE])}
                                    ELSE ${TablesListAliases.ECOMMERCE_TRANSACTION}.payment_type
                                END`;

                        select.push(`(${final}) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_METHOD])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_ID:
                            select.push(`${TablesListAliases.ECOMMERCE_TRANSACTION}.id_trans AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_ID])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_SUBTOTAL_PRICE:
                        if (!joinTransactionInfoAggregate) {
                            joinTransactionInfoAggregate = true;
                            from.push(`LEFT JOIN (${transactionInfoAggregateQuery}) AS ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE} ON ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE}.id_trans = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_trans`);
                        }

                        select.push(`CONCAT(CAST(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE}.total_price AS VARCHAR), ' ', ${TablesListAliases.ECOMMERCE_TRANSACTION}.payment_currency) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_SUBTOTAL_PRICE])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_DISCOUNT:
                        select.push(`CONCAT(CAST(${transactionDiscount} AS VARCHAR), ' ', ${TablesListAliases.ECOMMERCE_TRANSACTION}.payment_currency) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_DISCOUNT])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_TOTAL_PRICE:
                        if (!joinTransactionInfoAggregate) {
                            joinTransactionInfoAggregate = true;
                            from.push(`LEFT JOIN (${transactionInfoAggregateQuery}) AS ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE} ON ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE}.id_trans = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_trans`);
                        }

                        select.push(`CONCAT(CAST(ROUND(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE}.total_price - ${transactionDiscount}, 2) AS VARCHAR), ' ', ${TablesListAliases.ECOMMERCE_TRANSACTION}.payment_currency) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_TOTAL_PRICE])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_COUPON_CODE:
                        if (!joinEcommerceCouponFieldValue) {
                            joinEcommerceCouponFieldValue = true;
                            from.push(`LEFT JOIN ${TablesList.ECOMMERCE_COUPON} AS ${TablesListAliases.ECOMMERCE_COUPON} ON ${TablesListAliases.ECOMMERCE_COUPON}.id_coupon = ${TablesListAliases.ECOMMERCE_TRANSACTION}.id_coupon`);
                        }
                        select.push(`${TablesListAliases.ECOMMERCE_COUPON}.code AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_COUPON_CODE])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_COUPON_DESCRIPTION:
                        if (!joinEcommerceCouponFieldValue) {
                            joinEcommerceCouponFieldValue = true;
                            from.push(`LEFT JOIN ${TablesList.ECOMMERCE_COUPON} AS ${TablesListAliases.ECOMMERCE_COUPON} ON ${TablesListAliases.ECOMMERCE_COUPON}.id_coupon = ${TablesListAliases.ECOMMERCE_TRANSACTION}.id_coupon`);
                        }
                        select.push(`${TablesListAliases.ECOMMERCE_COUPON}.description AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_COUPON_DESCRIPTION])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_PRICE:
                        select.push(`
                            CONCAT(CASE
                                WHEN ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.item_type = ${athena.renderStringInQueryCase(EcommItemTypes.COURSESEATS)} AND ${seats} IS NOT NULL AND CAST(${seats} as INTEGER) > 0 THEN CAST(CAST(${price} AS DOUBLE)/CAST(${seats} AS INTEGER) AS VARCHAR)
                                ELSE ${price}
                            END, ' ', ${TablesListAliases.ECOMMERCE_TRANSACTION}.payment_currency) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_PRICE])}`
                        );
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_EXTERNAL_TRANSACTION_ID:
                        select.push(`${TablesListAliases.ECOMMERCE_TRANSACTION}.payment_txn_id AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_EXTERNAL_TRANSACTION_ID])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_CREATION_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.ECOMMERCE_TRANSACTION}.date_creation AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_CREATION_DATE])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_DATE:
                        const dateActivatedColumn = `${TablesListAliases.ECOMMERCE_TRANSACTION}.date_activated`;
                        const dateActivatedQuery = `${this.mapTimestampDefaultValueWithDLV2(dateActivatedColumn, this.info.timezone)} AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_DATE])}`;
                        select.push(dateActivatedQuery);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_QUANTITY:
                        select.push(`
                            CASE
                                WHEN JSON_EXTRACT_SCALAR(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.item_data_json, '$.seats') IS NOT NULL THEN CAST(JSON_EXTRACT_SCALAR(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.item_data_json, '$.seats') as INTEGER)
                                ELSE 1
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_QUANTITY])}`);
                        break;

                    // Ecommerce Transaction Item fields
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_CODE:
                        select.push(`${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.code AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_CODE])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_NAME:
                        select.push(`${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.name AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_NAME])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_TYPE:
                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.item_type = ${athena.renderStringInQueryCase(EcommItemTypes.COURSE)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSE])}
                                WHEN ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.item_type = ${athena.renderStringInQueryCase(EcommItemTypes.COURSEPATH)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSEPATH])}
                                WHEN ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.item_type = ${athena.renderStringInQueryCase(EcommItemTypes.COURSESEATS)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.COURSESEATS])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.SUBSCRIPTION_PLAN])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_TYPE])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_WEBINAR_SESSION_NAME:
                        if (!joinLtCourseSessionValue) {
                            joinLtCourseSessionValue = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION} AS ${TablesListAliases.LT_COURSE_SESSION} ON ${TablesListAliases.LT_COURSE_SESSION}.id_session = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_date AND ${TablesListAliases.LT_COURSE_SESSION}.course_id = ${TablesListAliases.LEARNING_COURSE}.idCourse AND ${TablesListAliases.LEARNING_COURSE}.course_type = '${CourseTypes.Classroom}'`);
                        }
                        if (!joinWebinarSessionValue) {
                            joinWebinarSessionValue = true;
                            from.push(`LEFT JOIN ${TablesList.WEBINAR_SESSION} AS ${TablesListAliases.WEBINAR_SESSION} ON ${TablesListAliases.WEBINAR_SESSION}.id_session = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_date AND ${TablesListAliases.WEBINAR_SESSION}.course_id = ${TablesListAliases.LEARNING_COURSE}.idCourse AND ${TablesListAliases.LEARNING_COURSE}.course_type = '${CourseTypes.Webinar}'`);
                        }
                        select.push(`COALESCE(${TablesListAliases.LT_COURSE_SESSION}.name, ${TablesListAliases.WEBINAR_SESSION}.name) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_WEBINAR_SESSION_NAME])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_START_DATE:
                        if (!joinLtCourseSessionValue) {
                            joinLtCourseSessionValue = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION} AS ${TablesListAliases.LT_COURSE_SESSION} ON ${TablesListAliases.LT_COURSE_SESSION}.id_session = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_date AND ${TablesListAliases.LT_COURSE_SESSION}.course_id = ${TablesListAliases.LEARNING_COURSE}.idCourse AND ${TablesListAliases.LEARNING_COURSE}.course_type = '${CourseTypes.Classroom}'`);
                        }
                        if (!joinWebinarSessionValue) {
                            joinWebinarSessionValue = true;
                            from.push(`LEFT JOIN ${TablesList.WEBINAR_SESSION} AS ${TablesListAliases.WEBINAR_SESSION} ON ${TablesListAliases.WEBINAR_SESSION}.id_session = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_date AND ${TablesListAliases.WEBINAR_SESSION}.course_id = ${TablesListAliases.LEARNING_COURSE}.idCourse AND ${TablesListAliases.LEARNING_COURSE}.course_type = '${CourseTypes.Webinar}'`);
                        }
                        select.push(`DATE_FORMAT(COALESCE(${TablesListAliases.LT_COURSE_SESSION}.date_begin, ${TablesListAliases.WEBINAR_SESSION}.date_begin) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_START_DATE])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_END_DATE:
                        if (!joinLtCourseSessionValue) {
                            joinLtCourseSessionValue = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION} AS ${TablesListAliases.LT_COURSE_SESSION} ON ${TablesListAliases.LT_COURSE_SESSION}.id_session = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_date AND ${TablesListAliases.LT_COURSE_SESSION}.course_id = ${TablesListAliases.LEARNING_COURSE}.idCourse AND ${TablesListAliases.LEARNING_COURSE}.course_type = '${CourseTypes.Classroom}'`);
                        }
                        if (!joinWebinarSessionValue) {
                            joinWebinarSessionValue = true;
                            from.push(`LEFT JOIN ${TablesList.WEBINAR_SESSION} AS ${TablesListAliases.WEBINAR_SESSION} ON ${TablesListAliases.WEBINAR_SESSION}.id_session = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_date AND ${TablesListAliases.WEBINAR_SESSION}.course_id = ${TablesListAliases.LEARNING_COURSE}.idCourse AND ${TablesListAliases.LEARNING_COURSE}.course_type = '${CourseTypes.Webinar}'`);
                        }
                        select.push(`DATE_FORMAT(COALESCE(${TablesListAliases.LT_COURSE_SESSION}.date_end, ${TablesListAliases.WEBINAR_SESSION}.date_end) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_END_DATE])}`);
                        break;
                    case FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_LOCATION:
                        if (!joinLtLocationValue) {
                            joinLtLocationValue = true;
                            from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION} AS ${TablesListAliases.LT_COURSE_SESSION}e ON ${TablesListAliases.LT_COURSE_SESSION}e.id_session = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.id_date AND ${TablesListAliases.LT_COURSE_SESSION}e.course_id = ${TablesListAliases.LEARNING_COURSE}.idCourse AND ${TablesListAliases.LEARNING_COURSE}.course_type = '${CourseTypes.Classroom}'`);
                            from.push(`LEFT JOIN ${TablesList.LT_LOCATION_AGGREGATE} AS ${TablesListAliases.LT_LOCATION_AGGREGATE} ON ${TablesListAliases.LT_LOCATION_AGGREGATE}.id_session = ${TablesListAliases.LT_COURSE_SESSION}e.id_session`);
                        }
                        select.push(`${TablesListAliases.LT_LOCATION_AGGREGATE}.locations AS ${athena.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_LOCATION])}`);
                        break;

                    // CONTENT PARTNERS FIELDS
                    case FieldsList.CONTENT_PARTNERS_AFFILIATE:
                        if (!joinContentPartners) {
                            joinContentPartners = true;
                            from.push(`LEFT JOIN (${contentPartnersView}) AS ${TablesListAliases.CONTENT_PARTNERS} ON ${TablesListAliases.CONTENT_PARTNERS}.transaction_id = ${TablesListAliases.ECOMMERCE_TRANSACTION}.id_trans`);
                        }

                        const contentPartnerValue = `
                            CASE
                                WHEN JSON_EXTRACT_SCALAR(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.item_data_json, '$.position_snapshot.is_affiliate') IS NOT NULL THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])} END`;

                        select.push(`${contentPartnerValue} AS ${athena.renderStringInQuerySelect(translations[FieldsList.CONTENT_PARTNERS_AFFILIATE])}`);
                        break;
                    case FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_CODE:
                        if (!joinContentPartners) {
                            joinContentPartners = true;
                            from.push(`LEFT JOIN (${contentPartnersView}) AS ${TablesListAliases.CONTENT_PARTNERS} ON ${TablesListAliases.CONTENT_PARTNERS}.transaction_id = ${TablesListAliases.ECOMMERCE_TRANSACTION}.id_trans`);
                       }

                        select.push(`REPLACE(JSON_EXTRACT_SCALAR(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}.item_data_json, '$.position_snapshot.referral_id'), '"', '') AS ${athena.renderStringInQuerySelect(translations[FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_CODE])}`);
                        break;
                    case FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_SOURCE:
                        if (!joinContentPartners) {
                            joinContentPartners = true;
                            from.push(`LEFT JOIN (${contentPartnersView}) AS ${TablesListAliases.CONTENT_PARTNERS} ON ${TablesListAliases.CONTENT_PARTNERS}.transaction_id = ${TablesListAliases.ECOMMERCE_TRANSACTION}.id_trans`);
                        }

                        select.push(`${TablesListAliases.CONTENT_PARTNERS}.name AS ${athena.renderStringInQuerySelect(translations[FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_SOURCE])}`);
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
                                            from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = ${TablesListAliases.ECOMMERCE_TRANSACTION}.id_user`);
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
                                                select.push(`${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                    select.push(`DATE_FORMAT(${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}, '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} AND ${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`${TablesListAliases.CORE_USER_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.YesNo:
                                                select.push(`
                                                CASE
                                                    WHEN ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                                    WHEN ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId} = 2 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                                    ELSE ''
                                                END AS ${athena.renderStringInQuerySelect(userField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Country:
                                                from.push(`LEFT JOIN ${TablesList.CORE_COUNTRY} AS ${TablesListAliases.CORE_COUNTRY}_${fieldId} ON ${TablesListAliases.CORE_COUNTRY}_${fieldId}.id_country = ${TablesListAliases.CORE_USER_FIELD_VALUE}.field_${fieldId}`);
                                                select.push(`${TablesListAliases.CORE_COUNTRY}_${fieldId}.name_country AS ${athena.renderStringInQuerySelect(userField.title)}`);
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
        ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(select, translations, {user: userExtraFields.data.items, course: [], userCourse: [], webinar: [], classroom: []});
        }

        // get the sql like LIMIT for the query
        query += this.getQueryExportLimit(limit);

        return query;

    }

    public async getQuerySnowflake(limit = 0, isPreview: boolean, checkPuVisibility = true, fromSchedule = false): Promise<string> {
        // To implement
        const translations = await this.loadTranslations();
        const hydra = this.session.getHydra();

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;
        let fullUsers = '';

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilterSnowflake();
        }

        const fullCourses = await this.calculateCourseFilterSnowflake(true);

        let allCourses = this.info.courses ? this.info.courses.all : false;
        let fullLPs: number[] = [];
        let lpInCondition = '';

        if (!allCourses) {
            fullLPs = this.info.learningPlans ? this.info.learningPlans.learningPlans.map(a => parseInt(a.id as string, 10)) : [];
            lpInCondition = fullLPs.join(',');
        }

        if (this.session.user.getLevel() === UserLevels.POWER_USER) {
            const puCourses = await hydra.getPuLPs();
            if (allCourses) {
                allCourses = false;
                lpInCondition = this.getLPSubQuery(this.session.user.getIdUser());
            } else {
                lpInCondition = this.getLPSubQuery(this.session.user.getIdUser(), fullLPs);
            }
        }

        // Needed to save some info for the select switch statement
        const queryHelper = {
            select: [],
            archivedSelect: [],
            from: [],
            join: [],
            cte: [],
            groupBy: [],
            archivedGroupBy: [],
            userAdditionalFieldsSelect: [],
            userAdditionalFieldsFrom: [],
            userAdditionalFieldsId: [],
            courseAdditionalFieldsSelect: [],
            courseAdditionalFieldsFrom: [],
            courseAdditionalFieldsId: [],
            checkPuVisibility,
            translations,
        };

        let table = `SELECT * FROM ${TablesList.ECOMMERCE_TRANSACTION} WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND "id_user" IN (${fullUsers})`;
        }

        queryHelper.from.push(`(${table}) AS ${TablesListAliases.ECOMMERCE_TRANSACTION}`);

        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `"valid" = 1 ` : 'true';

        if (hideExpiredUsers) {
            table += ` AND ("expiration" IS NULL OR "expiration" > current_timestamp())`;
        }
        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."id_user"`);

        table = `SELECT * FROM ${TablesList.ECOMMERCE_TRANSACTION_INFO} WHERE TRUE`;

        if (!allCourses && fullCourses !== '' && lpInCondition !== '') {
            table += ` AND ("id_course" IN (${fullCourses}) OR "id_path" IN (${lpInCondition}))`;
        } else if (fullCourses !== '') {
            table += ` AND "id_course" IN (${fullCourses})`;
        } else if (!allCourses && lpInCondition !== '') {
            table += ` AND "id_path" IN (${lpInCondition})`;
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO} ON ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_trans" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."id_trans"`);
        queryHelper.from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}."idcourse" = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_course" AND ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."item_type" IN ('course', 'courseseats')`);
        queryHelper.from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH} AS ${TablesListAliases.LEARNING_COURSEPATH} ON ${TablesListAliases.LEARNING_COURSEPATH}."id_path" = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_path" AND ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."item_type" IN ('coursepath')`);

        const where = [
            `AND ${TablesListAliases.CORE_USER}."userid" <> '/Anonymous'`
        ];

        const userExtraFields = await hydra.getUserExtraFields();
        this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');

        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            this.getAdditionalUsersFieldsFiltersSnowflake(queryHelper, userExtraFields);
        }

        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged =
                    this.querySelectUserFields(field, queryHelper) ||
                    this.querySelectTransactionFields(field, queryHelper) ||
                    this.querySelectTransactionItemFields(field, queryHelper) ||
                    this.querySelectContentPartnerFields(field, queryHelper) ||
                    this.queryWithUserAdditionalFields(field, queryHelper, userExtraFields);
            }
        }

        if (queryHelper.userAdditionalFieldsId.length > 0) {
            queryHelper.cte.push(this.additionalUserFieldQueryWith(queryHelper.userAdditionalFieldsId));
            queryHelper.cte.push(this.additionalFieldQueryWith(queryHelper.userAdditionalFieldsFrom, queryHelper.userAdditionalFieldsSelect, queryHelper.userAdditionalFieldsId, 'id_user', TablesList.CORE_USER_FIELD_VALUE_WITH, TablesList.USERS_ADDITIONAL_FIELDS_TRANSLATIONS));
        }

        let setEmptyQuery = false;
        if (this.session.user.getLevel() === UserLevels.POWER_USER && !this.session.user.canViewEcommerceTransaction()) {
            setEmptyQuery = true;
        }

        let query = `
        ${queryHelper.cte.length > 0 ? `WITH ${queryHelper.cte.join(', ')}` : ''}
        SELECT ${queryHelper.select.join(', ')}
        FROM ${queryHelper.from.join(' ')}
        WHERE ${setEmptyQuery ? `TRUE = FALSE` : `TRUE ${where.join(' ')}`}`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(queryHelper.select, translations, {user: userExtraFields.data.items, course: [], userCourse: [], webinar: [], classroom: []}, fromSchedule);
        }

        // get the sql like LIMIT for the query
        query += this.getQueryExportLimit(limit);

        return query;
    }

    private querySelectTransactionFields(field: string, queryModel: any): boolean {

        // Subquery
        const transactionInfoAggregateQuery = `SELECT "id_trans", SUM(CAST("price" AS DOUBLE)) AS "total_price" FROM ${TablesList.ECOMMERCE_TRANSACTION_INFO} GROUP BY "id_trans"`;

        // Query constants for calculations
        const seats = `json_extract_path_text(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."item_data_json", '$.seats')`;
        const price = `${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."price"`;
        const transactionDiscount = `CAST(${TablesListAliases.ECOMMERCE_TRANSACTION}."discount" AS DOUBLE)`;

        const {select, from, join, translations} = queryModel;

        switch (field) {
            // Ecommerce Transaction fields
            case FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_1:
                if (!join.includes(joinedTables.CORE_USER_BILLING)) {
                    join.push(joinedTables.CORE_USER_BILLING);
                    from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}."id" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."billing_info_id"`);
                }
                select.push(`${TablesListAliases.CORE_USER_BILLING}."bill_address1" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_1])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_2:
                if (!join.includes(joinedTables.CORE_USER_BILLING)) {
                    join.push(joinedTables.CORE_USER_BILLING);
                    from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}."id" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."billing_info_id"`);
                }
                select.push(`${TablesListAliases.CORE_USER_BILLING}."bill_address2" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_2])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_CITY:
                if (!join.includes(joinedTables.CORE_USER_BILLING)) {
                    join.push(joinedTables.CORE_USER_BILLING);
                    from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}."id" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."billing_info_id"`);
                }
                select.push(`${TablesListAliases.CORE_USER_BILLING}."bill_city" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_CITY])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_STATE:
                if (!join.includes(joinedTables.CORE_USER_BILLING)) {
                    join.push(joinedTables.CORE_USER_BILLING);
                    from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}."id" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."billing_info_id"`);
                }
                select.push(`${TablesListAliases.CORE_USER_BILLING}."bill_state" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_STATE])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_ZIP_CODE:
                if (!join.includes(joinedTables.CORE_USER_BILLING)) {
                    join.push(joinedTables.CORE_USER_BILLING);
                    from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}."id" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."billing_info_id"`);
                }
                select.push(`${TablesListAliases.CORE_USER_BILLING}."bill_zip" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ZIP_CODE])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_COMPANY_NAME:
                if (!join.includes(joinedTables.CORE_USER_BILLING)) {
                    join.push(joinedTables.CORE_USER_BILLING);
                    from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}."id" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."billing_info_id"`);
                }
                select.push(`${TablesListAliases.CORE_USER_BILLING}."bill_company_name" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_COMPANY_NAME])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_VAT_NUMBER:
                if (!join.includes(joinedTables.CORE_USER_BILLING)) {
                    join.push(joinedTables.CORE_USER_BILLING);
                    from.push(`JOIN ${TablesList.CORE_USER_BILLING} AS ${TablesListAliases.CORE_USER_BILLING} ON ${TablesListAliases.CORE_USER_BILLING}."id" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."billing_info_id"`);
                }
                select.push(`${TablesListAliases.CORE_USER_BILLING}."bill_vat_number" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_VAT_NUMBER])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_STATUS:
                select.push(`
                        CASE
                            WHEN CAST(${TablesListAliases.ECOMMERCE_TRANSACTION}."cancelled" AS INTEGER) = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.PAYMENT_STATUS_CANCELED])}
                            WHEN CAST(${TablesListAliases.ECOMMERCE_TRANSACTION}."paid" AS INTEGER) = 0 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.PAYMENT_STATUS_PENDING])}
                            WHEN CAST(${TablesListAliases.ECOMMERCE_TRANSACTION}."paid" AS INTEGER) = 1 THEN ${this.renderStringInQueryCase(translations[FieldTranslation.PAYMENT_STATUS_SUCCESSFUL])}
                            ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.PAYMENT_STATUS_FAILED])}
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_STATUS])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_METHOD:
                if (!join.includes(joinedTables.TRANSACTION_INFO_AGGREGATE)) {
                    join.push(joinedTables.TRANSACTION_INFO_AGGREGATE);
                    from.push(`LEFT JOIN (${transactionInfoAggregateQuery}) AS ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE} ON ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE}."id_trans" = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_trans"`);
                }

                const paymentMethodStatement = `
                    CASE
                        WHEN ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE}."total_price" - ${transactionDiscount} <= 0
                        THEN ${this.renderStringInQueryCase(translations[FieldTranslation.FREE_PURCHASE])}
                        ELSE ${TablesListAliases.ECOMMERCE_TRANSACTION}."payment_type"
                    END`;

                select.push(`(${paymentMethodStatement}) AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_METHOD])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_ID:
                select.push(`${TablesListAliases.ECOMMERCE_TRANSACTION}."id_trans" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_ID])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_SUBTOTAL_PRICE:
                if (!join.includes(joinedTables.TRANSACTION_INFO_AGGREGATE)) {
                    join.push(joinedTables.TRANSACTION_INFO_AGGREGATE);
                    from.push(`LEFT JOIN (${transactionInfoAggregateQuery}) AS ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE} ON ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE}."id_trans" = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_trans"`);
                }

                select.push(`CONCAT(CAST(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE}."total_price" AS VARCHAR), ' ', ${TablesListAliases.ECOMMERCE_TRANSACTION}."payment_currency") AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_SUBTOTAL_PRICE])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_DISCOUNT:
                select.push(`CONCAT(CAST(${transactionDiscount} AS VARCHAR), ' ', ${TablesListAliases.ECOMMERCE_TRANSACTION}."payment_currency") AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_DISCOUNT])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_TOTAL_PRICE:
                if (!join.includes(joinedTables.TRANSACTION_INFO_AGGREGATE)) {
                    join.push(joinedTables.TRANSACTION_INFO_AGGREGATE);
                    from.push(`LEFT JOIN (${transactionInfoAggregateQuery}) AS ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE} ON ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE}."id_trans" = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_trans"`);
                }

                select.push(`CONCAT(CAST(ROUND(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO_AGGREGATE}."total_price" - ${transactionDiscount}, 2) AS VARCHAR), ' ', ${TablesListAliases.ECOMMERCE_TRANSACTION}."payment_currency") AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_TOTAL_PRICE])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_COUPON_CODE:
                if (!join.includes(joinedTables.ECOMMERCE_COUPON)) {
                    join.push(joinedTables.ECOMMERCE_COUPON);
                    from.push(`LEFT JOIN ${TablesList.ECOMMERCE_COUPON} AS ${TablesListAliases.ECOMMERCE_COUPON} ON ${TablesListAliases.ECOMMERCE_COUPON}."id_coupon" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."id_coupon"`);
                }
                select.push(`${TablesListAliases.ECOMMERCE_COUPON}."code" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_COUPON_CODE])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_COUPON_DESCRIPTION:
                if (!join.includes(joinedTables.ECOMMERCE_COUPON)) {
                    join.push(joinedTables.ECOMMERCE_COUPON);
                    from.push(`LEFT JOIN ${TablesList.ECOMMERCE_COUPON} AS ${TablesListAliases.ECOMMERCE_COUPON} ON ${TablesListAliases.ECOMMERCE_COUPON}."id_coupon" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."id_coupon"`);
                }
                select.push(`${TablesListAliases.ECOMMERCE_COUPON}."description" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_COUPON_DESCRIPTION])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_PRICE:
                select.push(`
                        CONCAT(CASE
                            WHEN ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."item_type" = ${this.renderStringInQueryCase(EcommItemTypes.COURSESEATS)} AND ${seats} IS NOT NULL AND CAST(${seats} as INTEGER) > 0 THEN CAST(CAST(${price} AS DOUBLE)/CAST(${seats} AS INTEGER) AS VARCHAR)
                            ELSE ${price}
                        END, ' ', ${TablesListAliases.ECOMMERCE_TRANSACTION}."payment_currency") AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_PRICE])}`
                );
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_EXTERNAL_TRANSACTION_ID:
                select.push(`${TablesListAliases.ECOMMERCE_TRANSACTION}."payment_txn_id" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_EXTERNAL_TRANSACTION_ID])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_CREATION_DATE:
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.ECOMMERCE_TRANSACTION}."date_creation"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_CREATION_DATE])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_DATE:
                select.push(`${this.queryConvertTimezone(`${TablesListAliases.ECOMMERCE_TRANSACTION}."date_activated"`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_DATE])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_QUANTITY:
                select.push(`
                        CASE
                            WHEN json_extract_path_text(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."item_data_json", '$.seats') IS NOT NULL THEN CAST(json_extract_path_text(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."item_data_json", '$.seats') as INTEGER)
                            ELSE 1
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_QUANTITY])}`);
                break;
            default:
                return false;
        }
        return true;
    }

    private querySelectTransactionItemFields(field: string, queryModel: any): boolean {
        const {select, from, join, translations} = queryModel;

        switch (field) {
            // Ecommerce Transaction Item fields
            case FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_CODE:
                select.push(`${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."code" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_CODE])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_NAME:
                select.push(`${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."name" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_NAME])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_ITEM_TYPE:
                select.push(`
                        CASE
                            WHEN ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."item_type" = ${this.renderStringInQueryCase(EcommItemTypes.COURSE)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSE])}
                            WHEN ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."item_type" = ${this.renderStringInQueryCase(EcommItemTypes.COURSEPATH)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSEPATH])}
                            WHEN ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."item_type" = ${this.renderStringInQueryCase(EcommItemTypes.COURSESEATS)} THEN ${this.renderStringInQueryCase(translations[FieldTranslation.COURSESEATS])}
                            ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.SUBSCRIPTION_PLAN])}
                        END AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_TYPE])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_WEBINAR_SESSION_NAME:
                if (!join.includes(joinedTables.LT_COURSE_SESSION)) {
                    join.push(joinedTables.LT_COURSE_SESSION);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION} AS ${TablesListAliases.LT_COURSE_SESSION} ON ${TablesListAliases.LT_COURSE_SESSION}."id_session" = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_date" AND ${TablesListAliases.LT_COURSE_SESSION}."course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse" AND ${TablesListAliases.LEARNING_COURSE}."course_type" = '${CourseTypes.Classroom}'`);
                }
                if (!join.includes(joinedTables.WEBINAR_SESSION)) {
                    join.push(joinedTables.WEBINAR_SESSION);
                    from.push(`LEFT JOIN ${TablesList.WEBINAR_SESSION} AS ${TablesListAliases.WEBINAR_SESSION} ON ${TablesListAliases.WEBINAR_SESSION}."id_session" = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_date" AND ${TablesListAliases.WEBINAR_SESSION}."course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse" AND ${TablesListAliases.LEARNING_COURSE}."course_type" = '${CourseTypes.Webinar}'`);
                }
                select.push(`COALESCE(${TablesListAliases.LT_COURSE_SESSION}."name", ${TablesListAliases.WEBINAR_SESSION}."name") AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_WEBINAR_SESSION_NAME])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_ITEM_START_DATE:
                if (!join.includes(joinedTables.LT_COURSE_SESSION)) {
                    join.push(joinedTables.LT_COURSE_SESSION);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION} AS ${TablesListAliases.LT_COURSE_SESSION} ON ${TablesListAliases.LT_COURSE_SESSION}."id_session" = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_date" AND ${TablesListAliases.LT_COURSE_SESSION}."course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse" AND ${TablesListAliases.LEARNING_COURSE}."course_type" = '${CourseTypes.Classroom}'`);
                }
                if (!join.includes(joinedTables.WEBINAR_SESSION)) {
                    join.push(joinedTables.WEBINAR_SESSION);
                    from.push(`LEFT JOIN ${TablesList.WEBINAR_SESSION} AS ${TablesListAliases.WEBINAR_SESSION} ON ${TablesListAliases.WEBINAR_SESSION}."id_session" = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_date" AND ${TablesListAliases.WEBINAR_SESSION}."course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse" AND ${TablesListAliases.LEARNING_COURSE}."course_type" = '${CourseTypes.Webinar}'`);
                }
                select.push(`${this.queryConvertTimezone(`COALESCE(${TablesListAliases.LT_COURSE_SESSION}."date_begin", ${TablesListAliases.WEBINAR_SESSION}."date_begin")`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_START_DATE])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_ITEM_END_DATE:
                if (!join.includes(joinedTables.LT_COURSE_SESSION)) {
                    join.push(joinedTables.LT_COURSE_SESSION);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION} AS ${TablesListAliases.LT_COURSE_SESSION} ON ${TablesListAliases.LT_COURSE_SESSION}."id_session" = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_date" AND ${TablesListAliases.LT_COURSE_SESSION}."course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse" AND ${TablesListAliases.LEARNING_COURSE}."course_type" = '${CourseTypes.Classroom}'`);
                }
                if (!join.includes(joinedTables.WEBINAR_SESSION)) {
                    join.push(joinedTables.WEBINAR_SESSION);
                    from.push(`LEFT JOIN ${TablesList.WEBINAR_SESSION} AS ${TablesListAliases.WEBINAR_SESSION} ON ${TablesListAliases.WEBINAR_SESSION}."id_session" = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_date" AND ${TablesListAliases.WEBINAR_SESSION}."course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse" AND ${TablesListAliases.LEARNING_COURSE}."course_type" = '${CourseTypes.Webinar}'`);
                }
                select.push(`${this.queryConvertTimezone(`COALESCE(${TablesListAliases.LT_COURSE_SESSION}."date_end", ${TablesListAliases.WEBINAR_SESSION}."date_end")`)} AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_END_DATE])}`);
                break;
            case FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_LOCATION:
                if (!join.includes(joinedTables.LT_LOCATION_AGGREGATE)) {
                    join.push(joinedTables.LT_LOCATION_AGGREGATE);
                    from.push(`LEFT JOIN ${TablesList.LT_COURSE_SESSION} AS ${TablesListAliases.LT_COURSE_SESSION}e ON ${TablesListAliases.LT_COURSE_SESSION}e."id_session" = ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_date" AND ${TablesListAliases.LT_COURSE_SESSION}e."course_id" = ${TablesListAliases.LEARNING_COURSE}."idcourse" AND ${TablesListAliases.LEARNING_COURSE}."course_type" = '${CourseTypes.Classroom}'`);
                    from.push(`LEFT JOIN ${TablesList.LT_LOCATION_AGGREGATE} AS ${TablesListAliases.LT_LOCATION_AGGREGATE} ON ${TablesListAliases.LT_LOCATION_AGGREGATE}."id_session" = ${TablesListAliases.LT_COURSE_SESSION}e."id_session"`);
                }
                select.push(`${TablesListAliases.LT_LOCATION_AGGREGATE}."locations" AS ${this.renderStringInQuerySelect(translations[FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_LOCATION])}`);
                break;
            default:
                return false;
        }
        return true;
    }

    private querySelectContentPartnerFields(field: string, queryModel: any): boolean {
        // This view is necessary! We grouping by transaction_id in and we avoid duplicate
        const contentPartnersView = `
            SELECT ANY_VALUE(${TablesListAliases.CONTENT_PARTNERS}."name") AS "name", ${TablesListAliases.CONTENT_PARTNERS_REFERRAL_LOG}."transaction_id"
            FROM ${TablesList.CONTENT_PARTNERS_REFERRAL_LOG} AS ${TablesListAliases.CONTENT_PARTNERS_REFERRAL_LOG}
                  LEFT JOIN ${TablesList.ECOMMERCE_TRANSACTION_INFO} AS ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO} on ${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."id_trans" = ${TablesListAliases.CONTENT_PARTNERS_REFERRAL_LOG}."transaction_id"
                  LEFT JOIN ${TablesList.CONTENT_PARTNERS_AFFILIATES} AS ${TablesListAliases.CONTENT_PARTNERS_AFFILIATES}
                            ON ${TablesListAliases.CONTENT_PARTNERS_AFFILIATES}."id_partner" = ${TablesListAliases.CONTENT_PARTNERS_REFERRAL_LOG}."partner_id"
                                OR json_extract_path_text(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."item_data_json", '$.position_snapshot.is_affiliate') = '1'
                  LEFT JOIN ${TablesList.CONTENT_PARTNERS} AS ${TablesListAliases.CONTENT_PARTNERS}
                            ON ${TablesListAliases.CONTENT_PARTNERS}."id" = CAST(json_extract_path_text(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."item_data_json", '$.position_snapshot.id_partner') AS INTEGER)
            GROUP BY ${TablesListAliases.CONTENT_PARTNERS_REFERRAL_LOG}."transaction_id"`;

        const {select, from, join, translations} = queryModel;

        switch (field) {
            case FieldsList.CONTENT_PARTNERS_AFFILIATE:
                if (!join.includes(joinedTables.CONTENT_PARTNERS_VIEW)) {
                    join.push(joinedTables.CONTENT_PARTNERS_VIEW);
                    from.push(`LEFT JOIN (${contentPartnersView}) AS ${TablesListAliases.CONTENT_PARTNERS} ON ${TablesListAliases.CONTENT_PARTNERS}."transaction_id" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."id_trans"`);
                }

                const contentPartnerValue = `
                        CASE
                            WHEN json_extract_path_text(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."item_data_json", '$.position_snapshot.is_affiliate') IS NOT NULL THEN ${this.renderStringInQueryCase(translations[FieldTranslation.YES])}
                            ELSE ${this.renderStringInQueryCase(translations[FieldTranslation.NO])} END`;

                select.push(`${contentPartnerValue} AS ${this.renderStringInQuerySelect(translations[FieldsList.CONTENT_PARTNERS_AFFILIATE])}`);
                break;
            case FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_CODE:
                if (!join.includes(joinedTables.CONTENT_PARTNERS_VIEW)) {
                    join.push(joinedTables.CONTENT_PARTNERS_VIEW);
                    from.push(`LEFT JOIN (${contentPartnersView}) AS ${TablesListAliases.CONTENT_PARTNERS} ON ${TablesListAliases.CONTENT_PARTNERS}."transaction_id" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."id_trans"`);
                }

                select.push(`REPLACE(json_extract_path_text(${TablesListAliases.ECOMMERCE_TRANSACTION_INFO}."item_data_json", '$.position_snapshot.referral_id'), '"', '') AS ${this.renderStringInQuerySelect(translations[FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_CODE])}`);
                break;
            case FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_SOURCE:
                if (!join.includes(joinedTables.CONTENT_PARTNERS_VIEW)) {
                    join.push(joinedTables.CONTENT_PARTNERS_VIEW);
                    from.push(`LEFT JOIN (${contentPartnersView}) AS ${TablesListAliases.CONTENT_PARTNERS} ON ${TablesListAliases.CONTENT_PARTNERS}."transaction_id" = ${TablesListAliases.ECOMMERCE_TRANSACTION}."id_trans"`);
                }

                select.push(`${TablesListAliases.CONTENT_PARTNERS}."name" AS ${this.renderStringInQuerySelect(translations[FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_SOURCE])}`);
                break;
            default:
                return false;
        }
        return true;
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
         * COURSES IMPORT
         */
        this.legacyCourseImport(filterData, report, legacyReport.id_filter);

        /**
         * LEARNING PLANS IMPORT
         */
        this.legacyLearningPlansImport(filterData, report, legacyReport.id_filter);


        // map selected fields and manage order by
        if (filterData.fields) {
            let legacyOrderField: LegacyOrderByParsed | undefined;
            const userMandatoryFieldsMap = this.mandatoryFields.user.reduce((previousValue: { [key: string]: string }, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});
            const ecommerceTransactionMandatoryFieldsMap = this.mandatoryFields.ecommerceTransaction.reduce((previousValue: { [key: string]: string }, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});

            // user fields and order by
            const userFieldsDescriptor = this.mapUserSelectedFields(filterData.fields.user, filterData.order, userMandatoryFieldsMap);
            report.fields.push(...userFieldsDescriptor.fields);
            if (userFieldsDescriptor.orderByDescriptor) legacyOrderField = userFieldsDescriptor.orderByDescriptor;

            // ecommerce transaction fields and order by
            const ecommerceTransactionFieldsDescriptor = this.mapEcommerceTransactionSelectedFields(filterData.fields.ecommerce, filterData.order, ecommerceTransactionMandatoryFieldsMap);
            report.fields.push(...ecommerceTransactionFieldsDescriptor.fields);
            if (ecommerceTransactionFieldsDescriptor.orderByDescriptor) legacyOrderField = ecommerceTransactionFieldsDescriptor.orderByDescriptor;

            // ecommerce transaction item fields and order by
            const ecommerceTransactionItemFieldsDescriptor = this.mapEcommerceTransactionItemSelectedFields(filterData.fields.ecommerce_item, filterData.order, userMandatoryFieldsMap);
            report.fields.push(...ecommerceTransactionItemFieldsDescriptor.fields);
            if (ecommerceTransactionItemFieldsDescriptor.orderByDescriptor) legacyOrderField = ecommerceTransactionItemFieldsDescriptor.orderByDescriptor;

            // content partner fields and order by
            const contentPartnersFieldsDescriptor = this.mapContentPartnersSelectedFields(filterData.fields.content_partner, filterData.order, userMandatoryFieldsMap);
            report.fields.push(...contentPartnersFieldsDescriptor.fields);
            if (contentPartnersFieldsDescriptor.orderByDescriptor) legacyOrderField = contentPartnersFieldsDescriptor.orderByDescriptor;

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
