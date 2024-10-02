import {
    FieldsList,
    FieldTranslation,
    ReportAvailablesFields,
    ReportManagerCertificationsFilter,
    ReportManagerInfo,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility,
    TablesList,
    TablesListAliases
} from './report-manager';
import SessionManager from '../services/session/session-manager.session';
import httpContext from 'express-http-context';
import { LegacyOrderByParsed, LegacyReport, VisibilityRule } from './migration-component';
import { DateOptions, DateOptionsValueDescriptor, SortingOptions, VisibilityTypes } from './custom-report';
import { v4 } from 'uuid';
import { ReportsTypes } from '../reports/constants/report-types';
import { Utils } from '../reports/utils';
import { UserLevels } from '../services/session/user-manager.session';
import { AdditionalFieldsTypes, UserLevelsGroups } from './base';
import { UserExtraFieldsResponse } from '../services/hydra';
import { BaseReportManager } from './base-report-manager';

export class UsersCertificationsManager extends BaseReportManager {
    reportType = ReportsTypes.USERS_CERTIFICATIONS;
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
            FieldsList.USER_BRANCHES_CODES,
            FieldsList.USER_DIRECT_MANAGER
        ],
        certifications: [
            FieldsList.CERTIFICATION_TITLE,
            FieldsList.CERTIFICATION_CODE,
            FieldsList.CERTIFICATION_DESCRIPTION,
            FieldsList.CERTIFICATION_DURATION,
            FieldsList.CERTIFICATION_COMPLETED_ACTIVITY,
            FieldsList.CERTIFICATION_ISSUED_ON,
            FieldsList.CERTIFICATION_TO_RENEW_IN,
            FieldsList.CERTIFICATION_STATUS,
        ]
    };

    mandatoryFields = {
        user : [
            FieldsList.USER_USERID
        ],
        certifications: [
            FieldsList.CERTIFICATION_TITLE
        ],
    };

    public constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap | undefined) {
        super(session, reportDetails);
        this.logger = httpContext.get('logger');
    }

    async getAvailablesFields(): Promise<ReportAvailablesFields> {
        const result: any = await this.getBaseAvailableFields();

        const userExtraFields = await this.getAvailableUserExtraFields();
        result.user.push(...userExtraFields);

        return result;
    }

    public async getBaseAvailableFields(): Promise<ReportAvailablesFields> {
        const result: ReportAvailablesFields = {};
        const translations = await this.loadTranslations(true);

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

        // Recover certification fields
        result.certifications = [];
        for (const field of this.allFields.certifications) {
            result.certifications.push({
                field,
                idLabel: field,
                mandatory: this.mandatoryFields.certifications.includes(field),
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        return result;
    }
    async getQuery(limit: number, isPreview: boolean, checkPuVisibility = true): Promise<string> {
        const translations = await this.loadTranslations();
        const hydra = this.session.getHydra();
        const athena = this.session.getAthena();

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;
        let fullUsers = '';

        const allCertifications = this.info.certifications ? this.info.certifications.all : false;
        let fullCertifications: number[] = [];

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilter(checkPuVisibility);
        }

        if (!allCertifications) {
            fullCertifications = this.info.certifications ? this.info.certifications.certifications.map(a => a.id) : [];
        }

        let timestampNull = '';
        if (this.session.platform.isDatalakeV2Active()) {
            timestampNull = `OR ${TablesListAliases.CERTIFICATION_USER}.expire_at = TIMESTAMP '-0001-11-30 00:00:00.000'`;
        }

        const select = [];
        const from = [];
        let table = `SELECT * FROM ${TablesList.CERTIFICATION_USER} WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND id_user IN (${fullUsers})`;
        }

        from.push(`(${table}) AS ${TablesListAliases.CERTIFICATION_USER}`);


        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `valid ${this.getCheckIsValidFieldClause()}` : 'true';

        if (hideExpiredUsers) {
            table += ` AND (expiration IS NULL OR expiration > NOW())`;
        }
        from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.CERTIFICATION_USER}.id_user`);

        // JOIN CERTIFICATION ITEM
        from.push(`JOIN ${TablesList.CERTIFICATION_ITEM} AS ${TablesListAliases.CERTIFICATION_ITEM} ON ${TablesListAliases.CERTIFICATION_USER}.id_cert_item = ${TablesListAliases.CERTIFICATION_ITEM}.id`);

        table = `SELECT * FROM ${TablesList.CERTIFICATION} WHERE TRUE`;

        if (!allCertifications) {
            if (fullCertifications.length > 0) {
                table += ` AND id_cert IN (${fullCertifications.join(',')})`;
            } else {
                table += ' AND FALSE';
            }
        }

        from.push(`JOIN (${table}) AS ${TablesListAliases.CERTIFICATION} ON ${TablesListAliases.CERTIFICATION_ITEM}.id_cert = ${TablesListAliases.CERTIFICATION}.id_cert`);

        let where = [
            `AND ${TablesListAliases.CORE_USER}.userid <> '/Anonymous'`,
            `AND ${TablesListAliases.CERTIFICATION}.deleted ${this.getCheckIsInvalidFieldClause()}`
        ];

        // Certifications filters
        if (this.info.certifications) {
            const archivedCertificationsFilter = ` OR ${TablesListAliases.CERTIFICATION_USER}.archived = 1`;

            // Active or Expired filter
            if (this.info.certifications.activeCertifications && this.info.certifications.expiredCertifications) {
                // Fine in this way, we don't need any filter in this case
            } else if (this.info.certifications.activeCertifications) {

                let activeCertificationsFilter = `${TablesListAliases.CERTIFICATION_USER}.on_datetime <= CURRENT_TIMESTAMP
                    AND (${TablesListAliases.CERTIFICATION_USER}.expire_at > CURRENT_TIMESTAMP OR ${TablesListAliases.CERTIFICATION_USER}.expire_at IS NULL ${timestampNull})`;

                // If archivedCertification filter is enabled append it to the activeCertification filter (in OR condition)
                if (this.info.certifications.archivedCertifications) {
                    activeCertificationsFilter = activeCertificationsFilter + archivedCertificationsFilter;
                }
                where.push(`AND (${activeCertificationsFilter})`);

            } else if (this.info.certifications.expiredCertifications) {

                let expiredCertificationsFilter = `${TablesListAliases.CERTIFICATION_USER}.expire_at < CURRENT_TIMESTAMP`;
                const dl2TimestampFix = ` AND ${TablesListAliases.CERTIFICATION_USER}.expire_at != TIMESTAMP '-0001-11-30 00:00:00.000'`;

                if (this.session.platform.isDatalakeV2Active()) {
                    expiredCertificationsFilter = expiredCertificationsFilter + dl2TimestampFix;
                }

                // Append archivedCertifications to the previous filter
                if (this.info.certifications.archivedCertifications) {
                    expiredCertificationsFilter = expiredCertificationsFilter + archivedCertificationsFilter;
                }
                where.push(`AND (${expiredCertificationsFilter})`);
            }

            // Archived Certifications filter
            // Filter only for archived records
            if (this.info.certifications.archivedCertifications && !this.info.certifications.activeCertifications && !this.info.certifications.expiredCertifications) {
                where.push(`AND ${TablesListAliases.CERTIFICATION_USER}.archived = 1`);
            }
            // Exclude the archived records (case when active and expired certification filters are enabled)
            if (!this.info.certifications.archivedCertifications) {
                where.push(`AND ${TablesListAliases.CERTIFICATION_USER}.archived = 0`);
            }


            // Dates filter
            let certificationDateFilter = '';
            let expirationDateFilter = '';

            if (!this.info.certifications.certificationDate.any) {
                certificationDateFilter = this.buildDateFilter(TablesListAliases.CERTIFICATION_USER + '.on_datetime', this.info.certifications.certificationDate, '', true);
            }

            if (!this.info.certifications.certificationExpirationDate.any) {
                expirationDateFilter = this.buildDateFilter(TablesListAliases.CERTIFICATION_USER + '.expire_at', this.info.certifications.certificationExpirationDate, '', true);
            }

            if (certificationDateFilter !== '' && expirationDateFilter !== '' && this.info.certifications.conditions !== DateOptions.CONDITIONS) {
                where.push(`AND (${certificationDateFilter} OR ${expirationDateFilter})`);
            } else if (certificationDateFilter !== '' && expirationDateFilter !== '') {
                where.push(`AND (${certificationDateFilter} AND ${expirationDateFilter})`);
            } else if (certificationDateFilter !== '') {
                where.push(`AND ${certificationDateFilter}`);
            } else if (expirationDateFilter !== '') {
                where.push(`AND ${expirationDateFilter}`);
            }
        }

        // Variables to check if the specified table was already joined in the query
        let joinCoreUserFieldValue = false;
        let joinSkillManagersValue = false;

        // Variables to check if the specified materialized tables was already joined in the query
        let joinCoreUserLevels = false;
        let joinCoreUserBranches = false;
        let userExtraFields = {data: { items: []} } as UserExtraFieldsResponse;

        // load the translations of additional fields only if are selected
        if (this.info.fields.find(item => item.includes('user_extrafield_'))) {
            userExtraFields = await this.session.getHydra().getUserExtraFields();
            this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');
        }
        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            const tmp = await this.getAdditionalFieldsFilters(userExtraFields);
            if (tmp.length) {
                if (!joinCoreUserFieldValue) {
                    joinCoreUserFieldValue = true;
                    from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = ${TablesListAliases.CERTIFICATION_USER}.id_user`);
                }
                where = where.concat(tmp);
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
                            if (this.session.user.getLevel() === UserLevels.POWER_USER && checkPuVisibility === true) {
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
                            if (this.session.user.getLevel() === UserLevels.POWER_USER && checkPuVisibility === true) {
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

                    // Certification fields
                    case FieldsList.CERTIFICATION_TITLE:
                        select.push(`${TablesListAliases.CERTIFICATION}.title AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_TITLE])}`);
                        break;
                    case FieldsList.CERTIFICATION_CODE:
                        select.push(`${TablesListAliases.CERTIFICATION}.code AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_CODE])}`);
                        break;
                    case FieldsList.CERTIFICATION_DESCRIPTION:
                        select.push(`${TablesListAliases.CERTIFICATION}.description AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_DESCRIPTION])}`);
                        break;
                    case FieldsList.CERTIFICATION_DURATION:
                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.CERTIFICATION}.duration = 0 THEN ${athena.renderStringInQueryCase(FieldTranslation.NEVER)}
                                WHEN ${TablesListAliases.CERTIFICATION}.duration_unit = ${athena.renderStringInQueryCase('day')} THEN CONCAT(CAST(${TablesListAliases.CERTIFICATION}.duration AS VARCHAR), ' ', ${athena.renderStringInQueryCase(FieldTranslation.DAYS)})
                                WHEN ${TablesListAliases.CERTIFICATION}.duration_unit = ${athena.renderStringInQueryCase('week')} THEN CONCAT(CAST(${TablesListAliases.CERTIFICATION}.duration AS VARCHAR), ' ', ${athena.renderStringInQueryCase(FieldTranslation.WEEKS)})
                                WHEN ${TablesListAliases.CERTIFICATION}.duration_unit = ${athena.renderStringInQueryCase('month')} THEN CONCAT(CAST(${TablesListAliases.CERTIFICATION}.duration AS VARCHAR), ' ', ${athena.renderStringInQueryCase(FieldTranslation.MONTHS)})
                                WHEN ${TablesListAliases.CERTIFICATION}.duration_unit = ${athena.renderStringInQueryCase('year')} THEN CONCAT(CAST(${TablesListAliases.CERTIFICATION}.duration AS VARCHAR), ' ', ${athena.renderStringInQueryCase(FieldTranslation.YEARS)})
                                ELSE NULL
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_DURATION])}`);
                        break;
                    case FieldsList.CERTIFICATION_COMPLETED_ACTIVITY:
                        from.push(`LEFT JOIN ${TablesList.LEARNING_COURSE} AS ${TablesListAliases.LEARNING_COURSE} ON ${TablesListAliases.LEARNING_COURSE}.idCourse = ${TablesListAliases.CERTIFICATION_ITEM}.id_item`);
                        from.push(`LEFT JOIN ${TablesList.LEARNING_COURSEPATH} AS ${TablesListAliases.LEARNING_COURSEPATH} ON ${TablesListAliases.LEARNING_COURSEPATH}.id_path = ${TablesListAliases.CERTIFICATION_ITEM}.id_item`);
                        if (this.session.platform.checkPluginTranscriptEnabled()) {
                            from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_RECORD} AS ${TablesListAliases.TRANSCRIPTS_RECORD} ON ${TablesListAliases.TRANSCRIPTS_RECORD}.id_record = ${TablesListAliases.CERTIFICATION_ITEM}.id_item`);
                            from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_COURSE} AS ${TablesListAliases.TRANSCRIPTS_COURSE} ON ${TablesListAliases.TRANSCRIPTS_COURSE}.id = ${TablesListAliases.TRANSCRIPTS_RECORD}.course_id`);
                        }

                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.CERTIFICATION_ITEM}.item_type = ${athena.renderStringInQueryCase('plan')} THEN ${TablesListAliases.LEARNING_COURSEPATH}.path_name
                                WHEN ${TablesListAliases.CERTIFICATION_ITEM}.item_type = ${athena.renderStringInQueryCase('course')} THEN  ${TablesListAliases.LEARNING_COURSE}.name
                                ` + (this.session.platform.checkPluginTranscriptEnabled() ? `WHEN ${TablesListAliases.CERTIFICATION_ITEM}.item_type = ${athena.renderStringInQueryCase('transcript')} THEN CASE WHEN ${TablesListAliases.TRANSCRIPTS_RECORD}.course_name <> '' THEN ${TablesListAliases.TRANSCRIPTS_RECORD}.course_name ELSE ${TablesListAliases.TRANSCRIPTS_COURSE}.course_name END` : '') + `
                                ELSE ''
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_COMPLETED_ACTIVITY])}`);
                        break;
                    case FieldsList.CERTIFICATION_ISSUED_ON:
                        const formatOnDateTime = `DATE_FORMAT(${TablesListAliases.CERTIFICATION_USER}.on_datetime AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s')`;
                        if (this.session.platform.isDatalakeV2Active()) {
                            select.push(`IF(${TablesListAliases.CERTIFICATION_USER}.on_datetime != TIMESTAMP '-0001-11-30 00:00:00.000', ${formatOnDateTime}, '') AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_ISSUED_ON])}`);
                        } else {
                            select.push(`${formatOnDateTime} AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_ISSUED_ON])}`);
                        }
                        break;
                    case FieldsList.CERTIFICATION_TO_RENEW_IN:
                        const formatExpiredAt = `DATE_FORMAT(${TablesListAliases.CERTIFICATION_USER}.expire_at AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s')`;
                        if (this.session.platform.isDatalakeV2Active()) {
                            select.push(`IF(${TablesListAliases.CERTIFICATION_USER}.expire_at != TIMESTAMP '-0001-11-30 00:00:00.000', ${formatExpiredAt}, '') AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_TO_RENEW_IN])}`);

                        } else {
                            select.push(`${formatExpiredAt} AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_TO_RENEW_IN])}`);
                        }
                        break;
                    case FieldsList.CERTIFICATION_STATUS:
                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.CERTIFICATION_USER}.archived = 0 AND ${TablesListAliases.CERTIFICATION_USER}.on_datetime <= CURRENT_TIMESTAMP AND (${TablesListAliases.CERTIFICATION_USER}.expire_at > CURRENT_TIMESTAMP OR ${TablesListAliases.CERTIFICATION_USER}.expire_at IS NULL ${timestampNull})
                                  THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.CERTIFICATION_ACTIVE])}
                                WHEN ${TablesListAliases.CERTIFICATION_USER}.archived = 0 AND ${TablesListAliases.CERTIFICATION_USER}.expire_at < CURRENT_TIMESTAMP THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.CERTIFICATION_EXPIRED])}
                                WHEN ${TablesListAliases.CERTIFICATION_USER}.archived = 1 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.CERTIFICATION_ARCHIVED])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_STATUS])}`);
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
                                            from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = ${TablesListAliases.CERTIFICATION_USER}.id_user`);
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

    public async getQuerySnowflake(limit: number, isPreview: boolean, checkPuVisibility = true, fromSchedule = false): Promise<string> {
        const translations = await this.loadTranslations();
        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;

        let fullUsers = '';
        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilterSnowflake(checkPuVisibility);
        }

        const allCertifications = this.info.certifications ? this.info.certifications.all : false;
        let fullCertifications: number[] = [];
        if (!allCertifications) {
            fullCertifications = this.info.certifications ? this.info.certifications.certifications.map(a => a.id) : [];
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
            translations,
            checkPuVisibility
        };

        let table = `SELECT * FROM ${TablesList.CERTIFICATION_USER} WHERE TRUE`;
        if (fullUsers !== '') {
            table += ` AND "id_user" IN (${fullUsers})`;
        }
        queryHelper.from.push(`(${table}) AS ${TablesListAliases.CERTIFICATION_USER}`);

        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? `"valid" = 1 ` : 'true';

        if (hideExpiredUsers) {
            table += ` AND ("expiration" IS NULL OR "expiration" > current_timestamp())`;
        }
        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.CERTIFICATION_USER}."id_user"`);

        // JOIN CERTIFICATION ITEM
        queryHelper.from.push(`JOIN ${TablesList.CERTIFICATION_ITEM} AS ${TablesListAliases.CERTIFICATION_ITEM} ON ${TablesListAliases.CERTIFICATION_USER}."id_cert_item" = ${TablesListAliases.CERTIFICATION_ITEM}."id"`);

        table = `SELECT * FROM ${TablesList.CERTIFICATION} WHERE TRUE`;

        if (!allCertifications) {
            if (fullCertifications.length > 0) {
                table += ` AND "id_cert" IN (${fullCertifications.join(',')})`;
            } else {
                table += ` AND FALSE`;
            }
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CERTIFICATION} ON ${TablesListAliases.CERTIFICATION_ITEM}."id_cert" = ${TablesListAliases.CERTIFICATION}."id_cert"`);

        let where = [
            `AND ${TablesListAliases.CORE_USER}."userid" <> '/Anonymous'`,
            `AND ${TablesListAliases.CERTIFICATION}."deleted" = 0`
        ];

        const certFilters = this.getCertificationsFilterSnowflake();
        if (certFilters.length) {
            where = where.concat(certFilters);
        }

        let userExtraFields = {data: { items: []} } as UserExtraFieldsResponse;
        // load the translations of additional fields only if are selected
        if (this.info.fields.find(item => item.includes('user_extrafield_'))) {
            userExtraFields = await this.session.getHydra().getUserExtraFields();
            this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');
        }

        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            this.getAdditionalUsersFieldsFiltersSnowflake(queryHelper, userExtraFields);
        }

        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged =
                    this.querySelectUserFields(field, queryHelper) ||
                    this.querySelectCertificationFields(field, queryHelper) ||
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
            ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}`;

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

    protected getReportDefaultStructure(report: ReportManagerInfo, title: string, platform: string, idUser: number, description: string): ReportManagerInfo {
        const id = v4();
        const date = new Date();

        const tmpFields: string[] = [];

        this.mandatoryFields.user.forEach(element => {
            tmpFields.push(element);
        });

        this.mandatoryFields.certifications.forEach(element => {
            tmpFields.push(element);
        });

        report.fields = tmpFields;
        report.deleted = false;

        report.idReport = id;
        report.type = this.reportType;
        report.timezone = this.session.user.getTimezone();
        report.title = title;
        report.platform = platform;

        report.users = new ReportManagerInfoUsersFilter();
        report.users.all = true;

        report.certifications = new ReportManagerCertificationsFilter();

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

        // manage the planning default fields
        report.planning = this.getDefaultPlanningFields();
        report.sortingOptions = this.getSortingOptions();

        if (description) {
            report.description = description;
        }

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
         * CERTIFICATIONS IMPORT
         */
        this.legacyCertificationsImport(filterData, report, legacyReport.id_filter);

        // check filters section validity
        if (!filterData.filters) {
            this.logger.error(`No legacy filters section for id report: ${legacyReport.id_filter}`);
            throw new Error('No legacy filters section');
        }
        const filters = filterData.filters;

        // import the certifications status preference
        if (filters.certification_status && report.certifications) {
            switch (filters.certification_status) {
                case 'expired':
                    report.certifications.activeCertifications = false;
                    break;
                case 'active':
                    report.certifications.expiredCertifications = false;
                    break;
                case 'all':
                    report.certifications.archivedCertifications = true;
                    break;
            }
        }

        // Certification Date
        if (filters.start_date.type !== 'any' && report.certifications) {
            report.certifications.certificationDate = utils.parseLegacyFilterDate(report.certifications.certificationDate as DateOptionsValueDescriptor, filters.start_date);
        }
        // Certification Date
        if (filters.end_date.type !== 'any' && report.certifications) {
            report.certifications.certificationExpirationDate = utils.parseLegacyFilterDate(report.certifications.certificationExpirationDate as DateOptionsValueDescriptor, filters.end_date);
        }
        // Conditions
        if (filters.condition_status && report.certifications) {
            report.certifications.conditions = filters.condition_status === 'and' ? 'allConditions' : 'atLeastOneCondition';
        }

        // map selected fields and manage order by
        if (filterData.fields) {
            let legacyOrderField: LegacyOrderByParsed | undefined;
            const userMandatoryFieldsMap = this.mandatoryFields.user.reduce((previousValue: {[key: string]: string}, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});
            const certificationsMandatoryFieldsMap = this.mandatoryFields.certifications.reduce((previousValue: {[key: string]: string}, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});
            const userFieldsDescriptor = this.mapUserSelectedFields(filterData.fields.user, filterData.order, userMandatoryFieldsMap);
            report.fields.push(...userFieldsDescriptor.fields);
            if (userFieldsDescriptor.orderByDescriptor) legacyOrderField = userFieldsDescriptor.orderByDescriptor;

            const certificationsFieldsDescriptor = this.mapCertificationSelectedFields(filterData.fields.certification, filterData.order, certificationsMandatoryFieldsMap);
            report.fields.push(...certificationsFieldsDescriptor.fields);
            if (certificationsFieldsDescriptor.orderByDescriptor) legacyOrderField = certificationsFieldsDescriptor.orderByDescriptor;

            const enrollmentFieldsDescriptor = this.mapEnrollmentSelectedFields(filterData.fields.enrollment, filterData.order);
            report.fields.push(...enrollmentFieldsDescriptor.fields);
            if (enrollmentFieldsDescriptor.orderByDescriptor) legacyOrderField = enrollmentFieldsDescriptor.orderByDescriptor;

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
}
