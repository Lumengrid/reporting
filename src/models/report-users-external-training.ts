import { AdditionalFieldsTypes, ExternalTrainingStatus, UserLevelsGroups } from './base';
import {
    FieldsList,
    FieldTranslation,
    ReportAvailablesFields,
    ReportManagerInfo,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility,
    TablesList,
    TablesListAliases
} from './report-manager';
import { v4 } from 'uuid';
import { DateOptionsValueDescriptor, SortingOptions, VisibilityTypes } from './custom-report';
import SessionManager from '../services/session/session-manager.session';
import { UserLevels } from '../services/session/user-manager.session';
import { SessionLoggerService } from '../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { ReportsTypes } from '../reports/constants/report-types';
import { LegacyOrderByParsed, LegacyReport, VisibilityRule } from './migration-component';
import { Utils } from '../reports/utils';
import { BaseReportManager } from './base-report-manager';

export class UsersExternalTrainingManager extends BaseReportManager {
    reportType = ReportsTypes.USERS_EXTERNAL_TRAINING;
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
        externalTraining: [
            FieldsList.EXTERNAL_TRAINING_COURSE_NAME,
            FieldsList.EXTERNAL_TRAINING_COURSE_TYPE,
            FieldsList.EXTERNAL_TRAINING_SCORE,
            FieldsList.EXTERNAL_TRAINING_DATE,
            FieldsList.EXTERNAL_TRAINING_DATE_START,
            FieldsList.EXTERNAL_TRAINING_CREDITS,
            FieldsList.EXTERNAL_TRAINING_TRAINING_INSTITUTE,
            FieldsList.EXTERNAL_TRAINING_CERTIFICATE,
            FieldsList.EXTERNAL_TRAINING_STATUS
        ]
    };

    mandatoryFields = {
        user : [
            FieldsList.USER_USERID
        ],
        externalTraining: []
    };

    logger: SessionLoggerService;

    public constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap | undefined) {
        super(session, reportDetails);
        this.logger = httpContext.get('logger');
    }

    public async getQuery(limit = 0, isPreview: boolean): Promise<string> {
        const translations = await this.loadTranslations();
        const athena = this.session.getAthena();

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;
        let fullUsers = '';

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilter();
        }

        const select = [];
        const from = [];

        let table = `SELECT * FROM ${TablesList.TRANSCRIPTS_RECORD} WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND id_user IN (${fullUsers})`;
        }

        if (this.info.externalTrainingStatusFilter) {
            const toFilter: string[] = [];
            if (this.info.externalTrainingStatusFilter.approved) {
                toFilter.push(ExternalTrainingStatus.APPROVED);
            }
            if (this.info.externalTrainingStatusFilter.waiting) {
                toFilter.push(ExternalTrainingStatus.WAITING);
            }
            if (this.info.externalTrainingStatusFilter.rejected) {
                toFilter.push(ExternalTrainingStatus.REJECTED);
            }

            if (toFilter.length > 0 && toFilter.length < 3) {
                table += ` AND status IN ('${toFilter.join("','")}')`;
            }
        }

        if (this.info.externalTrainingDate?.any === false) {
            table += ' ' + this.buildDateFilter('to_date', this.info.externalTrainingDate, 'AND', true);
        }

        from.push(`(${table}) AS ${TablesListAliases.TRANSCRIPTS_RECORD}`);


        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? 'valid ' + this.getCheckIsValidFieldClause() : 'true';

        if (hideExpiredUsers) {
            table += ` AND (expiration IS NULL OR expiration > NOW())`;
        }
        from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.TRANSCRIPTS_RECORD}.id_user`);


        let where = [
            `AND ${TablesListAliases.CORE_USER}.userid <> '/Anonymous'`
        ];

        // Variables to check if the specified table was already joined in the query
        let joinCoreUserLevels = false;
        let joinCoreUserFieldValue = false;
        let joinCoreUserBranches = false;
        let joinTranscriptsCourse = false;
        let joinTranscriptsFieldValue = false;
        let joinTranscriptsInstitute = false;
        let joinSkillManagersValue = false;

        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        const translationValue = this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');
        const externalTrainingtExtraFields = await this.session.getHydra().getTranscriptExtraFields();
        this.updateExtraFieldsDuplicated(externalTrainingtExtraFields.data.items, translations, 'external-training', translationValue);

        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            const tmp = await this.getAdditionalFieldsFilters(userExtraFields);
            if (tmp.length) {
                if (!joinCoreUserFieldValue) {
                    joinCoreUserFieldValue = true;
                    from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = ${TablesListAliases.TRANSCRIPTS_RECORD}.id_user`);
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

                    // External Training fields
                    case FieldsList.EXTERNAL_TRAINING_COURSE_NAME:
                        if (!joinTranscriptsCourse) {
                            joinTranscriptsCourse = true;
                            from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_COURSE} AS ${TablesListAliases.TRANSCRIPTS_COURSE} ON ${TablesListAliases.TRANSCRIPTS_COURSE}.id = ${TablesListAliases.TRANSCRIPTS_RECORD}.course_id`);
                        }

                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.TRANSCRIPTS_COURSE}.id IS NOT NULL THEN ${TablesListAliases.TRANSCRIPTS_COURSE}.course_name
                                ELSE ${TablesListAliases.TRANSCRIPTS_RECORD}.course_name
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_COURSE_NAME])}`);
                        break;
                    case FieldsList.EXTERNAL_TRAINING_COURSE_TYPE:
                        if (!joinTranscriptsCourse) {
                            joinTranscriptsCourse = true;
                            from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_COURSE} AS ${TablesListAliases.TRANSCRIPTS_COURSE} ON ${TablesListAliases.TRANSCRIPTS_COURSE}.id = ${TablesListAliases.TRANSCRIPTS_RECORD}.course_id`);
                        }

                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.TRANSCRIPTS_COURSE}.id IS NOT NULL THEN ${TablesListAliases.TRANSCRIPTS_COURSE}.type
                                ELSE ${TablesListAliases.TRANSCRIPTS_RECORD}.course_type
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_COURSE_TYPE])}`);
                        break;
                    case FieldsList.EXTERNAL_TRAINING_CERTIFICATE:
                        select.push(`${TablesListAliases.TRANSCRIPTS_RECORD}.original_filename AS ${athena.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_CERTIFICATE])}`);
                        break;
                    case FieldsList.EXTERNAL_TRAINING_SCORE:
                        select.push(`${TablesListAliases.TRANSCRIPTS_RECORD}.score AS ${athena.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_SCORE])}`);
                        break;
                    case FieldsList.EXTERNAL_TRAINING_DATE:
                        select.push(`DATE_FORMAT(${TablesListAliases.TRANSCRIPTS_RECORD}.to_date, '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_DATE])}`);
                        break;
                    case FieldsList.EXTERNAL_TRAINING_DATE_START:
                        select.push(`DATE_FORMAT(${TablesListAliases.TRANSCRIPTS_RECORD}.from_date, '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_DATE_START])}`);
                        break;
                    case FieldsList.EXTERNAL_TRAINING_CREDITS:
                        select.push(`${TablesListAliases.TRANSCRIPTS_RECORD}.credits AS ${athena.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_CREDITS])}`);
                        break;
                    case FieldsList.EXTERNAL_TRAINING_TRAINING_INSTITUTE:
                        if (!joinTranscriptsCourse) {
                            joinTranscriptsCourse = true;
                            from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_COURSE} AS ${TablesListAliases.TRANSCRIPTS_COURSE} ON ${TablesListAliases.TRANSCRIPTS_COURSE}.id = ${TablesListAliases.TRANSCRIPTS_RECORD}.course_id`);
                        }
                        if (!joinTranscriptsInstitute) {
                            joinTranscriptsInstitute = true;
                            from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_INSTITUTE} AS ${TablesListAliases.TRANSCRIPTS_INSTITUTE} ON ${TablesListAliases.TRANSCRIPTS_INSTITUTE}.id = ${TablesListAliases.TRANSCRIPTS_COURSE}.institute_id`);
                        }

                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.TRANSCRIPTS_INSTITUTE}.id IS NOT NULL THEN ${TablesListAliases.TRANSCRIPTS_INSTITUTE}.institute_name
                                ELSE ${TablesListAliases.TRANSCRIPTS_RECORD}.training_institute
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_TRAINING_INSTITUTE])}`);
                        break;
                    case FieldsList.EXTERNAL_TRAINING_STATUS:
                        select.push(`
                            CASE
                                WHEN ${TablesListAliases.TRANSCRIPTS_RECORD}.status = ${athena.renderStringInQueryCase(ExternalTrainingStatus.REJECTED)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.EXTERNAL_TRAINING_STATUS_REJECTED])}
                                WHEN ${TablesListAliases.TRANSCRIPTS_RECORD}.status = ${athena.renderStringInQueryCase(ExternalTrainingStatus.WAITING)} THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.EXTERNAL_TRAINING_STATUS_WAITING])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.EXTERNAL_TRAINING_STATUS_APPROVED])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.EXTERNAL_TRAINING_STATUS])}`);
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
                                            from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = ${TablesListAliases.TRANSCRIPTS_RECORD}.id_user`);
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
                        } else if (this.isExternalActivityExtraField(field)) {
                            const fieldId = parseInt(field.replace('external_activity_extrafield_', ''), 10);
                            for (const externalTrainingtField of externalTrainingtExtraFields.data.items) {
                                if (externalTrainingtField.id === fieldId) {
                                    if (await this.checkExternalActivityAdditionalFieldInAthena(fieldId) === false) {
                                        select.push(`'' AS ${athena.renderStringInQuerySelect(externalTrainingtField.title)}`);
                                    } else {
                                        if (!joinTranscriptsFieldValue) {
                                            joinTranscriptsFieldValue = true;
                                            from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_FIELD_VALUE} AS ${TablesListAliases.TRANSCRIPTS_FIELD_VALUE} ON ${TablesListAliases.TRANSCRIPTS_FIELD_VALUE}.id_record = ${TablesListAliases.TRANSCRIPTS_RECORD}.id_record`);
                                        }
                                        switch (externalTrainingtField.type) {
                                            case AdditionalFieldsTypes.Textarea:
                                            case AdditionalFieldsTypes.Textfield:
                                                select.push(`${TablesListAliases.TRANSCRIPTS_FIELD_VALUE}.field_${fieldId} AS ${athena.renderStringInQuerySelect(externalTrainingtField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Date:
                                                    select.push(`DATE_FORMAT(${TablesListAliases.TRANSCRIPTS_FIELD_VALUE}.field_${fieldId}, '%Y-%m-%d') AS ${athena.renderStringInQuerySelect(externalTrainingtField.title)}`);
                                                break;
                                            case AdditionalFieldsTypes.Dropdown:
                                                from.push(`LEFT JOIN ${TablesList.TRANSCRIPTS_FIELD_DROPDOWN_TRANSLATIONS} AS ${TablesListAliases.TRANSCRIPTS_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId} ON ${TablesListAliases.TRANSCRIPTS_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.id_option = ${TablesListAliases.TRANSCRIPTS_FIELD_VALUE}.field_${fieldId} AND ${TablesListAliases.TRANSCRIPTS_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.lang_code = '${this.session.user.getLang()}'`);
                                                select.push(`${TablesListAliases.TRANSCRIPTS_FIELD_DROPDOWN_TRANSLATIONS}_${fieldId}.translation AS ${athena.renderStringInQuerySelect(externalTrainingtField.title)}`);
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
            query += this.addOrderByClause(select, translations, {user: userExtraFields.data.items, transcripts: externalTrainingtExtraFields.data.items});
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
            groupBy: [],
            archivedGroupBy: [],
            userAdditionalFieldsSelect: [],
            userAdditionalFieldsFrom: [],
            userAdditionalFieldsId: [],
            externalTrainingAdditionalFieldsSelect: [],
            externalTrainingAdditionalFieldsFrom: [],
            externalTrainingAdditionalFieldsId: [],
            translations,
            checkPuVisibility
        };

        let table = `SELECT * FROM ${TablesList.TRANSCRIPTS_RECORD} WHERE TRUE`;

        if (fullUsers !== '') {
            table += ` AND "id_user" IN (${fullUsers})`;
        }

        if (this.info.externalTrainingStatusFilter) {
            const toFilter: string[] = [];
            if (this.info.externalTrainingStatusFilter.approved) {
                toFilter.push(ExternalTrainingStatus.APPROVED);
            }
            if (this.info.externalTrainingStatusFilter.waiting) {
                toFilter.push(ExternalTrainingStatus.WAITING);
            }
            if (this.info.externalTrainingStatusFilter.rejected) {
                toFilter.push(ExternalTrainingStatus.REJECTED);
            }

            if (toFilter.length > 0 && toFilter.length < 3) {
                table += ` AND "status" IN ('${toFilter.join("','")}')`;
            }
        }

        if (this.info.externalTrainingDate?.any === false) {
            table += ' ' + this.buildDateFilter('to_date', this.info.externalTrainingDate, 'AND', true);
        }

        queryHelper.from.push(`(${table}) AS ${TablesListAliases.TRANSCRIPTS_RECORD}`);

        // JOIN CORE USER
        table = `SELECT * FROM ${TablesList.CORE_USER} WHERE `;
        table += hideDeactivated ? '"valid" = 1' : 'true';

        if (hideExpiredUsers) {
            table += ` AND ("expiration" IS NULL OR "expiration" > current_timestamp())`;
        }
        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.TRANSCRIPTS_RECORD}."id_user"`);

        const where = [
            `AND ${TablesListAliases.CORE_USER}."userid" <> '/Anonymous'`
        ];

        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        const translationValue = this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');
        let externalTrainingExtraFields = {data: { items: []} };
        if (this.info.fields.find(item => item.includes('external_activity_extrafield_'))) {
            externalTrainingExtraFields = await this.session.getHydra().getTranscriptExtraFields();
            this.updateExtraFieldsDuplicated(externalTrainingExtraFields.data.items, translations, 'external-training', translationValue);
        }
        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            this.getAdditionalUsersFieldsFiltersSnowflake(queryHelper, userExtraFields);
        }

        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged = this.querySelectUserFields(field, queryHelper) ||
                    this.querySelectExternalTrainingFields(field, queryHelper) ||
                    this.queryWithUserAdditionalFields(field, queryHelper, userExtraFields) ||
                    this.queryWithExternalTrainingAdditionalFields(field, queryHelper, externalTrainingExtraFields);
            }
        }

        if (queryHelper.userAdditionalFieldsId.length > 0) {
            queryHelper.cte.push(this.additionalUserFieldQueryWith(queryHelper.userAdditionalFieldsId));
            queryHelper.cte.push(this.additionalFieldQueryWith(queryHelper.userAdditionalFieldsFrom, queryHelper.userAdditionalFieldsSelect, queryHelper.userAdditionalFieldsId, 'id_user', TablesList.CORE_USER_FIELD_VALUE_WITH, TablesList.USERS_ADDITIONAL_FIELDS_TRANSLATIONS));
        }

        if (queryHelper.externalTrainingAdditionalFieldsId.length > 0) {
            queryHelper.cte.push(this.additionalExternalTrainingFieldQueryWith(queryHelper.externalTrainingAdditionalFieldsId));
            queryHelper.cte.push(this.additionalFieldQueryWith(queryHelper.externalTrainingAdditionalFieldsFrom, queryHelper.externalTrainingAdditionalFieldsSelect, queryHelper.externalTrainingAdditionalFieldsId, 'id_record', TablesList.TRANSCRIPTS_FIELD_VALUE_WITH, TablesList.EXTERNAL_TRAINING_ADDITIONAL_FIELDS_TRANSLATIONS));
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
                    transcripts: externalTrainingExtraFields.data.items,
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

        this.mandatoryFields.externalTraining.forEach(element => {
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
        report.externalTrainingDate = this.getDefaultDateOptions();
        report.externalTrainingStatusFilter = this.getDefaultExternalTrainingStatusFilter();

        if (description) {
            report.description = description;
        }

        return report;
    }

    /**
     * Get the default value for the Sorting Options
     */
    protected getSortingOptions(): SortingOptions {
        return {
            selector: 'default',
            selectedField: FieldsList.USER_USERID,
            orderBy: 'asc',
        };
    }

    public async getAvailablesFields(): Promise<ReportAvailablesFields> {
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

        // Transcript fields
        result.externalTraining = [];
        for (const field of this.allFields.externalTraining) {
            result.externalTraining.push({
                field,
                idLabel: field,
                mandatory: false,
                isAdditionalField: false,
                translation: translations[field]
            });
        }

        const transcriptExtraFields = await this.getAvailableTranscriptExtraFields();
        result.externalTraining.push(...transcriptExtraFields);

        return result;
    }

    public getBaseAvailableFields(): Promise<ReportAvailablesFields> {
        throw new Error('Method not implemented.');
    }

    /**
     * Set the sortingOptions object with the input passed
     * @param sortingOptions The object that describes a sortingOptions
     */
    setSortingOptions(item: SortingOptions): void {
        this.info.sortingOptions = {
            selector: item && item.selector ? item.selector : 'default',
            selectedField: item && item.selectedField ? item.selectedField : FieldsList.USER_USERID,
            orderBy: item && item.orderBy ? item.orderBy : 'asc'
        };
    }

    public parseLegacyReport(legacyReport: LegacyReport, platform: string, visibilityRules: VisibilityRule[]): ReportManagerInfo {
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

        if (filterData && filterData.filters.date_from || filterData && filterData.filters.date_to) {
            report.externalTrainingDate = utils.parseLegacyFilterDateRange(report.externalTrainingDate as DateOptionsValueDescriptor, filterData.filters.date_from, filterData.filters.date_to);
        }

        if (filterData && filterData.filters.subscription_status) {
            report.externalTrainingStatusFilter = this.getDefaultExternalTrainingStatusFilter();
            switch (filterData.filters.subscription_status) {
                case 'approved':
                    report.externalTrainingStatusFilter.waiting = false;
                    report.externalTrainingStatusFilter.rejected = false;
                    break;
                case 'waiting':
                    report.externalTrainingStatusFilter.approved = false;
                    report.externalTrainingStatusFilter.rejected = false;
                    break;
            }
        }

        if (filterData.fields) {
            let legacyOrderField: LegacyOrderByParsed | undefined;
            const userMandatoryFieldsMap = this.mandatoryFields.user.reduce((previousValue: {[key: string]: string}, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});
            // user fields and order by
            const userFieldsDescriptor = this.mapUserSelectedFields(filterData.fields.user, filterData.order, userMandatoryFieldsMap);
            report.fields.push(...userFieldsDescriptor.fields);
            if (userFieldsDescriptor.orderByDescriptor) legacyOrderField = userFieldsDescriptor.orderByDescriptor;

            const externalTrainingFields = this.mapExternalTrainingSelectedFields(filterData.fields.external_trainings);
            report.fields.push(...externalTrainingFields);

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
