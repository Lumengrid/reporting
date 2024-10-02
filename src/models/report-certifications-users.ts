import {
    FieldsList,
    FieldTranslation,
    ReportAvailablesFields,
    ReportManagerCertificationsFilter,
    ReportManagerInfo,
    ReportManagerInfoResponse,
    ReportManagerInfoUsersFilter,
    ReportManagerInfoVisibility,
    TablesList,
    TablesListAliases
} from './report-manager';
import SessionManager from '../services/session/session-manager.session';
import httpContext from 'express-http-context';
import { LegacyOrderByParsed, LegacyReport, VisibilityRule } from './migration-component';
import { DateOptions, DateOptionsValueDescriptor, SortingOptions, TimeFrameOptions, VisibilityTypes } from './custom-report';
import { v4 } from 'uuid';
import { ReportsTypes } from '../reports/constants/report-types';
import { Utils } from '../reports/utils';
import { UserLevels } from '../services/session/user-manager.session';
import { AdditionalFieldsTypes } from './base';
import { BaseReportManager } from './base-report-manager';

export class CertificationsUsersManager extends BaseReportManager {
    reportType = ReportsTypes.CERTIFICATIONS_USERS;
    allFields = {
        certifications: [
            FieldsList.CERTIFICATION_TITLE,
            FieldsList.CERTIFICATION_CODE,
            FieldsList.CERTIFICATION_DESCRIPTION,
            FieldsList.CERTIFICATION_DURATION,
        ],
        statistics: [
            FieldsList.STATS_ACTIVE,
            FieldsList.STATS_EXPIRED,
            FieldsList.STATS_ISSUED,
            FieldsList.STATS_ARCHIVED,
        ],
    };

    mandatoryFields = {
        certifications: [
            FieldsList.CERTIFICATION_TITLE
        ],
    };

    public constructor(session: SessionManager, reportDetails: AWS.DynamoDB.DocumentClient.AttributeMap | undefined) {
        super(session, reportDetails);
        this.logger = httpContext.get('logger');
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

        // Certifications
        report.certifications = new ReportManagerCertificationsFilter();

        // Users
        report.users = new ReportManagerInfoUsersFilter();
        report.users.all = true;


        /**
         * View Options Tab
         */
        const tmpFields: string[] = [];

        this.mandatoryFields.certifications.forEach(certification => {
            tmpFields.push(certification);
        });

        report.fields = tmpFields;
        report.sortingOptions = this.getSortingOptions();


        /**
         * Schedule Tab
         */
        report.planning = this.getDefaultPlanningFields();

        return report;
    }


    async getAvailablesFields(): Promise<ReportAvailablesFields> {
        const result: ReportAvailablesFields = {};
        const translations = await this.loadTranslations(true);

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

        result.statistics = [];
        for (const field of this.allFields.statistics) {
            result.statistics.push({
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
            selectedField: FieldsList.CERTIFICATION_TITLE,
            orderBy: 'asc',
        };
    }

    setSortingOptions(item: SortingOptions): void {
        this.info.sortingOptions = {
            selector: item && item.selector ? item.selector : 'default',
            selectedField: item && item.selectedField ? item.selectedField : FieldsList.CERTIFICATION_TITLE,
            orderBy: item && item.orderBy ? item.orderBy : 'asc'
        };
    }

    async getQuery(limit: number, isPreview: boolean): Promise<string> {
        const translations = await this.loadTranslations();
        const hydra = this.session.getHydra();
        const athena = this.session.getAthena();

        const allCertifications = this.info.certifications ? this.info.certifications.all : false;
        let fullCertifications: number[] = [];

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;
        let fullUsers = '';

        if (!allCertifications) {
            fullCertifications = this.info.certifications ? this.info.certifications.certifications.map(a => a.id) : [];
        }

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilter();
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

        if (this.info.certifications) {
            const archivedCertificationsFilter = ` OR ${TablesListAliases.CERTIFICATION_USER}.archived = 1`;

            // Active or Expired filter
            if (this.info.certifications.activeCertifications && this.info.certifications.expiredCertifications) {
                // Fine in this way, we don't need any filter in this case
            } else if (this.info.certifications.activeCertifications) {

                let activeCertificationsFilter = `${TablesListAliases.CERTIFICATION_USER}.on_datetime <= CURRENT_TIMESTAMP
                AND (${TablesListAliases.CERTIFICATION_USER}.expire_at > CURRENT_TIMESTAMP OR ${TablesListAliases.CERTIFICATION_USER}.expire_at IS NULL)`;

                // If archivedCertification filter is enabled append it to the activeCertification filter (in OR condition)
                if (this.info.certifications.archivedCertifications) {
                    activeCertificationsFilter = activeCertificationsFilter + archivedCertificationsFilter;
                }
                where.push(`AND (${activeCertificationsFilter})`);

            } else if (this.info.certifications.expiredCertifications) {
                let expiredCertificationsFilter = `${TablesListAliases.CERTIFICATION_USER}.expire_at < CURRENT_TIMESTAMP`;

                // Append archivedCertifications to expiredCertificationsFilter
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

        const userExtraFields = await this.session.getHydra().getUserExtraFields();
        this.updateExtraFieldsDuplicated(userExtraFields.data.items, translations, 'user');

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
                    // Certification fields
                    case FieldsList.CERTIFICATION_TITLE:
                        select.push(`ARBITRARY(${TablesListAliases.CERTIFICATION}.title) AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_TITLE])}`);
                        break;
                    case FieldsList.CERTIFICATION_CODE:
                        select.push(`ARBITRARY(${TablesListAliases.CERTIFICATION}.code) AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_CODE])}`);
                        break;
                    case FieldsList.CERTIFICATION_DESCRIPTION:
                        select.push(`ARBITRARY(${TablesListAliases.CERTIFICATION}.description) AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_DESCRIPTION])}`);
                        break;
                    case FieldsList.CERTIFICATION_DURATION:
                        select.push(`
                        CASE
                            WHEN ARBITRARY(${TablesListAliases.CERTIFICATION}.duration) = 0 THEN ${athena.renderStringInQueryCase(FieldTranslation.NEVER)}
                            WHEN ARBITRARY(${TablesListAliases.CERTIFICATION}.duration_unit) = ${athena.renderStringInQueryCase('day')} THEN CONCAT(CAST(ARBITRARY(${TablesListAliases.CERTIFICATION}.duration) AS VARCHAR), ' ', ${athena.renderStringInQueryCase(FieldTranslation.DAYS)})
                            WHEN ARBITRARY(${TablesListAliases.CERTIFICATION}.duration_unit) = ${athena.renderStringInQueryCase('week')} THEN CONCAT(CAST(ARBITRARY(${TablesListAliases.CERTIFICATION}.duration) AS VARCHAR), ' ', ${athena.renderStringInQueryCase(FieldTranslation.WEEKS)})
                            WHEN ARBITRARY(${TablesListAliases.CERTIFICATION}.duration_unit) = ${athena.renderStringInQueryCase('month')} THEN CONCAT(CAST(ARBITRARY(${TablesListAliases.CERTIFICATION}.duration) AS VARCHAR), ' ', ${athena.renderStringInQueryCase(FieldTranslation.MONTHS)})
                            WHEN ARBITRARY(${TablesListAliases.CERTIFICATION}.duration_unit) = ${athena.renderStringInQueryCase('year')} THEN CONCAT(CAST(ARBITRARY(${TablesListAliases.CERTIFICATION}.duration) AS VARCHAR), ' ', ${athena.renderStringInQueryCase(FieldTranslation.YEARS)})
                            ELSE NULL
                        END AS ${athena.renderStringInQuerySelect(translations[FieldsList.CERTIFICATION_DURATION])}`);
                        break;

                    // Statistics fields
                    case FieldsList.STATS_ISSUED:
                        select.push(`COUNT(${TablesListAliases.CERTIFICATION_USER}.id_user) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_ISSUED])}`);
                        break;
                    case FieldsList.STATS_EXPIRED:
                        // DL2 ingestion issue workaround
                        const statsExpiredExpireAtFix = `${this.mapTimestampDefaultValueWithDLV2(`${TablesListAliases.CERTIFICATION_USER}.expire_at`)}`;
                        select.push(`
                                SUM(CASE
                                    WHEN ${TablesListAliases.CERTIFICATION_USER}.archived = 0
                                        AND (${statsExpiredExpireAtFix}) <= CURRENT_TIMESTAMP
                                    THEN 1
                                    ELSE 0
                                END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_EXPIRED])}`);
                        break;
                    case FieldsList.STATS_ACTIVE:
                        // DL2 ingestion issue workaround
                        const statsActiveOnDateTimeFix = `${this.mapTimestampDefaultValueWithDLV2(`${TablesListAliases.CERTIFICATION_USER}.on_datetime`)}`;
                        const statsActiveExpireAtFix = `${this.mapTimestampDefaultValueWithDLV2(`${TablesListAliases.CERTIFICATION_USER}.expire_at`)}`;
                        select.push(`
                                SUM(CASE
                                    WHEN ${TablesListAliases.CERTIFICATION_USER}.archived = 0
                                        AND (${statsActiveOnDateTimeFix}) <= CURRENT_TIMESTAMP
                                        AND ( (${statsActiveExpireAtFix}) > CURRENT_TIMESTAMP
                                            OR (${statsActiveExpireAtFix}) IS NULL)
                                    THEN 1
                                    ELSE 0
                                END) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_ACTIVE])}`);
                        break;
                    case FieldsList.STATS_ARCHIVED:
                        select.push(`SUM(${TablesListAliases.CERTIFICATION_USER}.archived) AS ${athena.renderStringInQuerySelect(translations[FieldsList.STATS_ARCHIVED])}`);
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
                                            from.push(`LEFT JOIN ${TablesList.CORE_USER_FIELD_VALUE} AS ${TablesListAliases.CORE_USER_FIELD_VALUE} ON ${TablesListAliases.CORE_USER_FIELD_VALUE}.id_user = ${TablesListAliases.LEARNING_COURSEUSER_AGGREGATE}.idUser`);
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
        GROUP BY ${TablesListAliases.CERTIFICATION}.id_cert`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(select, translations, { user: userExtraFields.data.items, course: [], userCourse: [], webinar: [], classroom: [] });
        }

        // get the sql like LIMIT for the query
        query += this.getQueryExportLimit(limit);

        return query;
    }

    public async getQuerySnowflake(limit = 0, isPreview: boolean, checkPuVisibility = true, fromSchedule = false): Promise<string> {
        const translations = await this.loadTranslations();
        const allCertifications = this.info.certifications ? this.info.certifications.all : false;
        let fullCertifications: number[] = [];

        const allUsers = this.info.users ? this.info.users.all : false;
        const hideDeactivated = this.info.users?.hideDeactivated;
        const hideExpiredUsers = this.info.users?.hideExpiredUsers;
        let fullUsers = '';

        if (!allCertifications) {
            fullCertifications = this.info.certifications ? this.info.certifications.certifications.map(a => a.id) : [];
        }

        if (!allUsers || this.session.user.getLevel() === UserLevels.POWER_USER) {
            fullUsers = await this.calculateUserFilterSnowflake(checkPuVisibility);
        }
        const queryHelper = {
            select: [],
            archivedSelect: [],
            from: [],
            join: [],
            cte: [],
            groupBy: [`${TablesListAliases.CERTIFICATION}."id_cert"`],
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
        table += hideDeactivated ? `"valid" = 1` : 'true';

        if (hideExpiredUsers) {
            table += ` AND ("expiration" IS NULL OR "expiration" > current_timestamp())`;
        }
        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}."idst" = ${TablesListAliases.CERTIFICATION_USER}."id_user"`);

        queryHelper.from.push(`JOIN ${TablesList.CERTIFICATION_ITEM} AS ${TablesListAliases.CERTIFICATION_ITEM} ON ${TablesListAliases.CERTIFICATION_USER}."id_cert_item" = ${TablesListAliases.CERTIFICATION_ITEM}."id"`);

        table = `SELECT * FROM ${TablesList.CERTIFICATION} WHERE TRUE`;

        if (!allCertifications) {
            if (fullCertifications.length > 0) {
                table += ` AND "id_cert" IN (${fullCertifications.join(',')})`;
            } else {
                table += ' AND FALSE';
            }
        }

        queryHelper.from.push(`JOIN (${table}) AS ${TablesListAliases.CERTIFICATION} ON ${TablesListAliases.CERTIFICATION_ITEM}."id_cert" = ${TablesListAliases.CERTIFICATION}."id_cert"`);

        let where = [
            `AND ${TablesListAliases.CORE_USER}."userid" <> '/Anonymous'`,
            `AND ${TablesListAliases.CERTIFICATION}."deleted" = FALSE`
        ];

        const certFilters = this.getCertificationsFilterSnowflake();
        if (certFilters.length) {
            where = where.concat(certFilters);
        }

        // User additional field filter
        if (this.info.userAdditionalFieldsFilter && this.info.users?.isUserAddFields) {
            const userExtraFields = await this.session.getHydra().getUserExtraFields();
            this.getAdditionalUsersFieldsFiltersSnowflake(queryHelper, userExtraFields);
        }

        if (this.info.fields) {
            for (const field of this.info.fields) {
                const isManaged = this.querySelectCertificationFields(field, queryHelper) ||
                    this.querySelectUsageStatisticsFields(field, queryHelper);
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
            WHERE TRUE ${where.join(' ')}
            GROUP BY ${[...new Set(queryHelper.groupBy)].join(', ')}`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(
                queryHelper.select,
                queryHelper.translations,
                {
                    user: [],
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
         * CERTIFICATIONS IMPORT
         */
        this.legacyCertificationsImport(filterData, report, legacyReport.id_filter);
        /**
         * USERS IMPORT - populate the users field of the aamon report
         */
        this.legacyUserImport(filterData, report, legacyReport.id_filter);


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

            const certificationsMandatoryFieldsMap = this.mandatoryFields.certifications.reduce((previousValue: {[key: string]: string}, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});
            const certificationsFieldsDescriptor = this.mapCertificationSelectedFields(filterData.fields.certification, filterData.order, certificationsMandatoryFieldsMap);
            report.fields.push(...certificationsFieldsDescriptor.fields);
            if (certificationsFieldsDescriptor.orderByDescriptor) legacyOrderField = certificationsFieldsDescriptor.orderByDescriptor;

            const statsFieldsDescriptor = this.mapStatsSelectedFields(filterData.fields.stat, filterData.order);
            report.fields.push(...statsFieldsDescriptor.fields);
            if (statsFieldsDescriptor.orderByDescriptor) legacyOrderField = statsFieldsDescriptor.orderByDescriptor;

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
