import { v4 } from 'uuid';

import { ReportsTypes } from '../reports/constants/report-types';
import SessionManager from '../services/session/session-manager.session';
import { SortingOptions, VisibilityTypes, DateOptionsValueDescriptor } from './custom-report';
import { LegacyReport, VisibilityRule, LegacyOrderByParsed } from './migration-component';
import {
    FieldsList,
    ReportAvailablesFields,
    ReportManagerAssetsFilter,
    ReportManagerInfo,
    ReportManagerInfoVisibility,
    TablesList,
    TablesListAliases,
    FieldTranslation,
} from './report-manager';
import { Utils } from '../reports/utils';
import { BaseReportManager } from './base-report-manager';

export class AssetsStatisticsManager extends BaseReportManager {
    reportType = ReportsTypes.ASSETS_STATISTICS;

    // View Options Fields
    allFields = {
        assets: [
            FieldsList.ASSET_NAME,
            FieldsList.CHANNELS,
            FieldsList.PUBLISHED_BY,
            FieldsList.PUBLISHED_ON
        ],
        statistics: [
            FieldsList.ANSWER_DISLIKES,
            FieldsList.ANSWER_LIKES,
            FieldsList.ANSWERS,
            FieldsList.ASSET_RATING,
            FieldsList.AVERAGE_REACTION_TIME,
            FieldsList.BEST_ANSWERS,
            FieldsList.GLOBAL_WATCH_RATE,
            FieldsList.INVITED_PEOPLE,
            FieldsList.NOT_WATCHED,
            FieldsList.QUESTIONS,
            FieldsList.TOTAL_VIEWS,
            FieldsList.WATCHED
        ],
    };
    mandatoryFields = {
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

        // Assets
        report.assets = new ReportManagerAssetsFilter();
        report.assets.all = true;

        // Date options
        report.publishedDate = this.getDefaultDateOptions();

        /**
         * View Options Tab
         */
        const tmpFields: string[] = [];

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

        // Statistics
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
            selectedField: FieldsList.ASSET_NAME,
            orderBy: 'asc',
        };
    }
    setSortingOptions(item: SortingOptions): void {
        this.info.sortingOptions = {
            selector: item && item.selector ? item.selector : 'default',
            selectedField: item && item.selectedField ? item.selectedField : FieldsList.ASSET_NAME,
            orderBy: item && item.orderBy ? item.orderBy : 'asc'
        };
    }

    public async getQuery(limit = 0, isPreview: boolean): Promise<string> {
        const translations = await this.loadTranslations();
        const athena = this.session.getAthena();

        const allAssets = this.info.assets ? this.info.assets.all : false;
        let fullAssets: number[] = [];

        if (!allAssets) {
            fullAssets = await this.calculateAssetFilter();
        }

        const select = [];
        const from = [];

        let table = `SELECT * FROM ${TablesList.APP7020_CONTENT} WHERE TRUE`;

        if (!allAssets) {
            table += fullAssets.length > 0 ? ` AND id IN (${fullAssets.join(',')})` : ' AND FALSE';
        }

        if (this.info.publishedDate && !this.info.publishedDate.any) {
            table += this.buildDateFilter('created', this.info.publishedDate, 'AND', true);
        }

        from.push(`(${table}) AS ${TablesListAliases.APP7020_CONTENT}`);
        from.push(`JOIN ${TablesList.APP7020_CHANNEL_ASSETS} AS ${TablesListAliases.APP7020_CHANNEL_ASSETS} ON ${TablesListAliases.APP7020_CHANNEL_ASSETS}.idAsset = ${TablesListAliases.APP7020_CONTENT}.id AND ${TablesListAliases.APP7020_CHANNEL_ASSETS}.asset_type = 1`);

        from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_PUBLISHED} AS ${TablesListAliases.APP7020_CONTENT_PUBLISHED} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_CONTENT_PUBLISHED}.idContent AND ${TablesListAliases.APP7020_CONTENT_PUBLISHED}.actiontype = 1`);

        const where = [
            `AND ${TablesListAliases.APP7020_CONTENT}.conversion_status = 20 AND ${TablesListAliases.APP7020_CONTENT}.is_private = 0`,
            `AND ${TablesListAliases.APP7020_CONTENT_PUBLISHED}.idUser NOT IN
            (
                SELECT cast(user_id as integer)
                FROM rbac_assignment
                WHERE item_name = '/framework/level/erpadmin'
            )`
        ];

        // Variables to check if the specified table was already joined in the query
        let joinApp7020ChannelTranslation = false;
        let joinCoreUser = false;
        let joinApp7020Answer = false;
        let joinApp7020AnswerLike = false;
        let joinApp7020AnswerDislike = false;
        let joinApp7020ContentRating = false;
        let joinApp7020ContentHistory = false;
        let joinApp7020BestAnswer = false;
        let joinApp7020InvitationsAgg = false;
        let joinApp7020Question = false;
        let joinApp7020ContentHistoryAgg = false;
        let joinApp7020ContentHistoryTotalViewsAgg = false;


        if (this.info.fields) {
            for (const field of this.info.fields) {
                switch (field) {
                    // Assets fields
                    case FieldsList.ASSET_NAME:
                        select.push(`ARBITRARY(${TablesListAliases.APP7020_CONTENT}.title) AS ${athena.renderStringInQuerySelect(translations[FieldsList.ASSET_NAME])}`);
                        break;
                    case FieldsList.PUBLISHED_ON:
                        select.push(`DATE_FORMAT(ARBITRARY(${TablesListAliases.APP7020_CONTENT_PUBLISHED}.datePublished) AT TIME ZONE '${this.info.timezone}', '%Y-%m-%d %H:%i:%s') AS ${athena.renderStringInQuerySelect(translations[FieldsList.PUBLISHED_ON])}`);
                        break;
                    case FieldsList.CHANNELS:
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
                        select.push(`ARRAY_JOIN(ARRAY_AGG(DISTINCT(${TablesListAliases.APP7020_CHANNEL_TRANSLATION}.name)), ', ') AS ${athena.renderStringInQuerySelect(translations[FieldsList.CHANNELS])}`);
                        break;
                    case FieldsList.PUBLISHED_BY:
                        if (!joinCoreUser) {
                            joinCoreUser = true;
                            from.push(`LEFT JOIN ${TablesList.CORE_USER} AS ${TablesListAliases.CORE_USER} ON ${TablesListAliases.CORE_USER}.idst = ${TablesListAliases.APP7020_CONTENT}.userId`);
                        }

                        if (this.session.platform.getShowFirstNameFirst()) {
                            select.push(`ARBITRARY(CONCAT(${TablesListAliases.CORE_USER}.firstname, ' ', ${TablesListAliases.CORE_USER}.lastname)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.PUBLISHED_BY])}`);
                        } else {
                            select.push(`ARBITRARY(CONCAT(${TablesListAliases.CORE_USER}.lastname, ' ', ${TablesListAliases.CORE_USER}.firstname)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.PUBLISHED_BY])}`);
                        }

                        break;

                    // Stats fields
                    case FieldsList.ANSWERS:
                        if (!joinApp7020Answer) {
                            joinApp7020Answer = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_ANSWER_AGGREGATE} AS ${TablesListAliases.APP7020_ANSWER_AGGREGATE} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_ANSWER_AGGREGATE}.idContent`);
                        }
                        select.push(`
                            CASE WHEN ARBITRARY(${TablesListAliases.APP7020_ANSWER_AGGREGATE}.count) IS NOT NULL THEN ARBITRARY(${TablesListAliases.APP7020_ANSWER_AGGREGATE}.count) ELSE 0
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ANSWERS])}`);
                        break;

                    case FieldsList.ANSWER_LIKES:
                        if (!joinApp7020AnswerLike) {
                            joinApp7020AnswerLike = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_ANSWER_LIKE_AGGREGATE} AS ${TablesListAliases.APP7020_ANSWER_LIKE_AGGREGATE} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_ANSWER_LIKE_AGGREGATE}.idContent`);
                        }
                        select.push(`
                            CASE WHEN ARBITRARY(${TablesListAliases.APP7020_ANSWER_LIKE_AGGREGATE}.count) IS NOT NULL THEN ARBITRARY(${TablesListAliases.APP7020_ANSWER_LIKE_AGGREGATE}.count) ELSE 0
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ANSWER_LIKES])}`);
                        break;
                    case FieldsList.ANSWER_DISLIKES:
                        if (!joinApp7020AnswerDislike) {
                            joinApp7020AnswerDislike = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_ANSWER_DISLIKE_AGGREGATE} AS ${TablesListAliases.APP7020_ANSWER_DISLIKE_AGGREGATE} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_ANSWER_DISLIKE_AGGREGATE}.idContent`);
                        }
                        select.push(`
                        CASE WHEN ARBITRARY(${TablesListAliases.APP7020_ANSWER_DISLIKE_AGGREGATE}.count) IS NOT NULL THEN ARBITRARY(${TablesListAliases.APP7020_ANSWER_DISLIKE_AGGREGATE}.count) ELSE 0
                        END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ANSWER_DISLIKES])}`);
                        break;
                    case FieldsList.ASSET_RATING:
                        if (!joinApp7020ContentRating) {
                            joinApp7020ContentRating = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_RATING} AS ${TablesListAliases.APP7020_CONTENT_RATING} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_CONTENT_RATING}.idContent`);
                        }
                        select.push(`
                            CASE
                                WHEN COUNT(${TablesListAliases.APP7020_CONTENT_RATING}.id) > 0 THEN ROUND(CAST(SUM(${TablesListAliases.APP7020_CONTENT_RATING}.rating) AS DOUBLE) / CAST(COUNT(${TablesListAliases.APP7020_CONTENT_RATING}.id) AS DOUBLE), 2)
                                ELSE 0
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.ASSET_RATING])}`);
                        break;
                    case FieldsList.TOTAL_VIEWS:
                        if (!joinApp7020ContentHistoryTotalViewsAgg) {
                            joinApp7020ContentHistoryTotalViewsAgg = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE} AS ${TablesListAliases.APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE}.id`);
                        }
                        select.push(`
                            CASE
                                WHEN ARBITRARY(${TablesListAliases.APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE}.totalViews) IS NOT NULL THEN ARBITRARY(${TablesListAliases.APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE}.totalViews)
                                ELSE 0
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.TOTAL_VIEWS])}`);
                        break;
                    case FieldsList.BEST_ANSWERS:
                        if (!joinApp7020BestAnswer) {
                            joinApp7020BestAnswer = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_BEST_ANSWER_AGGREGATE} AS ${TablesListAliases.APP7020_BEST_ANSWER_AGGREGATE} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_BEST_ANSWER_AGGREGATE}.id`);
                        }
                        select.push(`COUNT(DISTINCT(${TablesListAliases.APP7020_BEST_ANSWER_AGGREGATE}.id)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.BEST_ANSWERS])}`);
                        break;
                    case FieldsList.QUESTIONS:
                        if (!joinApp7020Question) {
                            joinApp7020Question = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_QUESTION} AS ${TablesListAliases.APP7020_QUESTION} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_QUESTION}.idContent`);
                        }
                        select.push(`COUNT(DISTINCT(${TablesListAliases.APP7020_QUESTION}.id)) AS ${athena.renderStringInQuerySelect(translations[FieldsList.QUESTIONS])}`);
                        break;
                    case FieldsList.INVITED_PEOPLE:
                        if (!joinApp7020InvitationsAgg) {
                            joinApp7020InvitationsAgg = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_INVITATIONS_AGGREGATE} AS ${TablesListAliases.APP7020_INVITATIONS_AGGREGATE} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}.id`);
                        }
                        select.push(`
                            CASE
                               WHEN ARBITRARY(${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}.count_invite_watch) IS NOT NULL THEN ARBITRARY(${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}.count_invite_watch)
                               ELSE 0
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.INVITED_PEOPLE])}`);
                        break;
                    case FieldsList.WATCHED:
                        if (!joinApp7020ContentHistory) {
                            joinApp7020ContentHistory = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY} AS ${TablesListAliases.APP7020_CONTENT_HISTORY} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_CONTENT_HISTORY}.idContent`);
                        }
                        select.push(`
                            CASE
                                WHEN COUNT(DISTINCT(${TablesListAliases.APP7020_CONTENT_HISTORY}.id)) > 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.WATCHED])}`);
                        break;
                    case FieldsList.NOT_WATCHED:
                        if (!joinApp7020ContentHistory) {
                            joinApp7020ContentHistory = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY} AS ${TablesListAliases.APP7020_CONTENT_HISTORY} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_CONTENT_HISTORY}.idContent`);
                        }
                        select.push(`
                            CASE
                                WHEN COUNT(DISTINCT(${TablesListAliases.APP7020_CONTENT_HISTORY}.id)) > 0 THEN ${athena.renderStringInQueryCase(translations[FieldTranslation.NO])}
                                ELSE ${athena.renderStringInQueryCase(translations[FieldTranslation.YES])}
                            END AS ${athena.renderStringInQuerySelect(translations[FieldsList.NOT_WATCHED])}`);
                        break;
                    case FieldsList.GLOBAL_WATCH_RATE:
                        if (!joinApp7020InvitationsAgg) {
                            joinApp7020InvitationsAgg = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_INVITATIONS_AGGREGATE} AS ${TablesListAliases.APP7020_INVITATIONS_AGGREGATE} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}.id`);
                        }
                        if (!joinApp7020ContentHistoryAgg) {
                            joinApp7020ContentHistoryAgg = true;
                            from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_HISTORY_AGGREGATE} AS ${TablesListAliases.APP7020_CONTENT_HISTORY_AGGREGATE} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_CONTENT_HISTORY_AGGREGATE}.id`);
                        }
                        select.push(`
                                    CONCAT(CASE
                                        WHEN ARBITRARY(${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}.count_invite_watch) > 0 THEN CAST(ROUND(ARBITRARY(${TablesListAliases.APP7020_CONTENT_HISTORY_AGGREGATE}.views) * 100 / ARBITRARY(${TablesListAliases.APP7020_INVITATIONS_AGGREGATE}.count_invite_watch)) AS VARCHAR)
                                        ELSE CAST(0 AS VARCHAR)
                                    END, ' %') AS ${athena.renderStringInQuerySelect(translations[FieldsList.GLOBAL_WATCH_RATE])}`);
                        break;
                    case FieldsList.AVERAGE_REACTION_TIME:
                        from.push(`LEFT JOIN ${TablesList.APP7020_INVITATIONS_AVERAGE_TIME} AS ${TablesListAliases.APP7020_INVITATIONS_AVERAGE_TIME} ON ${TablesListAliases.APP7020_CONTENT}.id = ${TablesListAliases.APP7020_INVITATIONS_AVERAGE_TIME}.idContent`);

                        select.push(`ARBITRARY(${TablesListAliases.APP7020_INVITATIONS_AVERAGE_TIME}.reactionTime) AS ${athena.renderStringInQuerySelect(translations[FieldsList.AVERAGE_REACTION_TIME])}`);
                        break;

                    default:
                        break;
                }
            }
        }

        let query = `SELECT ${select.join(', ')}
        FROM ${from.join(' ')}
        ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}
        GROUP BY ${TablesListAliases.APP7020_CONTENT}.id`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(select, translations, {user: [], course: [], userCourse: [], webinar: [], classroom: []});
        }

        // get the sql like LIMIT for the query
        query += this.getQueryExportLimit(limit);

        return query;

    }

    public async getQuerySnowflake(limit = 0, isPreview: boolean, fromSchedule = false): Promise<string> {
        const translations = await this.loadTranslations();

        let table = `SELECT * FROM ${TablesList.APP7020_CONTENT} AS ${TablesListAliases.APP7020_CONTENT} WHERE TRUE`;

        // Assets Filters
        table += this.getAssetsFiltersSnowflake();

        if (this.info.publishedDate && !this.info.publishedDate.any) {
            table += this.buildDateFilter('created', this.info.publishedDate, 'AND', true);
        }

        // Needed to save some info for the select switch statement
        const queryHelper = {
            select: [],
            from: [],
            join: [],
            groupBy: [`${TablesListAliases.APP7020_CONTENT}."id"`],
            archivedGroupBy: [],
            translations
        };

        queryHelper.from.push(`(${table}) AS ${TablesListAliases.APP7020_CONTENT}`);
        queryHelper.from.push(`JOIN ${TablesList.APP7020_CHANNEL_ASSETS} AS ${TablesListAliases.APP7020_CHANNEL_ASSETS}
            ON ${TablesListAliases.APP7020_CHANNEL_ASSETS}."idasset" = ${TablesListAliases.APP7020_CONTENT}."id"
            AND ${TablesListAliases.APP7020_CHANNEL_ASSETS}."asset_type" = 1`);

        queryHelper.from.push(`LEFT JOIN ${TablesList.APP7020_CONTENT_PUBLISHED} AS ${TablesListAliases.APP7020_CONTENT_PUBLISHED}
            ON ${TablesListAliases.APP7020_CONTENT}."id" = ${TablesListAliases.APP7020_CONTENT_PUBLISHED}."idcontent"
            AND ${TablesListAliases.APP7020_CONTENT_PUBLISHED}."actiontype" = 1`);

        const where = [
            `AND ${TablesListAliases.APP7020_CONTENT}."conversion_status" = 20 AND ${TablesListAliases.APP7020_CONTENT}."is_private" = 0`,
            `AND ${TablesListAliases.APP7020_CONTENT_PUBLISHED}."iduser" NOT IN
                (
                    SELECT cast("user_id" as integer)
                    FROM rbac_assignment
                    WHERE "item_name" = '/framework/level/erpadmin'
                )`
        ];

        if (this.info.fields) {
            for (const field of this.info.fields) {
             const isManaged = this.querySelectAssetFields(field, queryHelper) ||
                this.querySelectAssetStatisticsFields(field, queryHelper);
            }
        }

        const groupByUniqueFields = [...new Set(queryHelper.groupBy)];
        let query = `SELECT ${queryHelper.select.join(', ')}
            FROM ${queryHelper.from.join(' ')}
            ${where.length > 0 ? ` WHERE TRUE ${where.join(' ')}` : ''}
            GROUP BY ${groupByUniqueFields.join(', ')}`;

        if (this.info.sortingOptions && !isPreview) {
            query += this.addOrderByClause(queryHelper.select, queryHelper.translations, { user: [], course: [], userCourse: [], webinar: [], classroom: [] }, fromSchedule);
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
        const filterData = JSON.parse(legacyReport.filter_data);

        /**
         * ASSETS IMPORT - populate the assets field of the aamon report
         */
        this.legacyAssetsImport(filterData, report, legacyReport.id_filter);

        /**
         * CHANNELS IMPORT - populate the channels field of the aamon report
         */
        this.legacyChannelsImport(filterData, report, legacyReport.id_filter);

        // check filters section validity
        if (!filterData.filters) {
            this.logger.error(`No legacy filters section for id report: ${legacyReport.id_filter}`);
            throw new Error('No legacy filters section');
        }

        // Published Date Options
        if (filterData.filters.start_date.type !== 'any' && report.publishedDate) {
            report.publishedDate = utils.parseLegacyFilterDate(report.publishedDate as DateOptionsValueDescriptor, filterData.filters.start_date);
        }

        // map selected fields and manage order by
        if (filterData.fields) {
            let legacyOrderField: LegacyOrderByParsed | undefined;
            const assetMandatoryFieldsMap = this.mandatoryFields.assets.reduce((previousValue: { [key: string]: string }, currentValue) => {
                previousValue[currentValue] = currentValue;
                return previousValue;
            }, {});

            // asset fields and order by
            const assetFieldsDescriptor = this.mapAssetSelectedFields(filterData.fields.asset, filterData.order, assetMandatoryFieldsMap);
            report.fields.push(...assetFieldsDescriptor.fields);
            if (assetFieldsDescriptor.orderByDescriptor) legacyOrderField = assetFieldsDescriptor.orderByDescriptor;

            // statistics fields and order by
            const statFieldsDescriptor = this.mapStatSelectedFields(filterData.fields.stat, filterData.order, assetMandatoryFieldsMap);
            report.fields.push(...statFieldsDescriptor.fields);
            if (statFieldsDescriptor.orderByDescriptor) legacyOrderField = statFieldsDescriptor.orderByDescriptor;

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
