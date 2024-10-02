import SessionManager from '../services/session/session-manager.session';
import { Dynamo } from '../services/dynamo';
import Hydra from '../services/hydra';
import { ReportService } from '../services/report';
import { NewReportManagerSwitcher } from './report-switcher';
import { SessionLoggerService } from '../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { ReportManagerInfo } from './report-manager';
import dynoItemSize from 'dyno-item-size';
import { ReportsTypes } from '../reports/constants/report-types';
import { DisabledReportTypeException } from '../exceptions';
import { BaseReportManager } from './base-report-manager';

export class MigrationComponent {

    private readonly dynamoService: Dynamo;
    private readonly hydraService: Hydra;
    private readonly reportService: ReportService;
    private readonly logger: SessionLoggerService;

    private readonly DYNAMO_LIMIT = 400000;

    constructor(private readonly session: SessionManager) {
        this.dynamoService = session.getDynamo();
        this.hydraService = session.getHydra();
        this.reportService = new ReportService(this.hydraService);
        this.logger = httpContext.get('logger');
    }

    async migrateReports(reportFiltersPayload: MigrateInputPayload): Promise<MigrationResponse> {
        let migratedReports: MigratedReportResponse[];

        // Return all legacy reports that have been already migrated and use it as filter if requested
        const legacyReportMigratedIds = await this.dynamoService.getAlreadyMigratedReportIds();
        const payload = {...reportFiltersPayload, legacyReportMigratedIds};
        const oldReportsResponse = await this.reportService.getOldReportsFromHydra(payload);

        // check reports existence
        if (!oldReportsResponse || !oldReportsResponse.reports || oldReportsResponse.reports.length === 0) {
            this.logger.debug(`No reports to migrate`);
            return {
                migrated: [],
                notMigrated: [],
            };
        }
        this.logger.debug(`Reports to migrate: ${oldReportsResponse.reports.length}`);
        const parsedResponse = this.parseOldReports(oldReportsResponse, this.session);
        migratedReports = await this.addParsedReportsToDynamo(parsedResponse.parsed);
        this.logger.debug(`Migrated: ${parsedResponse.parsed.length} - Not migrated: ${parsedResponse.notParsed.length}`);

        return {
            migrated: migratedReports,
            notMigrated: parsedResponse.notParsed.map((report) => {
                return {id: report.id_filter, title: report.filter_name, type: report.report_type_id};
            }),
        };
    }

    private parseOldReports(oldReports: LegacyReportsResponse, session: SessionManager): ReportsParsedResult {
        const parsedReports = [];
        const notParsedReport: LegacyReport[] = [];
        const platform = this.session.platform.getPlatformBaseUrl();
        for (const legacyReport of oldReports.reports) {
            const newReportType = legacyTypesMapper[legacyReport.report_type_id];
            if (!newReportType) {
                this.logger.error(`No aamon mappable type for the legacy type ${legacyReport.report_type_id}`);
                notParsedReport.push(legacyReport);
                continue;
            }
            let reportManagerByType: BaseReportManager;
            try {
                reportManagerByType = NewReportManagerSwitcher(session, newReportType);
            } catch (error: any) {
                if (error instanceof DisabledReportTypeException) {
                    this.logger.errorWithStack(`Required plug-in disabled for the legacy type ${legacyReport.report_type_id}`, error);
                    notParsedReport.push(legacyReport);
                    continue;
                } else {
                    throw(error);
                }
            }
            if (!reportManagerByType) {
                this.logger.error(`No aamon class to map this report type: ${newReportType}`);
                notParsedReport.push(legacyReport);
                continue;
            }
            try {
                const reportParsed = reportManagerByType.parseLegacyReport(legacyReport, platform, oldReports.visibilityRules);
                // now check the size of the object parsed
                const size = dynoItemSize(reportParsed);
                if (size > this.DYNAMO_LIMIT) {
                    throw new Error(`Size exceeds the dynamo limit of ${this.DYNAMO_LIMIT} bytes: ${size} bytes`);
                }
                parsedReports.push(reportParsed);
            } catch (e: any) {
                this.logger.errorWithStack(`Error during the parse of the legacy report: ${legacyReport.id_filter}`, e);
                notParsedReport.push(legacyReport);
            }
        }

        return {
            parsed: parsedReports,
            notParsed: notParsedReport,
        };
    }

    private async addParsedReportsToDynamo(parsedReports: ReportManagerInfo[]): Promise<MigratedReportResponse[]> {
        let migrated: MigratedReportResponse[];

        await this.dynamoService.batchWriteReports(parsedReports);

        migrated = parsedReports.map(report => {
            return {
                id: report.idReport,
                title: report.title,
            };
        });

        return migrated;
    }

}

export interface MigratedReportResponse {
    id: string;
    title: string;
}

export interface MigrationResponse {
    migrated: MigratedReportResponse[];
    notMigrated: any[];
}
export const legacyTypesMapper: {[key: string]: string} = {
    1: ReportsTypes.USERS_COURSES,
    2: ReportsTypes.USERS_ENROLLMENT_TIME,
    3: ReportsTypes.USERS_LEARNINGOBJECTS,
    4: ReportsTypes.COURSES_USERS,
    5: ReportsTypes.GROUPS_COURSES,
    6: ReportsTypes.ECOMMERCE_TRANSACTION,
    10: ReportsTypes.USERS_LP,
    20: ReportsTypes.USERS_CLASSROOM_SESSIONS,
    26: ReportsTypes.CERTIFICATIONS_USERS,
    27: ReportsTypes.USERS_CERTIFICATIONS,
    28: ReportsTypes.USERS_EXTERNAL_TRAINING,
    29: ReportsTypes.USERS_BADGES,
    50: ReportsTypes.ASSETS_STATISTICS,
    53: ReportsTypes.USER_CONTRIBUTIONS
};

export interface LegacyReportsResponse {
    reports: LegacyReport[];
    visibilityRules: VisibilityRule[];
}

export interface VisibilityRule {
    id_report: string;
    member_type: string;
    member_id: string;
    select_state: string;
}

export interface LegacyReport {
    id_filter: string;
    report_type_id: string;
    author: string;
    creation_date: string;
    filter_name: string;
    filter_data: any;
    is_public: string;
    views: string;
    is_standard: string;
    id_job: string;
    last_edit_by: string;
    last_edit: string;
    visibility_type: string;
}

export interface ReportsParsedResult {
    parsed: ReportManagerInfo[];
    notParsed: LegacyReport[];
}

export interface FieldsDescriptor {
    fields: string[];
    orderByDescriptor?: LegacyOrderByParsed;
}
export interface LegacyOrderByParsed {
    field: string;
    direction: string;
}

export interface MigrateInputPayload {
    types?: number[];
    name?: string;
    isMigrationWithOverwrite?: boolean;
    legacyReportMigratedIds?: number[];
}
