import Config from '../../config';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { ReportId } from '../value_objects/ReportId';
import { Report } from '../entities/Report';
import { ReportsRepository } from '../repositories/ReportsRepository';
import { ReportCheckChanges } from './ReportCheckChanges';
import { LoggerInterface } from '../../services/logger/logger-interface';
import Hydra from '../../services/hydra';
import UserManager from '../../services/session/user-manager.session';
import PlatformManager from '../../services/session/platform-manager.session';
import { ReportManagerInfo } from '../../models/report-manager';
import { ReportPatchInput } from '../interfaces/patch.interface';

export class ReportUpdate {
    public constructor(
        private readonly logger: LoggerInterface,
        private readonly hydra: Hydra,
        private readonly user: UserManager,
        private readonly platform: PlatformManager,
    ) {
    }

    public async execute(reportId: ReportId, patch: boolean, data: ReportManagerInfo | ReportPatchInput): Promise<ReportManagerInfo> {
        const config = new Config();
        const region = config.getAwsRegion();
        const documentDb = DynamoDBDocumentClient.from(
            new DynamoDBClient({region})
        );
        const repository = new ReportsRepository(documentDb, config.getReportsTableName());
        const reportBefore = await repository.getById(reportId);
        const report = await repository.getById(reportId);

        report.update(
            this.hydra.getHostname(),
            this.hydra.getSubfolder(),
            this.user.getIdUser(),
            this.user.getLevel(),
            this.platform.isDatalakeV2Active(),
            this.platform.getReportDownloadPermissionLink(),
            patch,
            data
        );

        try {
            await repository.update(report);
            const processChanges = new ReportCheckChanges(this.logger, this.hydra);
            await processChanges.checkForSidekiqScheduling(
                this.platform.isDatalakeV2Active(),
                this.platform.getPlatformBaseUrl(),
                reportBefore.Info.planning,
                report.Info.idReport,
                report.Info.planning
            );
            await processChanges.checkForSchedulingChangeEvent(reportBefore.Info.planning, report.Info);
            await processChanges.checkForUpdateReportEvent(reportBefore.Info, report.Info);

            return report.Info;
        } catch (ex) {
            await repository.update(new Report(reportId, reportBefore.Info));
            throw ex;
        }
    }
}
