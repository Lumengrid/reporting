import { ScheduledReportId } from '../value_objects/ScheduledReportId';
import { ScheduledReport, ScheduledReportDetails } from '../entities/ScheduledReport';
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb';
import { ScheduledReportNotFoundException } from '../exceptions/ScheduledReportNotFoundException';

export class ScheduledReportsRepository {
    public constructor(
        private readonly documentClient: DynamoDBDocumentClient,
        private readonly tableName: string,
    ) {
    }

    /**
     * @throws ScheduledReportNotFoundException
     */
    public async getById(id: ScheduledReportId): Promise<ScheduledReport> {
        const item = await this.documentClient.send(new GetCommand({
            TableName: this.tableName,
            Key: {
                idReport: id.ReportId,
                platform: id.Platform,
            },
        }));

        if (!item.Item || item.Item.deleted === true) {
            throw new ScheduledReportNotFoundException(`Cannot find scheduled report: ${id}`);
        }

        return new ScheduledReport(
            id,
            item.Item as ScheduledReportDetails
        );
    }
}
