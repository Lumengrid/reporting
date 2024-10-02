import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { ReportNotFoundException } from '../exceptions/ReportNotFoundException';
import { ReportId } from '../value_objects/ReportId';
import { Report } from '../entities/Report';
import { ReportManagerInfo } from '../../models/report-manager';

export class ReportsRepository {
    public constructor(
        private readonly documentClient: DynamoDBDocumentClient,
        private readonly tableName: string,
    ) {
    }

    /**
     * @throws ReportNotFoundException
     */
    public async getById(id: ReportId): Promise<Report> {
        const item = await this.documentClient.send(new GetCommand({
            TableName: this.tableName,
            Key: {
                idReport: id.ReportId,
                platform: id.Platform,
            },
        }));

        if (!item.Item || (item.Item.hasOwnProperty('deleted') && item.Item.deleted === true)) {
            throw new ReportNotFoundException(`Report not found ${id}`, 1002);
        }

        return new Report(
            id,
            item.Item as ReportManagerInfo
        );
    }

    public async update(report: Report): Promise<void> {
        const data = {
            ...report.Info
        };
        delete data['idReport'];
        delete data['platform'];
        await this.documentClient.send(new PutCommand({
            TableName: this.tableName,
            Item: {
                idReport: report.Id.ReportId,
                platform: report.Id.Platform,
                ...data,
            },
        }));
    }

    public async add(report: Report): Promise<void> {
        await this.update(report);
    }
}
