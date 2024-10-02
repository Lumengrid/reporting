import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { Extraction, ExtractionStatus } from '../entities/Extraction';
import moment from 'moment';
import { ExtractionId } from '../value_objects/ExtractionId';
import { ExtractionNotFoundException } from '../exceptions/ExtractionNotFoundException';

export class ExtractionsRepository {
    public constructor(
        private readonly documentClient: DynamoDBDocumentClient,
        private readonly tableName: string,
        private readonly ttlInDays = 32,
    ) {
    }

    /**
     * @throws ExtractionNotFoundException
     */
    public async getById(extractionId: ExtractionId): Promise<Extraction> {
        const item = await this.documentClient.send(new GetCommand({
            TableName: this.tableName,
            Key: {
                extraction_id: extractionId.Id,
                report_id: extractionId.ReportId,
            },
        }));

        if (!item.Item) {
            throw new ExtractionNotFoundException(`Cannot find extraction: ${extractionId}`);
        }

        return new Extraction(
            extractionId,
            item.Item as ExtractionStatus,
            global.snowflakePool
        );
    }

    public async add(extraction: Extraction): Promise<void> {
        await this.documentClient.send(new PutCommand({
            TableName: this.tableName,
            Item: {
                extraction_id: extraction.Id.Id,
                report_id: extraction.Id.ReportId,
                ...extraction.Status,
                ttl: moment().add(this.ttlInDays, 'd').unix(),
            },
        }));
    }

    public async update(extraction: Extraction): Promise<void> {
        await this.documentClient.send(new PutCommand({
            TableName: this.tableName,
            Item: {
                extraction_id: extraction.Id.Id,
                report_id: extraction.Id.ReportId,
                ...extraction.Status,
            },
        }));
    }
}
