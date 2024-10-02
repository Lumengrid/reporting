import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb';
import { PlatformSettings, PlatformSettingsDetails } from '../entities/PlatformSettings';
import { LoggerInterface } from '../../services/logger/logger-interface';

export class PlatformSettingsRepository {
    public constructor(
        private readonly documentClient: DynamoDBDocumentClient,
        private readonly tableName: string,
        private readonly logger: LoggerInterface,
    ) {
    }

    public async getByPlatform(platform: string): Promise<PlatformSettings> {
        const item = await this.documentClient.send(new GetCommand({
            TableName: this.tableName,
            Key: {
                platform,
            },
        }));

        this.logger.debug({message: `Found record for settings of "${platform}": ${JSON.stringify(item)} `});
        const details = (item.Item ?? {toggleDatalakeV3: false}) as PlatformSettingsDetails;

        return new PlatformSettings(platform, details);
    }
}
