import Config from '../../config';
import { SQSManager } from './sqs';
import { SQSClient } from '@aws-sdk/client-sqs';

export class SqsManagerFactory {
	private sqsManager: SQSManager | undefined;

	public constructor(private readonly config: Config) {
	}

	public getSqsManager(): SQSManager {
		if (!this.sqsManager) {
			this.sqsManager = new SQSManager(
				new SQSClient({ region: this.config.getAwsRegion() }),
				this.config.getDatalakeV3MessagingQueueUrl(),
				this.config.getDatalakeV3NumberOfMessagesToRead(),
				10,
			);
		}

		return this.sqsManager;
	}
}

export const sqsManagerFactory = new SqsManagerFactory(new Config());
