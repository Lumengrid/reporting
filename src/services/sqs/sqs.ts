import {
	SQSClient,
	SendMessageCommand,
	SendMessageCommandOutput,
	ReceiveMessageCommand,
	ReceiveMessageCommandOutput,
	DeleteMessageCommand,
	DeleteMessageCommandOutput,
} from '@aws-sdk/client-sqs';
import { EventName } from '../../handlers/impl/base-message-handler';

export class SQSManager {
	public constructor(
		private readonly sqsClient: SQSClient,
		private readonly schedulingQueueUrl: string,
		private readonly maxNumberOfMessagesToRead: number,
		private readonly secondsToWaitForMessages: number,
	) {
	}

	/**
	 * Pass a scheduled report to the queue
	 *
	 * @param idReport ID of the scheduled report
	 * @param platform url of the platform
	 *
	 */
	public async sendNewExtractionToQueue(idReport: string, platform: string): Promise<SendMessageCommandOutput> {
		const body = JSON.stringify({
			name: EventName.NewExtraction,
			payload: {
				id_report: idReport,
				platform
			}
		});

		return await this.sendMessageToExtractionQueue(body, platform);
	}

	public async sendMessageToExtractionQueue(body: string, domain: string) {
		const command = new SendMessageCommand({
			QueueUrl: this.schedulingQueueUrl,
			MessageBody: body,
			MessageAttributes: {
				Domain: {
					DataType: 'String',
					StringValue: domain
				}
			},
		});

		return this.sqsClient.send(command);
	}

	public async getMessagesFromQueue(): Promise<ReceiveMessageCommandOutput> {
		const command = new ReceiveMessageCommand({
			QueueUrl: this.schedulingQueueUrl,
			MaxNumberOfMessages: this.maxNumberOfMessagesToRead,
			WaitTimeSeconds: this.secondsToWaitForMessages,
			MessageAttributeNames: ['Domain'],
		});

		return this.sqsClient.send(command);
	}

	public async deleteMessageFromQueue(receiptHandle: string): Promise<DeleteMessageCommandOutput> {
		const command = new DeleteMessageCommand({
			QueueUrl: this.schedulingQueueUrl,
			ReceiptHandle: receiptHandle,
		});

		return this.sqsClient.send(command);
	}
}
