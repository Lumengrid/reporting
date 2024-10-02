import { IncomingMessage, MessageHandler } from '../handlers/message-handler';
import { Message } from '@aws-sdk/client-sqs';
import { LoggerInterface } from '../services/logger/logger-interface';
import { SQSManager } from '../services/sqs/sqs';
import { Utils } from '../reports/utils';

export class ConcreteQueueConsumer {
	private stopped = false;
	private shouldStop = false;
	private onEnd: () => void;

	public constructor(
		private readonly sqsManager: SQSManager,
		private readonly logger: LoggerInterface,
	) {}

	private doLogDebug(message: string, domain?: string): void {
		this.logger.debug({ message, domain });
	}

	private doLogError(message: string, error: Error, domain: string): void {
		this.logger.error({ message: `${message} "${error.message}" - Error stack: ${error.stack}`, domain });
	}

	public stop() {
		if (this.stopped) {
			return;
		}

		this.shouldStop = true;

		return new Promise<void>((resolve) => {
			this.onEnd = resolve;
		});
	}

	private async handleMessage(message: Message, messageHandler: MessageHandler): Promise<void> {
		let domain = ''
		try {
			this.doLogDebug(`Processing message with id '${message.MessageId}'`);
			const content = JSON.parse(message.Body);
			domain = message.MessageAttributes?.Domain?.StringValue ?? '';
			const messageContent = {...content, domain} as IncomingMessage;

			this.doLogDebug(`Message with id '${message.MessageId}' turns out to be of type "${messageContent.name}"`);
			await messageHandler.handleMessage(messageContent);

			this.doLogDebug(`Message with id '${message.MessageId}' processed correctly`, domain);
			await this.sqsManager.deleteMessageFromQueue(message.ReceiptHandle);
			this.doLogDebug(`Deleting message '${message.MessageId}' from queue`, domain);
		} catch (error: any) {
			this.doLogError(`Message with id '${message.MessageId}' failed to be processed: not being deleted from the queue`, error, domain);
		}
	}

	public async startConsumingQueue(messageHandler: MessageHandler): Promise<void> {
		this.doLogDebug('Starting to receive messages from the queue');

		while (!this.shouldStop) {
			const messages = await this.sqsManager.getMessagesFromQueue();

			if (!messages.Messages || !messages.Messages.length) {
				if (this.shouldStop) {
					this.doLogDebug('No messages received from the queue but shouldStop flag is true, exit');
					break;
				}

				await Utils.sleep(1000);
				continue;
			}

			await Promise.all(messages.Messages.map((message) => this.handleMessage(message, messageHandler)));
		}

		this.doLogDebug('Exiting the queue consumer');
		this.stopped = true;
		this.onEnd();
		this.onEnd = undefined;
	}
}
