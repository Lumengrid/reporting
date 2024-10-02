import { MessageHandler } from '../handlers/message-handler';
import { ConcreteQueueConsumer } from './concrete-queue-consumer';

export class GroupQueueConsumer {
	public constructor(
		private readonly consumers: Readonly<ConcreteQueueConsumer>[],
	) {}

	public async startConsumingQueue(handler: MessageHandler): Promise<void> {
		return Promise.all(
			this.consumers.map((consumer) => consumer.startConsumingQueue(handler))
		).then(() => undefined);
	}

	public async stop(): Promise<void> {
		return Promise.all(
			this.consumers.map((consumer) => consumer.stop())
		).then(() => undefined);
	}
}
