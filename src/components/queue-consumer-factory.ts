import { GroupQueueConsumer } from './group-queue-consumer';
import { ConcreteQueueConsumer } from './concrete-queue-consumer';
import { loggerFactory } from '../services/logger/logger-factory';
import { sqsManagerFactory } from '../services/sqs/sqs-manager-factory';

class QueueConsumerFactory {
	public buildQueueConsumer(numberOfWorkers: number): GroupQueueConsumer {
		const sqsManager = sqsManagerFactory.getSqsManager();

		const consumers = Array(numberOfWorkers)
			.fill(0)
			.map((n, index) => new ConcreteQueueConsumer(
				sqsManager,
				loggerFactory.buildLogger(`[QueueConsumer-${index.toString().padStart(2, '0')}]`)
			));

		return new GroupQueueConsumer(consumers);
	}
}

export const queueConsumerFactory = new QueueConsumerFactory();
