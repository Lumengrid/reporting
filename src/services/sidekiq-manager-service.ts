import { SidekiqSchedulerItem, SidekiqSchedulerWorkerClass } from '../reports/interfaces/extraction.interface';
import { AamonRedisClient } from './redis/AamonRedisClient';
import { RedisConnectionException } from './redis/redis.exceptions';
import { Utils } from '../reports/utils';

export class SidekiqManagerService {
    public constructor(
        private readonly redisClient: AamonRedisClient,
        private readonly db: number,
        private readonly tasksQueueKey: string,
    ) {
    }

    public async storeSidekiqSchedulerItem(item: SidekiqSchedulerItem): Promise<void> {
        try {
            const result = await this.redisClient.sendCommand('LPUSH', [this.tasksQueueKey, JSON.stringify(item)], this.db);

            if (result === 0) {
                throw new Error(`LPUSH failed. Returned: ${result}`);
            }
        } catch (error: any) {
            console.error(`Error while scheduling Sidekiq item`, error);
            throw new RedisConnectionException(error.message);
        }
    }

    public async deleteSidekiqSchedulerItem(reportId: string, platform: string): Promise<void> {
        const utils = new Utils();
        const item: SidekiqSchedulerItem = {
            class: SidekiqSchedulerWorkerClass.REMOVE_TASK,
            args: [reportId, platform],
            retry: true,
            enqueued_at: utils.getMicroTime() as number
        };

        await this.storeSidekiqSchedulerItem(item);
    }
}
