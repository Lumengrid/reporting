import { Utils } from '../../../reports/utils';
import { CloseableAamonRedisClient } from '../CloseableAamonRedisClient';

export class RedisRetryWrapper implements CloseableAamonRedisClient {
    public constructor(
      private readonly client: CloseableAamonRedisClient,
      private readonly maxRetries = 5,
      private readonly delay = 500,
    ) {}

    public async sendCommand(command: string, params: readonly any[], db: number): Promise<any> {
        let attempts = 0;

        while (true) {
            try {
                return await this.client.sendCommand(command, params, db);
            } catch (error: any) {
                console.error(`[Redis] Error sending command "${command}" on db ${db}`, error);

                if (++attempts > this.maxRetries) {
                    console.debug(`[Redis] Too many attempts performed for command "${command}": letting the exception pass`);
                    throw error;
                }

                console.debug(`[Redis] Performing retry ${attempts} of ${this.maxRetries} in ${this.delay} ms`);
                await Utils.sleep(this.delay);
            }
        }
    }

    public async close(): Promise<void> {
        return this.client.close();
    }
}
