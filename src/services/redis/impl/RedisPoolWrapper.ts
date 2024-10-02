import { AamonRedisClient } from '../AamonRedisClient';
import { Pool } from 'generic-pool';

export class RedisPoolWrapper implements AamonRedisClient {
    public constructor(
      private readonly pool: Pool<AamonRedisClient>,
    ) {}

    public async sendCommand(command: string, params: readonly any[], db: number): Promise<any> {
      return this.pool.use(
        (connection) => connection.sendCommand(command, params, db)
      );
    }
}
