import { RedisClientType } from 'redis';
import { CloseableAamonRedisClient } from '../CloseableAamonRedisClient';

export class RedisAdapter implements CloseableAamonRedisClient {
  public constructor(
    private readonly client: RedisClientType,
  ) {
  }

  private async switchToDb(db: number): Promise<void> {
    return this.client.sendCommand(['SELECT', `${db}`]);
  }

  public async sendCommand(command: string, params: readonly any[], db: number): Promise<any> {
    await this.switchToDb(db);
    return this.client.sendCommand([command, ...params]);
  }

  public async close(): Promise<void> {
    await this.client.quit();
  }
}
