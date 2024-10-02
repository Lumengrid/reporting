import { AamonRedisClient } from './AamonRedisClient';
import { createClient, RedisClientType } from 'redis';
import { RedisRetryWrapper } from './impl/RedisRetryWrapper';
import { RedisAdapter } from './impl/RedisAdapter';
import { createPool, Pool } from 'generic-pool';
import { RedisPoolWrapper } from './impl/RedisPoolWrapper';
import { Redis } from './redis';
import { SidekiqManagerService } from '../sidekiq-manager-service';
import { RedisSidekiqSchedulerKey } from '../../reports/interfaces/extraction.interface';
import { CloseableAamonRedisClient } from './CloseableAamonRedisClient';
import { AbstractCache } from '../cache/AbstractCache';
import Cache from '../cache/cache';
import Config from '../../config';

export class RedisFactory {
  private readonly inMemoryCache: AbstractCache = new Cache();
  private redisPool: Pool<CloseableAamonRedisClient>;
  private sidekiqPool: Pool<CloseableAamonRedisClient>;
  private redisClient: AamonRedisClient;
  private sidekiq: SidekiqManagerService;
  private redis: Redis;

  public constructor(
    private readonly config: Config,
  ) {}

  private buildPool(
    scheme: string,
    host: string,
    port: number,
    poolMin: number,
    poolMax: number,
    idleTimeout = 30000,
  ): Pool<CloseableAamonRedisClient> {
    console.debug(`[REDIS] Creating a POOL of redis connection with scheme=${scheme}, host=${host}, port=${port}, min=${poolMin}, max=${poolMax}, idle_timeout=${idleTimeout}`);

    return createPool<CloseableAamonRedisClient>({
        async create(): Promise<CloseableAamonRedisClient> {
          const redisClient: RedisClientType = createClient({
            socket: {
              host,
              port,
              tls: scheme.toLowerCase() === 'tls'
            }
          });

          redisClient.on('error', (error) => console.error(`[RedisPool] Got an error`, error));

          await redisClient.connect();

          return new RedisRetryWrapper(
            new RedisAdapter(redisClient),
          );
        },
        async destroy(client: CloseableAamonRedisClient): Promise<void> {
          await client.close();
        },
      },
      {
        min: poolMin,
        max: poolMax,
        fifo: false,
        idleTimeoutMillis: idleTimeout, // destroy all extra-clients that sits unused for this amount of time
        evictionRunIntervalMillis: 10000,
      });
  }

  private getRedisClient(): AamonRedisClient {
    if (!this.redisClient) {
      const redisScheme = this.config.getRedisScheme();
      const redisHost = this.config.getRedisHost();
      const redisPort = this.config.getRedisPort();
      const poolMin = this.config.getRedisPoolMinSize();
      const poolMax = this.config.getRedisPoolMaxSize();
      const connectionIdleTimeout = this.config.getRedisPoolConnectionIdleTimeout();

      this.redisPool = this.buildPool(
        redisScheme,
        redisHost,
        redisPort,
        poolMin,
        poolMax,
        connectionIdleTimeout,
      );

      this.redisClient = new RedisPoolWrapper(this.redisPool);
    }

    return this.redisClient;
  }

  public getRedis(): Redis {
    if (!this.redis) {
      this.redis = Redis.NewInstance(
        this.getRedisClient(),
        this.inMemoryCache,
      );
    }

    return this.redis;
  }

  public async getSidekiqClient(): Promise<SidekiqManagerService> {
    if (!this.sidekiq) {
      const redis = this.getRedisClient();
      const scheme = await redis.sendCommand('HGET', ['platform_main_config:common', 'REDIS_JOBS_SCHEME'], 0) || 'tcp';
      const host = await redis.sendCommand('HGET', ['platform_main_config:common', 'REDIS_JOBS_HOST'], 0);
      const port = await redis.sendCommand('HGET', ['platform_main_config:common', 'REDIS_JOBS_PORT'], 0);
      const db = await redis.sendCommand('HGET', ['platform_main_config:common', 'REDIS_JOBS_DB'], 0);

      this.sidekiqPool = this.buildPool(scheme, host, port, 0, 5);

      this.sidekiq = new SidekiqManagerService(
        new RedisPoolWrapper(this.sidekiqPool),
        db,
        RedisSidekiqSchedulerKey.QUEUE_SCHEDULER,
      );
    }

    return this.sidekiq;
  }

  public async drainPools(): Promise<void> {
    if (this.redisPool) {
      await this.redisPool.drain();
      await this.redisPool.clear();
    }

    if (this.sidekiqPool) {
      await this.sidekiqPool.drain();
      await this.sidekiqPool.clear();
    }
  }
}

export const redisFactory = new RedisFactory(new Config());
