import Config from '../../config';
import { ConnectionParameters, Parameters, PoolParameters } from './interfaces/snowflake.interface';
import { SnowflakeDefaults } from '../session/platform-manager.session';
import { redisFactory } from '../redis/RedisFactory';
import { LoggerInterface } from '../logger/logger-interface';

export class ParametersFactory {

    public constructor(
      private readonly config: Config,
      private readonly logger: LoggerInterface,
    ) {}

    private doLogDebug(message: string): void {
        this.logger.debug({ message });
    }

    private doLogError(message: string): void {
        this.logger.error({ message });
    }

    private validateParam(value: string, redisKey: string): boolean {
        if (!value || value === '') {
            this.doLogError(`COMMON PARAM ${redisKey} EMPTY`);
            return false;
        }
        return true;
    }

    private validateMandatoryParams(params: {[key: string]: string}): boolean {
        const { snowflakeLocator, snowflakeUsername, snowflakePassword, snowflakeDatabase, snowflakeRole, snowflakeWarehouse } = params;

        return this.validateParam(snowflakeDatabase, 'SNOWFLAKE_DATABASE') &&
            this.validateParam(snowflakeLocator, 'SNOWFLAKE_LOCATOR') &&
            this.validateParam(snowflakePassword, 'SNOWFLAKE_PASSWORD') &&
            this.validateParam(snowflakeRole, 'SNOWFLAKE_ROLE') &&
            this.validateParam(snowflakeUsername, 'SNOWFLAKE_USERNAME') &&
            this.validateParam(snowflakeWarehouse, 'SNOWFLAKE_WAREHOUSE');
}

    public async getParameters(): Promise<Parameters | null> {
        const commonParams = await redisFactory.getRedis().getRedisCommonParams();

        if (!commonParams) {
            this.doLogError('PARAMS NOT FOUND IN REDIS');
            return;
        }

        if (!this.validateMandatoryParams(commonParams)) {
            return;
        }

        let timeout = SnowflakeDefaults.TIMEOUT;

        if (commonParams.snowflakeTimeout && commonParams.snowflakeTimeout !== '') {
            timeout = Number(commonParams.snowflakeTimeout);
        }
        // @ts-ignore
        let clientSessionKeepAlive = SnowflakeDefaults.CLIENT_SESSION_KEEP_ALIVE_ENABLED === 1;
        if (commonParams.snowflakeClientSessionKeepAliveEnabled && commonParams.snowflakeClientSessionKeepAliveEnabled !== '') {
            clientSessionKeepAlive = Number(commonParams.snowflakeClientSessionKeepAliveEnabled) === 1;
        }
        let clientSessionKeepAliveHeartbeatFrequency = SnowflakeDefaults.CLIENT_SESSION_KEEP_ALIVE_FREQUENCY;
        if (commonParams.snowflakeClientSessionKeepAliveFrequency && commonParams.snowflakeClientSessionKeepAliveFrequency !== '') {
            clientSessionKeepAliveHeartbeatFrequency = Number(commonParams.snowflakeClientSessionKeepAliveFrequency);
        }
        const connection: ConnectionParameters = {
            account: commonParams.snowflakeLocator,
            username: commonParams.snowflakeUsername,
            password: commonParams.snowflakePassword,
            database: commonParams.snowflakeDatabase,
            schema: commonParams.snowflakeDefaultSchema !== '' ? commonParams.snowflakeDefaultSchema : SnowflakeDefaults.DEFAULT_SCHEMA,
            role: commonParams.snowflakeRole,
            warehouse: commonParams.snowflakeWarehouse,
            timeout,
            clientSessionKeepAlive,
            clientSessionKeepAliveHeartbeatFrequency
        };

        const poolMinSize = commonParams.snowflakePoolMin && commonParams.snowflakePoolMin !== ''
          ? Number(commonParams.snowflakePoolMin)
          : SnowflakeDefaults.POOL_MIN;

        this.doLogDebug(`Found pool min size: ${poolMinSize}`);

        const poolMaxSize = commonParams.snowflakePoolMax && commonParams.snowflakePoolMax !== ''
          ? Number(commonParams.snowflakePoolMax)
          : SnowflakeDefaults.POOL_MAX;

        this.doLogDebug(`Found pool max size: ${poolMaxSize}`);

        const acquireTimeoutMillis = commonParams.snowflakeAcquireTimeout && commonParams.snowflakeAcquireTimeout !== ''
          ? Number(commonParams.snowflakeAcquireTimeout)
          : SnowflakeDefaults.ACQUIRE_TIMEOUT;

        this.doLogDebug(`Found pool acquire timeout (ms): ${acquireTimeoutMillis}`);

        const idleTimeout = Number(this.config.getEnvVar(
          'SNOWFLAKE_IDLE_TIMEOUT',
          `${1000 * 60 * 5}`, // 5 minutes
        ));

        this.doLogDebug(`Found connection idle timeout (ms): ${idleTimeout}`);

        const evictionInterval = Number(this.config.getEnvVar(
          'SNOWFLAKE_EVICTION_INTERVAL',
          `${1000 * 60}`
        ));

        this.doLogDebug(`Found eviction run interval (ms): ${evictionInterval}`);

        const pool: PoolParameters = {
            minSize: poolMinSize,
            maxSize: poolMaxSize,
            acquireTimeoutMillis,
            idleTimeoutMillis: idleTimeout,
            evictionRunMillis: evictionInterval,
        };

        return {
            connection,
            pool,
        };
    }
}
