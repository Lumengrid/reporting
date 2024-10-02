import { Pool } from 'generic-pool';
import { DBConnection, PoolParameters } from './services/snowflake/interfaces/snowflake.interface';
import { ParametersFactory } from './services/snowflake/parametersFactory';
import { PoolFactory } from './services/snowflake/poolFactory';
import Config from './config';
import { loggerFactory } from './services/logger/logger-factory';

export async function CreateSnowflakeConnectionsPool(poolConfig?: PoolParameters): Promise<Pool<DBConnection>> {
    const logger = loggerFactory.buildLogger('[SnowflakePool]');
    logger.debug({message: `Creating Snowflake connections pool using config: ${JSON.stringify(poolConfig, undefined, 2)}`});

    try {
        if (global.snowflakePool !== undefined) {
            logger.warning({message: 'Pool already instantiated, nothing to do here'});
            return;
        }

        const factory = new ParametersFactory(new Config(), loggerFactory.buildLogger('[ParametersFactory]'));
        const parameters = await factory.getParameters();

        if (!parameters) {
            logger.error({message: 'Error creating the pool: parameter are not valid'});
            return;
        }

        const poolFactory = new PoolFactory(logger);

        if (!poolConfig) {
            poolConfig = {
                ...parameters.pool,
            };
        }

        const pool = poolFactory.createPool(
            {
                ...parameters.connection,
            },
            poolConfig,
        );

        logger.info({message: 'Pool created successfully'});
        return pool;
    } catch (error: any) {
        logger.errorWithException({message: `Error creating the pool`}, error);
    }
}
