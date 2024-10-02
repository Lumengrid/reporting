import { createPool, Pool, Options as PoolOptions } from 'generic-pool';
import {
    CloseableDBConnection,
    ConnectionParameters,
    DBConnection,
    PoolParameters
} from './interfaces/snowflake.interface';
import { createConnection } from 'snowflake-sdk';
import { Connection } from './connection';
import { ConnectionDataSourceException } from '../../exceptions/connectionDataSourceException';
import { ErrorsCode } from '../../models/base';
import { RetryWrapper } from './retryWrapper';
import { Utils } from '../../reports/utils';
import { LoggerInterface } from '../logger/logger-interface';
import { loggerFactory } from '../logger/logger-factory';

export class PoolFactory {
    private readonly MAX_ATTEMPTS_CONNECTION_VALID = 10;
    private readonly DELAY_BETWEEN_ATTEMPTS = 200;

    public constructor(
        private readonly logger: LoggerInterface
    ) {
    }

    private doLogDebug(message: string): void {
        this.logger.debug({ message });
    }

    private doLogError(message: string, exception?: any): void {
        if (exception === undefined) {
            this.logger.error({ message });
        } else {
            this.logger.errorWithException({ message }, exception);
        }
    }

    public createPool(connectionParameters: ConnectionParameters, poolParameters: PoolParameters): Pool<DBConnection> {
        const poolOptions: PoolOptions = {
            min: poolParameters.minSize,
            max: poolParameters.maxSize,
            acquireTimeoutMillis: poolParameters.acquireTimeoutMillis,
            testOnBorrow: true,
            autostart: true,
            fifo: false,
            idleTimeoutMillis: poolParameters.idleTimeoutMillis,
            evictionRunIntervalMillis: poolParameters.evictionRunMillis,
        };

        this.doLogDebug(`Creating pool using the following parameters: ${JSON.stringify(poolOptions)}`);

        return createPool<CloseableDBConnection>(
            {
                create: () => this.createDBConnection(connectionParameters),
                destroy: (conn) => this.destroyDBConnection(conn),
                validate: (conn) => this.validateDBConnection(conn),
            },
            poolOptions,
        );
    }

    private async connectionIsValidWithRetry(connection: Connection, retryNumber?: number): Promise<boolean> {
        retryNumber = retryNumber ?? 1;

        try {
            if (await connection.isValid()) {
                return true;
            }
        } catch (exception) {
            this.doLogError(`Exception to validate the connection`, exception);
        }

        this.doLogError(`Failed attempt ${retryNumber} to validate the connection`);

        if (retryNumber > this.MAX_ATTEMPTS_CONNECTION_VALID) {
            this.doLogError(`All ${this.MAX_ATTEMPTS_CONNECTION_VALID} attempts exhausted to validate the connection`);
            throw new ConnectionDataSourceException('Exceed attempts to acquire a connection from the pooL', ErrorsCode.ConnectionErrorDataSource);
        }

        await Utils.sleep(this.DELAY_BETWEEN_ATTEMPTS);
        return await this.connectionIsValidWithRetry(connection, retryNumber + 1);
    }

    private async createDBConnection(connectionParameters: ConnectionParameters): Promise<CloseableDBConnection> {
        try {
            this.doLogDebug(`Creating new connection`);
            const conn = createConnection({ ...connectionParameters });
            const connection = new Connection(conn, loggerFactory.buildLogger('[SnowflakeConnection]'));
            await connection.connect();

            if (await this.connectionIsValidWithRetry(connection)) {
                return new RetryWrapper(connection, loggerFactory.buildLogger('[SnowflakeRetryWrapper]'));
            }
        } catch (err) {
            this.doLogError('Failed to establish a connection', err);
            throw err;
        }
    }

    private async validateDBConnection(conn: CloseableDBConnection): Promise<boolean> {
        const isValid = await conn.isValid();

        if (!isValid) {
            this.doLogDebug(`Connection is no longer valid`);
        }

        return isValid;
    }

    private destroyDBConnection(conn: CloseableDBConnection): Promise<void> {
        this.doLogDebug(`Destroying connection`);
        return conn.close();
    }
}
