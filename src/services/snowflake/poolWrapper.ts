import { Pool } from 'generic-pool';
import { SessionLoggerService } from '../logger/session-logger.service';
import { DBConnection } from './interfaces/snowflake.interface';
import { ConnectionDataSourceException } from '../../exceptions/connectionDataSourceException';
import { ErrorsCode } from '../../models/base';
import { ExtractionFailedException } from '../../exceptions/extractionFailedException';

export class PoolWrapper implements DBConnection {
    public constructor(
        private readonly pool: Pool<DBConnection> | null,
        private readonly logger: SessionLoggerService,
        private readonly database: string,
        private readonly defaultSchema: string,
        private readonly schema: string
    ) {}

    private logError(message: string, exception?: any): void {
        message = `** SNOWFLAKE POOL WRAPPER ** ${message}`;

        if (exception === undefined) {
            this.logger.error(message);
        } else {
            this.logger.errorWithStack(message, exception);
        }
    }

    private async switchSchema(conn: DBConnection, database: string, schema: string): Promise<void> {
        const sqlQuery = `USE "${database}"."${schema}"`;

        try {
            await conn.runQuery(sqlQuery, true, false, false);
        } catch (err: any) {
            this.logError(`SET SCHEMA EXCEPTION ${sqlQuery}`, err);
            throw err;
        }
    }

    private async getConnection(): Promise<DBConnection> {
        if (!this.pool) {
            throw new ConnectionDataSourceException('Pool not instantiated', ErrorsCode.PoolNotInstantiated);
        }

        if (this.database === '' || this.schema === '') {
            this.logError('FAIL TO ACQUIRE A CONNECTION FROM THE POOL EMPTY DATABASE OR SCHEMA');
            throw new ConnectionDataSourceException('Empty database or schema', ErrorsCode.DatabaseSchemaError);
        }
        const conn = await this.pool.acquire();
        try {
            await this.switchSchema(conn, this.database, this.schema);
            return conn;
        } catch (error: any) {
            await this.returnConnectionToThePool(conn);
            throw new ConnectionDataSourceException('Fail to acquire a connection', ErrorsCode.ConnectionErrorDataSource);
        }
    }

    private async returnConnectionToThePool(conn: DBConnection): Promise<void> {
        if (!this.pool) {
            return;
        }

        try {
            await this.switchSchema(conn, this.database, this.defaultSchema);
        } catch (err: any) {
            this.logError('SWITCH BACK SCHEMA EXCEPTION', err);
        }

        try {
            await this.pool.release(conn);
        } catch (err: any) {
            this.logError('RELEASE CONNECTION EXCEPTION', err);
        }
    }

    public async runQuery(sql: string, waitResults: boolean, streamResult: boolean, returnQueryId = false): Promise<any> {
        const connection = await this.getConnection();

        try {
            return await connection.runQuery(sql, waitResults, streamResult, returnQueryId);
        } catch (err: any) {
            throw new ExtractionFailedException(err.message, ErrorsCode.ExtractionFailed);
        } finally {
            await this.returnConnectionToThePool(connection);
        }
    }

    public async getQueryStatus(queryId: string): Promise<string> {
        return this.pool.use((connection) => connection.getQueryStatus(queryId));
    }

    public async isStillRunning(queryStatus: string): Promise<boolean> {
        return this.pool.use((connection) => connection.isStillRunning(queryStatus));
    }

    public async isErrorStatus(queryStatus: string): Promise<boolean> {
        return this.pool.use((connection) => connection.isErrorStatus(queryStatus));
    }
}
