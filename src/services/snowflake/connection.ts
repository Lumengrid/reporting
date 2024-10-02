import { Connection as SnowflakeConnection } from 'snowflake-sdk';
import { CloseableDBConnection } from './interfaces/snowflake.interface';
import { LoggerInterface } from '../logger/logger-interface';

export class Connection implements CloseableDBConnection {
    public constructor(
        private readonly connection: SnowflakeConnection,
        private readonly logger: LoggerInterface
    ) {}

    private logError(message: string, exception?: any): void {
        if (exception === undefined) {
            this.logger.error({ message });
        } else {
            this.logger.errorWithException({ message }, exception);
        }
    }

    private async performQuery(sql: string, waitResults: boolean, streamResult: boolean, returnQueryId: boolean): Promise<any> {
        const logger = this.logger;

        return new Promise((resolve: any, reject: any) => {
            this.connection.execute({
                sqlText: sql,
                streamResult,
                // @ts-ignore
                asyncExec: !waitResults,
                complete(err, stmt, rows) {
                    const lastQueryId = stmt.getStatementId();
                    if (err) {
                        logger.error({ message: `Query error: "${err.message}" - QueryId: "${lastQueryId} - Code: "${err.code}" - Error stack: ${err.stack} - Query: ${sql}` });
                        reject(err);
                    } else {
                        if (returnQueryId || !waitResults) {
                            resolve(lastQueryId);
                        }

                        resolve(rows ?? []);
                    }
                }
            });
        });
    }

    public async runQuery(sql: string, waitResults: boolean, streamResult: boolean, returnQueryId = false): Promise<any> {
        return await this.performQuery(sql, waitResults, streamResult, returnQueryId);
    }

    public async connect(): Promise<void> {
        return new Promise((resolve, reject) => {
            this.connection.connect((err) => {
                if (err) {
                    this.logError('Error connecting to Snowflake', err);
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    public async isValid(): Promise<boolean> {
        return await this.connection.isValidAsync();
    }

    public async close(): Promise<void> {
        return new Promise((resolve, reject) => {
            this.connection.destroy((err) => {
                if (err) {
                    this.logError('Error destroying the Snowflake connection', err);
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    public async getQueryStatus(queryId: string): Promise<string> {
        // @ts-ignore
        return this.connection.getQueryStatus(queryId);
    }

    public async isStillRunning(queryStatus: string): Promise<boolean> {
        // @ts-ignore
        return this.connection.isStillRunning(queryStatus);
    }

    public async isErrorStatus(queryStatus: string): Promise<boolean> {
        // @ts-ignore
        return this.connection.isAnError(queryStatus);
    }
}
