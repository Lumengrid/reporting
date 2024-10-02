import { Connection } from './connection';
import { CloseableDBConnection } from './interfaces/snowflake.interface';
import { Utils } from '../../reports/utils';
import { LoggerInterface } from '../logger/logger-interface';

export class RetryWrapper implements CloseableDBConnection {
    private readonly MAX_ATTEMPTS = 10;
    private readonly DELAY_BETWEEN_ATTEMPTS = 200;

    public constructor(
        private readonly connection: Connection,
        private readonly logger: LoggerInterface,
    ) {
    }

    private doLogError(message: string, exception?: any): void {
        if (exception === undefined) {
            this.logger.error({ message });
        } else {
            this.logger.errorWithException({ message }, exception);
        }
    }

    private async retry<T>(func: () => Promise<T>): Promise<any> {
        for (let retryNumber = 1; retryNumber <= this.MAX_ATTEMPTS; retryNumber++) {
            try {
                return await func();
            } catch (exception: any) {
                const exceptionType = exception.name;
                const statusCode = exception.response?.status ?? 0;
                const message = `ATTEMPT ${retryNumber} FAILED WITH EXCEPTION ${exceptionType} STATUS CODE ${statusCode}`;

                if (retryNumber >= this.MAX_ATTEMPTS) {
                    this.doLogError(`${message} - ALL ${this.MAX_ATTEMPTS} RETRY ATTEMPTS EXHAUSTED`);
                    throw exception;
                }

                if (statusCode >= 500) {
                    this.doLogError(`${message} - RETRY OPERATION`);
                    await Utils.sleep(this.DELAY_BETWEEN_ATTEMPTS);
                } else {
                    this.doLogError(`${message} - THROW EXCEPTION`);
                    throw exception;
                }
            }
        }
    }

    public async runQuery(sql: string, waitResults: boolean, streamResult: boolean, returnQueryId = false): Promise<any> {
        return await this.retry(() => this.connection.runQuery(sql, waitResults, streamResult, returnQueryId));
    }
    public async isValid(): Promise<boolean> {
        return await this.retry(() => this.connection.isValid());
    }
    public async close(): Promise<void> {
        return await this.retry(() => this.connection.close());
    }

    public async getQueryStatus(queryId: string): Promise<string> {
        return await this.retry(() => this.connection.getQueryStatus(queryId));
    }

    public async isErrorStatus(queryStatus: string): Promise<boolean> {
        return await this.retry(() => this.connection.isErrorStatus(queryStatus));
    }

    public async isStillRunning(queryStatus: string): Promise<boolean> {
        return await this.retry(() => this.connection.isStillRunning(queryStatus));
    }
}
