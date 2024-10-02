// @ts-ignore
import { Column, ErrorCode, SnowflakeError, Statement, StatementStatus, StreamOptions } from 'snowflake-sdk';
import { PassThrough, Readable } from 'stream';
import { v4 } from 'uuid';

export class MockSnowflakeStatement implements Statement {
    constructor(private readonly query: string) {
    }

    getSqlText(): string {
        return this.query;
    }
    getStatus(): StatementStatus {
        return StatementStatus.Complete;
    }
    getColumns(): Column[] {
        return [];
    }
    getColumn(columnIdentifier: string | number): Column {
        return {} as Column;
    }
    getNumRows(): number {
        return 0;
    }
    getSessionState(): any {
        return {};
    }
    getRequestId(): string {
        return v4();
    }
    getStatementId(): string {
        return v4();
    }
    getNumUpdatedRows(): number {
        return 0;
    }
    cancel(fn: (err: SnowflakeError | undefined, stmt: Statement) => void): void {
        return;
    }
    streamRows(options?: StreamOptions): Readable {
        return new PassThrough({
            objectMode: true
          });
    }
}

export interface RetrySequence {
    error: boolean;
    code: number;
}

export enum RetryType {
    RUN_QUERY = 0,
    IS_VALID = 1,
    CLOSE = 2
}

export class CustomResponse {
    constructor(readonly status: number) {

    }
}

export class CustomError extends Error {
    public response: CustomResponse;
    public name: 'custom_error';

    constructor(message: string, readonly statusCode: number, readonly attempt: number) {
        super(message);
        this.response = new CustomResponse(statusCode);
    }
}

export class MockSnowflakeConnection {
    private isConnected = false;
    private retryAttempt = 0;

    constructor(
        private readonly failConnect: boolean,
        private readonly failSwitchSchema: boolean,
        private readonly failExecute: boolean,
        private readonly failValid: boolean,
        private readonly failDestroy: boolean,
        private readonly retrySequences: RetrySequence[] = []
        ) {
        this.retryAttempt = 0;
    }

    getError(code: ErrorCode): SnowflakeError {
        return {
            code,
            sqlState: 'error sqlState',
            responseBody: 'error responseBody',
            message: 'error message',
            name: 'error name',
            stack: 'error stack',
            isFatal: true
        };
    }

    connect(fn: (err: SnowflakeError | undefined) => void): void {
        this.isConnected = !this.failConnect;
        if (this.failConnect) {
            fn(this.getError(ErrorCode.ERR_CONN_CONNECT_STATUS_CONNECTING));
        } else {
            fn(undefined);
        }
    }

    execute(options: {
        sqlText: string;
        streamResult?: boolean | undefined;
        complete?: (err: SnowflakeError | undefined, stmt: Statement, rows: any[] | undefined) => void;
    }): void {
        const fail = options.sqlText.startsWith('USE') ? this.failSwitchSchema : this.failExecute;
        const stmt = new MockSnowflakeStatement(options.sqlText);
        if (!this.isConnected || fail) {
            if (this.retrySequences.length === 0 || this.retrySequences.length <= this.retryAttempt) {
                options.complete(this.getError(ErrorCode.ERR_CONN_EXEC_STMT_INVALID_SQL_TEXT), stmt, []);
            } else {
                if (this.retrySequences[this.retryAttempt].error) {
                    this.retryAttempt ++;
                    throw new CustomError('error', this.retrySequences[this.retryAttempt].code, this.retryAttempt);
                } else {
                    options.complete(undefined, stmt, []);
                }
            }
        } else {
            options.complete(undefined, stmt, []);
        }
    }

    async isValidAsync(): Promise<boolean> {
        if (this.failValid) {
            if (this.retrySequences.length === 0 || this.retrySequences.length <= this.retryAttempt) {
                return false;
            } else {
                if (this.retrySequences[this.retryAttempt].error) {
                    this.retryAttempt ++;
                    throw new CustomError('error', this.retrySequences[this.retryAttempt].code, this.retryAttempt);
                } else {
                    return true;
                }
            }
        } else {
            return true;
        }
    }

    destroy(fn: (err: SnowflakeError | undefined) => void): void {
        if (this.failDestroy) {
            if (this.retrySequences.length === 0 || this.retrySequences.length <= this.retryAttempt) {
                fn(this.getError(ErrorCode.ERR_CONN_DESTROY_STATUS_DISCONNECTED));
            } else {
                if (this.retrySequences[this.retryAttempt].error) {
                    this.retryAttempt ++;
                    throw new CustomError('error', this.retrySequences[this.retryAttempt].code, this.retryAttempt);
                } else {
                    fn(undefined);
                }
            }
        } else {
            this.isConnected = false;
            fn(undefined);
        }
    }
}
