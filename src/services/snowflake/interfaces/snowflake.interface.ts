export interface DBConnection {
    runQuery(sql: string, waitResults: boolean, streamResult: boolean, returnQueryId: boolean): Promise<any>;
    getQueryStatus(queryId: string): Promise<string>;
    isStillRunning(queryStatus: string): Promise<boolean>;
    isErrorStatus(queryStatus: string): Promise<boolean>;
}

export interface CloseableDBConnection extends DBConnection {
    isValid(): Promise<boolean>;
    close(): Promise<void>;
}

export interface ConnectionParameters {
    readonly account: string;
    readonly username: string;
    readonly password: string;
    readonly database: string;
    readonly schema: string;
    readonly role: string;
    readonly warehouse: string;
    readonly timeout: number;
    readonly clientSessionKeepAlive: boolean;
    readonly clientSessionKeepAliveHeartbeatFrequency: number;
}

export interface PoolParameters {
    readonly minSize: number;
    readonly maxSize: number;
    readonly acquireTimeoutMillis: number;
    readonly idleTimeoutMillis?: number;
    readonly evictionRunMillis?: number;
}

export interface Parameters {
    readonly connection: ConnectionParameters;
    readonly pool: PoolParameters;
}

export interface RefreshDetails {
    status: string;
    lastRefreshStart: string;
}
