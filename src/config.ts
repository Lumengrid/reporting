export default class Config {

    // Aaamon settings
    public port: number = process.env.AAMON_PORT ? +process.env.AAMON_PORT : 3000;
    public secureConnection: boolean = process.env.AAMON_USE_SSL ? process.env.AAMON_USE_SSL === '1' : false;
    public sslKeyFile: string = process.env.SSL_KEY_FILE ? process.env.SSL_KEY_FILE : 'server.key';
    public sslCertFile: string = process.env.SSL_CERT_FILE ? process.env.SSL_CERT_FILE : 'server.crt';
    public urlPrefix = '/analytics/v1';
    public internalUrlPrefix = '/aamon';

    // Redis settings
    private redisScheme: string = (process.env.REDIS_SCHEME || 'tcp').toLowerCase();
    private redisHost: string = process.env.REDIS_HOST || '127.0.0.1';
    private redisPort: number = process.env.REDIS_PORT ? +process.env.REDIS_PORT : 6379;

    // Dynamo tables name
    private reportsTableName: string = process.env.REPORTS_TABLE || 'learn4-datalake-custom-reports';
    private reportExtractionsTableName = process.env.REPORT_EXTRACTIONS_TABLE || 'learn4-datalake-reports-extractions';
    private dataLakeRefreshInfoTableName = process.env.REPORTS_INFO_TABLE || 'learn4-datalake-refresh-info';
    private refreshOnDemandTokensTableName = process.env.REPORTS_REFRESH_ON_DEMAND_TOKENS_TABLE || 'learn4-datalake-refresh-on-demand-tokens';
    private reportsSettingsTableName = process.env.REPORTS_SETTINGS_TABLE_NAME || 'learn4-datalake-reports-settings';
    private datalakeSchemasTableName = process.env.AAMON_DATALAKE_V2_SCHEMAS_TABLE || 'bi-euw1-ecs-datalake-v2-schemas';

    // Lambda name for the start refresh data lake function
    private sqsRefreshOnDemandQueue: string = process.env.REFRESH_ON_DEMAND_SQS_URL || 'https://sqs.eu-west-1.amazonaws.com/594973096343/test-rick';

    // ARN of the StepFunction for the data lake V2.5
    private datalakeV2StepFunctionARN = process.env.AAMON_DATALAKE_STEP_FUNCTION_ARN || 'arn:aws:states:eu-west-1:594973096343:stateMachine:bi-euw1-ecs-datalake-v2-refresh-schema';
    private sqsDatalakeV25Refresh: string = process.env.SQS_DATALAKE_REFRESH || 'https://sqs.eu-west-1.amazonaws.com/594973096343/bi-euw1-ecs-datalake-v2-sqs-refresh-schema.fifo';

    public getEnvVar(name: string, fallbackValue: string): string {
        const value = process.env[name];

        if (value === undefined) {
            console.debug(`No ${name} environment variable found, using fallback value: "${fallbackValue}"`);
            return fallbackValue;
        }

        console.debug(`Found ${name} environment variable with value: "${value}"`);
        return value;
    }

    public getAwsRegion(): string {
        return this.getDatalakeV3MessagingQueueUrl().split('.')[1] ?? 'us-east-1';
    }

    public getRedisScheme(): string {
        return this.redisScheme;
    }

    public getRedisHost(): string {
        return this.redisHost;
    }

    public getRedisPort(): number {
        return this.redisPort;
    }

    public getRedisPoolMinSize(): number {
        return Number(this.getEnvVar('REDIS_POOL_MIN', '0'));
    }

    public getRedisPoolMaxSize(): number {
        return Number(this.getEnvVar('REDIS_POOL_MAX', '10'));
    }

    public getRedisPoolConnectionIdleTimeout(): number {
        return Number(this.getEnvVar('REDIS_POOL_IDLE_MILLISECONDS', '30000'));
    }

    public getReportsTableName(): string {
        return this.reportsTableName;
    }

    public getReportExtractionsTableName(): string {
        return this.reportExtractionsTableName;
    }

    public getDataLakeRefreshInfoTableName(): string {
        return this.dataLakeRefreshInfoTableName;
    }

    public getRefreshOnDemandTokensTableName(): string {
        return this.refreshOnDemandTokensTableName;
    }

    public getReportsSettingsTableName(): string {
        return this.reportsSettingsTableName;
    }

    public getDatalakeSchemasTableName(): string {
        return this.datalakeSchemasTableName;
    }

    // Logger settings
    private useConsoleLog: boolean = process.env.LOG_TO_CONSOLE !== '0';
    private useFileLog: boolean = process.env.LOG_TO_FILE !== '0';

    public readonly loggerAppName = 'aamon';
    public readonly loggerHost = 'logs-01.loggly.com';
    public readonly loggerPort = '514';

    public isLogToConsoleEnabled(): boolean {
        return this.useConsoleLog;
    }

    public isLogToFileEnabled(): boolean {
        return this.useFileLog;
    }

    public getStartRefreshSQSUrl(): string {
        return this.sqsRefreshOnDemandQueue;
    }

    public getDatalakeV2StepFunctionARN(): string {
        return this.datalakeV2StepFunctionARN;
    }

    public getSqsDatalakeV25Refresh(): string {
        return this.sqsDatalakeV25Refresh;
    }

    public getLoggerAppName(): string {
        return this.loggerAppName;
    }

    public getLoggerHost(): string {
        return this.loggerHost;
    }

    public getLoggerPort(): string {
        return this.loggerPort;
    }

    public getDatalakeV3MessagingQueueUrl(): string {
        return this.getEnvVar('DATALAKE_V3_MESSAGES_QUEUE_URL', 'https://sqs.us-east-1.amazonaws.com/594973096343/learn4_schedulation_test');
    }

    public getDatalakeV3MaxNumberOfWorkers(): number {
        return Number(this.getEnvVar('DATALAKE_V3_MAX_WORKERS', '10'));
    }

    public getDatalakeV3NumberOfMessagesToRead(): number {
        return Number(this.getEnvVar('DATALAKE_V3_MAX_CONCURRENT_MESSAGES', '1'));
    }
}
