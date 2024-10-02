import Config from '../../config';
import * as fs from 'fs';
import * as winston from 'winston';

import { Facility, Severity, Syslog } from '@docebo/syslog-logger';
import { redisFactory } from '../redis/RedisFactory';

type LoggerParams = {
    appName: string;
    tag: string;
    isLoggerEnabled: boolean;
    host: string;
    port: number;
    levels: LogLevel[];
    key: string;
};

export class Logger {
    private static loggerParams: LoggerParams;
    private static syslogInstance: Syslog;

    private static logInstance: winston.Logger = winston.createLogger({
        transports: [
            new winston.transports.File({ filename: '/dev/null' })
        ]
    });

    /**
     * Based on the passed domain name,
     * Return either the Current domain log file,
     * OR if the domain is empty return the main.log as fallback
     */
    private static getSplitDomainName(domain?: string): string {
        let rootDir = process.cwd();
        const logPath = ['logs'];
        if (domain) {
            logPath.push(domain.slice(0, 1), domain.slice(1, 2), domain);
        }

        for (const dir of logPath) {
            rootDir += '/' + dir;
            if (!fs.existsSync(rootDir)) {
                fs.mkdirSync(rootDir, '0777');
            }
        }

        const logName = domain || 'main';
        return rootDir + '/' + logName + '.log';
    }

    /**
     * Create a Logger instance for the application
     * The logs location can be customized using the config parameters
     */
    static async createLoggerInstance(config: Config, domain: string): Promise<void> {

        const availableTransport = [];

        if (config.isLogToConsoleEnabled()) {
            availableTransport.push(new winston.transports.Console({
                level: LogLevel.debug,
                format: winston.format.combine(
                    winston.format.timestamp(),
                    winston.format.errors({stack: true}),
                    winston.format.metadata(),
                    winston.format.json(),
                    winston.format.printf(Logger.printTemplate)
                ),
            }));
        }
        if (config.isLogToFileEnabled()) {
            availableTransport.push(new winston.transports.File({
                level: LogLevel.info,
                filename: this.getSplitDomainName(domain),
                maxsize: 1024 * 1024, // 1MB per file
            }));
        }

        try {
            await this.configureLoggerParams();
            if (this.loggerParams.isLoggerEnabled) {
                Logger.syslogInstance = new Syslog(
                    this.loggerParams.host,
                    this.loggerParams.port
                );
            } else {
                console.error('Syslog is not enabled!');
            }
        } catch (err: any) {
            console.error('Error while trying to read syslog params during logger creation! Syslog will not be used as a transport!');
        }

        // put a default logger if none is enabled
        if (availableTransport.length === 0) {
            availableTransport.push(new winston.transports.Console({
                level: LogLevel.debug,
            }));
        }

        this.logInstance = winston.createLogger({
            transports: availableTransport,
        });

    }

    /**
     * Log message using syslog lib if enabled
     * @param message message to log
     * @param logLevel log level enum value
     * @param hostname hostname of the server
     * @param meta additional values to be logged
     */
    private static syslogLog(message: string, logLevel: LogLevel, hostname: string, ...meta: any[]) {
        if (Logger.syslogInstance && this.loggerParams.levels.includes(logLevel)) {
            Logger.syslogInstance.sendMessage(
                {
                    message: `${message}${meta ? ` - ${JSON.stringify(meta)}` : ''}`,
                    currentDomain: hostname,
                    logTime: new Date().toISOString(),
                },
                hostname,
                this.loggerParams.appName,
                this.convertLogToSeverity(logLevel),
                Facility.UserLevelMessages,
                this.loggerParams.tag,
                this.loggerParams.key
            );
        }
    }

    /**
     * Log message using both logInstance and syslogInstance
     * @param message message to log
     * @param logLevel log level enum value
     * @param hostname hostname of the server
     * @param meta additional values to be logged
     */
    static log(message: string, logLevel: LogLevel = LogLevel.debug, hostname: string, ...meta: any[]) {
        this.logInstance.log(logLevel, message, meta);
        this.syslogLog(message, logLevel, hostname, meta);
    }

    static printTemplate(info: any) {
        return `${info.metadata.timestamp} - ${info.metadata['0'] ? info.metadata['0'] : ''} - Level: ${info.level} - Message: ${info.message} ${info.metadata['1'] ? JSON.stringify(info.metadata['1']) : ''} - ${info.metadata.stack ? info.metadata.stack : ''}`;
    }

    /**
     * Convert log level enum value to syslog library level value
     * @param type LogLevel value to be converted
     */
    private static convertLogToSeverity(type: LogLevel): number {
        switch (type) {
            case LogLevel.error:
                return Severity.Error;
            case LogLevel.warning:
                return Severity.Warning;
            case LogLevel.info:
                return Severity.Informational;
            case LogLevel.silly:
            case LogLevel.verbose:
            case LogLevel.debug:
            default:
                return Severity.Debug;
        }
    }

    /**
     * Configure logger params from SYSLOG_* redis keys and use LOGGLY_* and config as fallback
     */
    private static async configureLoggerParams(): Promise<void> {
        const config = new Config();
        const redis = redisFactory.getRedis();
        const [syslogParams, logglyParams] = await Promise.all([redis.getSyslogParams(), redis.getLogglyParams()]);

        this.loggerParams = {
            appName: config.getLoggerAppName(),
            tag: `${config.getLoggerAppName()}_devel`,
            isLoggerEnabled: (syslogParams.SYSLOG_ENABLED ?? logglyParams.LOGGLY_ENABLED) === '1' ,
            host: syslogParams.SYSLOG_REMOTE_SERVER ?? config.getLoggerHost(),
            port:  parseInt(syslogParams.SYSLOG_REMOTE_PORT ?? config.getLoggerPort()),
            levels: (syslogParams.SYSLOG_LEVELS ?? logglyParams.LOGGLY_LEVELS).split(',').map(level => level.trim()) as LogLevel[],
            key: `${logglyParams.LOGGLY_INPUT_KEY}@41058`
        };
    }
}

export const enum LogLevel {
    error = 'error',
    warning = 'warning',
    info = 'info',
    verbose = 'verbose',
    debug = 'debug',
    silly = 'silly'
}
