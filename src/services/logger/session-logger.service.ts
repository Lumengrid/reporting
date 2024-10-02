import { Logger, LogLevel } from './logger';

export class SessionLoggerService {
    private readonly hostname: string;
    constructor(hostname: string) {
        this.hostname = hostname;
    }

    info(message: string, ...meta: any[]): void {
        Logger.log(message.replace(`/\\r?\\n|\\r/g`, ''), LogLevel.info, this.hostname, meta);
    }

    debug(message: string, ...meta: any[]) {
        Logger.log(message.replace(`/\\r?\\n|\\r/g`, ''), LogLevel.debug, this.hostname, meta);
    }

    error(message: string, ...meta: any[]) {
        Logger.log(message.replace(`/\\r?\\n|\\r/g`, ''), LogLevel.error, this.hostname, meta);
    }

    errorWithStack(message: string, error: any, ...meta: any[]) {
        if (error.hasOwnProperty('message')) {
            message += ` "${error.message}"`
        }
        if (error.hasOwnProperty('stack')) {
            message += ', Error stack: ' + error.stack;
        } else {
            message += ', Error details: ' + error.toString();
        }
        Logger.log(message.replace(`/\\r?\\n|\\r/g`, ''), LogLevel.error, this.hostname, meta);
    }
}
