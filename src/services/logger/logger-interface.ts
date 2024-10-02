import { Logger, LogLevel } from './logger';

export type LogMessage = {
	message: string;
	domain?: string;
};

export interface LoggerInterface {
	debug(message: LogMessage): void;
	info(message: LogMessage): void;
	warning(message: LogMessage): void;
	error(message: LogMessage): void;
	errorWithException(message: LogMessage, exception: Error): void;
}

export class ConsoleLogger implements LoggerInterface {
	public constructor(
		private readonly logPrefix = '',
		private readonly logTimestamp: boolean,
		private readonly domain: string = '',
	) {
	}

	private generateMessage(message: LogMessage): string {
		let prefix = this.logPrefix;
		const domain = message.domain ?? this.domain ?? '';
		if (domain) {
			prefix += `[domain="${domain}"]`;
		}

		if (prefix !== '') {
			prefix += ' ';
		}

		if (this.logTimestamp) {
			prefix = `${new Date().toISOString()} - ${prefix}`;
		}

		return `${prefix}${message.message}`;
	}

	public debug(message: LogMessage): void {
	  console.debug(`${this.generateMessage(message)}`);
	}
	public info(message: LogMessage): void {
		console.info(`${this.generateMessage(message)}`);
	}
	public warning(message: LogMessage): void {
		console.warn(`${this.generateMessage(message)}`);
	}
	public error(message: LogMessage): void {
		console.error(`${this.generateMessage(message)}`);
	}
	public errorWithException(message: LogMessage, exception: Error): void {
		console.error(`${this.generateMessage(message)} "${exception.message}" - Error stack: ${exception.stack}`);
	}
}

export class DataDogLoggerAdapter implements LoggerInterface {
	public constructor(
		private readonly logPrefix = '',
		private readonly domain: string = '',
	) {}

	public debug(message: LogMessage): void {
	  Logger.log(`${this.logPrefix} ${message.message.replace(`/\\r?\\n|\\r/g`, '')}`, LogLevel.debug, message.domain ?? this.domain);
	}
	public info(message: LogMessage): void {
		Logger.log(`${this.logPrefix} ${message.message.replace(`/\\r?\\n|\\r/g`, '')}`, LogLevel.info, message.domain ?? this.domain);
	}
	public warning(message: LogMessage): void {
		Logger.log(`${this.logPrefix} ${message.message.replace(`/\\r?\\n|\\r/g`, '')}`, LogLevel.warning, message.domain ?? this.domain);
	}
	public error(message: LogMessage): void {
		Logger.log(`${this.logPrefix} ${message.message.replace(`/\\r?\\n|\\r/g`, '')}`, LogLevel.error, message.domain ?? this.domain);
	}
	public errorWithException(message: LogMessage, exception: Error): void {
		Logger.log(`${this.logPrefix} ${message.message} "${exception.message}" - Error stack: ${exception.stack}`, LogLevel.error, message.domain ?? this.domain);
	}
}

export class LogAggregator implements LoggerInterface {
	public constructor(
		private readonly loggers: readonly LoggerInterface[],
	) {}

	public debug(message: LogMessage): void {
	  this.loggers.forEach((logger) => logger.debug(message));
	}
	public info(message: LogMessage): void {
		this.loggers.forEach((logger) => logger.info(message));
	}
	public warning(message: LogMessage): void {
		this.loggers.forEach((logger) => logger.warning(message));
	}
	public error(message: LogMessage): void {
		this.loggers.forEach((logger) => logger.error(message));
	}
	public errorWithException(message: LogMessage, exception: Error): void {
		this.loggers.forEach((logger) => logger.errorWithException(message, exception));
	}
}

export class NullLogger implements LoggerInterface {
	public debug(message: LogMessage): void {
		return;
	}
	public info(message: LogMessage): void {
		return;
	}
	public warning(message: LogMessage): void {
		return;
	}
	public error(message: LogMessage): void {
		return;
	}
	public errorWithException(message: LogMessage, exception: Error): void {
		return;
	}
}
