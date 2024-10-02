import { ConsoleLogger, DataDogLoggerAdapter, LogAggregator, LoggerInterface, NullLogger } from './logger-interface';

class LoggerFactory {
	private nullLogger = new NullLogger();

	private isSyslogLoggerDisabled(): boolean {
		return process.env.USE_SYSLOG === 'false';
	}

	private logTimestamp(): boolean {
		return process.env.LOG_TIMESTAMP === 'true';
	}

	public buildLogger(logPrefix = '', domain = ''): LoggerInterface {
		const loggers: LoggerInterface[] = [
			new ConsoleLogger(
				logPrefix,
				this.logTimestamp(),
				domain,
			),
		];

		if (!this.isSyslogLoggerDisabled()) {
			loggers.push(new DataDogLoggerAdapter(logPrefix, domain));
		}

		return new LogAggregator(loggers);
	}

	public getNullLogger(): LoggerInterface {
		return this.nullLogger;
	}
}

export const loggerFactory = new LoggerFactory();
