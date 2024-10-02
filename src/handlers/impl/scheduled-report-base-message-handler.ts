import { BaseMessageHandler } from './base-message-handler';
import { IncomingMessage } from '../message-handler';
import { LoggerInterface } from '../../services/logger/logger-interface';
import { ExtractionNotFoundException } from '../../domain/exceptions/ExtractionNotFoundException';
import { InvalidExtractionStatusException } from '../../domain/exceptions/InvalidExtractionStatusException';
import {
    ReportExtractionApplicationServiceFactory
} from '../../domain/factories/ReportExtractionApplicationServiceFactory';

export abstract class ScheduledReportBaseMessageHandler extends BaseMessageHandler {
    private extractionId = '';
    private reportId = '';

    public constructor(
        applicationServiceFactory: ReportExtractionApplicationServiceFactory,
        logger: LoggerInterface,
    ) {
        super(applicationServiceFactory, logger);
    }

    protected doLogInfo(message: string): void {
        this.logger.info({message: `[${this.constructor.name}] ${message}`});
    }

    protected doLogDebug(message: string): void {
        this.logger.debug({message: `[${this.constructor.name}] ${message}`});
    }

    protected doLogerrorWithStack(message: string, error: Error): void {
        this.logger.error({message: `[${this.constructor.name}] ${message} ${error.stack}`});
    }

    protected checkMessageValidity(message: IncomingMessage) {
        if (!('extraction_id' in message.payload)) {
            throw new Error(`Message does not contain the expected "extraction_id" property`);
        }

        if (typeof message.payload.extraction_id !== 'string') {
            throw new Error(`Message property "extraction_id" is expected to be a string, but it is ${typeof message.payload.extraction_id}`);
        }

        if (!('report_id' in message.payload)) {
            throw new Error(`Message does not contain the expected "report_id" property`);
        }

        if (typeof message.payload.report_id !== 'string') {
            throw new Error(`Message property "report_id" is expected to be a string, but it is ${typeof message.payload.report_id}`);
        }

        this.extractionId = message.payload.extraction_id;
        this.reportId = message.payload.report_id;
    }

    protected get ExtractionId(): string {
        return this.extractionId;
    }

    protected get ReportId(): string {
        return this.reportId;
    }

    public handlerName(): string {
        return this.constructor.name;
    }

    public async handleMessage(message: IncomingMessage): Promise<void> {
        try {
            return await super.handleMessage(message);
        } catch (error: any) {
            if (error instanceof ExtractionNotFoundException || error instanceof InvalidExtractionStatusException) {
                this.logger.debug({message: `${error.message}, doing nothing.`});
            } else {
                throw error;
            }
        }
    }
}
