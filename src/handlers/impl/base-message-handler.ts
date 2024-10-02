import { IncomingMessage, MessageHandler } from '../message-handler';
import { LoggerInterface } from '../../services/logger/logger-interface';
import {
    ReportExtractionApplicationServiceFactory
} from '../../domain/factories/ReportExtractionApplicationServiceFactory';
export enum EventName {
    NewExtraction = 'NewExtraction',
    Initialized = 'Initialized',
    CheckQueryStatus = 'CheckQueryStatus',
    QueryCompleted = 'QueryCompleted',
    QueryFailed = 'QueryFailed',
    CheckExportStatus = 'CheckExportStatus',
    ExportCompleted = 'ExportCompleted',
    ExportFailed = 'ExportFailed',
    ExportContentChecked = 'ExportContentChecked',
    ReportConverted = 'ReportConverted',
    ReportConversionSkipped = 'ReportConversionSkipped',
}

export abstract class BaseMessageHandler implements MessageHandler {
    protected constructor(
        protected readonly applicationServiceFactory: ReportExtractionApplicationServiceFactory,
        protected readonly logger: LoggerInterface,
    ) {
    }

    public abstract handlerName(): string;

    public async handleMessage(message: IncomingMessage): Promise<void> {
        try {
            this.checkIsValidIncomingMessage(message);
            this.checkMessageValidity(message);
        } catch (error: any) {
            this.logger.errorWithException(
                {
                    message: `[${this.handlerName()}] Error checking incoming message validity`,
                    domain: message.domain,
                }, error
            );
            throw error;
        }
        return this.doHandleMessage(message);
    }

    private checkIsValidIncomingMessage(message: unknown): void {
        if (typeof message !== 'object' || !message) {
            throw new Error(`Message is not an object`);
        }

        const obj = message as Object;

        if (!('name' in obj)) {
            throw new Error(`Message does not contain the 'name' property`);
        }

        if (typeof obj.name !== 'string') {
            throw new Error(`Message name is expected to be a string, but it is ${typeof obj.name}`);
        }

        if (!('payload' in obj)) {
            return;
        }

        const payload = obj.payload;

        if (typeof payload !== 'object') {
            throw new Error(`Message payload is expected to be an object, but it is ${typeof payload}`);
        } else if (!payload) {
            throw new Error(`Message payload cannot be null`);
        }
    }

    /**
     * Any implementation of this methods will receive a message that has already been verified to contain
     * the two "name" and "payload" properties. This method implementation must ensure that the payload contains the
     * expected types/values for the message to handle.
     * Any exception thrown by this method will prevent the message from being handled.
     *
     * @param message The message to be validated
     * @throws Error Any error, with an explanatory message
     * @protected
     */
    protected abstract checkMessageValidity(message: IncomingMessage): void;

    /**
     * Effectively handles the message
     *
     * @param message A message to handle. The message has already been validated.
     * @throws Error Any error, with an explanatory message
     * @protected
     */
    protected abstract doHandleMessage(message: IncomingMessage): Promise<void>;
}
