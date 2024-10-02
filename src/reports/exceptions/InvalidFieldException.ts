import { ReportException } from './ReportException';

export class InvalidFieldException extends ReportException {
    constructor(field: string) {
        super(`Invalid field "${field}"`, 1004);
    }
}
