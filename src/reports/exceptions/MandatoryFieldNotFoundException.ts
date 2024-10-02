import { ReportException } from './ReportException';

export class MandatoryFieldNotFoundException extends ReportException {
    constructor(field: string) {
        super(`Mandatory field "${field}" not found`, 1003);
    }
}
