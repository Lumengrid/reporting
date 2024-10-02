import { ReportException } from './ReportException';

export class FieldNotEditableException extends ReportException {
    constructor(field: string) {
        super(`Field "${field}" not editable`, 1005);
    }
}
