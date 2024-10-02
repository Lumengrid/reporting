import { InvalidExtractionIdException } from '../exceptions/InvalidExtractionIdException';

export class ExtractionId {
    public constructor(private readonly id: string, private readonly reportId: string) {
        this.checkIsValidId(id);
        this.checkIsValidReportId(reportId);
    }

    private isValidId(id: unknown): boolean {
        return typeof id === 'string' && id.match(/^[a-z0-9]{8}(?:-[a-z0-9]{4}){3}-[a-z0-9]{12}$/) !== null;
    }

    private checkIsValidId(id: unknown): void {
        if (!this.isValidId(id)) {
            throw new InvalidExtractionIdException(`Id is not a valid UUID`);
        }
    }

    private checkIsValidReportId(reportId: unknown): void {
        if (!this.isValidId(reportId)) {
            throw new InvalidExtractionIdException(`ReportId is not a valid UUID`);
        }
    }

    public toString(): string {
        return `[Id="${this.id}", ReportId="${this.reportId}"]`;
    }

    public get Id(): string {
        return this.id;
    }

    public get ReportId(): string {
        return this.reportId;
    }
}
