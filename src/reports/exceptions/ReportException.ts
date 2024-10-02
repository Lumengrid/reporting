export class ReportException extends Error {
    constructor(message: string, protected readonly code?: number) {
        super(message);
    }

    public getCode(): number | undefined {
        return this.code;
    }
}
