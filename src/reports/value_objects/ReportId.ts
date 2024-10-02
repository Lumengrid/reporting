import { InvalidReportIdException } from '../exceptions/InvalidReportIdException';

export class ReportId {
    public constructor(
        private readonly reportId: string,
        private readonly platform: string,
    ) {
        this.checkIsValidReportId(reportId);
        this.checkIsValidPlatform(platform);
    }

    private checkIsValidReportId(id: unknown): void {
        if (typeof id !== 'string' || !id.match(/^[a-z0-9]{8}(?:-[a-z0-9]{4}){3}-[a-z0-9]{12}$/)) {
            throw new InvalidReportIdException(`Report id is not a valid UUID`, 1000);
        }
    }

    private checkIsValidPlatform(platform: unknown): void {
        if (typeof platform !== 'string' || !platform.length) {
            throw new InvalidReportIdException(`Platform is not a valid string`, 1001);
        }
    }

    public get ReportId(): string {
        return this.reportId;
    }

    public get Platform(): string {
        return this.platform;
    }

    public toString(): string {
        return `[ReportId="${this.ReportId}", Platform="${this.Platform}"]`;
    }
}
