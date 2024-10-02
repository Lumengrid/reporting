import { InvalidScheduledReportIdException } from '../exceptions/InvalidScheduledReportIdException';

export class ScheduledReportId {
    public constructor(
        private readonly reportId: string,
        private readonly platform: string,
    ) {
        this.checkIsValidReportId(reportId);
        this.checkIsValidPlatform(platform);
    }

    private checkIsValidReportId(id: unknown): void {
        if (typeof id !== 'string' || !id.match(/^[a-z0-9]{8}(?:-[a-z0-9]{4}){3}-[a-z0-9]{12}$/)) {
            throw new InvalidScheduledReportIdException(`Report id is not a valid UUID`);
        }
    }

    private checkIsValidPlatform(platform: unknown): void {
        if (typeof platform !== 'string' || !platform.length) {
            throw new InvalidScheduledReportIdException(`Platform is not a valid string`);
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
