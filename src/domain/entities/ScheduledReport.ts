import { ScheduledReportId } from '../value_objects/ScheduledReportId';
import { Extraction } from './Extraction';
import { ReportCannotBeGeneratedException } from '../exceptions/ReportCannotBeGeneratedException';
import { ExtractionId } from '../value_objects/ExtractionId';
import { ReportNotScheduledForTodayException } from '../exceptions/ReportNotScheduledForTodayException';
import { TimeFrameOptions } from '../../models/custom-report';
import { v4 as uuid } from 'uuid';
import moment from 'moment-timezone';
import 'moment-recur-ts';

export interface ScheduledReportDetails {
    readonly author: number;
    readonly title: string;
    readonly platform: string;
    readonly planning: {
        readonly active: boolean;
        readonly option: {
            readonly scheduleFrom: string | null;
            readonly timeFrame: string;
            readonly hostname: string;
            readonly subfolder: string | null;
            readonly recipients: string[] | null;
            readonly timezone: string;

            readonly [key: string]: any;
        };

        readonly [key: string]: any;
    };

    readonly [key: string]: any;
}

export class ScheduledReport {
    public constructor(
        private readonly id: ScheduledReportId,
        private readonly details: ScheduledReportDetails
    ) {
    }

    private canBeExtractedToday(): boolean {
        const planningOptions = this.details.planning.option;

        const now = moment.tz(planningOptions.timezone);
        const scheduleFrom = moment.tz(planningOptions.scheduleFrom, planningOptions.timezone);
        const schedulingStartDate = scheduleFrom.startOf('d');
        if (now.isSame(schedulingStartDate, 'd')) {
            return true;
        }

        switch (planningOptions.timeFrame) {
            case TimeFrameOptions.days:
                return schedulingStartDate.recur().every(planningOptions.every).days().matches(now);

            case TimeFrameOptions.weeks:
                return schedulingStartDate.recur().every(planningOptions.every).week().matches(now);

            case TimeFrameOptions.months:
                return schedulingStartDate.recur().every(planningOptions.every).month().matches(now);

            default:
                return false;
        }
    }

    private checkCanBeExtracted(): void {
        if (this.details.planning.active !== true) {
            throw new ReportCannotBeGeneratedException(`Report cannot be generated, report scheduling is not enabled`);
        }

        if (this.details.planning.option.recipients?.length === 0) {
            throw new ReportCannotBeGeneratedException(`Report cannot be generated, report recipients not set`);
        }

        if (this.details.planning.every <= 0) {
            throw new ReportCannotBeGeneratedException(`Report cannot be generated, missing periodicity`);
        }

        if (!this.canBeExtractedToday()) {
            throw new ReportNotScheduledForTodayException(`Report not scheduled for today`);
        }
    }

    /**
     * Starts the generation of a scheduled report, returning a new instance of Report class.
     * Throws an error if the requested report cannot be generated.
     *
     * @throws ReportCannotBeGeneratedException if the report cannot (and must not in future) asked to be extracted
     * @throws ReportNotScheduledForTodayException if the report can be generated, but today is not the right day
     *
     */
    public startExtraction(): Extraction {
        this.checkCanBeExtracted();

        return Extraction.createNew(
            new ExtractionId(uuid(), this.id.ReportId),
            this.details,
        );
    }
}
