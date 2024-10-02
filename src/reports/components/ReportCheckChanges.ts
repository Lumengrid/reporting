import { LoggerInterface } from '../../services/logger/logger-interface';
import { ReportManagerInfo } from '../../models/report-manager';
import { CheckReportUpdates } from '../../models/check-report-updates';
import Hydra from '../../services/hydra';
import { Planning, PlanningOption } from '../../models/custom-report';
import { Utils } from '../utils';
import { SidekiqScheduler } from '../../models/sidekiq-scheduler';

export class ReportCheckChanges {
    public constructor(
        private readonly logger: LoggerInterface,
        private readonly hydra: Hydra,
    ) {
    }

    /**
     * Create a Job item and store it in Redis for the Sidekiq Scheduling
     * @param {string, required} idReport the report id
     * @param {string, required} platformBaseUrl The platform base url
     * @param {Planning, required} cachedPlanning The planning object stored in dynamo before the report update
     * @param {Planning, required} paramPlanning The planning object passed as payload when update the report
     */
    private async manageSidekiqScheduling(idReport: string, platformBaseUrl: string, cachedPlanning: Planning, paramPlanning: Planning): Promise<void> {
        // Check if the values in the Schedule section are changed, if so go on and save in Redis the job for the Sidekiq Scheduler
        const isPlanningChanged = ReportCheckChanges.isPlanningChanged(cachedPlanning, paramPlanning);
        if (!isPlanningChanged) {
            return;
        }
        // If planning is active create "AddNewTask" Job for Sidekiq Scheduling, else remove scheduling by creating a "RemoveTask" Job
        const sidekiqScheduler = new SidekiqScheduler(platformBaseUrl);
        if (paramPlanning.active) {
            await sidekiqScheduler.activateScheduling(idReport, paramPlanning.option);
        } else {
            await sidekiqScheduler.removeScheduling(idReport);
        }
    }

    public async checkForSidekiqScheduling(isDatalakeV2Active: boolean,
                                           platformBaseUrl: string,
                                           cachedPlanning: Planning,
                                           idReport: string,
                                           planning: Planning): Promise<void> {
        if (!isDatalakeV2Active) {
            return;
        }
        await this.manageSidekiqScheduling(idReport, platformBaseUrl, cachedPlanning, planning);
    }

    public async checkForUpdateReportEvent(infoBefore: ReportManagerInfo, infoAfter: ReportManagerInfo): Promise<void> {
        const check = new CheckReportUpdates();
        const propertiesChanged = check.propertiesChanged(infoBefore, infoAfter);
        const filtersChanged = check.filtersChanged(infoBefore, infoAfter);
        const viewOptionsChanged = check.viewOptionsChanged(infoBefore, infoAfter);
        if (!((propertiesChanged && Object.keys(propertiesChanged).length > 0) || filtersChanged || viewOptionsChanged)) {
            return;
        }
        this.logger.debug({ message: `Report update changed for reportId: ${infoBefore.idReport} report title: ${infoBefore.title}`});
        let changes = {};
        if (propertiesChanged && Object.keys(propertiesChanged).length > 0) {
            changes = { properties: propertiesChanged };
        }
        if (filtersChanged) {
            changes = { ...changes, filters: true };
        }
        if (viewOptionsChanged) {
            changes = { ...changes, view_options: true };
        }
        const payload = {
            entity_id: infoBefore.idReport,
            entity_name: infoAfter.title ?? '',
            entity_attributes: {
                type: infoAfter.type ?? '',
                description: infoAfter.description ?? '',
                changes,
            },
            event_name: 'update-custom-report',
        };
        await this.hydra.generateEventOnEventBus(payload);
    }

    public async checkForSchedulingChangeEvent(planningInfoBefore: Planning, infoAfter: ReportManagerInfo): Promise<void> {
        if (!ReportCheckChanges.isPlanningChanged(planningInfoBefore, infoAfter.planning)) {
            this.logger.debug({ message: `Scheduling Not changed for reportId: ${infoAfter.idReport} report title: ${infoAfter.title} - planning data: ${JSON.stringify(planningInfoBefore)}`});
            return;
        }
        this.logger.debug({ message: `Scheduling changed for reportId: ${infoAfter.idReport} report title: ${infoAfter.title} - before: ${JSON.stringify(planningInfoBefore)} after: ${JSON.stringify(infoAfter.planning)}`});

        const payload = {
            entity_id: infoAfter.idReport,
            entity_name: infoAfter.title,
            entity_attributes: {
                type: infoAfter.type,
                description: infoAfter.description,
                source: 'new_reports',
                active: infoAfter.planning.active,
                recipients: infoAfter.planning.option.recipients,
                startHour: infoAfter.planning.option.startHour,
                timezone: infoAfter.planning.option.timezone,
                every: infoAfter.planning.option.every,
                timeFrame: infoAfter.planning.option.timeFrame,
                scheduleFrom: infoAfter.planning.option.scheduleFrom
            } ,
            event_name: 'custom-report-schedule-changed',
        };
        await this.hydra.generateEventOnEventBus(payload);
    }

    public static isPlanningChanged(cachedPlanning: Planning, paramPlanning: Planning): boolean {
        if (!cachedPlanning.active && !paramPlanning.active) {
            return false;
        }
        // Object keys are ordered for the comparison
        cachedPlanning = {
            ...cachedPlanning,
            option: {
                ...Utils.orderObjectKeys(cachedPlanning.option) as PlanningOption,
                // tslint:disable-next-line: no-null-keyword
                subfolder: null
            }
        };
        paramPlanning = {
            ...paramPlanning,
            option: {
                ...Utils.orderObjectKeys(paramPlanning.option) as PlanningOption,
                // tslint:disable-next-line: no-null-keyword
                subfolder: null
            }
        };

        return JSON.stringify(cachedPlanning) !== JSON.stringify(paramPlanning);
    }
}
