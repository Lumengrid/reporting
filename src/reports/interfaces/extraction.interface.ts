import { Planning } from '../../models/custom-report';
import { DataLakeRefreshStatus } from '../../models/base';

export interface ExtractionModel {
    idReport: string;
    platform: string;
    planning: Planning;
    author: number;
}

export interface ExtractionMapper {
    [key: string]: UsersExtractionsMapper;
}

export interface UsersExtractionsMapper {
    [key: number]: string[];
}

export interface UsersReport {
    userId: number;
    reports: string[];
}
export interface ExportsPayload {
    exports: UsersReport[];
}

export interface DataLakeRefreshItem {
    platform: string;
    refreshOnDemandLastDateUpdate?: string;
    refreshTimezoneLastDateUpdate?: string;
    refreshTimezoneLastDateUpdateV2?: string;
    lastRefreshStartDateV3?: string;
    refreshOnDemandStatus?: DataLakeRefreshStatus;
    refreshTimeZoneStatus?: DataLakeRefreshStatus;
    token?: number;
    lastRefreshStartDate?: string;
    errorCount?: number;
    stepFunctionExecutionId?: string;
}
export interface LastRefreshDate {
    refreshDate: string;
    refreshStatus?: DataLakeRefreshStatus;
    isRefreshNeeded?: boolean;
    errorCount?: number;
    lastRefreshStartDate?: string;
}

// Sidekiq Scheduler
export enum RedisSidekiqSchedulerKey {
    QUEUE_SCHEDULER = 'queue:scheduler'
}
export enum SidekiqSchedulerWorkerClass {
    ADD_NEW_TASK = 'AddNewTask',
    REMOVE_TASK = 'RemoveTask'
}
export interface SidekiqSchedulerItem {
    class: SidekiqSchedulerWorkerClass;
    args: string[];
    retry: boolean;
    enqueued_at: number;
}
