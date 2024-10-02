import { BaseResponse } from './base';
import { ReportManagerUpdateResponse } from './report-manager';
import {
    BranchesSummary, BranchUserEnrollments,
    BranchesList,
    CoursesCharts, CoursesEnrollments, CourseSummary,
    PrivacyCharts, PrivacyUsersList,
    UserEnrollmentsByCourse
} from '../dashboards/interfaces/dashboard.interface';

export enum VisibilityTypes {
    ALL_GODADMINS = 1,
    ALL_GODADMINS_AND_PU = 2,
    ALL_GODADMINS_AND_SELECTED_PU = 3
}

export enum TimeFrameOptions {
    days = 'days',
    weeks = 'weeks',
    months = 'months',
}

export enum DateOptions {
    CONDITIONS = 'allConditions'
}

export type PlanningOption = {
    isPaused?: boolean;
    recipients: string[];
    every: number;
    timeFrame: TimeFrameOptions;
    scheduleFrom: string;
    hostname?: string;
    subfolder?: string;
    startHour: string; // formatted like this: "23:59"
    timezone: string;
};

export type Planning = {
    active: boolean;
    option?: PlanningOption;
};

export type DateOptionsValueDescriptor = {
    any: boolean;
    operator: string;
    type: string;
    from: string;
    to: string;
    days: number;
};

export type SortingOptions = {
    selector: string;
    selectedField: string;
    orderBy: string;
};

export type Enrollment = {
    completed: boolean;
    inProgress: boolean;
    notStarted: boolean;
    waitingList: boolean;
    suspended: boolean;
    enrollmentsToConfirm: boolean,
    subscribed: boolean,
    overbooking: boolean,
    enrollmentTypes?: EnrollmentTypes,
};

export type SessionDates = {
    startDate: DateOptionsValueDescriptor;
    endDate: DateOptionsValueDescriptor;
    conditions: string;
};

export type InstructorsFilter = {
    all: boolean;
    instructors: SelectionInfo[];
};

export class Fullname {
    firstname: string;
    lastname: string;
    userId: string;
    constructor() {
        this.firstname = '';
        this.lastname = '';
        this.userId = '';
    }
}

export class Report {
    idReport: number|string;
    name: string;
    description = '';
    standard: boolean;
    type: string;
    createdBy: string|number;
    createdByDescription: Fullname;
    creationDate: string;
    visibility: number;
    planning: Planning;
    constructor() {
        this.idReport = this.name = this.type = this.createdBy = this.creationDate = '';
        this.standard = true;
        this.visibility = VisibilityTypes.ALL_GODADMINS_AND_PU;
        this.planning = {
            active: false,
        };
        this.createdByDescription = new Fullname();
    }
}

export class ReportsResponse implements BaseResponse {
    success: boolean;
    data?: Report[];
    error?: string;
    constructor(success = true, data?: Report[], error?: string) {
        this.success = success;
        this.data = data;
        this.error = error;
    }
}

export class PrivacyResponse implements BaseResponse {
    success: boolean;
    data?: PrivacyUsersList;
    error?: string;
    constructor(success = true, data?: PrivacyUsersList, error?: string) {
        this.success = success;
        this.data = data;
        this.error = error;
    }
}

export class ExportCsvUrl {
    url: string;
}

export class DashboardExportCsvResponse implements BaseResponse {
    success: boolean;
    data?: ExportCsvUrl;
    error?: ErrorResponse;
    constructor(success = true, data?: ExportCsvUrl, error?: ErrorResponse) {
        this.success = success;
        this.data = data;
        this.error = error;
    }
}

export class ErrorResponse {
    code: number;
    message: string | string[];
    constructor(code: number, message: string | string[]) {
        this.code = code;
        this.message = code === 500 ? 'Generic error. See the logs for more information' : message;
    }
}

export class CoursesEnrollmentsResponse implements BaseResponse {
    success: boolean;
    data?: CoursesEnrollments;
    error?: ErrorResponse;
    constructor(success = true, data?: CoursesEnrollments, error?: ErrorResponse) {
        this.success = success;
        this.data = data;
        this.error = error;
    }
}

export class CourseSummaryResponse implements BaseResponse {
    success: boolean;
    data?: CourseSummary;
    error?: ErrorResponse;
    constructor(success = true, data?: CourseSummary, error?: ErrorResponse) {
        this.success = success;
        this.data = data;
        this.error = error;
    }
}

export class CourseChartsResponse implements BaseResponse {
    success: boolean;
    data?: CoursesCharts;
    error?: ErrorResponse;
    constructor(success = true, data?: CoursesCharts, error?: ErrorResponse) {
        this.success = success;
        this.data = data;
        this.error = error;
    }
}

export class BranchesSummaryResponse implements BaseResponse {
    success: boolean;
    data?: BranchesSummary;
    error?: ErrorResponse;
    constructor(success = true, data?: BranchesSummary, error?: ErrorResponse) {
        this.success = success;
        this.data = data;
        this.error = error;
    }
}

export class BranchEnrollmentsResponse implements BaseResponse {
    success: boolean;
    data?: BranchUserEnrollments;
    error?: ErrorResponse;
    constructor(success = true, data?: BranchUserEnrollments, error?: ErrorResponse) {
        this.success = success;
        this.data = data;
        this.error = error;
    }
}

export class BranchesListResponse implements BaseResponse {
    success: boolean;
    data?: BranchesList;
    error?: ErrorResponse;
    constructor(success = true, data?: BranchesList, error?: ErrorResponse) {
        this.success = success;
        this.data = data;
        this.error = error;
    }
}

export class UserEnrollmentsByCourseResponse implements BaseResponse {
    success: boolean;
    data?: UserEnrollmentsByCourse;
    error?: string;
    constructor(success = true, data?: UserEnrollmentsByCourse, error?: string) {
        this.success = success;
        this.data = data;
        this.error = error;
    }
}

export class PrivacyChartsResponse implements BaseResponse {
    success: boolean;
    data?: PrivacyCharts;
    error?: string;
    constructor(success = true, data?: PrivacyCharts, error?: string) {
        this.success = success;
        this.data = data;
        this.error = error;
    }
}

export class ReportCreationResponse implements BaseResponse {
    success: boolean;
    data?: ReportCreationData;
    error?: string;
    constructor(idReport: string|undefined) {
        this.success = true;
        if (idReport !== undefined) {
            this.data = new ReportCreationData(idReport);
        }
    }
}

export class ReportCreationData {
    idReport: string;
    constructor(idReport: string) {
        this.idReport = idReport;
    }
}

export class ReportUpdateResponse implements BaseResponse {
    success: boolean;
    data?: ReportManagerUpdateResponse;
    error?: string;
    constructor() {
        this.success = true;
    }
}

export class SelectionInfo {
    id: number;
    name: string;
    descendants?: boolean;

    public constructor() {
        this.id = 0;
        this.name = '';
    }
}

export type UserAdditionalFieldType = {
    id: number,
    title: string,
    sequence: number,
    options: UserAdditionalFieldOptions[],
};

export type UserAdditionalFieldOptions = {
    value: number,
    label: string,
};

export type ExternalTrainingStatusFilter = {
    approved: boolean;
    waiting: boolean;
    rejected: boolean;
};

export type UserAdditionalFieldsFilterParam = {
    key: number;
    value: number;
};

export enum TextFilterOptions {
    equals = 'equals',
    like = 'like',
    notEquals = 'notEquals',
    isEmpty = 'isEmpty'
}

export type PublishStatusFilter = {
    published: boolean;
    unpublished: boolean;
};

export type SessionAttendanceType = {
    blended: boolean; // blended
    fullOnsite: boolean; // onsite
    fullOnline: boolean; // online
    flexible: boolean; // flexible
};

export enum EnrollmentTypes {
    active = 1,
    archived = 2,
    activeAndArchived = 3
}

export type Properties = {
    name?: string;
    description?: string;
    login_required_download_report?: boolean;
    timezone?: string;
    visibility_rules?: string;
};
