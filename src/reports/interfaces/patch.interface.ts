import {
    EnrollmentTypes,
    TimeFrameOptions,
    VisibilityTypes
} from '../../models/custom-report';
import { CourseTypeFilter } from '../../models/base';

export type DateOptionsFilter = {
    any?: boolean;
    operator?: string;
    type?: string;
    from?: string;
    to?: string;
    days?: number;
};

export class PatchField {
    name: string;
    simpleFields: string[];
    infoFilterFields: string[];
    dateFilterFields: string[];

    public constructor(name: string, simpleFields?: string[], infoFilterFields?: string[], dateFilterFields?: string[]) {
        this.name = name;
        this.simpleFields = simpleFields ?? [];
        this.infoFilterFields = infoFilterFields ?? [];
        this.dateFilterFields = dateFilterFields ?? [];
    }
}

export enum Selectors {
    default = 'default',
    custom = 'custom'
}

export enum TypeOperator {
    relative = 'relative',
    absolute = 'absolute',
    range = 'range'
}

export enum Conditions {
    allConditions = 'allConditions',
    atLeastOneCondition = 'atLeastOneCondition'
}

export enum Operators {
    isBefore = 'isBefore',
    isAfter = 'isAfter',
    range = 'range',
    expiringIn = 'expiringIn',
    isEqual = 'isEqual',
}

export type InfoFilter = {
    id: number;
    descendants?: boolean;
}

export type InfoVisibility = {
    type?: VisibilityTypes;
    users?: InfoFilter[];
    groups?: InfoFilter[];
    branches?: InfoFilter[];
}

export type SortingOptions = {
    selector?: string;
    selectedField?: string;
    orderBy?: string;
};

export type Enrollment = {
    completed?: boolean;
    inProgress?: boolean;
    notStarted?: boolean;
    waitingList?: boolean;
    suspended?: boolean;
    enrollmentsToConfirm?: boolean,
    subscribed?: boolean,
    overbooking?: boolean,
    enrollmentTypes?: EnrollmentTypes,
};

export type PlanningOption = {
    recipients?: string[];
    every?: number;
    timeFrame?: TimeFrameOptions;
    scheduleFrom?: string;
    startHour?: string;
    timezone?: string;
};

export type Planning = {
    active: boolean;
    option?: PlanningOption;
};

export type InstructorsFilter = {
    all: boolean;
    instructors: InfoFilter[];
};

export type SessionDates = {
    startDate?: DateOptionsFilter;
    endDate?: DateOptionsFilter;
    conditions?: string;
};

export type SessionAttendanceType = {
    blended?: boolean;
    fullOnsite?: boolean;
    fullOnline?: boolean;
    flexible?: boolean;
};

export type UsersFilter = {
    all?: boolean;
    hideDeactivated?: boolean;
    showOnlyLearners?: boolean;
    hideExpiredUsers?: boolean;
    isUserAddFields?: boolean;
    users?: InfoFilter[];
    groups?: InfoFilter[];
    branches?: InfoFilter[];
}

export type CoursesFilter = {
    all?: boolean;
    courses?: InfoFilter[];
    categories?: InfoFilter[];
    instructors?: InfoFilter[];
    courseType?: CourseTypeFilter;
}

export type SurveysFilter = {
    all?: boolean;
    surveys?: InfoFilter[];
}

export type PlansFilter = {
    all?: boolean;
    learningPlans?: InfoFilter[];
}

export type BadgesFilter = {
    all?: boolean;
    badges?: InfoFilter[];
}

export type AssetsFilter = {
    all?: boolean;
    assets?: InfoFilter[];
    channels?: InfoFilter[];
}

export type SessionsFilter = {
    all?: boolean;
    sessions?: InfoFilter[];
}

export type CertificationsFilter = {
    all?: boolean;
    certifications?: InfoFilter[];
    activeCertifications?: boolean;
    expiredCertifications?: boolean;
    archivedCertifications?: boolean;
    certificationDate?: DateOptionsFilter;
    certificationExpirationDate?: DateOptionsFilter;
    conditions?: string;
}

export type ExternalTrainingStatusFilter = {
    approved?: boolean;
    waiting?: boolean;
    rejected?: boolean;
};

export type PublishStatusFilter = {
    published?: boolean;
    unpublished?: boolean;
};


export class ReportPatchInput {
    platform: string;
    loginRequired?: boolean;
    description?: string;
    title?: string;
    timezone?: string;
    fields?: string[];
    conditions?: string;
    userAdditionalFieldsFilter?: { [key: string]: number; };
    loTypes?: { [key: string]: boolean };
    visibility?: InfoVisibility;
    planning?: Planning;
    sortingOptions?: SortingOptions;
    users?: UsersFilter;
    courses?: CoursesFilter;
    surveys?: SurveysFilter;
    learningPlans?: PlansFilter;
    badges?: BadgesFilter;
    assets?: AssetsFilter;
    sessions?: SessionsFilter;
    instructors?: InstructorsFilter;
    certifications?: CertificationsFilter;
    enrollment?: Enrollment;
    sessionDates?: SessionDates;
    externalTrainingStatusFilter?: ExternalTrainingStatusFilter;
    publishStatus?: PublishStatusFilter;
    sessionAttendanceType?: SessionAttendanceType;
    enrollmentDate?: DateOptionsFilter;
    completionDate?: DateOptionsFilter;
    surveyCompletionDate?: DateOptionsFilter;
    archivingDate?: DateOptionsFilter;
    courseExpirationDate?: DateOptionsFilter;
    issueDate?: DateOptionsFilter;
    creationDateOpts?: DateOptionsFilter;
    expirationDateOpts?: DateOptionsFilter;
    publishedDate?: DateOptionsFilter;
    contributionDate?: DateOptionsFilter;
    externalTrainingDate?: DateOptionsFilter;
}
