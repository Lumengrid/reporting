// Base interface for all the fields in Dynamo DB Table
import {
    CoursesEntitiesLimits,
    InformationReport, LearningPlans,
    SelectionInfo,
    UsersEntitiesLimits
} from '../../models/report-manager';
import { DateOptionsValueDescriptor } from '../../models/custom-report';

export interface DynamoCustomReportType {
    id: string;
    name: string;
    platform: string;
    description: string;
    creationDate: string;
    authorId: string|number;
    status: number;
    lastEditBy: number;
    lastEditByDate: string;
    sql?: string;
    json?: string;
    deleted?: boolean;
}

export interface CustomReportType extends DynamoCustomReportType {
    createdBy: AuthorName;
}

export interface AuthorName {
    firstname: string;
    lastname: string;
    username: string;
}

export interface CustomReportTypesResponse {
    success: boolean;
    data?: CustomReportType[];
    error?: string;
}

export interface CustomReportTypeDetail extends DynamoCustomReportType {
    lastEditByDetails: LastEditByDetails;
    relatedReports?: InformationReport[];
}
export interface LastEditByDetails {
    firstname: string;
    lastname: string;
    username: string;
    avatar: string;
}

export interface CustomReportTypeDetailsResponse {
    success: boolean;
    data?: CustomReportTypeDetail;
    error?: string;
}

export interface JsonFilter {
    field: string;
    type: string;
    description?: string;
    caseInsensitive?: string|number|boolean;
}

export class JsonUserFilter {
    type: string;
    all: boolean;
    hideDeactivated: boolean;
    showOnlyLearners: boolean;
    hideExpiredUsers: boolean;
    isUserAddFields: boolean;
    users: SelectionInfo[];
    groups: SelectionInfo[];
    branches: SelectionInfo[];
    entitiesLimits: UsersEntitiesLimits;
    userAdditionalFieldsFilter?: { [key: number]: number; };

    public constructor() {
        this.all = true;
        this.hideDeactivated = true;
        this.showOnlyLearners = false;
        this.hideExpiredUsers = true;
        this.isUserAddFields = false;
        this.users = this.groups = this.branches = [];
        this.entitiesLimits = {} as UsersEntitiesLimits;
        this.userAdditionalFieldsFilter = {};
        this.type = 'users';
    }
}


export class JsonCourseFilter {
    all: boolean;
    courses: SelectionInfo[];
    categories: SelectionInfo[];
    entitiesLimits: CoursesEntitiesLimits;
    type: string;
    learningPlans: LearningPlans[];
    courseExpirationDate: DateOptionsValueDescriptor;

    public constructor() {
        this.type = 'courses';
        this.all = true;
        this.courses = this.categories = this.learningPlans = [];
        this.entitiesLimits = {} as CoursesEntitiesLimits;
        this.courseExpirationDate = {
            any: true,
            days: 1,
            type: '',
            operator: '',
            to: '',
            from: ''
        };
    }
}

export class JsonDateFilter {
    type: string;
    filterConfiguration: DateOptionsValueDescriptor;
    description: string;

    public constructor(description) {
        this.type = 'date';
        this.description = description;
        this.filterConfiguration = {
            any: true,
            days: 1,
            type: '',
            operator: '',
            to: '',
            from: ''
        };
    }
}

export class JsonTextFilter {
    type: string;
    description: string;
    operator: string;
    value: string;
    any: boolean;

    public constructor(description) {
        this.type = 'text';
        this.description = description;
        this.operator = '';
        this.value = '';
        this.any = true;
    }
}

