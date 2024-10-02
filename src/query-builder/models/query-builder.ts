import { BaseResponse } from '../../models/base';

export class CustomReportTypesCreationData {
    idCustomReportTypes: string;
    constructor(idCustomReportTypes: string) {
        this.idCustomReportTypes = idCustomReportTypes;
    }
}

export class CustomReportTypesCreationResponse implements BaseResponse {
    success: boolean;
    data?: CustomReportTypesCreationData;
    error?: string;
    constructor(idCustomReportTypes: string|undefined) {
        this.success = true;
        if (idCustomReportTypes !== undefined) {
            this.data = new CustomReportTypesCreationData(idCustomReportTypes);
        }
    }
}

export class CustomReportTypesResponse implements BaseResponse {
    success: boolean;
    data?: any;
    error?: string;
    errorMessageSql?: string;
    errorMessageJson?: string;
    errorCode?: number;
    constructor() {
        this.success = true;
    }
}

export const QUERY_BUILDER_FILTER_TYPE_USERS = 'users';
export const QUERY_BUILDER_FILTER_TYPE_COURSES = 'courses';
export const QUERY_BUILDER_FILTER_TYPE_BRANCHES = 'branches';
export const QUERY_BUILDER_FILTER_TYPE_DATE = 'date';
export const QUERY_BUILDER_FILTER_TYPE_TEXT = 'text';

export const FILTER_TYPE_ACCEPTED = [
    QUERY_BUILDER_FILTER_TYPE_COURSES,
    QUERY_BUILDER_FILTER_TYPE_USERS,
    QUERY_BUILDER_FILTER_TYPE_BRANCHES,
    QUERY_BUILDER_FILTER_TYPE_DATE,
    QUERY_BUILDER_FILTER_TYPE_TEXT,
];
