export enum ErrorCode {
    MISSING_TOKEN = 1,
    HYDRA_AUTH_ERROR = 2,
    HYDRA_SERVER_ERROR = 3,
    HYDRA_NOT_FOUND = 4,
    MISSING_URL = 5,
    HYDRA_BAD_REQUEST = 6,
    HYDRA_UNEXPECTED_ERROR = 7,
    DISABLED_REPORT_TYPE = 8,
    REPORT_NOT_FOUND = 9,
    SESSION_DATA_NOT_FOUND = 10,
    SESSION_DATA_PLATFORM_NOT_FOUND = 11,
    SESSION_DATA_PLATFORM_BASEURL_FOUND_OR_INVALID = 12,
    SESSION_DATA_PLATFORM_PLUGINS_NOT_FOUND_OR_INVALID = 13,
    SESSION_DATA_PLATFORM_CONFIGS_NOT_FOUND_OR_INVALID = 14,
    QUERY_BUILDER_RELATED_REPORT = 15,
    WRONG_JSON = 16,
    MORE_FILTER_IN_JSON = 17,
    FILTER_NOT_FOUND_IN_JSON = 18,
    MISSING_FIELD_IN_JSON_FILTER = 19,
    MISSING_TYPE_IN_JSON_FILTER = 20,
    WRONG_TYPE_IN_JSON_FILTER = 21,
    JSON_AREA_EMPTY = 22,
    JSON_AREA_FILLED = 23,
    WRONG_SQL = 24,
    MISSING_DESCRIPTION_IN_JSON_FILTER = 25,
    NO_MEMBER_IN_TEAM = 26,
    ENROLLMENT_STATUS_NOT_VALID = 27,
    ENROLLMENT_DATE_NOT_VALID = 28,
    REPORT_TYPE_NOT_VALID = 29,
    USER_ADD_FIELD_FORMAT_NOT_VALID = 30,
    USER_ADD_FIELD_TYPE_NOT_VALID = 31,
    USER_ADD_FIELD_NOT_AVAILABLE = 32,
    MISSING_NAME_FIELD = 33,
}

export const JSON_AREA_ERROR = [
    ErrorCode.WRONG_JSON,
    ErrorCode.MORE_FILTER_IN_JSON,
    ErrorCode.FILTER_NOT_FOUND_IN_JSON,
    ErrorCode.MISSING_FIELD_IN_JSON_FILTER,
    ErrorCode.MISSING_TYPE_IN_JSON_FILTER,
    ErrorCode.WRONG_TYPE_IN_JSON_FILTER,
    ErrorCode.JSON_AREA_EMPTY,
    ErrorCode.JSON_AREA_FILLED,
    ErrorCode.MISSING_DESCRIPTION_IN_JSON_FILTER,
];
