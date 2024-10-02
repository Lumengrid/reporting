export interface BaseResponse {
    success: boolean;
    data?: any;
    error?: any;
    errorCode?: ErrorsCode;
}

export interface ReportsSettings {
    platform: string;
    csvExportLimit?: number;
    xlxExportLimit?: number;
    previewExportLimit?: number;
    entityUsersLimit?: number;
    entityGroupsLimit?: number;
    entityBranchesLimit?: number;
    entityCoursesLimit?: number;
    entityLPLimit?: number;
    entityCourseInstructorsLimit?: number;
    entityClassroomLimit?: number;
    entityWebinarLimit?: number;
    entitySessionsLimit?: number;
    monthlyRefreshTokens?: number;
    dailyRefreshTokens?: number;
    toggleDatalakeV2?: boolean;
    toggleDatalakeV3?: boolean;
    errorCount?: number;
    datalakeV2ExpirationTime?: number;
    extractionTimeLimit?: number;
    snowflakeTimeout?: number;
    toggleHydraMinimalVersion?: boolean;
}

export class ReportsSettingsResponse implements BaseResponse {
    success: boolean;
    data?: ReportsSettings;
    error?: string;
    errorCode?: number;
    constructor() {
        this.success = true;
    }
}

export enum EnrollmentStatuses {
    WaitingList = -2,
    Confirmed = -1,
    Subscribed = 0,
    InProgress = 1,
    Completed = 2,
    Suspend = 3,
    Overbooking = 4
}

export enum UserLevelsGroups {
    User = '/framework/level/user',
    PowerUser = '/framework/level/admin',
    GodAdmin = '/framework/level/godadmin'
}

export enum CourseStatuses {
    Preparation = 0,
    Active = 2
}

export enum CourseuserLevels {
    Student = 3,
    Tutor = 4,
    Teacher = 6
}

export enum CourseTypes {
    Elearning = 'elearning',
    Classroom = 'classroom',
    Webinar = 'webinar'
}

export enum CourseTypeFilter {
    ALL = 0,
    E_LEARNING = 1,
    ILT = 2
}

export enum AdditionalFieldsTypes {
    CodiceFiscale = 'codicefiscale',
    Country = 'country',
    Date = 'date',
    Dropdown = 'dropdown',
    FreeText = 'freetext',
    GMail = 'gmail',
    ICQ = 'icq',
    MSN = 'msn',
    Skype = 'skype',
    Textfield = 'textfield',
    Text = 'text',
    Textarea = 'textarea',
    Upload = 'upload',
    Yahoo = 'yahoo',
    YesNo = 'yesno'
}

export enum LOTypes {
    AICC = 'aicc',
    ELUCIDAT = 'elucidat',
    GOOGLEDRIVE = 'googledrive',
    LTI = 'lti',
    AUTHORING = 'authoring',
    DELIVERABLE = 'deliverable',
    FILE = 'file',
    HTMLPAGE = 'htmlpage',
    POLL = 'poll',
    SCORM = 'scormorg',
    TEST = 'test',
    TINCAN = 'tincan',
    VIDEO = 'video'
}

export enum LOStatus {
    AB_INITIO = 'ab-initio',
    ATTEMPTED = 'attempted',
    COMPLETED = 'completed',
    FAILED = 'failed',
    PASSED = 'passed'
}

export enum LOQuestTypes {
    TITLE = 'title',
    BREAK_PAGE = 'break_page',
    CHOICE = 'choice',
    CHOICE_MULTIPLE = 'choice_multiple',
    INLINE_CHOICE = 'inline_choice',
    EXTENDED_TEXT = 'extended_text',
    LIKERT_SCALE = 'likert_scale'
}

export enum ScoresTypes {
    FINAL_SCORE_TYPE_KEY_OBJECT = 'single_LO_as_final',
    INITIAL_SCORE_TYPE_KEY_OBJECT = 'single_LO_as_initial'
}

export enum ExternalTrainingStatus {
    APPROVED = 'approved',
    REJECTED = 'rejected',
    WAITING = 'waiting_approval'
}

export enum EcommItemTypes {
    COURSE = 'course',
    COURSEPATH = 'coursepath',
    COURSESEATS = 'courseseats',
    SUBSCRIPTION_PLAN = 'subscription_plan'
}

export enum SessionEvaluationStatus {
    PASSED = 1,
    FAILED = -1
}

export enum HydraEvents {
    ROG_ERROR = 'ROGError'
}

export enum AssignmentTypes {
    Mandatory = 1,
    Required = 2,
    Recommended = 3,
    Optional = 4
}

export class GeneralErrorResponse {
    success: boolean;
    data?: any;
    error: string;
    errorCode: number;
    public constructor(error: string, errorCode: number) {
        this.success = false;
        this.error = error;
        this.errorCode = errorCode;
    }
}

export enum ErrorsCode {
    ExtractionAlreadyInExecution = 1000,
    WrongParameter = 1001,
    ReportNotExist = 1002,
    DatabaseError = 1003,
    ExtractionNotExist = 1004,
    ExtractionNotComplete = 1005,
    DataLakeRefreshInProgress = 1006,
    ExtractionExpired = 1007,
    UnexpectedError = 1100,
    ExtractionFailed = 1008,
    QueryExecutionIdNotFound = 1009,
    CompleteTimeNotFound = 1010,
    NotDataFresher = 1011,
    XLSXConversionInProgress = 1012,
    ConnectionErrorDataSource = 1013,
    AuthenticationErrorDataSource = 1014,
    PoolNotInstantiated = 1015,
    DatabaseSchemaError = 1016,
}

export enum DataLakeRefreshStatus {
    RefreshInProgress = 'InProgress',
    RefreshError = 'Error',
    RefreshSucceeded = 'Succeeded',
}

export enum AttendancesTypes {
    BLENDED = 'blended',
    FULLONSITE = 'onsite',
    FULLONLINE = 'online',
    FLEXIBLE = 'flexible'
}

export const DEFAULT_TOKEN_INITIAL_VALUE = 5;

export class QueryBuilderAdminsResponse implements BaseResponse {
    success: boolean;
    data?: string[];
    error?: string;
    errorCode?: number;
}

export const joinedTables = Object.freeze({
    APP7020_ANSWER_AGGREGATE: 'app7020AnswerAggregate',
    APP7020_ANSWER_DISLIKE_AGGREGATE: 'app7020AnswerDislike',
    APP7020_ANSWER_LIKE_AGGREGATE: 'app7020AnswerLike',
    APP7020_BEST_ANSWER_AGGREGATE: 'app7020BestAnswer',
    APP7020_CHANNEL_ASSETS: 'app7020ChannelAssets',
    APP7020_CHANNEL_TRANSLATION: 'app7020ChannelTranslation',
    APP7020_CONTENT_HISTORY: 'app7020ContentHistory',
    APP7020_CONTENT_HISTORY_AGGREGATE: 'app7020ContentHistoryAgg',
    APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE: 'app7020ContentHistoryTotalViewsAggregate',
    APP7020_CONTENT_RATING: 'app7020ContentRating',
    APP7020_CONTENT_PUBLISHED_AGGREGATE_EDIT: 'app7020ContentPublishedAggregateEdit',
    APP7020_CONTENT_PUBLISHED_AGGREGATE_PUBLISH: 'app7020ContentPublishedAggregatePublish',
    APP7020_INVITATIONS_AGGREGATE: 'app7020InvitationsAgg',
    APP7020_INVOLVED_CHANNELS_AGGREGATE: 'app7020InvolvedChannelsAggregate',
    APP7020_QUESTION: 'app7020Question',
    APP7020_TAG: 'app7020Tag',
    APP7020_TAG_LINK: 'app7020TagLink',
    ASSET_TYPE: 'assetType',
    CONTENT_PARTNERS_VIEW: 'contentPartnersView',
    CORE_GROUP_MEMBERS: 'coreGroupMembers',
    CORE_GROUP_LEVEL: 'coreGroupLevel',
    CORE_LANG_LANGUAGE: 'coreLangLanguage',
    CORE_USER: 'coreUser',
    CORE_USER_BILLING: 'coreUserBilling',
    CORE_USER_BRANCHES: 'coreUserBranches',
    CORE_USER_FIELD_VALUE: 'coreUserFieldValue',
    CORE_USER_LEVELS: 'coreUserLevels',
    CORE_USER_MODIFY: 'coreUserModify',
    CORE_USER_PUBLISH: 'coreUserPublish',
    CORE_USER_2FA_SECRETS: 'coreUser2FaSecrets',
    CORE_SETTING_USER_LANGUAGE: 'coreSettingUserLanguage',
    CORE_SETTING_USER_TIMEZONE: 'coreSettingUserTimezone',
    COURSE_CATEGORIES: 'courseCategories',
    COURSE_SESSION_TIME_AGGREGATE: 'courseSessionTimeAggregate',
    ECOMMERCE_COUPON: 'ecommerceCoupon',
    LEARNING_COMMONTRACK: 'learningCommontrack',
    LEARNING_REPOSITORY_OBJECT_VERSION: 'learningRepositoryObjectVersion',
    LEARNING_COURSE: 'learningCourse',
    LEARNING_COURSE_FIELD_VALUE: 'learningCourseFieldValue',
    LEARNING_COURSE_RATING: 'learningCourseRating',
    LEARNING_COURSEPATH_USER_COMPLETED_COURSES: 'learningCoursepathUserCompletedCourses',
    LEARNING_COURSEUSER_AGGREGATE: 'learningCourseUserAggregate',
    LEARNING_COURSEPATH_COURSES: 'learningCoursepathCourses',
    LEARNING_COURSEPATH_COURSES_COUNT: 'learningCoursepathCoursesCount',
    LEARNING_COURSEPATH_FIELD_VALUE: 'learningCoursepathFieldValue',
    LEARNING_COURSEUSER_SIGN: 'learningCourseuserSign',
    LEARNING_COMMONTRACK_COMPLETED: 'learningCommontrackCompleted',
    LEARNING_ORGANIZATION_COUNT: 'learningOrganizationCount',
    LEARNING_POLL_LIKERT_SCALE: 'learningPollLikertScale',
    LEARNING_POLLQUEST: 'learningPollquest',
    LEARNING_POLLQUEST_ANSWER: 'learningPollquestAnswer',
    LEARNING_REPOSITORY_OBJECT: 'learningRepositoryObject',
    LEARNING_TRACKSESSION_AGGREGATE: 'learningTracksessionAggregate',
    LT_COURSE_SESSION: 'ltCourseSession',
    LT_COURSE_SESSION_AGGREGATE: 'ltCourseSessionAggregate',
    LT_COURSE_SESSION_DATE: 'ltCourseSessionDate',
    LT_COURSE_SESSION_DATE_ATTENDANCE: 'lt_course_session_date_attendance',
    LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE: 'lt_course_session_date_attendance_aggregate',
    LT_COURSE_SESSION_DATE_WEBINAR_SETTING: 'lt_course_session_date_webinar_setting',
    LT_COURSE_SESSION_FIELD_VALUES: 'lt_course_session_field_values',
    LT_COURSE_SESSION_INSTRUCTOR: 'lt_course_session_instructor',
    LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE: 'lt_course_session_instructor_aggregate',
    LT_COURSEUSER_SESSION: 'ltCourseUserSession',
    LT_LOCATION_AGGREGATE: 'ltLocationAggregate',
    SKILL_MANAGERS: 'skillManagers',
    SKILL_SKILLS: 'skillSkills',
    SKILL_SKILLS_OBJECTS: 'skillSkillsObjects',
    TRANSACTION_INFO_AGGREGATE: 'transactionInfoAggregate',
    TRANSCRIPTS_COURSE: 'transcriptsCourse',
    TRANSCRIPTS_FIELD_VALUE: 'transcriptsFieldValue',
    TRANSCRIPTS_INSTITUTE: 'transcriptsInstitute',
    WEBINAR_SESSION: 'webinarSession',
    CORE_USER_BRANCHES_REFACTORED: 'core_user_branches_refactored',
});
