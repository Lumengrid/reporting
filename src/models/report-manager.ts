import { BaseResponse, CourseTypeFilter } from './base';
import {
    DateOptions,
    DateOptionsValueDescriptor,
    Enrollment,
    ExternalTrainingStatusFilter,
    InstructorsFilter,
    Planning,
    PublishStatusFilter,
    SessionAttendanceType,
    SessionDates,
    SortingOptions,
    TimeFrameOptions,
    VisibilityTypes
} from './custom-report';
import { ReportExtractionDetails } from '../services/dynamo';
import { UserProps } from '../services/hydra';
import { ReportManagerData } from './report-users-courses';

export class ReportManagerDeleteResponse implements BaseResponse {
    success: boolean;
    error?: string;
    errorCode?: number;
    constructor() {
        this.success = true;
    }
}

export class BaseReportManagerResponse implements BaseResponse {
    success: boolean;
    data?: any;
    error?: string;
    errorCode?: number;
    constructor() {
        this.success = true;
    }
}

export class BasePaginateResultResponse implements BaseResponse {
    success: boolean;
    data?: any;
    error?: string;
    errorCode?: number;
    nextToken?: string;
    constructor() {
        this.success = true;
    }
}

export class ReportManagerInfoResponse extends BaseReportManagerResponse {
    data?: ReportManagerInfo;
    error?: string;
    errorCode?: number;
    constructor() {
        super();
    }
}

export class ReportManagerUpdateResponse extends ReportManagerInfoResponse {
}

export class ReportManagerInfo {
    // Common fields to all report
    idReport: string;
    queryBuilderId?: string;
    queryBuilderName?: string;
    platform: string;
    title: string;
    description: string;
    type: string;
    timezone: string;
    author: number;
    creationDate: string;
    lastEdit: string;
    lastEditBy: UserProps;
    standard: boolean;
    visibility: ReportManagerInfoVisibility;
    isCustomColumnSortingEnabled: boolean;
    planning: Planning;
    sortingOptions: SortingOptions;
    fields: string[];
    deleted: boolean;
    loginRequired: boolean;
    isReportDownloadPermissionLink: boolean;

    // Specific fields not necessary present in all reports
    enrollmentDate?: DateOptionsValueDescriptor;
    completionDate?: DateOptionsValueDescriptor;
    surveyCompletionDate?: DateOptionsValueDescriptor;
    archivingDate?: DateOptionsValueDescriptor;
    courseExpirationDate?: DateOptionsValueDescriptor;
    issueDate?: DateOptionsValueDescriptor;
    creationDateOpts?: DateOptionsValueDescriptor;
    expirationDateOpts?: DateOptionsValueDescriptor;
    publishedDate?: DateOptionsValueDescriptor;
    contributionDate?: DateOptionsValueDescriptor;
    users?: ReportManagerInfoUsersFilter;
    courses?: ReportManagerInfoCoursesFilter;
    surveys?: ReportManagerInfoSurveysFilter;
    learningPlans?: ReportManagerLearningPlansFilter;
    badges?: ReportManagerBadgesFilter;
    assets?: ReportManagerAssetsFilter;
    conditions?: string;
    enrollment?: Enrollment;
    importedFromLegacyId?: string;
    sessionDates?: SessionDates;
    instructors?: InstructorsFilter;
    userAdditionalFieldsFilter?: { [key: number]: number; };
    loTypes?: {[key: string]: boolean};
    certifications?: ReportManagerCertificationsFilter;
    externalTrainingDate?: DateOptionsValueDescriptor;
    externalTrainingStatusFilter?: ExternalTrainingStatusFilter;
    queryBuilderFilters?: object;
    publishStatus?: PublishStatusFilter;
    sessionAttendanceType?: SessionAttendanceType;
    sessions?: ReportManagerInfoSessionsFilter;

    // VILT specific field for upgrade of the old reports to the new filter format
    vILTUpdated?: boolean;

    public constructor() {
        this.idReport = this.title = this.description = this.type = this.timezone = this.creationDate = this.lastEdit = this.platform = '';
        this.author = 0;
        this.lastEditBy = {
            idUser: 0,
            firstname: '',
            lastname: '',
            username: '',
            avatar: ''
        };
        this.standard = this.deleted = false;
        this.visibility = new ReportManagerInfoVisibility();
        this.planning = {
            active: false,
            option: {
                isPaused: false,
                recipients: [],
                every: 1,
                timeFrame: TimeFrameOptions.days,
                scheduleFrom: '',
                startHour: '00:00',
                timezone: '',
            }
        };
        this.sortingOptions = {
            selector: '',
            selectedField: '',
            orderBy: ''
        };
        this.fields = [];
        this.loginRequired = true;
        this.isReportDownloadPermissionLink = false;
        this.isCustomColumnSortingEnabled = false;
    }
}

export class SelectionInfo {
    id: number;
    name?: string;
    descendants?: boolean;
    public constructor(id?: number, name?: string, descendants?: boolean) {
        this.id = 0;
        if (typeof id !== 'undefined') {
            this.id = id;
        }
        if (typeof name !== 'undefined') {
            this.name = name;
        }
        if (typeof descendants !== 'undefined') {
            this.descendants = descendants;
        }
    }
}

export interface EntitiesLimits {
    users: UsersEntitiesLimits;
    courses: CoursesEntitiesLimits;
    classrooms: CoursesEntitiesLimits;
    webinars: CoursesEntitiesLimits;
    lpLimit: number;
    certificationsLimit: number;
    badgesLimit: number;
    surveysLimit: number;
    assets: AssetsEntitiesLimits;
}

export interface UsersEntitiesLimits {
    usersLimit?: number;
    groupsLimit: number;
    branchesLimit: number;
}

export interface CoursesEntitiesLimits {
    coursesLimit?: number;
    lpLimit?: number;
    classroomLimit?: number;
    webinarLimit?: number;
    courseInstructorsLimit?: number;
    sessionLimit?: number;
}

export interface AssetsEntitiesLimits {
    assetsLimit?: number;
    channelsLimit?: number;
}

export class ReportManagerInfoUsersFilter {
    all: boolean;
    hideDeactivated: boolean;
    showOnlyLearners: boolean;
    hideExpiredUsers: boolean;
    isUserAddFields: boolean;
    users: SelectionInfo[];
    groups: SelectionInfo[];
    branches: SelectionInfo[];
    entitiesLimits: UsersEntitiesLimits;
    public constructor () {
        this.all = false;
        this.hideDeactivated = true;
        this.showOnlyLearners = false;
        this.hideExpiredUsers = true;
        this.isUserAddFields = false;
        this.users = this.groups = this.branches = [];
        this.entitiesLimits = {} as UsersEntitiesLimits;
    }
}

export class ReportManagerInfoCoursesFilter {
    all: boolean;
    courses: SelectionInfo[];
    categories: SelectionInfo[];
    instructors: SelectionInfo[];
    courseType: CourseTypeFilter;
    entitiesLimits: CoursesEntitiesLimits;
    public constructor () {
        this.all = false;
        this.courses = this.categories = this.instructors = [];
        this.entitiesLimits = {} as CoursesEntitiesLimits;
        this.courseType = CourseTypeFilter.ALL;
    }
}

export class ReportManagerInfoSurveysFilter {
    all: boolean;
    surveys: SelectionInfo[];
    entitiesLimits: number;
    public constructor () {
        this.all = false;
        this.surveys = [];
        this.entitiesLimits = 0;
    }
}

export class ReportManagerInfoSessionsFilter {
    all: boolean;
    sessions: SelectionInfo[];
    entitiesLimits: number;
    public constructor () {
        this.all = false;
        this.sessions = [];
        this.entitiesLimits = 0;
    }
}

export interface LearningPlans {
    id: number | string;
    name?: string;
}

export class ReportManagerLearningPlansFilter {
    all: boolean;
    learningPlans: LearningPlans[];
    entitiesLimits: number;
    public constructor () {
        this.all = false;
        this.learningPlans = [];
        this.entitiesLimits = 0;
    }
}
export class ReportManagerBadgesFilter {
    all: boolean;
    badges: SelectionInfo[];
    entitiesLimits: number;
    public constructor () {
        this.all = false;
        this.badges = [];
        this.entitiesLimits = 0;
    }
}

export class ReportManagerAssetsFilter {
    all: boolean;
    assets: SelectionInfo[];
    channels: SelectionInfo[];
    entitiesLimits: AssetsEntitiesLimits;
    public constructor () {
        this.all = false;
        this.assets = [];
        this.channels = [];
        this.entitiesLimits = {} as AssetsEntitiesLimits;
    }
}

export class ReportManagerCertificationsFilter {
    all: boolean;
    certifications: SelectionInfo[];
    activeCertifications: boolean;
    expiredCertifications: boolean;
    archivedCertifications: boolean;
    certificationDate: DateOptionsValueDescriptor;
    certificationExpirationDate: DateOptionsValueDescriptor;
    conditions: string;
    entitiesLimits: number;

    public constructor() {
        this.all = true;
        this.certifications = [];
        this.activeCertifications = true;
        this.expiredCertifications = true;
        this.archivedCertifications = false;
        this.certificationDate = this.getDefaultDate();
        this.certificationExpirationDate = this.getDefaultDate();
        this.conditions = DateOptions.CONDITIONS;
        this.entitiesLimits = 0;
    }

    getDefaultDate(): DateOptionsValueDescriptor {
        return {
            any: true,
            days: 1,
            type: '',
            operator: '',
            to: '',
            from: ''
        };
    }
}

export class ReportManagerInfoVisibility {
    type: VisibilityTypes;
    users: SelectionInfo[];
    groups: SelectionInfo[];
    branches: SelectionInfo[];
    public constructor () {
        this.type = VisibilityTypes.ALL_GODADMINS;
        this.users = [];
        this.groups = [];
        this.branches = [];
    }
}

export class ReportManagerExportResponseData {
    executionId?: string;
    refreshError?: boolean;
    public constructor(executionId?: string) {
        this.executionId = executionId;
    }
}


export class ReportManagerExportResponse implements BaseResponse {
    success: boolean;
    error?: string;
    data?: ReportManagerExportResponseData;
    errorCode?: number;
    constructor() {
        this.success = true;
    }
}


export class ReportManagerExportDetailsResponse implements BaseResponse {
    success: boolean;
    error?: string;
    data?: ReportExtractionDetails;
    constructor() {
        this.success = true;
    }
}

export interface ReportLPCoursesFilterRecover {
    id_item: number;
}
export interface ReportChannelsFilterRecover {
    idasset: number;
}

export interface ReportUserFilterRecover {
    idstmember: number;
}

export interface ReportGroupsFilterRecover {
    idst_oc: number;
}

export interface MassDeleteResponse extends BaseResponse {
    success: boolean;
    data: MassDeleteResponseData;
}

export interface MassDeleteResponseData {
    deleted: string[];
    notDeleted: string[];
    deletingErrors: string[];
}

export interface ReportAvailablesFields {
    user?: ReportField[];
    group?: ReportField[];
    course?: ReportField[];
    courseuser?: ReportField[];
    statistics?: ReportField[];
    usageStatistics?: ReportField[];
    mobileAppStatistics?: ReportField[];
    flowStatistics?: ReportField[];
    flowMsTeamsStatistics?: ReportField[];
    session?: ReportField[];
    event?: ReportField[];
    customFields?: ReportField[];
    webinarSessionUser?: ReportField[];
    learningPlans?: ReportField[];
    learningPlansEnrollments?: ReportField[];
    learningPlansStatistics?: ReportField[];
    courseEnrollments?: ReportField[];
    trainingMaterials?: ReportField[];
    enrollment?: ReportField[];
    certifications?: ReportField[];
    badge?: ReportField[];
    badgeAssignment?: ReportField[];
    externalTraining?: ReportField[];
    ecommerceTransaction?: ReportField[];
    ecommerceTransactionItem?: ReportField[];
    contentPartners?: ReportField[];
    assets?: ReportField[];
    assetsStatusFields?: ReportField[];
    sessionStatistics?: ReportField[];
    courseUsageStatistics?: ReportField[];
    survey?: ReportField[];
    surveyQuestionAnswer?: ReportField[];
}

export interface ReportField {
    field: string;
    idLabel: string;
    mandatory: boolean;
    isAdditionalField: boolean;
    translation: string;
}

export class ReportManagerAvailablesFieldsResponse extends BaseReportManagerResponse {
    data?: ReportAvailablesFields;
    error?: string;
    errorCode?: number;
    constructor() {
        super();
    }
}

export interface InformationReport {
    idReport: string;
    title: string;
}

export enum ExportStatuses {
    INITIALIZING = 'INITIALIZING',
    QUEUED = 'QUEUED',
    RUNNING = 'RUNNING',
    CONVERTING = 'CONVERTING',
    COMPRESSED = 'COMPRESSED',
    COMPRESSING = 'COMPRESSING',
    COMPRESSION_SKIPPED = 'COMPRESSION_SKIPPED',
    QUERY_CHECKED = 'QUERY_CHECKED',
    QUERY_COMPLETED = 'QUERY_COMPLETED',
    EXPORT_CSV_STARTED = 'EXPORT_CSV_STARTED',
    EXPORT_CSV_CHECKED = 'EXPORT_CSV_CHECKED',
    EXPORT_CSV_COMPLETED = 'EXPORT_CSV_COMPLETED',
    EXPORT_CSV_CONTENT_CHECKED = 'EXPORT_CSV_CONTENT_CHECKED',
    EXPORT_CONVERTED = 'EXPORT_CONVERTED',
    EXPORT_CONVERSION_SKIPPED = 'EXPORT_CONVERSION_SKIPPED',
    SUCCEEDED = 'SUCCEEDED',
    FAILED = 'FAILED',
}

export enum ExportLimit {
    CSV = 2000000,
    XLX = 1000000,
    PREVIEW = 100,
}

export interface AthenaFieldsDataResponse {
    Items: AthenaFieldsDataResponseElement[];
}

export interface AthenaFieldsDataResponseElement {
    id: string;
}

export enum TablesList {
    AUDIT_TRAIL_LOG = 'audit_trail_log',
    CERTIFICATION = 'certification',
    CERTIFICATION_ITEM = 'certification_item',
    CERTIFICATION_USER = 'certification_user',
    CORE_GROUP = 'core_group',
    CORE_GROUP_MEMBERS = 'core_group_members',
    CORE_ORG_CHART = 'core_org_chart',
    CORE_ORG_CHART_TREE = 'core_org_chart_tree',
    CORE_PLUGIN = 'core_plugin',
    CORE_SETTING = 'core_setting',
    CORE_SETTING_USER = 'core_setting_user',
    CORE_USER = 'core_user',
    CORE_USER_PU = 'core_user_pu',
    CORE_USER_PU_COURSE = 'core_user_pu_course',
    CORE_USER_FIELD = 'core_user_field',
    CORE_USER_FIELD_DROPDOWN = 'core_user_field_dropdown',
    CORE_USER_FIELD_DROPDOWN_TRANSLATIONS = 'core_user_field_dropdown_translations',
    CORE_USER_FIELD_VALUE = 'core_user_field_value',
    CORE_USER_FIELD_VALUE_WITH = 'core_user_field_value_with',
    CORE_USER_FIELD_FILTER_WITH = 'core_user_field_filter_with',
    CORE_LANG_LANGUAGE = 'core_lang_language',
    SKILL_MANAGERS = 'skill_managers',
    SKILL_SKILLS_OBJECTS = 'skill_skills_objects',
    SKILL_SKILLS = 'skill_skills',
    SKILLS_WITH = 'skills_with',

    GAMIFICATION_ASSIGNED_BADGES = 'gamification_assigned_badges',
    GAMIFICATION_BADGE = 'gamification_badge',
    GAMIFICATION_BADGE_TRANSLATION = 'gamification_badge_translation',

    LEARNING_CATEGORY = 'learning_category',
    LEARNING_COMMONTRACK = 'learning_commontrack',
    LEARNING_COURSE = 'learning_course',
    LEARNING_COURSE_FIELD = 'learning_course_field',
    LEARNING_COURSE_FIELD_DROPDOWN = 'learning_course_field_dropdown',
    LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS = 'learning_course_field_dropdown_translations',
    LEARNING_COURSE_FIELD_VALUE = 'learning_course_field_value',
    LEARNING_COURSE_FIELD_VALUE_WITH = 'learning_course_field_value_with',
    LEARNING_COURSEPATH_FIELD_VALUE = 'learning_coursepath_field_value',
    LEARNING_COURSEPATH_FIELD_VALUE_WITH = 'learning_coursepath_field_value_with',
    LEARNING_COURSE_RATING = 'learning_course_rating',
    LEARNING_COURSEPATH = 'learning_coursepath',
    LEARNING_COURSEPATH_COURSES = 'learning_coursepath_courses',
    LEARNING_COURSEPATH_COURSESUSER_MANDATORY_COMPLETE_WITH = 'learning_coursepath_coursesuser_mandatory_complete_with',
    LEARNING_COURSEPATH_USER = 'learning_coursepath_user',
    LEARNING_COURSEUSER = 'learning_courseuser',
    LEARNING_COURSEUSER_AGGREGATE = 'learning_courseuser_aggregate',
    LEARNING_COURSEUSER_SIGN = 'learning_courseuser_sign',
    LEARNING_ENROLLMENT_FIELDS = 'learning_enrollment_fields',
    LEARNING_ENROLLMENT_FIELDS_DROPDOWN = 'learning_enrollment_fields_dropdown',
    LEARNING_ORGANIZATION = 'learning_organization',
    LEARNING_POLL = 'learning_poll',
    LEARNING_POLL_LIKERT_SCALE = 'learning_poll_likert_scale',
    LEARNING_POLLQUEST = 'learning_pollquest',
    LEARNING_POLLQUEST_ANSWER = 'learning_pollquestanswer',
    LEARNING_POLLQUEST_WITH = 'learning_pollquest_with',
    LEARNING_POLLTRACK = 'learning_polltrack',
    LEARNING_POLLTRACK_ANSWER = 'learning_polltrack_answer',
    LEARNING_REPOSITORY_OBJECT = 'learning_repository_object',
    LEARNING_REPOSITORY_OBJECT_VERSION = 'learning_repository_object_version',
    LEARNING_TRACKSESSION = 'learning_tracksession',

    WEBINAR_SESSION = 'webinar_session',
    WEBINAR_SESSION_USER = 'webinar_session_user',
    WEBINAR_SESSION_DATE = 'webinar_session_date',
    WEBINAR_SESSION_DATE_ATTENDANCE = 'webinar_session_date_attendance',
    WEBINAR_SESSION_FIELD_VALUE = 'webinar_session_field_value',
    SUB_FIELDS = 'sub_fields',
    SUB_FIELDS_DROPDOWN = 'sub_fields_dropdown',
    SUB_FIELDS_DROPDOWN_TRANSLATIONS = 'sub_fields_dropdown_translations',
    LT_COURSE_SESSION = 'lt_course_session',
    LT_COURSE_SESSION_FIELD_VALUES = 'lt_course_session_field_value',
    LT_COURSE_SESSION_FIELD_VALUES_WITH = 'lt_course_session_field_value_with',
    LT_COURSE_SESSION_INSTRUCTOR = 'lt_course_session_instructor',
    LT_COURSEUSER_SESSION = 'lt_courseuser_session',
    LT_COURSE_SESSION_DATE_WEBINAR_SETTING = 'lt_course_session_date_webinar_setting',
    TRANSCRIPTS_COURSE = 'transcripts_course',
    TRANSCRIPTS_FIELD = 'transcripts_field',
    TRANSCRIPTS_FIELD_DROPDOWN_TRANSLATIONS = 'transcripts_field_dropdown_translations',
    TRANSCRIPTS_FIELD_VALUE = 'transcripts_field_value',
    TRANSCRIPTS_FIELD_VALUE_WITH = 'transcripts_field_value_with',
    TRANSCRIPTS_INSTITUTE = 'transcripts_institute',
    TRANSCRIPTS_RECORD = 'transcripts_record',
    CORE_USER_2FA_SECRETS = 'core_user_2fa_secrets',

    ECOMMERCE_TRANSACTION = 'ecommerce_transaction',
    ECOMMERCE_TRANSACTION_INFO = 'ecommerce_transaction_info',
    CORE_USER_BILLING = 'core_user_billing',
    ECOMMERCE_COUPON = 'ecommerce_coupon',
    ECOMMERCE_TRANSACTION_INFO_AGGREGATE = 'ecommerce_transaction_info_aggregate',
    ECOMMERCE_COUPON_COURSES = 'ecommerce_coupon_courses',
    LT_LOCATION = 'lt_location',
    CORE_COUNTRY = 'core_country',
    LT_COURSE_SESSION_DATE = 'lt_course_session_date',
    CONTENT_PARTNERS = 'content_partners',
    CONTENT_PARTNERS_AFFILIATES = 'content_partners_affiliates',
    CONTENT_PARTNERS_REFERRAL_LOG = 'content_partners_referral_log',

    APP7020_CONTENT = 'app7020_content', // Referred to Assets
    APP7020_CONTENT_PUBLISHED = 'app7020_content_published',
    APP7020_CONTENT_PUBLISHED_AGGREGATE = 'app7020_content_published_aggregate',
    APP7020_CHANNEL_ASSETS = 'app7020_channel_assets',
    APP7020_CHANNEL_TRANSLATION = 'app7020_channel_translation',

    APP7020_CHANNELS = 'app7020_channels',
    APP7020_ANSWER = 'app7020_answer',
    APP7020_ANSWER_LIKE = 'app7020_answer_like',
    APP7020_CONTENT_RATING = 'app7020_content_rating',
    APP7020_CONTENT_HISTORY = 'app7020_content_history',
    APP7020_INVITATIONS = 'app7020_invitations',
    APP7020_QUESTION = 'app7020_question',
    APP7020_TAG_LINK = 'app7020_tag_link',
    APP7020_TAG = 'app7020_tag',

    // WITH tables
    USERS_ADDITIONAL_FIELDS_TRANSLATIONS = 'users_additional_fields_translations',
    COURSES_ADDITIONAL_FIELDS_TRANSLATIONS = 'courses_additional_fields_translations',
    LEARNING_PLAN_ADDITIONAL_FIELDS_TRANSLATIONS = 'learning_plan_additional_fields_translations',
    CLASSROOM_ADDITIONAL_FIELDS_TRANSLATIONS = 'classroom_additional_fields_translations',
    EXTERNAL_TRAINING_ADDITIONAL_FIELDS_TRANSLATIONS = 'external_training_additional_fields_translations',
    CORE_GROUP_MEMBERS_BRANCHES = 'core_group_members_branches',
    CORE_ORG_CHART_TREE_TRANSLATIONS = 'core_org_chart_tree_translations',
    CORE_ORG_CHART_PATHS_REFACTORED = 'core_org_chart_paths_refactored',
    CORE_USER_BRANCHES_REFACTORED = 'core_user_branches_refactored',

    // Materialized tables
    CORE_ORG_CHART_PATHS = 'core_org_chart_paths',
    CORE_ORG_CHART_TRANSLATIONS = 'core_org_chart_translations',
    CORE_USER_BRANCHES = 'core_user_branches',
    CORE_USER_LEVELS = 'core_user_levels',
    LT_COURSE_SESSION_DATE_ATTENDANCE = 'lt_course_session_date_attendance',
    LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE = 'lt_course_session_date_attendance_aggregate',
    LEARNING_COMMONTRACK_COMPLETED = 'learning_commontrack_completed',
    LEARNING_COURSEPATH_COURSES_COUNT = 'learning_coursepath_courses_count',
    LEARNING_COURSEPATH_USER_COMPLETED_COURSES = 'learning_coursepath_user_completed_courses',
    LEARNING_ORGANIZATION_COUNT = 'learning_organization_count',
    LEARNING_TRACKSESSION_AGGREGATE = 'learning_tracksession_aggregate',
    LT_LOCATION_AGGREGATE = 'lt_location_aggregate',
    WEBINAR_SESSION_USER_DETAILS = 'webinar_session_user_details',
    LT_COURSEUSER_SESSION_DETAILS = 'lt_courseuser_session_details',
    LEARNING_COURSEUSER_STATUS = 'learning_courseuser_status',
    COURSE_SESSION_TIME_AGGREGATE = 'course_session_time_aggregate',
    LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE = 'lt_course_session_instructor_aggregate',

    APP7020_ANSWER_AGGREGATE = 'app7020_answer_aggregate',
    APP7020_ANSWER_LIKE_AGGREGATE = 'app7020_answer_like_aggregate',
    APP7020_ANSWER_DISLIKE_AGGREGATE = 'app7020_answer_dislike_aggregate',
    APP7020_BEST_ANSWER_AGGREGATE = 'app7020_best_answer_aggregate',
    APP7020_INVITATIONS_AGGREGATE = 'app7020_invitations_aggregate',
    APP7020_INVITATIONS_AVERAGE_TIME = 'app7020_invitations_average_time',
    APP7020_CONTENT_HISTORY_AGGREGATE = 'app7020_content_history_aggregate',
    APP7020_INVOLVED_CHANNELS_AGGREGATE = 'app7020_involved_channels_aggregate',
    APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE = 'app7020_content_history_total_views_aggregate',

    ARCHIVED_ENROLLMENT_COURSE = 'archived_enrollment_course',
    ARCHIVED_ENROLLMENT_SESSION = 'archived_enrollment_session',

    // Dashboards
    CORE_MULTI_DOMAIN = 'core_multidomain',
    TC_POLICY_VERSIONS = 'tc_policy_versions',
    TC_POLICIES = 'tc_policies',
    TC_POLICIES_TRACK = 'tc_policies_track',
    TC_SUB_POLICIES_TRACK = 'tc_sub_policies_track',
    RBAC_ASSIGNMENT = 'rbac_assignment',
}

export enum TablesListAliases {
    AUDIT_TRAIL_LOG = 'atl',
    CERTIFICATION = 'ce',
    CERTIFICATION_ITEM = 'cei',
    CERTIFICATION_USER = 'ceu',
    CORE_GROUP = 'cg',
    CORE_GROUP_MEMBERS = 'cgm',
    CORE_ORG_CHART = 'coc',
    CORE_ORG_CHART_2 = 'cocf',
    CORE_ORG_CHART_TREE = 'coct',
    CORE_ORG_CHART_TREE_2 = 'cocts',
    CORE_ORG_CHART_TREE_3 = 'coctd',
    CORE_PLUGIN = 'cp',
    CORE_SETTING = 'cs',
    CORE_SETTING_USER = 'csu',
    CORE_USER = 'cu',
    CORE_USER_PU = 'cup',
    CORE_USER_PU_COURSE = 'cupc',
    CORE_USER_FIELD = 'cuf',
    CORE_USER_FIELD_DROPDOWN = 'cufd',
    CORE_USER_FIELD_DROPDOWN_TRANSLATIONS = 'cufdt',
    CORE_USER_FIELD_VALUE = 'cufv',
    CORE_USER_FIELD_FILTER_WITH = 'cuff',
    CORE_LANG_LANGUAGE = 'cll',
    SKILL_MANAGERS = 'sm',
    SKILL_SKILLS_OBJECTS = 'sso',
    SKILL_SKILLS = 'ss',
    SKILLS_WITH = 'ssw',

    GAMIFICATION_ASSIGNED_BADGES = 'gab',
    GAMIFICATION_BADGE = 'gb',
    GAMIFICATION_BADGE_TRANSLATION = 'gbt',

    LEARNING_CATEGORY = 'lca',
    LEARNING_COMMONTRACK = 'lco',
    LEARNING_COURSE = 'lc',
    LEARNING_COURSE_FIELD = 'lcf',
    LEARNING_COURSE_FIELD_DROPDOWN = 'lcfd',
    LEARNING_COURSE_FIELD_DROPDOWN_TRANSLATIONS = 'lcfdt',
    LEARNING_COURSE_FIELD_VALUE = 'lcfv',
    LEARNING_COURSE_RATING = 'lcr',
    LEARNING_COURSEPATH = 'lcp',
    LEARNING_COURSEPATH_COURSES = 'lcpc',
    LEARNING_COURSEPATH_COURSESUSER_MANDATORY_COMPLETE_WITH = 'lcpcumcw',
    LEARNING_COURSEPATH_USER = 'lcpu',
    LEARNING_COURSEPATH_FIELD_VALUE = 'lcpfv',
    LEARNING_COURSEUSER = 'lcu',
    LEARNING_COURSEUSER_AGGREGATE = 'lcu_a',
    LEARNING_COURSEUSER_SIGN = 'lcus',
    LEARNING_ENROLLMENT_FIELDS = 'lef',
    LEARNING_ENROLLMENT_FIELDS_DROPDOWN = 'lefd',
    LEARNING_ORGANIZATION = 'lo',
    LEARNING_POLL = 'lp',
    LEARNING_POLL_LIKERT_SCALE = 'lpls',
    LEARNING_POLLQUEST = 'lpq',
    LEARNING_POLLQUEST_ANSWER = 'lpqa',
    LEARNING_POLLTRACK = 'lpt',
    LEARNING_POLLTRACK_ANSWER = 'lpta',
    LEARNING_REPOSITORY_OBJECT = 'lro',
    LEARNING_REPOSITORY_OBJECT_VERSION = 'lrov',
    LEARNING_TRACKSESSION = 'lt',

    WEBINAR_SESSION = 'ws',
    WEBINAR_SESSION_USER = 'wsu',
    WEBINAR_SESSION_DATE = 'wsd',
    WEBINAR_SESSION_DATE_ATTENDANCE = 'wsda',
    WEBINAR_SESSION_FIELD_VALUE = 'wsfv',

    SUB_FIELDS = 'sf',
    SUB_FIELDS_DROPDOWN = 'sfd',
    SUB_FIELDS_DROPDOWN_TRANSLATIONS = 'sfdt',
    LT_COURSE_SESSION = 'ltcs',
    LT_COURSE_SESSION_FIELD_VALUES = 'ltcsfv',
    LT_COURSE_SESSION_INSTRUCTOR = 'ltcsi',
    LT_COURSEUSER_SESSION = 'ltcus',
    LT_COURSE_SESSION_DATE_ATTENDANCE = 'ltcsda',
    LT_COURSE_SESSION_DATE_ATTENDANCE_AGGREGATE = 'ltcsdaa',
    LT_COURSE_SESSION_DATE_WEBINAR_SETTING = 'lcsdws',
    TRANSCRIPTS_COURSE = 'tc',
    TRANSCRIPTS_FIELD = 'tf',
    TRANSCRIPTS_FIELD_DROPDOWN_TRANSLATIONS = 'tfdt',
    TRANSCRIPTS_FIELD_VALUE = 'tfv',
    TRANSCRIPTS_INSTITUTE = 'ti',
    TRANSCRIPTS_RECORD = 'tr',
    CORE_USER_2FA_SECRETS = 'cu2fas',

    ECOMMERCE_TRANSACTION = 'et',
    ECOMMERCE_TRANSACTION_INFO = 'eti',
    CORE_USER_BILLING = 'cubill',
    ECOMMERCE_COUPON = 'ec',
    ECOMMERCE_TRANSACTION_INFO_AGGREGATE = 'etia',
    ECOMMERCE_COUPON_COURSES = 'ecc',
    LT_LOCATION = 'll',
    CORE_COUNTRY = 'cc',
    LT_COURSE_SESSION_DATE = 'lcsd',
    LT_COURSE_SESSION_USER_DATE = 'lcsud',
    CONTENT_PARTNERS = 'cpa',
    CONTENT_PARTNERS_AFFILIATES = 'cpaa',
    CONTENT_PARTNERS_REFERRAL_LOG = 'cparl',

    APP7020_CONTENT = 'c',
    APP7020_CONTENT_PUBLISHED = 'cop',
    APP7020_CONTENT_PUBLISHED_AGGREGATE = 'cop_aggregate',
    APP7020_CONTENT_PUBLISHED_AGGREGATE_EDIT = 'cop_aggregate_edit',
    APP7020_CONTENT_PUBLISHED_AGGREGATE_MAX = 'cop_aggregate_max',
    APP7020_CONTENT_PUBLISHED_AGGREGATE_PUBLISH = 'cop_aggregate_publish',
    APP7020_CHANNEL_ASSETS = 'cha',
    APP7020_CHANNEL_TRANSLATION = 'cht',

    APP7020_CHANNELS = 'ch',
    APP7020_ANSWER = 'an',
    APP7020_ANSWER_LIKE = 'anl',
    APP7020_CONTENT_RATING = 'cr',
    APP7020_CONTENT_HISTORY = 'coh',
    APP7020_INVITATIONS = 'i',
    APP7020_QUESTION = 'q',
    APP7020_TAG_LINK = 'tag_link',
    APP7020_TAG = 'tag',

    ARCHIVED_ENROLLMENT_COURSE = 'aec',
    ARCHIVED_ENROLLMENT_SESSION = 'aes',

    // Materialized tables aliases
    CORE_ORG_CHART_MEMBERS = 'cocm',
    CORE_ORG_CHART_PATHS = 'cocp',
    CORE_ORG_CHART_TRANSLATIONS = 'coctr',
    CORE_USER_BRANCHES = 'cub',
    CORE_USER_BRANCHES_NAMES = 'cubn',
    CORE_USER_LEVELS = 'cul',
    LEARNING_COMMONTRACK_COMPLETED = 'lcoc',
    LEARNING_COURSEPATH_COURSES_COUNT = 'lcpcc',
    LEARNING_COURSEPATH_USER_COMPLETED_COURSES = 'lcpucc',
    LEARNING_ORGANIZATION_COUNT = 'loc',
    LEARNING_TRACKSESSION_AGGREGATE = 'lta',
    LT_LOCATION_AGGREGATE = 'lla',
    WEBINAR_SESSION_USER_DETAILS = 'wsud',
    LT_COURSEUSER_SESSION_DETAILS = 'ltcusd',
    LEARNING_COURSEUSER_STATUS = 'lcust',
    COURSE_SESSION_TIME_AGGREGATE = 'csta',
    LT_COURSE_SESSION_INSTRUCTOR_AGGREGATE = 'ltcsia',

    APP7020_ANSWER_AGGREGATE = 'aa',
    APP7020_ANSWER_LIKE_AGGREGATE = 'ala',
    APP7020_ANSWER_DISLIKE_AGGREGATE = 'ada',
    APP7020_BEST_ANSWER_AGGREGATE = 'baa',
    APP7020_INVITATIONS_AGGREGATE = 'ia',
    APP7020_INVITATIONS_AVERAGE_TIME = 'iat',
    APP7020_CONTENT_HISTORY_AGGREGATE = 'coha',
    APP7020_INVOLVED_CHANNELS_AGGREGATE = 'ica',
    APP7020_CONTENT_HISTORY_TOTAL_VIEWS_AGGREGATE = 'cohtva',

    // Dashboards
    CORE_MULTI_DOMAIN = 'cm',
    TC_POLICY_VERSIONS = 'vers',
    TC_POLICIES = 'pol',
    TC_POLICY_TRACK = 'track',
    TC_POLICY_TREE = 'tree',
    TC_SUB_POLICIES_TRACK = 'spt',
    TC_SUB_POLICIES_TRACK_1 = 'spt_1',
    TC_SUB_POLICIES_TRACK_2 = 'spt_2',
    TC_SUB_POLICIES_TRACK_3 = 'spt_3',
    RBAC_ASSIGNMENT = 'ra',
    WITH_BRANCH_WEBINARS = 'with_webinars',
    WITH_BRANCH_LT_COURSES = 'with_lt_courses',
    WITH_BRACH_MEMBERS = 'with_members_branch',
}

export enum FieldsList {
    // User fields
    USER_ID = 'user_id',
    USER_USERID = 'user_userid',
    USER_FIRSTNAME = 'user_firstname',
    USER_LASTNAME = 'user_lastname',
    USER_FULLNAME = 'user_fullname',
    USER_EMAIL = 'user_email',
    USER_EMAIL_VALIDATION_STATUS = 'user_email_validation_status',
    USER_LEVEL = 'user_level',
    USER_DEACTIVATED = 'user_deactivated',
    USER_EXPIRATION = 'user_expiration',
    USER_SUSPEND_DATE = 'user_suspend_date',
    USER_REGISTER_DATE = 'user_register_date',
    USER_LAST_ACCESS_DATE = 'user_last_access_date',
    USER_BRANCH_NAME = 'user_branch_name',
    USER_BRANCHES = 'user_branches',
    USER_BRANCHES_CODES = 'user_branches_codes',
    USER_BRANCH_PATH = 'user_branch_path',
    USER_AUTH_APP_PAIRED = 'user_auth_app_paired',
    USER_MANAGER_PERMISSIONS = 'user_manager_permissions',
    USER_TIMEZONE = 'user_timezone',
    USER_LANGUAGE = 'user_language',
    USER_DIRECT_MANAGER = 'user_direct_manager',

    // Course fields
    COURSE_ID = 'course_id',
    COURSE_CODE = 'course_code',
    COURSE_NAME = 'course_name',
    COURSE_CATEGORY_CODE = 'course_category_code',
    COURSE_CATEGORY_NAME = 'course_category',
    COURSE_STATUS = 'course_status',
    COURSE_CREDITS = 'course_credits',
    COURSE_DURATION = 'course_duration',
    COURSE_TYPE = 'course_type',
    COURSE_DATE_BEGIN = 'course_date_begin',
    COURSE_DATE_END = 'course_date_end',
    COURSE_EXPIRED = 'course_expired',
    COURSE_CREATION_DATE = 'course_creation_date',
    COURSE_E_SIGNATURE = 'course_e_signature',
    COURSE_E_SIGNATURE_HASH = 'course_e_signature_hash',
    COURSE_LANGUAGE = 'course_language',
    COURSE_UNIQUE_ID = 'course_unique_id',
    COURSE_SKILLS = 'course_skills',

    // Webinar session
    WEBINAR_SESSION_NAME = 'webinar_session_name',
    WEBINAR_SESSION_EVALUATION_SCORE_BASE = 'webinar_session_score_base',
    WEBINAR_SESSION_START_DATE = 'webinar_session_start_date',
    WEBINAR_SESSION_END_DATE = 'webinar_session_end_date',
    WEBINAR_SESSION_SESSION_TIME = 'webinar_session_session_time',
    WEBINAR_SESSION_WEBINAR_TOOL = 'webinar_session_webinar_tool',
    WEBINAR_SESSION_TOOL_TIME_IN_SESSION = 'webinar_session_tool_time_in_session',

    // Webinar session user
    WEBINAR_SESSION_USER_LEVEL = 'webinar_session_user_level',
    WEBINAR_SESSION_USER_ENROLL_DATE = 'webinar_session_user_enroll_date',
    WEBINAR_SESSION_USER_STATUS = 'webinar_session_user_status',
    WEBINAR_SESSION_USER_LEARN_EVAL = 'webinar_session_user_learn_eval',
    WEBINAR_SESSION_USER_EVAL_STATUS = 'webinar_session_user_eval_status',
    WEBINAR_SESSION_USER_INSTRUCTOR_FEEDBACK = 'webinar_session_user_instructor_feedback',
    WEBINAR_SESSION_USER_ENROLLMENT_STATUS = 'webinar_session_user_enrollment_status',
    WEBINAR_SESSION_USER_SUBSCRIBE_DATE = 'webinar_session_user_subscribe_date',
    WEBINAR_SESSION_USER_COMPLETE_DATE = 'webinar_session_user_complete_date',


    // Courseuser fields
    COURSEUSER_LEVEL = 'courseuser_level',
    COURSEUSER_DATE_INSCR = 'courseuser_date_inscr',
    COURSEUSER_DATE_FIRST_ACCESS = 'courseuser_date_first_access',
    COURSEUSER_DATE_LAST_ACCESS = 'courseuser_date_last_access',
    COURSEUSER_DATE_COMPLETE = 'courseuser_date_complete',
    COURSEUSER_EXPIRATION_DATE = 'courseuser_expiration_date',
    COURSEUSER_STATUS = 'courseuser_status',
    COURSEUSER_DATE_BEGIN_VALIDITY = 'courseuser_date_begin_validity',
    COURSEUSER_DATE_EXPIRE_VALIDITY = 'courseuser_date_expire_validity',
    COURSEUSER_SCORE_GIVEN = 'courseuser_score_given',
    COURSEUSER_INITIAL_SCORE_GIVEN = 'courseuser_initial_score_given',
    COURSEUSER_DAYS_LEFT = 'courseuser_days_left',
    COURSEUSER_ENROLLMENT_CODESET = 'courseuser_enrollment_codeset',
    COURSEUSER_ENROLLMENT_CODE = 'courseuser_enrollment_code',
    COURSEUSER_ASSIGNMENT_TYPE = 'courseuser_assignment_type',
    ENROLLMENT_ARCHIVED = 'enrollment_archived',
    ENROLLMENT_ARCHIVING_DATE = 'enrollment_archiving_date',

    // Groups fields
    GROUP_GROUP_OR_BRANCH_NAME = 'group_group_or_branch_name',
    GROUP_MEMBERS_COUNT = 'group_members_count',

    // Learningplans fields
    LP_NAME = 'lp_name',
    LP_CODE = 'lp_code',
    LP_CREDITS = 'lp_credits',
    LP_UUID = 'lp_uuid',
    LP_LAST_EDIT = 'lp_last_edit',
    LP_CREATION_DATE = 'lp_creation_date',
    LP_DESCRIPTION = 'lp_description',
    LP_ASSOCIATED_COURSES = 'lp_associated_courses',
    LP_MANDATORY_ASSOCIATED_COURSES = 'lp_mandatory_associated_courses',
    LP_STATUS = 'lp_status',
    LP_LANGUAGE = 'lp_language',

    // Learning Plans enrollment fields
    LP_ENROLLMENT_DATE = 'lp_enrollment_date',
    LP_ENROLLMENT_COMPLETION_DATE = 'lp_enrollment_completion_date',
    LP_ENROLLMENT_STATUS = 'lp_enrollment_status',
    LP_ENROLLMENT_START_OF_VALIDITY = 'lp_enrollment_start_of_validity',
    LP_ENROLLMENT_END_OF_VALIDITY = 'lp_enrollment_end_of_validity',
    LP_ENROLLMENT_ASSIGNMENT_TYPE = 'lp_enrollment_assignment_type',

    // Learning Plan usage statistics Fields
    // If toggleUsersLearningPlansReportEnhancement is OFF, LP_ENROLLMENT_COMPLETION_PERCENTAGE will be shown in Learning Plans enrollment fields
    LP_ENROLLMENT_COMPLETION_PERCENTAGE = 'lp_enrollment_completion_percentage',
    LP_STAT_PROGRESS_PERCENTAGE_MANDATORY = 'lp_stat_progress_percentage_mandatory',
    LP_STAT_PROGRESS_PERCENTAGE_OPTIONAL = 'lp_stat_progress_percentage_optional',

    LP_STAT_DURATION = 'lp_stat_duration',
    LP_STAT_DURATION_MANDATORY = 'lp_stat_duration_mandatory',
    LP_STAT_DURATION_OPTIONAL = 'lp_stat_duration_optional',

    LP_COURSE_LANGUAGE = 'lp_course_language',

    // LP Course enrollment fields
    COURSE_ENROLLMENT_DATE_INSCR = 'course_enrollment_date_inscr',
    COURSE_ENROLLMENT_DATE_COMPLETE = 'course_enrollment_date_complete',
    COURSE_ENROLLMENT_DATE_BEGIN_VALIDITY = 'course_enrollment_date_begin_validity',
    COURSE_ENROLLMENT_DATE_EXPIRE_VALIDITY = 'course_enrollment_date_expire_validity',
    COURSE_ENROLLMENT_STATUS = 'course_enrollment_status',

    // Learning Objects fields
    LO_TITLE = 'lo_title',
    LO_BOOKMARK = 'lo_bookmark',
    LO_DATE_ATTEMPT = 'lo_date_attempt',
    LO_FIRST_ATTEMPT = 'lo_first_attempt',
    LO_SCORE = 'lo_score',
    LO_STATUS = 'lo_status',
    LO_TYPE = 'lo_type',
    LO_VERSION = 'lo_version',
    LO_DATE_COMPLETE = 'lo_date_complete',

    // Statistic fields
    STATS_USER_COURSE_COMPLETION_PERCENTAGE = 'stats_user_course_completion_percentage',
    STATS_TOTAL_TIME_IN_COURSE = 'stats_total_time_in_course',
    STATS_TOTAL_SESSIONS_IN_COURSE = 'stats_total_sessions_in_course',
    STATS_NUMBER_OF_ACTIONS = 'stats_number_of_actions',
    STATS_ENROLLED_USERS = 'stats_enrolled_users',
    STATS_USERS_ENROLLED_IN_COURSE = 'stats_users_enrolled_in_course',
    STATS_NOT_STARTED_USERS = 'stats_not_started_users',
    STATS_NOT_STARTED_USERS_PERCENTAGE = 'stats_not_started_users_percentage',
    STATS_IN_PROGRESS_USERS = 'stats_in_progress_users',
    STATS_IN_PROGRESS_USERS_PERCENTAGE = 'stats_in_progress_users_percentage',
    STATS_COMPLETED_USERS = 'stats_completed_users',
    STATS_COMPLETED_USERS_PERCENTAGE = 'stats_completed_users_percentage',
    STATS_COURSE_RATING = 'stats_course_rating',
    STATS_ACTIVE = 'stats_active',
    STATS_EXPIRED = 'stats_expired',
    STATS_ISSUED = 'stats_issued',
    STATS_ARCHIVED = 'stats_archived',
    STATS_SESSION_TIME = 'stats_session_time',
    STATS_USER_FLOW = 'stats_user_flow',
    STATS_USER_FLOW_PERCENTAGE = 'stats_user_flow_percentage',
    STATS_USER_FLOW_YES_NO = 'stats_user_flow_yes_no',
    STATS_USER_COURSE_FLOW_PERCENTAGE = 'stats_user_course_flow_percentage',
    STATS_USER_COURSE_TIME_SPENT_BY_FLOW = 'stats_user_course_time_spent_by_flow',

    STATS_USER_FLOW_MS_TEAMS_YES_NO = 'stats_user_flow_ms_teams_yes_no',
    STATS_USER_COURSE_FLOW_MS_TEAMS_PERCENTAGE = 'stats_user_course_flow_ms_teams_percentage',
    STATS_USER_COURSE_TIME_SPENT_BY_FLOW_MS_TEAMS = 'stats_user_course_time_spent_by_flow_ms_teams',
    STATS_USER_FLOW_MS_TEAMS = 'stats_user_flow_ms_teams',
    STATS_USER_FLOW_MS_TEAMS_PERCENTAGE = 'stats_user_flow_ms_teams_percentage',

    STATS_COURSE_ACCESS_FROM_MOBILE = 'stats_course_access_from_mobile',
    STATS_PERCENTAGE_OF_COURSE_FROM_MOBILE = 'stats_percentage_of_course_from_mobile',
    STATS_TIME_SPENT_FROM_MOBILE = 'stats_time_spent_from_mobile',
    STATS_ACCESS_FROM_MOBILE = 'stats_access_from_mobile', // Number of Users enrolled in the course who accessed from the Mobile App
    STATS_PERCENTAGE_ACCESS_FROM_MOBILE = 'stats_percentage_access_from_mobile', // Percentage of Users enrolled in the course who accessed from the Mobile App

    STATS_PATH_ENROLLED_USERS = 'stats_path_enrolled_users',
    STATS_PATH_NOT_STARTED_USERS = 'stats_path_not_started_users',
    STATS_PATH_NOT_STARTED_USERS_PERCENTAGE = 'stats_path_not_started_users_percentage',
    STATS_PATH_IN_PROGRESS_USERS = 'stats_path_in_progress_users',
    STATS_PATH_IN_PROGRESS_USERS_PERCENTAGE = 'stats_path_in_progress_users_percentage',
    STATS_PATH_COMPLETED_USERS = 'stats_path_completed_users',
    STATS_PATH_COMPLETED_USERS_PERCENTAGE = 'stats_path_completed_users_percentage',

    // Session fields
    SESSION_UNIQUE_ID = 'session_unique_id',
    SESSION_INTERNAL_ID = 'session_internal_id',
    SESSION_NAME = 'session_name',
    SESSION_CODE = 'session_code',
    SESSION_EVALUATION_SCORE_BASE = 'session_evaluation_score_base',
    SESSION_START_DATE = 'session_start_date',
    SESSION_END_DATE = 'session_end_date',
    SESSION_TIME_SESSION = 'session_time_session',
    SESSION_INSTRUCTOR_USERIDS = 'session_instructor_userids',
    SESSION_INSTRUCTOR_FULLNAMES = 'session_instructor_fullnames',
    SESSION_ATTENDANCE_TYPE = 'session_attendance_type',
    SESSION_MAXIMUM_ENROLLMENTS = 'session_maximum_enrollments',

    // Session event fields
    SESSION_EVENT_NAME = 'session_event_name',
    SESSION_EVENT_ID = 'session_event_id',
    SESSION_EVENT_DATE = 'session_event_date',
    SESSION_EVENT_START_DATE = 'session_event_start_date',
    SESSION_EVENT_DURATION = 'session_event_duration',
    SESSION_EVENT_TIMEZONE = 'session_event_timezone',
    SESSION_EVENT_TYPE = 'session_event_type',
    SESSION_EVENT_INSTRUCTOR_USER_NAME = 'session_event_instructor_user_name',
    SESSION_EVENT_INSTRUCTOR_FULLNAME = 'session_event_instructor_fullname',
    SESSION_INSTRUCTOR_LIST = 'session_instructor_list',
    SESSION_MINIMUM_ENROLLMENTS = 'session_minimum_enrollments',
    SESSION_COMPLETION_RATE = 'session_completion_rate',
    SESSION_HOURS = 'session_hours',

    // Session usage statistics fields
    SESSION_USER_ENROLLED = 'session_user_enrolled',
    SESSION_USER_COMPLETED = 'session_user_completed',
    SESSION_USER_WAITING = 'session_user_waiting',
    SESSION_USER_IN_PROGRESS = 'session_user_in_progress',
    SESSION_COMPLETION_MODE = 'session_completion_mode',
    SESSION_EVALUATION_STATUS_NOT_SET = 'session_evaluation_status_not_set',
    SESSION_EVALUATION_STATUS_NOT_PASSED = 'session_evaluation_status_not_passed',
    SESSION_EVALUATION_STATUS_PASSED = 'session_evaluation_status_passed',
    SESSION_ENROLLED_USERS = 'session_enrolled_users',
    SESSION_SESSION_TIME = 'session_session_time',
    SESSION_TRAINING_MATERIAL_TIME = 'session_training_material_time',


    // Event fields
    EVENT_INSTRUCTORS_LIST = 'event_instructor_list',
    EVENT_ATTENDANCE_STATUS_NOT_SET = 'event_attendance_status_not_set_perc',
    EVENT_ATTENDANCE_STATUS_ABSENT_PERC = 'event_attendance_status_absent_perc',
    EVENT_ATTENDANCE_STATUS_PRESENT_PERC = 'event_attendance_status_prsent_perc',
    EVENT_AVERAGE_SCORE = 'event_average_score',

    // Enrollment fields
    ENROLLMENT_ATTENDANCE = 'enrollment_attendance',
    ENROLLMENT_DATE = 'enrollment_date',
    ENROLLMENT_ENROLLMENT_STATUS = 'enrollment_enrollment_status',
    ENROLLMENT_EVALUATION_STATUS = 'enrollment_evaluation_status',
    ENROLLMENT_INSTRUCTOR_FEEDBACK = 'enrollment_instructor_feedback',
    ENROLLMENT_LEARNER_EVALUATION = 'enrollment_learner_evaluation',
    ENROLLMENT_USER_COURSE_LEVEL = 'enrollment_user_course_level',
    ENROLLMENT_USER_SESSION_STATUS = 'enrollment_user_session_status',
    ENROLLMENT_USER_SESSION_SUBSCRIBE_DATE = 'enrollment_user_session_subscribe_date',
    ENROLLMENT_USER_SESSION_COMPLETE_DATE = 'enrollment_user_session_complete_date',
    ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_HOURS = 'enrollment_user_session_event_attendance_hours',
    ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS = 'enrollment_user_session_event_attendance_status',


    // Certifications
    CERTIFICATION_TITLE = 'certification_title',
    CERTIFICATION_CODE = 'certification_code',
    CERTIFICATION_DESCRIPTION = 'certification_description',
    CERTIFICATION_DURATION = 'certification_duration',
    CERTIFICATION_COMPLETED_ACTIVITY = 'certification_completed_activity',
    CERTIFICATION_ISSUED_ON = 'certification_issued_on',
    CERTIFICATION_TO_RENEW_IN = 'certification_to_renew_in',
    CERTIFICATION_STATUS = 'certification_status',

    // Badge fields
    BADGE_DESCRIPTION = 'badge_description',
    BADGE_NAME = 'badge_name',
    BADGE_SCORE = 'badge_score',

    // Badge Assignment fields
    BADGE_ISSUED_ON = 'badge_issued_on',

    // External Training fields
    EXTERNAL_TRAINING_COURSE_NAME = 'external_training_course_name',
    EXTERNAL_TRAINING_COURSE_TYPE = 'external_training_course_type',
    EXTERNAL_TRAINING_SCORE = 'external_training_score',
    EXTERNAL_TRAINING_DATE = 'external_training_date',
    EXTERNAL_TRAINING_DATE_START = 'external_training_date_start',
    EXTERNAL_TRAINING_CREDITS = 'external_training_credits',
    EXTERNAL_TRAINING_TRAINING_INSTITUTE = 'external_training_training_institute',
    EXTERNAL_TRAINING_CERTIFICATE = 'external_training_certificate',
    EXTERNAL_TRAINING_STATUS = 'external_training_status',

    // Ecommerce Transaction fields
    ECOMMERCE_TRANSACTION_ADDRESS_1 = 'ecommerce_transaction_address_1',
    ECOMMERCE_TRANSACTION_ADDRESS_2 = 'ecommerce_transaction_address_2',
    ECOMMERCE_TRANSACTION_CITY = 'ecommerce_transaction_city',
    ECOMMERCE_TRANSACTION_COMPANY_NAME = 'ecommerce_transaction_company_name',
    ECOMMERCE_TRANSACTION_COUPON_CODE = 'ecommerce_transaction_coupon_code',
    ECOMMERCE_TRANSACTION_COUPON_DESCRIPTION = 'ecommerce_transaction_coupon_description',
    ECOMMERCE_TRANSACTION_DISCOUNT = 'ecommerce_transaction_discount',
    ECOMMERCE_TRANSACTION_EXTERNAL_TRANSACTION_ID = 'ecommerce_transaction_external_transaction_id',
    ECOMMERCE_TRANSACTION_PAYMENT_DATE = 'ecommerce_transaction_payment_date',
    ECOMMERCE_TRANSACTION_PAYMENT_METHOD = 'ecommerce_transaction_payment_method',
    ECOMMERCE_TRANSACTION_PAYMENT_STATUS = 'ecommerce_transaction_payment_status',
    ECOMMERCE_TRANSACTION_PRICE = 'ecommerce_transaction_price',
    ECOMMERCE_TRANSACTION_QUANTITY = 'ecommerce_transaction_quantity',
    ECOMMERCE_TRANSACTION_STATE = 'ecommerce_transaction_state',
    ECOMMERCE_TRANSACTION_SUBTOTAL_PRICE = 'ecommerce_transaction_subtotal_price',
    ECOMMERCE_TRANSACTION_TOTAL_PRICE = 'ecommerce_transaction_total_price',
    ECOMMERCE_TRANSACTION_TRANSACTION_CREATION_DATE = 'ecommerce_transaction_transaction_creation_date',
    ECOMMERCE_TRANSACTION_TRANSACTION_ID = 'ecommerce_transaction_transaction_id',
    ECOMMERCE_TRANSACTION_VAT_NUMBER = 'ecommerce_transaction_vat_number',
    ECOMMERCE_TRANSACTION_ZIP_CODE = 'ecommerce_transaction_zip_code',

    // Ecommerce Transaction Item fields
    ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_CODE = 'ecommerce_transaction_item_course_lp_code',
    ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_NAME = 'ecommerce_transaction_item_course_lp_name',
    ECOMMERCE_TRANSACTION_ITEM_START_DATE = 'ecommerce_transaction_item_start_date',
    ECOMMERCE_TRANSACTION_ITEM_END_DATE = 'ecommerce_transaction_item_end_date',
    ECOMMERCE_TRANSACTION_ITEM_ILT_WEBINAR_SESSION_NAME = 'ecommerce_transaction_item_ilt_webinar_session_name',
    ECOMMERCE_TRANSACTION_ITEM_ILT_LOCATION = 'ecommerce_transaction_item_ilt_location',
    ECOMMERCE_TRANSACTION_ITEM_TYPE = 'ecommerce_transaction_item_type',

    // Content Partner fields
    CONTENT_PARTNERS_AFFILIATE = 'content_partners_affiliate',
    CONTENT_PARTNERS_REFERRAL_LINK_CODE = 'content_partners_referral_link_code',
    CONTENT_PARTNERS_REFERRAL_LINK_SOURCE = 'content_partners_referral_link_source',

    // Asset fields
    ASSET_NAME = 'asset_name',
    CHANNELS = 'channels',
    PUBLISHED_BY = 'published_by',
    PUBLISHED_ON = 'published_on',
    LAST_EDIT_BY = 'last_edit_by',
    ASSET_TYPE = 'asset_type',
    ASSET_AVERAGE_REVIEW = 'asset_average_review',
    ASSET_DESCRIPTION = 'asset_description',
    ASSET_TAG = 'asset_tag',
    ASSET_SKILL = 'asset_skill',
    ASSET_LAST_ACCESS = 'asset_last_access',
    ASSET_FIRST_ACCESS = 'asset_first_access',
    ASSET_NUMBER_ACCESS = 'asset_number_access',

    // Asset statistics fields
    ANSWER_DISLIKES = 'answer_dislikes',
    ANSWER_LIKES = 'answer_likes',
    ANSWERS = 'answers',
    ASSET_RATING = 'asset_rating',
    AVERAGE_REACTION_TIME = 'average_reaction_time',
    BEST_ANSWERS = 'best_answers',
    GLOBAL_WATCH_RATE = 'global_watch_rate',
    INVITED_PEOPLE = 'invited_people',
    NOT_WATCHED = 'not_watched',
    QUESTIONS = 'questions',
    TOTAL_VIEWS = 'total_views',
    WATCHED = 'watched',

    // User - Assets ( ex User - Contributions)
    INVOLVED_CHANNELS = 'involved_channels',
    PUBLISHED_ASSETS = 'published_assets',
    UNPUBLISHED_ASSETS = 'unpublished_assets',
    PRIVATE_ASSETS = 'private_assets',
    UPLOADED_ASSETS = 'uploaded_assets',

    // Survey
    SURVEY_COMPLETION_DATE = 'survey_completion_date',
    SURVEY_COMPLETION_ID = 'survey_completion_id',
    ANSWER_USER = 'answer_user',
    QUESTION_ID = 'question_id',
    QUESTION = 'question',
    QUESTION_TYPE = 'question_type',
    QUESTION_MANDATORY = 'question_mandatory',
    SURVEY_DESCRIPTION = 'survey_description',
    SURVEY_ID = 'survey_id',
    SURVEY_TITLE = 'survey_title',
    SURVEY_TRACKING_TYPE = 'survey_tracking_type',
}

/**
 * Contains the fields that are of type string in athena
 * and so if added to the ORDER BY clause can be passed as input to the LOWER function
 */
export const fieldListTypeString: string[] = [
    FieldsList.USER_USERID,
    FieldsList.USER_FIRSTNAME,
    FieldsList.USER_LASTNAME,
    FieldsList.USER_FULLNAME,
    FieldsList.USER_EMAIL,
    FieldsList.USER_DIRECT_MANAGER,
    FieldsList.COURSE_NAME,
    FieldsList.COURSE_CODE,
    FieldsList.COURSE_CATEGORY_CODE,
    FieldsList.COURSE_CATEGORY_NAME,
    FieldsList.COURSE_CATEGORY_CODE,
    FieldsList.COURSE_CATEGORY_NAME,
    FieldsList.SESSION_NAME,
    FieldsList.SESSION_CODE,
    FieldsList.WEBINAR_SESSION_NAME,
    FieldsList.WEBINAR_SESSION_WEBINAR_TOOL,
    FieldsList.GROUP_GROUP_OR_BRANCH_NAME,
    FieldsList.LP_NAME,
    FieldsList.LP_CODE,
    FieldsList.INVOLVED_CHANNELS,
    FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_1,
    FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_2,
    FieldsList.ECOMMERCE_TRANSACTION_CITY,
    FieldsList.ECOMMERCE_TRANSACTION_COMPANY_NAME,
    FieldsList.ECOMMERCE_TRANSACTION_COUPON_CODE,
    FieldsList.ECOMMERCE_TRANSACTION_COUPON_DESCRIPTION,
    FieldsList.ECOMMERCE_TRANSACTION_EXTERNAL_TRANSACTION_ID,
    FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_METHOD,
    FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_STATUS,
    FieldsList.ECOMMERCE_TRANSACTION_STATE,
    FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_CODE,
    FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_NAME,
    FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_WEBINAR_SESSION_NAME,
    FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_LOCATION,
    FieldsList.ECOMMERCE_TRANSACTION_ITEM_TYPE,
    FieldsList.CONTENT_PARTNERS_AFFILIATE,
    FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_CODE,
    FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_SOURCE,
    FieldsList.ASSET_NAME,
    FieldsList.PUBLISHED_BY,
    FieldsList.CHANNELS
];

export enum FieldTranslation {
    // Standard translations
    YES = 'yes',
    NO = 'no',
    NEVER = 'never',

    HR = 'hr',
    MIN = 'min',

    // User translations
    USER_LEVEL_USER = 'user_level_user',
    USER_LEVEL_POWERUSER = 'user_level_poweruser',
    USER_LEVEL_GODADMIN = 'user_level_godadmin',

    // Course fields
    COURSE_STATUS_PREPARATION = 'course_status_preparation',
    COURSE_STATUS_EFFECTIVE = 'course_status_effective',
    COURSE_TYPE_ELEARNING = 'course_type_elearning',
    COURSE_TYPE_CLASSROOM = 'course_type_classroom',
    COURSE_TYPE_WEBINAR = 'course_type_webinar',

    // Courseuser fields
    COURSEUSER_LEVEL_STUDENT = 'courseuser_level_students',
    COURSEUSER_LEVEL_TUTOR = 'courseuser_level_tutor',
    COURSEUSER_LEVEL_TEACHER = 'courseuser_level_teacher',
    COURSEUSER_STATUS_WAITING_LIST = 'courseuser_status_waiting_list',
    COURSEUSER_STATUS_CONFIRMED = 'courseuser_status_confirmed',
    COURSEUSER_STATUS_SUBSCRIBED = 'courseuser_status_subscribed',
    COURSEUSER_STATUS_IN_PROGRESS = 'courseuser_status_in_progress',
    COURSEUSER_STATUS_COMPLETED = 'courseuser_status_completed',
    COURSEUSER_STATUS_SUSPENDED = 'courseuser_status_suspended',
    COURSEUSER_STATUS_OVERBOOKING = 'courseuser_status_overbooking',
    COURSEUSER_STATUS_WAITING = 'courseuser_status_waiting',
    COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM = 'courseuser_status_enrollments_to_confirm',
    COURSEUSER_STATUS_ENROLLED = 'courseuser_status_enrolled',
    COURSEUSER_STATUS_NEW_TRANSLATION = 'courseuser_status_new_translation',

    // Assignment Types
    ASSIGNMENT_TYPE_MANDATORY = 'assignment_type_mandatory',
    ASSIGNMENT_TYPE_REQUIRED = 'assignment_type_required',
    ASSIGNMENT_TYPE_RECOMMENDED = 'assignment_type_recommended',
    ASSIGNMENT_TYPE_OPTIONAL = 'assignment_type_optional',

    // Webinar fields
    WEBINAR_SESSION_USER_EVAL_STATUS_PASSED = 'webinar_session_user_eval_status_passed',
    WEBINAR_SESSION_USER_EVAL_STATUS_FAILED = 'webinar_session_user_eval_status_failed',

    // LO fields
    LO_BOOKMARK_START = 'lo_bookmark_start',
    LO_BOOKMARK_FINAL = 'lo_bookmark_fianl',
    LO_STATUS_COMPLETED = 'lo_status_completed',
    LO_STATUS_FAILED = 'lo_status_failed',
    LO_STATUS_IN_ITINERE = 'lo_status_in_itinere',
    LO_STATUS_NOT_STARTED = 'lo_status_not_started',
    LO_TYPE_AUTHORING = 'lo_type_authoring',
    LO_TYPE_DELIVERABLE = 'lo_type_deliverable',
    LO_TYPE_FILE = 'lo_type_file',
    LO_TYPE_HTMLPAGE = 'lo_type_htmlpage',
    LO_TYPE_POLL = 'lo_type_poll',
    LO_TYPE_SCORM = 'lo_type_scorm',
    LO_TYPE_TEST = 'lo_type_test',
    LO_TYPE_TINCAN = 'lo_type_tincan',
    LO_TYPE_VIDEO = 'lo_type_video',
    LO_TYPE_AICC = 'lo_type_aicc',
    LO_TYPE_ELUCIDAT = 'lo_type_elucidat',
    LO_TYPE_GOOGLEDRIVE = 'lo_type_googledrive',
    LO_TYPE_LTI = 'lo_type_lti',
    LO_DATE_COMPLETE = 'lo_date_complete',

    // Time Units
    DAYS = 'days',
    WEEKS = 'weeks',
    MONTHS = 'months',
    YEARS = 'years',

    // External Training Status
    EXTERNAL_TRAINING_STATUS_APPROVED = 'external_training_status_approved',
    EXTERNAL_TRAINING_STATUS_WAITING = 'external_training_status_waiting',
    EXTERNAL_TRAINING_STATUS_REJECTED = 'external_training_status_rejected',

    PAYMENT_STATUS_CANCELED = 'payment_status_canceled',
    PAYMENT_STATUS_PENDING = 'payment_status_pending',
    PAYMENT_STATUS_SUCCESSFUL = 'payment_status_successful',
    PAYMENT_STATUS_FAILED = 'payment_status_failed',

    COURSE = 'course',
    COURSEPATH = 'coursepath',
    COURSESEATS = 'courseseats',
    SUBSCRIPTION_PLAN = 'subscription_plan',

    FREE_PURCHASE = 'free_purchase',

    // Asset type
    VIDEO = 'video',
    DOC = 'doc',
    EXCEL = 'excel',
    PPT = 'ppt',
    PDF = 'pdf',
    TEXT = 'text',
    IMAGE = 'image',
    QUESTION = 'question',
    RESPONSE = 'response',
    OTHER = 'other',
    DEFAULT_OTHER = 'default_other',
    DEFAULT_MUSIC = 'default_music',
    DEFAULT_ARCHIVE = 'default_archive',
    LINKS = 'links',
    GOOGLE_DRIVE_DOCS = 'google_drive_docs',
    GOOGLE_DRIVE_SHEETS = 'google_drive_sheets',
    GOOGLE_DRIVE_SLIDES = 'google_drive_slides',
    PLAYLIST = 'playlist',
    YOUTUBE = 'youtube',
    VIMEO = 'vimeo',
    WISTIA = 'wistia',

    CERTIFICATION_ACTIVE = 'certification_active',
    CERTIFICATION_EXPIRED = 'certification_expired',
    CERTIFICATION_ARCHIVED = 'certification_archived',

    // Attendances Types
    SESSION_ATTENDANCE_TYPE_BLEENDED = 'session_attendance_type_blended',
    SESSION_ATTENDANCE_TYPE_FLEXIBLE = 'session_attendance_type_flexible',
    SESSION_ATTENDANCE_TYPE_FULLONLINE = 'session_attendance_type_fullOnline',
    SESSION_ATTENDANCE_TYPE_FULLONSITE = 'session_attendance_type_fullOnsite',
    SESSION_MANUAL = 'session_manual',
    SESSION_EVALUATION_BASED = 'session_evaluation_based',
    SESSION_ATTENDANCE_BASED = 'session_attendance_based',
    SESSION_TRAINING_MATERIAL_BASED = 'session_training_material_based',

    // Attendance Statuses
    ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_PRESENT = 'enrollment_user_session_event_attendance_status_present',
    ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_ABSENT = 'enrollment_user_session_event_attendance_status_ansent',
    ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_NOT_SET = 'enrollment_user_session_event_attendance_status_not_set',

    // Survey
    LOCAL_TRACKING = 'local_tracking',
    SHARED_TRACKING = 'shared_tracking',
    CHOICE = 'choice',
    CHOICE_MULTIPLE = 'choice_multiple',
    INLINE_CHOICE = 'inline_choice',
    EXTENDED_TEXT = 'extended_text',
    LIKERT_SCALE = 'likert_scale',

    LP_ENROLLMENT_COMPLETION_PERCENTAGE_OLD_TRANSLATION = 'lp_enrollment_completion_percentage_old_translation',
    LP_UNDER_MAINTENANCE = 'lp_under_maintenance',
    LP_PUBLISHED = 'lp_published',
}

export class ReportManagerDataResponse extends BaseReportManagerResponse {
    data?: ReportManagerData;
    error?: string;
    constructor() {
        super();
    }
}

export const fieldTranslationsKey = {
    // Standard translations
    [FieldTranslation.YES]: {key: '_YES', module: 'report'},
    [FieldTranslation.NO]: {key: '_NO', module: 'report'},
    [FieldTranslation.NEVER]: {key: '_NEVER', module: 'report'},
    [FieldTranslation.FREE_PURCHASE]: {key: 'Free purchase', module: 'report'},
    [FieldTranslation.HR]: {key: 'hr', module: 'report'},
    [FieldTranslation.MIN]: {key: 'min', module: 'report'},

    // Users translations
    [FieldsList.USER_ID]: {key: 'User unique ID', module: 'report'},
    [FieldsList.USER_USERID]: {key: 'Username', module: 'report'},
    [FieldsList.USER_FIRSTNAME]: {key: 'First Name', module: 'report'},
    [FieldsList.USER_LASTNAME]: {key: 'Last Name', module: 'report'},
    [FieldsList.USER_FULLNAME]: {key: 'Fullname', module: 'report'},
    [FieldsList.USER_EMAIL]: {key: 'Email', module: 'report'},
    [FieldsList.USER_EMAIL_VALIDATION_STATUS]: {key: 'Email Validation Status', module: 'report'},
    [FieldsList.USER_LEVEL]: {key: 'User Level', module: 'report'},
    // User level details
    [FieldTranslation.USER_LEVEL_USER]: {key: '_DIRECTORY_/framework/level/user', module: 'report'},
    [FieldTranslation.USER_LEVEL_POWERUSER]: {key: '_DIRECTORY_/framework/level/admin', module: 'report'},
    [FieldTranslation.USER_LEVEL_GODADMIN]: {key: '_DIRECTORY_/framework/level/godadmin', module: 'report'},
    [FieldsList.USER_DEACTIVATED]: {key: 'Deactivated', module: 'report'},
    [FieldsList.USER_EXPIRATION]: {key: 'User expiration date', module: 'report'},
    [FieldsList.USER_SUSPEND_DATE]: {key: 'User Suspension Date', module: 'report'},
    [FieldsList.USER_REGISTER_DATE]: {key: 'User Creation Date', module: 'report'},
    [FieldsList.USER_LAST_ACCESS_DATE]: {key: 'User last access date', module: 'report'},
    [FieldsList.USER_BRANCH_NAME]: {key: 'Branch name', module: 'report'},
    [FieldsList.USER_BRANCH_PATH]: {key: 'Branch path', module: 'report'},
    [FieldsList.USER_BRANCHES_CODES]: {key: 'Branches Codes', module: 'report'},
    [FieldsList.USER_AUTH_APP_PAIRED]: {key: 'Authenticator App Paired', module: 'report'},
    [FieldsList.USER_MANAGER_PERMISSIONS]: {key: 'Manager Permissions', module: 'report'},
    [FieldsList.USER_TIMEZONE]: {key: 'Timezone', module: 'report'},
    [FieldsList.USER_LANGUAGE]: {key: 'Language', module: 'report'},
    [FieldsList.USER_DIRECT_MANAGER]: {key: 'Direct Manager', module: 'report'},

    // Course fields
    [FieldsList.COURSE_ID]: {key: 'Course Internal Id', module: 'report'},
    [FieldsList.COURSE_CODE]: {key: '_COURSE_CODE', module: 'report'},
    [FieldsList.COURSE_NAME]: {key: '_COURSE_NAME', module: 'report'},
    [FieldsList.COURSE_CATEGORY_CODE]: {key: 'Course Category Code', module: 'report'},
    [FieldsList.COURSE_CATEGORY_NAME]: {key: 'Course Category', module: 'report'},
    [FieldsList.COURSE_STATUS]: {key: 'Course Status', module: 'report'},
    [FieldsList.COURSE_UNIQUE_ID]: {key: 'Course Unique ID', module: 'report'},
    [FieldsList.COURSE_SKILLS]: {key: 'Skills in course', module: 'report'},
    // Course status details
    [FieldTranslation.COURSE_STATUS_PREPARATION]: {key: '_CST_PREPARATION', module: 'report'}, // 0
    [FieldTranslation.COURSE_STATUS_EFFECTIVE]: {key: '_CST_CONFIRMED', module: 'report'}, // 2
    [FieldsList.COURSE_CREDITS]: {key: '_CREDITS', module: 'report'},
    [FieldsList.COURSE_DURATION]: {key: 'Course duration', module: 'report'},
    [FieldsList.COURSE_TYPE]: {key: 'Course Type', module: 'report'},
    // Course type details
    [FieldTranslation.COURSE_TYPE_ELEARNING]: {key: 'E-Learning', module: 'report'},
    [FieldTranslation.COURSE_TYPE_CLASSROOM]: {key: 'Classroom', module: 'report'},
    [FieldTranslation.COURSE_TYPE_WEBINAR]: {key: 'Webinar', module: 'report'},
    [FieldsList.COURSE_DATE_BEGIN]: {key: 'Course Start Date', module: 'report'},
    [FieldsList.COURSE_DATE_END]: {key: '_COURSE_END', module: 'report'},
    [FieldsList.COURSE_EXPIRED]: {key: 'Course has expired', module: 'report'},
    [FieldsList.COURSE_CREATION_DATE]: {key: 'Course Creation Date', module: 'report'},
    [FieldsList.COURSE_E_SIGNATURE]: {key: 'E-Signature', module: 'report'},
    [FieldsList.COURSE_E_SIGNATURE_HASH]: {key: 'E-Signature Hash', module: 'report'},
    [FieldsList.COURSE_LANGUAGE]: {key: 'Language', module: 'report'},
    // [FieldsList.COURSEUSER_ENROLLMENT_CODE]: {key: 'Enrollment Code', module: 'report'},
    // [FieldsList.COURSEUSER_ENROLLMENT_CODESET]: {key: 'Enrollment Code Set', module: 'report'},

    // Webinar Session
    [FieldsList.WEBINAR_SESSION_NAME]: {key: 'Session name', module: 'report'},
    [FieldsList.WEBINAR_SESSION_EVALUATION_SCORE_BASE]: {key: 'Evaluation score base', module: 'report'},
    [FieldsList.WEBINAR_SESSION_START_DATE]: {key: 'Session date begin', module: 'report'},
    [FieldsList.WEBINAR_SESSION_END_DATE]: {key: 'Session date end', module: 'report'},
    [FieldsList.WEBINAR_SESSION_SESSION_TIME]: {key: 'Time in Session', module: 'report'},
    [FieldsList.WEBINAR_SESSION_WEBINAR_TOOL]: {key: 'Webinar Tool', module: 'report'},
    [FieldsList.WEBINAR_SESSION_TOOL_TIME_IN_SESSION]: {key: 'Webinar Tool Time in Session', module: 'report'},

    // Webinar Session User
    [FieldsList.WEBINAR_SESSION_USER_LEVEL]: {key: 'User Course Level', module: 'report'},
    [FieldsList.WEBINAR_SESSION_USER_ENROLL_DATE]: {key: 'Enrollment date', module: 'report'},
    [FieldsList.WEBINAR_SESSION_USER_STATUS]: {key: 'Course Enrollment Status', module: 'report'},
    [FieldsList.WEBINAR_SESSION_USER_LEARN_EVAL]: {key: 'Learner\'s evaluation', module: 'report'},
    [FieldsList.WEBINAR_SESSION_USER_EVAL_STATUS]: {key: 'Evaluation Status', module: 'report'},
    [FieldsList.WEBINAR_SESSION_USER_INSTRUCTOR_FEEDBACK]: {key: 'Instructor Feedback', module: 'report'},
    [FieldsList.WEBINAR_SESSION_USER_ENROLLMENT_STATUS]: {key: 'Session Enrollment Status', module: 'report'},
    [FieldsList.WEBINAR_SESSION_USER_SUBSCRIBE_DATE]: {key: 'Session Enrollment Date', module: 'report'},
    [FieldsList.WEBINAR_SESSION_USER_COMPLETE_DATE]: {key: 'Session Completion Date', module: 'report'},


    // Courseuser fields
    [FieldsList.COURSEUSER_LEVEL]: {key: 'User Course Level', module: 'report'},
    // Courseuser level details
    [FieldTranslation.COURSEUSER_LEVEL_STUDENT]: {key: '_LEVEL_3', module: 'report'}, // 3
    [FieldTranslation.COURSEUSER_LEVEL_TUTOR]: {key: '_LEVEL_4', module: 'report'}, // 4
    [FieldTranslation.COURSEUSER_LEVEL_TEACHER]: {key: '_LEVEL_6', module: 'report'}, // 6
    [FieldsList.COURSEUSER_DATE_INSCR]: {key: 'Enrollment date', module: 'report'},
    [FieldsList.COURSEUSER_DATE_FIRST_ACCESS]: {key: 'Course First Access Date', module: 'report'},
    [FieldsList.COURSEUSER_DATE_LAST_ACCESS]: {key: 'Course Last Access Date', module: 'report'},
    [FieldsList.COURSEUSER_DATE_COMPLETE]: {key: 'Completion date', module: 'report'},
    [FieldsList.COURSEUSER_EXPIRATION_DATE]: {key: 'Enrollment Expiration Date', module: 'report'},
    [FieldsList.COURSEUSER_STATUS]: {key: 'Enrollment status', module: 'report'},
    [FieldTranslation.COURSEUSER_STATUS_NEW_TRANSLATION]: {key: 'Course Enrollment Status', module: 'report'},
    // Courseuser status details
    [FieldTranslation.COURSEUSER_STATUS_WAITING_LIST]: {key: '_WAITING_USERS', module: 'report'}, // status -2 waiting 1
    [FieldTranslation.COURSEUSER_STATUS_CONFIRMED]: {key: '_USER_STATUS_CONFIRMED', module: 'report'}, // -1
    [FieldTranslation.COURSEUSER_STATUS_ENROLLMENTS_TO_CONFIRM]: {key: 'Enrollments to confirm', module: 'report'}, // status -1 waiting 1
    [FieldTranslation.COURSEUSER_STATUS_ENROLLED]: {key: '_ENROL_COUNT', module: 'report'}, // status -1 waiting 1
    [FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED]: {key: 'Enrolled', module: 'report'}, // status 0 waiting 0
    [FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS]: {key: '_USER_STATUS_BEGIN', module: 'report'}, // status 1 waiting 0
    [FieldTranslation.COURSEUSER_STATUS_COMPLETED]: {key: '_USER_STATUS_END', module: 'report'}, // // status 2 waiting 0
    [FieldTranslation.COURSEUSER_STATUS_SUSPENDED]: {key: '_USER_STATUS_SUSPEND', module: 'report'}, // // status 3 waiting 0
    [FieldTranslation.COURSEUSER_STATUS_OVERBOOKING]: {key: '_USER_STATUS_OVERBOOKING', module: 'report'}, // // status 4 waiting 0
    [FieldTranslation.COURSEUSER_STATUS_WAITING]: {key: '_WAITING', module: 'report'}, // waiting = 1
    [FieldTranslation.WEBINAR_SESSION_USER_EVAL_STATUS_PASSED]: {key: '_PASSED', module: 'report'},
    [FieldTranslation.WEBINAR_SESSION_USER_EVAL_STATUS_FAILED]: {key: '_PROGRESS_FAILED', module: 'report'},
    [FieldsList.COURSEUSER_DATE_BEGIN_VALIDITY]: {key: 'Enrollment Start Date', module: 'report'},
    [FieldsList.COURSEUSER_DATE_EXPIRE_VALIDITY]: {key: 'Enrollment End Date', module: 'report'},
    [FieldsList.COURSEUSER_SCORE_GIVEN]: {key: 'Final score', module: 'report'},
    [FieldsList.COURSEUSER_INITIAL_SCORE_GIVEN]: {key: '_COURSES_FILTER_SCORE_INIT', module: 'report'},
    [FieldsList.COURSEUSER_DAYS_LEFT]: {key: 'Days Left', module: 'report'},
    [FieldsList.COURSEUSER_ASSIGNMENT_TYPE]: {key: 'Assignment Type', module: 'report'},
    [FieldsList.ENROLLMENT_ARCHIVED]: {key: 'Archived Enrollment (Yes / No)', module: 'report'},
    [FieldsList.ENROLLMENT_ARCHIVING_DATE]: {key: 'Archive Date', module: 'report'},

    // Groups fields
    [FieldsList.GROUP_GROUP_OR_BRANCH_NAME]: {key: 'Group/Branch Name', module: 'report'},
    [FieldsList.GROUP_MEMBERS_COUNT]: {key: 'Members', module: 'report'},

    // Statistic fields
    [FieldsList.STATS_USER_COURSE_COMPLETION_PERCENTAGE]: {key: 'Course Progression (%)', module: 'report'},
    [FieldsList.STATS_TOTAL_TIME_IN_COURSE]: {key: 'Training Material Time (sec)', module: 'report'},
    [FieldsList.STATS_TOTAL_SESSIONS_IN_COURSE]: {key: '_TH_USER_NUMBER_SESSION', module: 'report'},
    [FieldsList.STATS_NUMBER_OF_ACTIONS]: {key: 'Number of Actions', module: 'report'},
    [FieldsList.STATS_ENROLLED_USERS]: {key: 'Enrolled users', module: 'report'},
    [FieldsList.STATS_USERS_ENROLLED_IN_COURSE]: {key: 'Users Enrolled in Course', module: 'report'},
    [FieldsList.STATS_NOT_STARTED_USERS]: {key: 'Not Started User Status', module: 'report'},
    [FieldsList.STATS_NOT_STARTED_USERS_PERCENTAGE]: {key: 'Not Started User Status Percentage', module: 'report'},
    [FieldsList.STATS_IN_PROGRESS_USERS]: {key: 'In Progress User Status', module: 'report'},
    [FieldsList.STATS_IN_PROGRESS_USERS_PERCENTAGE]: {key: 'In Progress User Status Percentage', module: 'report'},
    [FieldsList.STATS_COMPLETED_USERS]: {key: 'Completed User Status', module: 'report'},
    [FieldsList.STATS_COMPLETED_USERS_PERCENTAGE]: {key: 'Completed User Status Percentage', module: 'report'},
    [FieldsList.STATS_PATH_ENROLLED_USERS]: {key: 'Users Enrolled In Learning Plan', module: 'report'},
    [FieldsList.STATS_PATH_NOT_STARTED_USERS]: {key: 'Not Started User Status', module: 'report'},
    [FieldsList.STATS_PATH_NOT_STARTED_USERS_PERCENTAGE]: {key: 'Not Started User Status Percentage', module: 'report'},
    [FieldsList.STATS_PATH_IN_PROGRESS_USERS]: {key: 'In Progress User Status', module: 'report'},
    [FieldsList.STATS_PATH_IN_PROGRESS_USERS_PERCENTAGE]: {key: 'In Progress User Status Percentage', module: 'report'},
    [FieldsList.STATS_PATH_COMPLETED_USERS]: {key: 'Completed User Status', module: 'report'},
    [FieldsList.STATS_PATH_COMPLETED_USERS_PERCENTAGE]: {key: 'Completed User Status Percentage', module: 'report'},
    [FieldsList.STATS_COURSE_RATING]: {key: 'Course Rating', module: 'report'},
    [FieldsList.STATS_ACTIVE]: {key: 'Active', module: 'report'},
    [FieldsList.STATS_EXPIRED]: {key: 'Expired', module: 'report'},
    [FieldsList.STATS_ISSUED]: {key: 'Issued', module: 'report'},
    [FieldsList.STATS_SESSION_TIME]: {key: 'Session Time (min)', module: 'report'},
    [FieldsList.STATS_ARCHIVED]: {key: 'Archived', module: 'report'},
    [FieldsList.STATS_USER_FLOW]: {key: 'Training Material Access from Flow', module: 'report'},
    [FieldsList.STATS_USER_FLOW_PERCENTAGE]: {key: 'Training Material Access % from Flow', module: 'report'},
    [FieldsList.STATS_USER_FLOW_YES_NO]: {key: 'Training Material Access from Flow', module: 'report'},
    [FieldsList.STATS_USER_COURSE_FLOW_PERCENTAGE]: {key: '% of Training Material from Flow', module: 'report'},
    [FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW]: {key: 'Time in Training Material from Flow', module: 'report'},

    [FieldsList.STATS_USER_FLOW_MS_TEAMS_YES_NO]: {key: 'Training Material Access From Flow for Microsoft Teams', module: 'report'},
    [FieldsList.STATS_USER_COURSE_FLOW_MS_TEAMS_PERCENTAGE]: {key: '% Of Training Material From Flow for Microsoft Teams', module: 'report'},
    [FieldsList.STATS_USER_COURSE_TIME_SPENT_BY_FLOW_MS_TEAMS]: {key: 'Time In Training Material From Flow for Microsoft Teams', module: 'report'},
    [FieldsList.STATS_USER_FLOW_MS_TEAMS]: {key: 'Training Material Access From Flow for Microsoft Teams', module: 'report'},
    [FieldsList.STATS_USER_FLOW_MS_TEAMS_PERCENTAGE]: {key: 'Training Material Access % From Flow for Microsoft Teams', module: 'report'},

    [FieldsList.STATS_ACCESS_FROM_MOBILE]: {key: 'Training Material Access from Mobile App', module: 'report'},
    [FieldsList.STATS_PERCENTAGE_ACCESS_FROM_MOBILE]: {key: 'Training Material Access % from Mobile App', module: 'report'},
    [FieldsList.STATS_COURSE_ACCESS_FROM_MOBILE]: {key: 'Training Material Access from Mobile App', module: 'report'},
    [FieldsList.STATS_PERCENTAGE_OF_COURSE_FROM_MOBILE]: {key: '% of Training Material from Mobile App', module: 'report'},
    [FieldsList.STATS_TIME_SPENT_FROM_MOBILE]: {key: 'Time in Training Material from Mobile App', module: 'report'},

    // Learning Plans fields
    [FieldsList.LP_NAME]: { key: 'Learning Plan Name', module: 'report'},
    [FieldsList.LP_CODE]: { key: 'Learning Plan Code', module: 'report'},
    [FieldsList.LP_CREDITS]: { key: '_CREDITS', module: 'report'},
    [FieldsList.LP_UUID]: { key: 'Learning Plan UUID', module: 'report'},
    [FieldsList.LP_LAST_EDIT]: { key: 'Learning Plan Last Edit', module: 'report'},
    [FieldsList.LP_CREATION_DATE]: { key: 'Learning Plan Creation Date', module: 'report'},
    [FieldsList.LP_DESCRIPTION]: { key: 'Learning Plan Description', module: 'report'},
    [FieldsList.LP_ASSOCIATED_COURSES]: { key: 'Number of Associated Courses', module: 'report'},
    [FieldsList.LP_MANDATORY_ASSOCIATED_COURSES]: { key: 'Number of Mandatory Associated Courses', module: 'report'},
    [FieldsList.LP_STATUS]: { key: 'Learning Plan Status', module: 'report'},
    [FieldTranslation.LP_UNDER_MAINTENANCE]: { key: 'Under Maintenance', module: 'report'},
    [FieldTranslation.LP_PUBLISHED]: { key: 'Published', module: 'report'},
    [FieldsList.LP_LANGUAGE]: { key: 'Learning Plan Language', module: 'report'},
    [FieldsList.LP_ENROLLMENT_ASSIGNMENT_TYPE]: {key: 'Assignment Type', module: 'report'},

    // Learning Plans Enrollment fields
    [FieldsList.LP_ENROLLMENT_DATE]: { key: 'Enrollment date', module: 'report'},
    [FieldsList.LP_ENROLLMENT_COMPLETION_DATE]: { key: 'Completion date', module: 'report'},
    [FieldsList.LP_ENROLLMENT_STATUS]: { key: 'Learning Plan Enrollment Status', module: 'report'},
    // This is the old tranlsation for the same key (without new learning plan toggle)
    [FieldTranslation.LP_ENROLLMENT_COMPLETION_PERCENTAGE_OLD_TRANSLATION]: { key: 'Completion Percentage', module: 'report'},
    [FieldsList.LP_ENROLLMENT_START_OF_VALIDITY]: { key: 'Start of validity', module: 'report'},
    [FieldsList.LP_ENROLLMENT_END_OF_VALIDITY]: { key: 'End of validity', module: 'report'},

    // Learning Plans Enrollment fields
    [FieldsList.LP_STAT_DURATION]: { key: 'Learning Plan Duration (All Courses)', module: 'report'},
    [FieldsList.LP_STAT_DURATION_MANDATORY]: { key: 'Learning Plan Duration (Mandatory Courses)', module: 'report'},
    [FieldsList.LP_STAT_DURATION_OPTIONAL]: { key: 'Learning Plan Duration (Optional Courses)', module: 'report'},

     // Learning Plan Usage Statistics Fields
    [FieldsList.LP_ENROLLMENT_COMPLETION_PERCENTAGE]: { key: 'Progress Percentage (All Courses)', module: 'report'}, // If toggle new LP is ON get this translation
    [FieldsList.LP_STAT_PROGRESS_PERCENTAGE_MANDATORY]: { key: 'Progress Percentage (Mandatory Courses)', module: 'report'},
    [FieldsList.LP_STAT_PROGRESS_PERCENTAGE_OPTIONAL]: { key: 'Progress Percentage (Optional Courses)', module: 'report'},

    [FieldsList.LP_COURSE_LANGUAGE]: {key: 'Course Language', module: 'report'},

    // Learning Plan course enrollment fields (new translations)
    [FieldsList.COURSE_ENROLLMENT_DATE_COMPLETE]: {key: 'Course Completion Date', module: 'report'}, // Course Completion Date
    [FieldsList.COURSE_ENROLLMENT_DATE_INSCR]: {key: 'Course Enrollment Date', module: 'report'}, // Course Enrollment Date
    [FieldsList.COURSE_ENROLLMENT_DATE_EXPIRE_VALIDITY]: {key: 'Course Enrollment End Date', module: 'report'}, // Course Enrollment End Date
    [FieldsList.COURSE_ENROLLMENT_DATE_BEGIN_VALIDITY]: {key: 'Course Enrollment Start Date', module: 'report'}, // Course Enrollment Start Date
    [FieldsList.COURSE_ENROLLMENT_STATUS]: {key: 'Course Enrollment Status', module: 'report'},

    // Training materials fields
    [FieldsList.LO_TITLE]: { key: 'Training Material Title', module: 'report'},
    [FieldsList.LO_BOOKMARK]: { key: '_ORGMILESTONE', module: 'report'},
    // Training materials bookmark details
    [FieldTranslation.LO_BOOKMARK_START]: { key: 'Start', module: 'report'},
    [FieldTranslation.LO_BOOKMARK_FINAL]: { key: 'End', module: 'report'},
    [FieldsList.LO_DATE_ATTEMPT]: { key: 'Date Attempt', module: 'report'},
    [FieldsList.LO_DATE_COMPLETE]: { key: 'Training Material Last Completion Date', module: 'report'},
    [FieldsList.LO_FIRST_ATTEMPT]: { key: '_LO_COL_FIRSTATT', module: 'report'},
    [FieldsList.LO_SCORE]: { key: 'Training Material Score', module: 'report'},
    [FieldsList.LO_STATUS]: { key: 'Training Material Status', module: 'report'},
    // Training materials status details
    [FieldTranslation.LO_STATUS_COMPLETED]: { key: '_COMPLETED', module: 'report'},
    [FieldTranslation.LO_STATUS_FAILED]: { key: 'failed', module: 'report'},
    [FieldTranslation.LO_STATUS_IN_ITINERE]: { key: '_USER_STATUS_BEGIN', module: 'report'},
    [FieldTranslation.LO_STATUS_NOT_STARTED]: { key: '_NOT_STARTED', module: 'report'},
    [FieldsList.LO_TYPE]: { key: 'Training Material Type', module: 'report'},
    // Training materials type details
    [FieldTranslation.LO_TYPE_AUTHORING]: { key: '_AUTHORING', module: 'report'},
    [FieldTranslation.LO_TYPE_DELIVERABLE]: { key: '_LONAME_deliverable', module: 'report'},
    [FieldTranslation.LO_TYPE_FILE]: { key: '_LONAME_item', module: 'report'},
    [FieldTranslation.LO_TYPE_HTMLPAGE]: { key: '_LONAME_htmlpage', module: 'report'},
    [FieldTranslation.LO_TYPE_POLL]: { key: '_LONAME_poll', module: 'report'},
    [FieldTranslation.LO_TYPE_SCORM]: { key: '_LONAME_scormorg', module: 'report'},
    [FieldTranslation.LO_TYPE_TEST]: { key: '_LONAME_test', module: 'report'},
    [FieldTranslation.LO_TYPE_TINCAN]: { key: 'TinCan', module: 'report'},
    [FieldTranslation.LO_TYPE_VIDEO]: { key: 'Video', module: 'report'},
    [FieldTranslation.LO_TYPE_AICC]: { key: 'AICC', module: 'report'},
    [FieldTranslation.LO_TYPE_ELUCIDAT]: { key: 'Elucidat', module: 'report'},
    [FieldTranslation.LO_TYPE_GOOGLEDRIVE]: { key: 'Google Drive', module: 'report'},
    [FieldTranslation.LO_TYPE_LTI]: { key: 'LTI', module: 'report'},
    [FieldsList.LO_VERSION]: { key: 'Training Material Version', module: 'report'},

    // Session fields
    [FieldsList.SESSION_UNIQUE_ID]: { key: 'Session Unique ID', module: 'report'},
    [FieldsList.SESSION_INTERNAL_ID]: { key: 'Session Internal ID', module: 'report'},
    [FieldsList.SESSION_NAME]: { key: 'Session name', module: 'report'},
    [FieldsList.SESSION_CODE]: { key: 'Session code', module: 'report'},
    [FieldsList.SESSION_EVALUATION_SCORE_BASE]: { key: 'Evaluation score base', module: 'report'},
    [FieldsList.SESSION_START_DATE]: { key: 'Session date begin', module: 'report'},
    [FieldsList.SESSION_END_DATE]: { key: 'Session End Date', module: 'report'},
    [FieldsList.SESSION_TIME_SESSION]: { key: 'Time in Session', module: 'report'},
    [FieldsList.SESSION_INSTRUCTOR_USERIDS]: { key: 'Session Instructor Username', module: 'report'},
    [FieldsList.SESSION_INSTRUCTOR_FULLNAMES]: { key: 'Session Instructor Full Name', module: 'report'},
    [FieldsList.SESSION_ATTENDANCE_TYPE]: { key: 'Session Attendance Type', module: 'report'},
    [FieldsList.SESSION_MAXIMUM_ENROLLMENTS]: { key: 'Session Maximum Enrollments', module: 'report'},
    [FieldsList.SESSION_MINIMUM_ENROLLMENTS]: { key: 'Session Minimum Enrollments', module: 'report'},
    // Attendances Types Details
    [FieldTranslation.SESSION_ATTENDANCE_TYPE_BLEENDED]: { key: 'Blended', module: 'report'},
    [FieldTranslation.SESSION_ATTENDANCE_TYPE_FLEXIBLE]: { key: 'Flexible', module: 'report'},
    [FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONLINE]: { key: 'Full Online', module: 'report'},
    [FieldTranslation.SESSION_ATTENDANCE_TYPE_FULLONSITE]: { key: 'Full Onsite', module: 'report'},
    // Attendances Statuses
    [FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_PRESENT]: { key: 'Present', module: 'report'},
    [FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_ABSENT]: { key: 'Absent', module: 'report'},
    [FieldTranslation.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS_NOT_SET]: { key: 'Not set', module: 'report'},

    // Session event fields
    [FieldsList.SESSION_EVENT_NAME]: { key: 'Event Name', module: 'report'},
    [FieldsList.SESSION_EVENT_ID]: { key: 'Event ID', module: 'report'},
    [FieldsList.SESSION_EVENT_DATE]: { key: 'Event Date', module: 'report'},
    [FieldsList.SESSION_EVENT_START_DATE]: { key: 'Event Start Date', module: 'report'},
    [FieldsList.SESSION_EVENT_DURATION]: { key: 'Event Duration', module: 'report'},
    [FieldsList.SESSION_EVENT_TIMEZONE]: { key: 'Event Time Zone', module: 'report'},
    [FieldsList.SESSION_EVENT_TYPE]: { key: 'Event Type', module: 'report'},
    [FieldsList.SESSION_EVENT_INSTRUCTOR_USER_NAME]: { key: 'Event Instructor User Name', module: 'report'},
    [FieldsList.SESSION_EVENT_INSTRUCTOR_FULLNAME]: { key: 'Event Instructor Full Name', module: 'report'},
    [FieldsList.SESSION_INSTRUCTOR_LIST]: { key: 'Session Instructors List', module: 'report'},
    [FieldsList.SESSION_COMPLETION_RATE]: { key: 'Session completion rate %', module: 'report'},
    [FieldsList.SESSION_HOURS]: { key: 'Session hours', module: 'report'},

    // Event fields
    [FieldsList.EVENT_INSTRUCTORS_LIST]: { key: 'Event instructors list', module: 'report'},
    [FieldsList.EVENT_ATTENDANCE_STATUS_NOT_SET]: { key: 'Attendance Status (Not Set)', module: 'report'},
    [FieldsList.EVENT_ATTENDANCE_STATUS_ABSENT_PERC]: { key: 'Attendance Status (Absent) %', module: 'report'},
    [FieldsList.EVENT_ATTENDANCE_STATUS_PRESENT_PERC]: { key: 'Attendance Status (Present) %', module: 'report'},
    [FieldsList.EVENT_AVERAGE_SCORE]: { key: 'Average Score', module: 'report'},

    // Enrollment fields
    [FieldsList.ENROLLMENT_ATTENDANCE]: { key: '_ATTENDANCE', module: 'report'},
    [FieldsList.ENROLLMENT_DATE]: { key: 'Enrollment date', module: 'report'},
    [FieldsList.ENROLLMENT_ENROLLMENT_STATUS]: {key: 'Course Enrollment Status', module: 'report'},
    [FieldsList.ENROLLMENT_EVALUATION_STATUS]: { key: 'Evaluation Status', module: 'report'},
    [FieldsList.ENROLLMENT_INSTRUCTOR_FEEDBACK]: { key: 'Instructor Feedback', module: 'report'},
    [FieldsList.ENROLLMENT_LEARNER_EVALUATION]: { key: 'Learner\'s evaluation', module: 'report'},
    [FieldsList.ENROLLMENT_USER_COURSE_LEVEL]: { key: 'User Course Level', module: 'report'},
    [FieldsList.ENROLLMENT_USER_SESSION_STATUS]: { key: 'Session Enrollment Status', module: 'report'},
    [FieldsList.ENROLLMENT_USER_SESSION_SUBSCRIBE_DATE]: { key: 'Session Enrollment Date', module: 'report'},
    [FieldsList.ENROLLMENT_USER_SESSION_COMPLETE_DATE]: { key: 'Session Completion Date', module: 'report'},
    [FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_HOURS]: { key: 'Event Attendance (Hours)', module: 'report'},
    [FieldsList.ENROLLMENT_USER_SESSION_EVENT_ATTENDANCE_STATUS]: { key: 'Event Attendance (Status)', module: 'report'},

    // Session usage statistics

    [FieldsList.SESSION_USER_ENROLLED]: { key: 'Users Enrolled in Session', module: 'report'},
    [FieldsList.SESSION_USER_COMPLETED]: { key: 'Users Completed Session', module: 'report'},
    [FieldsList.SESSION_USER_WAITING]: { key: 'Users Waiting list Session', module: 'report'},
    [FieldsList.SESSION_USER_IN_PROGRESS]: { key: 'Users In Progress Session', module: 'report'},
    [FieldsList.SESSION_COMPLETION_MODE]: { key: 'Session Completion Mode', module: 'report'},
    [FieldsList.SESSION_EVALUATION_STATUS_NOT_SET]: { key: 'Evaluation Status "Not Set"', module: 'report'},
    [FieldsList.SESSION_EVALUATION_STATUS_NOT_PASSED]: { key: 'Evaluation Status "Not Passed" %', module: 'report'},
    [FieldsList.SESSION_EVALUATION_STATUS_PASSED]: { key: 'Evaluation Status "Passed" %', module: 'report'},
    [FieldsList.SESSION_ENROLLED_USERS]: { key: 'Enrolled Users', module: 'report'},
    [FieldsList.SESSION_SESSION_TIME]: { key: 'Session Time', module: 'report'},
    [FieldsList.SESSION_TRAINING_MATERIAL_TIME]: { key: 'Training Material time', module: 'report'},

    // Certifications
    [FieldsList.CERTIFICATION_TITLE]: { key: 'Certification Title', module: 'report'},
    [FieldsList.CERTIFICATION_TO_RENEW_IN]: { key: 'To renew in', module: 'report'},
    [FieldsList.CERTIFICATION_ISSUED_ON]: { key: 'Issued on', module: 'report'},
    [FieldsList.CERTIFICATION_COMPLETED_ACTIVITY]: { key: 'Completed Activity', module: 'report'},
    [FieldsList.CERTIFICATION_DURATION]: { key: 'Certification Duration', module: 'report'},
    [FieldsList.CERTIFICATION_STATUS]: { key: 'Certification Status', module: 'report'},
    [FieldTranslation.CERTIFICATION_ACTIVE]: { key: 'Active', module: 'report'},
    [FieldTranslation.CERTIFICATION_EXPIRED]: { key: 'Expired', module: 'report'},
    [FieldTranslation.CERTIFICATION_ARCHIVED]: { key: 'Archived', module: 'report'},
    // Certification duration units
    [FieldTranslation.DAYS]: { key: 'Days', module: 'report'},
    [FieldTranslation.WEEKS]: { key: 'Weeks', module: 'report'},
    [FieldTranslation.MONTHS]: { key: 'Months', module: 'report'},
    [FieldTranslation.YEARS]: { key: 'Years', module: 'report'},
    [FieldsList.CERTIFICATION_DESCRIPTION]: { key: 'Certification Description', module: 'report'},
    [FieldsList.CERTIFICATION_CODE]: { key: 'Certification Code', module: 'report'},

    // Badge fields
    [FieldsList.BADGE_DESCRIPTION]: { key: 'Badge Description', module: 'report'},
    [FieldsList.BADGE_NAME]: { key: 'Badge Name', module: 'report'},
    [FieldsList.BADGE_SCORE]: { key: 'Badge Score', module: 'report'},

    // Badge Assignment fields
    [FieldsList.BADGE_ISSUED_ON]: { key: 'Issued on', module: 'report'},

    // External Training fields
    [FieldsList.EXTERNAL_TRAINING_COURSE_NAME]: { key: '_COURSE_NAME', module: 'report'},
    [FieldsList.EXTERNAL_TRAINING_COURSE_TYPE]: { key: 'Course Type', module: 'report'},
    [FieldsList.EXTERNAL_TRAINING_SCORE]: { key: 'External Training Score', module: 'report'},
    [FieldsList.EXTERNAL_TRAINING_DATE]: { key: 'The date refers to the completion of the course', module: 'report'},
    [FieldsList.EXTERNAL_TRAINING_DATE_START]: { key: 'Course Start Date', module: 'report'},
    [FieldsList.EXTERNAL_TRAINING_CREDITS]: { key: '_CREDITS', module: 'report'},
    [FieldsList.EXTERNAL_TRAINING_CERTIFICATE]: { key: 'Certificate', module: 'report'},
    [FieldsList.EXTERNAL_TRAINING_TRAINING_INSTITUTE]: { key: 'Training institute', module: 'report'},
    [FieldsList.EXTERNAL_TRAINING_STATUS]: { key: 'External Training Status', module: 'report'},
    // External Training Status details
    [FieldTranslation.EXTERNAL_TRAINING_STATUS_APPROVED]: { key: 'Approved', module: 'report'},
    [FieldTranslation.EXTERNAL_TRAINING_STATUS_WAITING]: { key: 'Waiting', module: 'report'},
    [FieldTranslation.EXTERNAL_TRAINING_STATUS_REJECTED]: { key: 'Rejected', module: 'report'},

    // Ecommerce Transaction fields
    [FieldTranslation.PAYMENT_STATUS_CANCELED]: { key: 'Canceled', module: 'report'},
    [FieldTranslation.PAYMENT_STATUS_PENDING]: { key: '_NOT_PAID', module: 'report'},
    [FieldTranslation.PAYMENT_STATUS_SUCCESSFUL]: { key: 'Paid', module: 'report'},
    [FieldTranslation.PAYMENT_STATUS_FAILED]: { key: 'failed', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_1]: { key: 'Address 1', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_ADDRESS_2]: { key: 'Address 2', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_CITY]: { key: 'City', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_COMPANY_NAME]: { key: 'Company name', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_COUPON_CODE]: { key: 'Coupon code', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_COUPON_DESCRIPTION]: { key: 'Coupon description', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_DISCOUNT]: { key: 'Transaction Discount', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_EXTERNAL_TRANSACTION_ID]: { key: 'Payment Txn Id', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_DATE]: { key: 'Payment Date', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_METHOD]: { key: 'Payment method', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_PAYMENT_STATUS]: { key: 'Payment status', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_PRICE]: { key: 'Item Price', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_QUANTITY]: { key: 'Quantity', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_STATE]: { key: '_STATE', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_SUBTOTAL_PRICE]: { key: 'Transaction Subtotal Price', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_TOTAL_PRICE]: { key: 'Transaction Total Price', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_CREATION_DATE]: { key: 'Transaction Creation Date', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_TRANSACTION_ID]: { key: 'Transaction ID', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_VAT_NUMBER]: { key: 'VAT Number', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_ZIP_CODE]: { key: 'ZIP Code', module: 'report'},

    // Ecommerce Transaction Item fields
    [FieldTranslation.COURSE]: { key: '_COURSE', module: 'report'},
    [FieldTranslation.COURSEPATH]: { key: '_COURSEPATH', module: 'report'},
    [FieldTranslation.COURSESEATS]: { key: 'Seats', module: 'report'},
    [FieldTranslation.SUBSCRIPTION_PLAN]: { key: 'Subscription plan', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_CODE]: { key: 'Course/LP code', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_ITEM_COURSE_LP_NAME]: { key: 'Course/LP name', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_ITEM_START_DATE]: { key: 'startDate', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_ITEM_END_DATE]: { key: 'endDate', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_WEBINAR_SESSION_NAME]: { key: 'ILT/Webinar session name', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_ITEM_ILT_LOCATION]: { key: 'ILT location', module: 'report'},
    [FieldsList.ECOMMERCE_TRANSACTION_ITEM_TYPE]: { key: 'Type', module: 'report'},

    // Content Partner Fields
    [FieldsList.CONTENT_PARTNERS_AFFILIATE]: { key: 'Affiliate', module: 'report'},
    [FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_CODE]: { key: 'Referral Link Code', module: 'report'},
    [FieldsList.CONTENT_PARTNERS_REFERRAL_LINK_SOURCE]: { key: 'Referral Link Source', module: 'report'},

    // Asset fields
    [FieldsList.ASSET_NAME]: {key: 'Asset Name', module: 'report'},
    [FieldsList.CHANNELS]: {key: 'Channels', module: 'report'},
    [FieldsList.PUBLISHED_BY]: {key: 'Published by', module: 'report'},
    [FieldsList.PUBLISHED_ON]: {key: 'Published on', module: 'report'},
    [FieldsList.LAST_EDIT_BY]: {key: 'Last edit by', module: 'report'},
    [FieldsList.ASSET_TYPE]: {key: 'Type (Link, Video, etc)', module: 'report'},
    [FieldsList.ASSET_AVERAGE_REVIEW]: {key: 'Average Review', module: 'report'},
    [FieldsList.ASSET_DESCRIPTION]: {key: 'Asset Description', module: 'report'},
    [FieldsList.ASSET_TAG]: {key: 'Asset Tags', module: 'report'},
    [FieldsList.ASSET_SKILL]: {key: 'Asset Skills', module: 'report'},
    [FieldsList.ASSET_LAST_ACCESS]: {key: 'Last access date', module: 'report'},
    [FieldsList.ASSET_FIRST_ACCESS]: {key: '_DATE_FIRST_ACCESS', module: 'report'},
    [FieldsList.ASSET_NUMBER_ACCESS]: {key: 'Access Count', module: 'report'},

    // Asset statistics fields
    [FieldsList.ANSWER_DISLIKES]: {key: 'Answers dislikes', module: 'report'},
    [FieldsList.ANSWER_LIKES]: {key: 'Answers likes', module: 'report'},
    [FieldsList.ANSWERS]: {key: 'Answers', module: 'report'},
    [FieldsList.ASSET_RATING]: {key: 'Asset rating', module: 'report'},
    [FieldsList.AVERAGE_REACTION_TIME]: {key: 'Average reaction time', module: 'report'},
    [FieldsList.BEST_ANSWERS]: {key: 'Best answers', module: 'report'},
    [FieldsList.GLOBAL_WATCH_RATE]: {key: 'Global watch rate', module: 'report'},
    [FieldsList.INVITED_PEOPLE]: {key: 'Total invited people', module: 'report'},
    [FieldsList.NOT_WATCHED]: {key: 'Not watched', module: 'report'},
    [FieldsList.QUESTIONS]: {key: 'Questions', module: 'report'},
    [FieldsList.TOTAL_VIEWS]: {key: 'Total views', module: 'report'},
    [FieldsList.WATCHED]: {key: 'Watched', module: 'report'},

    // User Assets fields
    [FieldsList.INVOLVED_CHANNELS] : {key: 'Involved Channels', module: 'report'},
    [FieldsList.PUBLISHED_ASSETS] : {key: 'Published assets', module: 'report'},
    [FieldsList.UNPUBLISHED_ASSETS] : {key: 'Unpublished assets', module: 'report'},
    [FieldsList.PRIVATE_ASSETS] : {key: 'Private Assets', module: 'report'},
    [FieldsList.UPLOADED_ASSETS] : {key: 'Uploaded Assets', module: 'report'},

    // Assets type
    [FieldTranslation.VIDEO] : {key: 'Video', module: 'report'},
    [FieldTranslation.DOC] : {key: 'Document', module: 'report'},
    [FieldTranslation.EXCEL] : {key: 'Excel', module: 'report'},
    [FieldTranslation.PPT] : {key: 'PPT', module: 'report'},
    [FieldTranslation.PDF] : {key: 'PDF', module: 'report'},
    [FieldTranslation.TEXT] : {key: 'Text', module: 'report'},
    [FieldTranslation.IMAGE] : {key: 'Image', module: 'report'},
    [FieldTranslation.QUESTION] : {key: 'Question', module: 'report'},
    [FieldTranslation.RESPONSE] : {key: 'Response', module: 'report'},
    [FieldTranslation.OTHER] : {key: 'Other', module: 'report'},
    [FieldTranslation.DEFAULT_OTHER] : {key: 'Other', module: 'report'},
    [FieldTranslation.DEFAULT_MUSIC] : {key: '_USER_AUDIO', module: 'report'},
    [FieldTranslation.DEFAULT_ARCHIVE] : {key: 'Archive', module: 'report'},
    [FieldTranslation.LINKS] : {key: 'Link', module: 'report'},
    [FieldTranslation.GOOGLE_DRIVE_DOCS] : {key: 'Google Document', module: 'report'},
    [FieldTranslation.GOOGLE_DRIVE_SHEETS] : {key: 'Google Sheet', module: 'report'},
    [FieldTranslation.GOOGLE_DRIVE_SLIDES] : {key: 'Google Slide', module: 'report'},
    [FieldTranslation.PLAYLIST] : {key: 'Playlist', module: 'report'},
    [FieldTranslation.YOUTUBE] : {key: 'Video', module: 'report'},
    [FieldTranslation.VIMEO] : {key: 'Video', module: 'report'},
    [FieldTranslation.WISTIA] : {key: 'Video', module: 'report'},
    [FieldTranslation.SESSION_MANUAL] : {key: 'Manual', module: 'report'},
    [FieldTranslation.SESSION_EVALUATION_BASED] : {key: 'Evaluation Based', module: 'report'},
    [FieldTranslation.SESSION_ATTENDANCE_BASED] : {key: 'Attendance Based', module: 'report'},
    [FieldTranslation.SESSION_TRAINING_MATERIAL_BASED] : {key: 'Training Material Based', module: 'report'},

    // Surveys
    [FieldsList.SURVEY_ID] : {key: 'Survey ID', module: 'report'},
    [FieldsList.SURVEY_TITLE] : {key: 'Survey Title', module: 'report'},
    [FieldsList.SURVEY_DESCRIPTION] : {key: 'Survey Description', module: 'report'},
    [FieldsList.SURVEY_TRACKING_TYPE] : {key: 'Survey Tracking Type', module: 'report'},
    [FieldsList.SURVEY_COMPLETION_ID] : {key: 'Survey Completion ID', module: 'report'},
    [FieldsList.SURVEY_COMPLETION_DATE] : {key: 'Survey Completion Date', module: 'report'},
    [FieldsList.QUESTION_ID] : {key: 'Question ID', module: 'report'},
    [FieldsList.QUESTION_TYPE] : {key: 'Question Type', module: 'report'},
    [FieldsList.QUESTION_MANDATORY] : {key: 'Mandatory Question (Yes / No)', module: 'report'},
    [FieldsList.ANSWER_USER] : {key: 'User Answer to Question (Text)', module: 'report'},

    // Survey Type
    [FieldTranslation.LOCAL_TRACKING] : {key: 'Local tracking', module: 'report'},
    [FieldTranslation.SHARED_TRACKING] : {key: 'Shared tracking', module: 'report'},
    [FieldTranslation.LIKERT_SCALE] : {key: 'Likert scale', module: 'report'},
    [FieldTranslation.CHOICE]: {key: 'Single Choice', module: 'report'},
    [FieldTranslation.CHOICE_MULTIPLE]: {key: 'Multiple Choice', module: 'report'},
    [FieldTranslation.INLINE_CHOICE]: {key: 'Inline choice', module: 'report'},
    [FieldTranslation.EXTENDED_TEXT]: {key: '_QUEST_EXTENDED_TEXT', module: 'report'},

    // Assignment type
    [FieldTranslation.ASSIGNMENT_TYPE_MANDATORY] : {key: 'Mandatory', module: 'report'},
    [FieldTranslation.ASSIGNMENT_TYPE_REQUIRED] : {key: 'Required', module: 'report'},
    [FieldTranslation.ASSIGNMENT_TYPE_RECOMMENDED] : {key: 'Recommended', module: 'report'},
    [FieldTranslation.ASSIGNMENT_TYPE_OPTIONAL]: {key: 'Optional', module: 'report'},
};

export interface ReportFieldsArray {
    assets?: string[];
    badge?: string[];
    badgeAssignment?: string[];
    certifications?: string[];
    contentPartners?: string[];
    course?: string[];
    courseuser?: string[];
    ecommerceTransaction?: string[];
    ecommerceTransactionItem?: string[];
    externalTraining?: string[];
    enrollment?: string[];
    group?: string[];
    lp?: string[];
    lpenrollment?: string[];
    learningPlansStatistics?: string[];
    courseEnrollments?: string[];
    session?: string[];
    event?: string[];
    statistics?: string[];
    usageStatistics?: string[];
    mobileAppStatistics?: string[];
    flowStatistics?: string[];
    flowMsTeamsStatistics?: string[];
    trainingMaterials?: string[];
    user?: string[];
    webinarSessionUser?: string[];
    survey?: string[];
    surveyQuestionAnswer?: string[];
}
