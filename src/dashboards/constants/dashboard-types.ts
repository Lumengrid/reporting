export enum FilterOperation {
    CONTAINS = 'contains',
    LIKE = 'like',
    EQUAL = '=',
    NOT_EQUAL = 'not_equal',
    NOT_EQUAL_V2 = '!=',
    ENDS_WITH = 'ends_with',
    STARTS_WITH = 'starts_with',
    IS_EMPTY = 'is_empty',
    NOT_EMPTY = 'not_empty',
    NOT_START_WITH = 'not_start_with',
    NOT_END_WITH = 'not_end_with',
    NOT_CONTAINS = 'not_contains',
    GREATER = '>',
    GREATER_EQUAL = '>=',
    LESSER = '<',
    LESSER_EQUAL = '<=',
}

export enum FieldTranslation {
    // Dashboards
    ACCEPTANCE_DATE = 'acceptance_date',
    DASHBOARDS_CURRENT_VERSION = 'dashboards_current_version',
    YES = 'yes',
    NO = 'no',
    NO_ANSWER = 'no_answer',
    BRANCH_ID = 'branch_id',
    COMPLETION_DATE = 'completion_date',
    COURSE_CODE = 'code',
    COURSE_NAME = 'name',
    COURSE_TYPE = 'type',
    COURSE_TYPE_ELEARNING = 'course_type_elearning',
    COURSE_TYPE_CLASSROOM = 'course_type_classroom',
    COURSE_TYPE_WEBINAR = 'course_type_webinar',
    COURSEUSER_STATUS_COMPLETED = 'courseuser_status_completed',
    COURSEUSER_STATUS_CONFIRMED = 'courseuser_status_confirmed',
    COURSEUSER_STATUS_ENROLLED = 'courseuser_status_enrolled',
    COURSEUSER_STATUS_IN_PROGRESS = 'courseuser_status_in_progress',
    COURSEUSER_STATUS_NOT_STARTED = 'courseuser_status_not_started',
    COURSEUSER_STATUS_OVERBOOKING = 'courseuser_status_overbooking',
    COURSEUSER_STATUS_SUBSCRIBED = 'courseuser_status_subscribed',
    COURSEUSER_STATUS_SUSPENDED = 'courseuser_status_suspended',
    COURSEUSER_STATUS_WAITING_LIST = 'courseuser_status_waiting_list',
    CREDITS = 'credits',
    DOMAIN = 'domain',
    EMAIL = 'email',
    ENROLLMENT_DATE = 'enrollment_date',
    FIRST_NAME = 'first_name',
    FULL_NAME = 'full_name',
    HAS_CHILDREN = 'has_children',
    HAS_ESIGNATURE_ENABLED = 'has_esignature_enabled',
    IDCOURSE = 'idcourse',
    LAST_ACCESS = 'last_access',
    LAST_LOGIN = 'last_login',
    LAST_NAME = 'last_name',
    OTHER_COURSES = 'other_courses',
    OVERDUE = 'overdue',
    POLICY_ACCEPTED = 'policy_accepted',
    POLICY_NAME = 'policy_name',
    SCORE = 'score',
    SESSION_TIME = 'session_time',
    STATUS = 'status',
    TIME_IN_COURSE = 'time_in_course',
    TITLE = 'title',
    TOTAL_USERS = 'total_users',
    TRACK_ID = 'track_id',
    USER_ID = 'user_id',
    USERNAME = 'username',
    VERSION = 'version',
    VERSION_ID = 'version_id',
}

export enum LearningCourseuser {
    STATUS_SUBSCRIBED = 0,
    STATUS_IN_PROGRESS = 1,
    STATUS_COMPLETED = 2,
}

export enum TimeFrame {
    ANY = 'any',
    CUSTOM = 'custom',
    THIS_MONTH = 'this_month',
    THIS_WEEK = 'this_week',
    THIS_YEAR = 'this_year',
}

export const TIMEFRAME_TYPES = [
        TimeFrame.ANY,
        TimeFrame.CUSTOM,
        TimeFrame.THIS_MONTH,
        TimeFrame.THIS_WEEK,
        TimeFrame.THIS_YEAR
];

export enum EnrollmentStatus {
    COMPLETED = 'completed',
    IN_PROGRESS = 'in_progress',
    SUBSCRIBED = 'subscribed',
}

export enum DashboardTypes {
    PRIVACY_POLICIES = 'privacy_policies',
    COURSES = 'courses',
    BRANCHES = 'branches',
    BRANCHES_USERS = 'branches_users',
}

export const ENROLLMENT_STATUSES_MAP = {
    0: EnrollmentStatus.SUBSCRIBED,
    1: EnrollmentStatus.IN_PROGRESS,
    2: EnrollmentStatus.COMPLETED,
};
