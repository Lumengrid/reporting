import { FieldsList } from '../../models/report-manager';
import SessionManager from '../../services/session/session-manager.session';

// Users
export const reportManagerUserFields = [
    FieldsList.USER_USERID,
    FieldsList.USER_FIRSTNAME,
    FieldsList.USER_LASTNAME,
    FieldsList.USER_EMAIL,
    FieldsList.USER_BRANCH_PATH,
    FieldsList.USER_DIRECT_MANAGER,
];

// Courses
export const reportManagerCourseFields = [
    FieldsList.COURSE_NAME,
    FieldsList.COURSE_CODE,
    FieldsList.COURSE_CATEGORY_NAME,
    FieldsList.COURSE_CREDITS,
    FieldsList.COURSE_DURATION,
    FieldsList.COURSE_TYPE,
    FieldsList.COURSE_DATE_BEGIN,
    FieldsList.COURSE_DATE_END,
];

// Enrollments
export const reportManagerCourseUserFields = (session: SessionManager) => {
    const courseUserFields = [
        FieldsList.COURSEUSER_DATE_INSCR,
        FieldsList.COURSEUSER_DATE_FIRST_ACCESS,
        FieldsList.COURSEUSER_DATE_LAST_ACCESS,
        FieldsList.COURSEUSER_DATE_COMPLETE,
        FieldsList.COURSEUSER_STATUS,
        FieldsList.COURSEUSER_DATE_BEGIN_VALIDITY,
        FieldsList.COURSEUSER_DATE_EXPIRE_VALIDITY,
        FieldsList.COURSEUSER_SCORE_GIVEN,
        FieldsList.COURSEUSER_INITIAL_SCORE_GIVEN
    ];

    if (session.platform.checkPluginESignatureEnabled()) {
        courseUserFields.push(FieldsList.COURSE_E_SIGNATURE_HASH);
    }

    return courseUserFields;
};

// Usage Statistics
export const reportManagerUsageStatisticsFields = [
    FieldsList.STATS_USER_COURSE_COMPLETION_PERCENTAGE,
    FieldsList.STATS_TOTAL_SESSIONS_IN_COURSE,
    FieldsList.STATS_SESSION_TIME,
];

// Certifications
export const reportManagerCertificationsFields = [
    FieldsList.CERTIFICATION_TITLE,
    FieldsList.CERTIFICATION_CODE,
    FieldsList.CERTIFICATION_DESCRIPTION,
    FieldsList.CERTIFICATION_DURATION,
    FieldsList.CERTIFICATION_COMPLETED_ACTIVITY,
    FieldsList.CERTIFICATION_ISSUED_ON,
    FieldsList.CERTIFICATION_TO_RENEW_IN,
    FieldsList.CERTIFICATION_STATUS,
];

// Session Fields (ILT)
export const reportManagerSessionFields = [
    FieldsList.SESSION_NAME,
    FieldsList.SESSION_CODE,
    FieldsList.SESSION_END_DATE,
    FieldsList.SESSION_EVALUATION_SCORE_BASE,
    FieldsList.SESSION_TIME_SESSION,
    FieldsList.SESSION_START_DATE,
    FieldsList.WEBINAR_SESSION_WEBINAR_TOOL,
    FieldsList.WEBINAR_SESSION_TOOL_TIME_IN_SESSION
];

// Enrollment Fields (ILT)
export const reportManageEnrollmentFields = [
    FieldsList.ENROLLMENT_ATTENDANCE,
    FieldsList.ENROLLMENT_DATE,
    FieldsList.ENROLLMENT_ENROLLMENT_STATUS,
    FieldsList.ENROLLMENT_EVALUATION_STATUS,
    FieldsList.ENROLLMENT_INSTRUCTOR_FEEDBACK,
    FieldsList.ENROLLMENT_LEARNER_EVALUATION,
    FieldsList.ENROLLMENT_USER_SESSION_STATUS,
    FieldsList.ENROLLMENT_USER_SESSION_SUBSCRIBE_DATE,
    FieldsList.ENROLLMENT_USER_SESSION_COMPLETE_DATE,
    FieldsList.COURSEUSER_DATE_COMPLETE,
];

// Learning Plans fields
export const reportManagerLpFields = [
    FieldsList.LP_NAME,
    FieldsList.LP_CREDITS
];

export const reportManagerLpEnrollmentFields = [
    FieldsList.LP_ENROLLMENT_DATE,
    FieldsList.LP_ENROLLMENT_COMPLETION_DATE,
    FieldsList.LP_ENROLLMENT_STATUS,
    FieldsList.LP_ENROLLMENT_COMPLETION_PERCENTAGE
];
