import { FilterOperation } from '../constants/dashboard-types';

export interface PrivacyFilter {
    option: FilterOperation;
    value: any;
}

export interface PrivacyFilters {
    policy_accepted?: PrivacyFilter;
    policy_name?: PrivacyFilter;
    username?: PrivacyFilter;
    version?: PrivacyFilter;
}

export interface ChartData {
    accepted: number;
    rejected: number;
    no_answer: number;
}

export interface PrivacyBarChart {
    day: ChartData;
    week: ChartData;
    month: ChartData;
    year: ChartData;
    more_than_year: ChartData;
}

export interface PrivacyCharts {
    bar_charts_data: PrivacyBarChart;
    donut_chart_data: ChartData;
}

export interface PrivacyUsersCount {
    countYesDay: number;
    countNoDay: number;
    countYesWeek: number;
    countNoWeek: number;
    countYesMonth: number;
    countNoMonth: number;
    countYesYear: number;
    countNoYear: number;
    countYesMoreThanYear: number;
    countNoMoreThanYear: number;
    countYes: number;
    countNo: number;
    countAll: number;
}

export interface Pagination {
    query_id: string;
    current_page: number;
    current_page_size: number;
    has_more_data: number;
    total_count: number;
    total_page_count: number;
}

export interface CourseSummary {
    completed?: number;
    enrolled?: number;
    in_progress?: number;
    not_started?: number;
}

export interface BranchesSummary extends BranchEnrollments {
    id: number;
    root: boolean;
    title: string;
    code: string;
    has_children: boolean;
    total_users: number;
}

export interface Branch {
    id: number;
    title: string;
    has_children: boolean;
    total_users: number;
}

export interface BranchEnrollments {
    completed: number;
    enrolled: number;
    in_progress: number;
    subscribed: number;
}

export interface BranchesSummary extends Branch, BranchEnrollments {
    root: boolean;
    code: string;
}

export interface BranchChildren extends Branch, BranchEnrollments {
    overdue: number;
}

export interface BranchesList extends Pagination {
    branch_name: string;
    items: BranchChildren[];
}

export interface OrgChartTree {
    idorg: number;
    code: string;
    idparent: number;
    lev: number;
    ileft: number;
    iright: number;
}

export interface BranchUserEnrollment {
    username: string;
    fullname: string;
    course_name: string;
    course_code?: string;
    enrollment_date: string;
    completion_date?: string;
    status: string;
    score: number;
    session_time: number;
    credits: number;
}

export interface BranchUserEnrollments {
    items: BranchUserEnrollment[];
}

export interface UserEnrollmentByCourse extends CourseSummary {
    username: string;
    first_name: string;
    last_name: string;
    status: string;
    enrollment_date: string;
    time_in_course: string;
    last_access: string;
    completion_date: string;
    score: string;
    course_id: number;
    code: string;
    name: string;
    type: string;
    has_esignature_enabled: boolean;
}

export interface CourseCompletion extends CourseSummary {
    year: number;
    month: number;
    date: string;
}

export interface CourseEnrollment extends CourseSummary {
    has_esignature_enabled?: string;
    idcourse: number;
    code: string;
    name: string;
    type: string;
}

export interface TopCourseByEnrollment extends CourseSummary {
    idcourse: number;
    code?: string;
    name: string;
}

export interface Course {
    code: string;
    title: string;
    description: string;
}

export interface UserEnrollmentsByCourse extends Pagination {
    items: UserEnrollmentByCourse[];
}

export interface CoursesEnrollments extends Pagination {
    items: CourseEnrollment[];
}

export interface CoursesCharts {
    code?: string;
    description?: string;
    title?: string;
    completion: CourseCompletion[];
    partecipation: TopCourseByEnrollment[];
}

export interface PrivacyUser {
    username: string;
    email: string;
    last_login: string;
    track_id: number;
    domain: string;
    policy_id: number;
    version_id: number;
    version: string;
    policy_name: string;
    policy_accepted: string;
    acceptance_date: string;
    user_id: number;
    firstname: string;
    lastname: string;
    answer_sub_policy_1: string;
    answer_sub_policy_2: string;
    answer_sub_policy_3: string;
}

export interface PrivacyUsersList extends Pagination {
    items: PrivacyUser[];
}

export interface ExportTranslation {
    column: string;
    valuesOverride?: string;
    translation?: string;
}