import { NextFunction, Request, Response } from 'express';
import { NotFoundException, UnauthorizedException } from '../../exceptions';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { DashboardCourses } from '../../dashboards/models/dashboard-courses';
import { CoursesEnrollmentsResponse, ErrorResponse, UserEnrollmentsByCourseResponse } from '../../models/custom-report';

/**
 * @category Dashboard
 * @summary Get enrollment report for a given course or all course
 * @method GET
 * @notes This endpoint retrieves the enrollment report for a given course or all course. Available for datalake v3.
 * @toggle release/weekly/datalake-v3
 * @toggle toggle/admin/report/courses-dashboard-on-athena
 * @url /analytics/v1/dashboard/courses/list
 *
 * @parameter timeframe [enum(any, this_year, this_week, this_month, custom), optional] Timeframe filter on enrollment date, default: any
 * @parameter startDate [date, optional] Used only when timeframe parameter is set to custom. Start date filter for enrollment date. The expected format is "2023-01-31"
 * @parameter endDate [date, optional] Used only when timeframe parameter is set to custom. End date filter for enrollment date. The expected format is "2023-01-31"
 * @parameter timeframe_completion [enum(any, this_year, this_week, this_month, custom), optional] Timeframe filter on course completion date, default: any
 * @parameter startDate_completion [date, optional] Used only when timeframe_completion parameter is set to custom. Start date filter for course completion date. The expected format is "2023-01-31"
 * @parameter endDate_completion [date, optional] Used only when timeframe_completion parameter is set to custom. End date filter for course completion date. The expected format is "2023-01-31"
 * @parameter hide_deactivated_users [boolean, optional] Returns only active users if set to TRUE. Otherwise, returns both active and inactive users. The default value is FALSE. This parameter is optional.
 * @parameter branch_id [integer, optional] Branch to filter the results by
 * @parameter course_id [integer, optional] Parameter for showing results for a single course.
 * @parameter search_text [string, optional] Search phrase for Course name column or Enrolled user's username if idCourse parameter is provided.
 * @parameter sort_attr [enum(username, first_name, last_name, code, name, enrolled, completed, in_progress, not_started), optional] Sort by this field, if course_id parameter is set default is username otherwise enrolled.
 * @parameter sort_dir [enum(asc, desc), optional] Sorting Direction: asc = Ascending, desc = descending, default - asc
 * @parameter page [integer, optional] Page to return, default 1
 * @parameter page_size [integer, optional] Maximum number of results per page. Default to the platform setting for pagination
 * @parameter query_id [string, optional] The ID of the query
 *
 * @response success [boolean, required] Whether the operation was successful
 * @response data [object, required] Statistics for course enrollments
 *      @item items [array, required] Items of the report
 *          @item username [string, optional] Username of the enrolled user. Returned only when course_id parameter is provided and its value is an id of existing course.
 *          @item first_name [string, optional] First name of the enrolled user. Returned only when course_id parameter is provided and its value is an id of existing course.
 *          @item last_name [string, optional] Last name of the enrolled user. Returned only when course_id parameter is provided and its value is an id of existing course.
 *          @item status [string, optional] Status of the enrolled user in the course. Returned only when course_id parameter is provided and its value is an id of existing course.
 *          @item enrollment_date [datetime, optional] Date of the enrollment. Returned only when course_id parameter is provided and its value is an id of existing course.
 *          @item time_in_course [integer, optional] Time in seconds that user has been in this course. Returned only when course_id parameter is provided and its value is an id of existing course.
 *          @item last_access [datetime, optional] Last access date in this course. Returned only when course_id parameter is provided and its value is an id of existing course.
 *          @item completion_date [datetime, optional] Completion date of the course. Returned only when course_id parameter is provided and its value is an id of existing course.
 *          @item score [integer, optional] Given score in the course. Returned only when course_id parameter is provided and its value is an id of existing course.
 *          @item idcourse [integer, optional] ID of the course. Returned only when course_id parameter is not provided.
 *          @item code [string, optional] Code of the course. Returned only when course_id parameter is not provided.
 *          @item name [string, optional] Name of the course. Returned only when course_id parameter is not provided.
 *          @item type [enum(elearning, webinar, classroom), optional] Type of the course. Returned only when course_id parameter is not provided.
 *          @item has_esignature_enabled [boolean, optional] If the course has ESignature enabled. Returned only when course_id parameter is not provided.
 *          @item enrolled [integer, optional] Number of the enrolled users in this course. Returned only when course_id parameter is not provided.
 *          @item completed [integer, optional] Number of the users that completed this course. Returned only when course_id parameter is not provided.
 *          @item in_progress [integer, optional] Number of the users that are in progress in this course. Returned only when course_id parameter is not provided.
 *          @item not_started [integer, optional] Number of the users that still not started this course. Returned only when course_id parameter is not provided.
 *      @end
 *      @item has_more_data [bool, required] True if the current page is not the last page
 *      @item current_page [integer, required] Page number of the current page
 *      @item current_page_size [integer, required] Number of items per page
 *      @item total_page_count [integer, optional] Total number of pages returned
 *      @item total_count [integer, optional] Total number of Items
 *      @item query_id [string, optional] The ID of the query
 * @end
 *
 * @status 1001 Invalid Parameter
 * @status 1002 Invalid Course
 * @status 1009 Invalid QueryId
 */
export const getDashboardCoursesEnrollments = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const dashboard: DashboardCourses = new DashboardCourses(session);
    const courseId = req.query.course_id !== undefined ? Number(req.query.course_id.toString()) : undefined;
    const response: CoursesEnrollmentsResponse | UserEnrollmentsByCourseResponse =
        courseId ? new UserEnrollmentsByCourseResponse() : new CoursesEnrollmentsResponse();
    res.type('application/json');
    try {
        response.data = courseId ? await dashboard.getReportUserEnrollmentsByCourse(req) : await dashboard.getReportCoursesEnrollments(req);
        res.status(200);
        res.json(response);
    } catch (err: any) {
        if (err instanceof UnauthorizedException) {
            res.status(401);
        } else if (err instanceof NotFoundException) {
            res.status(400);
        } else {
            res.status(500);
        }
        logger.errorWithStack(`Error while performing get dashboard course enrollments call.`, err);
        response.success = false;
        response.error = new ErrorResponse(err.code, err.message);
        res.json(response);
    }
};




