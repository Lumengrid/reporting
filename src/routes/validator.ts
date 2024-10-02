import { body, validationResult } from 'express-validator';
import { SessionLoggerService } from '../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { query } from 'express-validator/src/middlewares/validation-chain-builders';
import { ENROLLMENT_STATUSES_MAP, TIMEFRAME_TYPES } from '../dashboards/constants/dashboard-types';

const DATETIME_VALIDATOR = /^(\d{4})-(\d{2})-(\d{2})( (\d{2}):(\d{2}):(\d{2}))?$/;
export const validateReport = [
    body('planning.option.recipients.*').if(body('planning.active').exists().custom((value) => {
        // validate recipients only if planning active is true
        return value;
    }))
        .notEmpty()
        .isEmail()
        .withMessage('Invalid recipient email'),
    (req, res, next) => {
        const result = validationResult(req);
        const hasErrors = !result.isEmpty();
        if (hasErrors) {
            const logger: SessionLoggerService = httpContext.get('logger');
            const errorToString = JSON.stringify(result.array());
            logger.error(`Error on validation reportId ${req.body.idReport} Errors: ${errorToString}`);
            return res.status(400).json({success: false});
        }
        next();
    },
];

export const validateSetting = [
    body('datalakeV2ExpirationTime').optional().not().isString().withMessage('datalakeV2ExpirationTime is not a number (is a string)'),
    body('datalakeV2ExpirationTime').optional().notEmpty().isInt().withMessage('datalakeV2ExpirationTime is not a integer'),
    body('datalakeV2ExpirationTime').optional().isIn([7200, 14400, 21600, 28800, 43200, 86400]).withMessage('Invalid value for datalakeV2ExpirationTime. Values allowed are: 7200, 14400, 21600, 28800, 43200, 86400'),
    (req, res, next) => {
        const result = validationResult(req);
        const hasErrors = !result.isEmpty();
        if (hasErrors) {
            const logger: SessionLoggerService = httpContext.get('logger');
            const errorToString = JSON.stringify(result.array());
            logger.error(`Error on validation settings Errors: ${errorToString}`);
            return res.status(400).json({success: false});
        }
        next();
    },
];

export const checkError = [
    (req, res, next) => {
        const result = validationResult(req);
        const hasErrors = !result.isEmpty();
        if (hasErrors) {
            const logger: SessionLoggerService = httpContext.get('logger');
            const errorToString = result.array().length > 1 ? result.array().map((item) => item.msg).toString() : result.array()[0].msg;
            logger.error(`Error on validation : ${JSON.stringify(result)}`);
            return res.status(400).json({success: false, error: { code: 1001, message: 'Invalid Parameter: ' + errorToString }});
        }
        next();
    },
];

export const validatePagination = [
    query('sort_attr').optional().isString().withMessage('sort_attr is not a string'),
    query('sort_dir').optional().isString().withMessage('sort_dir is not a string'),
    query('sort_dir').optional().toUpperCase().isIn(['ASC', 'DESC']).withMessage('sort_dir must be ASC or DESC'),
    query('page').optional().notEmpty().isInt().withMessage('page is not a integer'),
    query('page').optional().notEmpty().isInt({ min: 1 }).withMessage('page must be greater than 0'),
    query('page_size').optional().notEmpty().isInt().withMessage('page_size is not a integer'),
    query('page_size').optional().notEmpty().isInt({ min: 1 }).withMessage('page_size must be greater than 0'),
    query('query_id').optional().notEmpty().isString().withMessage('query_id is not a string'),
    ...checkError,
];

export const validatePrivacyDashboard = [
    query('current_version_only').optional().isBoolean().withMessage('current_version_only is not a boolean'),
    query('hide_deactivated_user').optional().isBoolean().withMessage('hide_deactivated_user is not a boolean'),
    query('all_fields').optional().isBoolean().withMessage('all_fields is not a boolean'),
    query('multidomain_ids').optional().isArray().withMessage('multidomain_ids is not array')
        .custom((value) => {
            return value.every((e) => {
                if (e.match(/^\d+$/) === null) throw new Error('multidomain_ids does not contain Integers');
                return true;
            });
        }),
    query('user_ids').optional().isArray().withMessage('user_ids is not array')
        .custom((value) => {
            return value.every((e) => {
                if (e.match(/^\d+$/) === null) throw new Error('user_ids does not contain Integers');
                return true;
            });
        }),
    query('branch_id').optional().notEmpty().isInt().withMessage('branch_id is not a integer'),
    query('selection_status').optional().notEmpty().isIn([1, 2]).withMessage('selection_status must be 1 or 2'),
    ...checkError,
];

export const validateCoursesEnrollments = [
    query('timeframe').optional().notEmpty().isIn(TIMEFRAME_TYPES).withMessage('timeframe must be ' + TIMEFRAME_TYPES.join(', ')),
    query('timeframe_completion').optional().notEmpty().isIn(TIMEFRAME_TYPES).withMessage('timeframe_completion must be ' + TIMEFRAME_TYPES.join(', ')),
    query('startDate').optional().notEmpty().matches(DATETIME_VALIDATOR).withMessage('startDate is not a date'),
    query('endDate').optional().notEmpty().matches(DATETIME_VALIDATOR).withMessage('endDate is not a date'),
    query('startDate_completion').optional().notEmpty().matches(DATETIME_VALIDATOR).withMessage('startDate_completion is not a date'),
    query('endDate_completion').optional().notEmpty().matches(DATETIME_VALIDATOR).withMessage('endDate_completion is not a date'),
    query('branch_id').optional().notEmpty().isInt().withMessage('branch_id is not a integer'),
    query('course_id').optional().notEmpty().isInt().withMessage('course_id is not a integer'),
    query('hide_deactivated_users').optional().isBoolean().withMessage('hide_deactivated_user is not a boolean'),
    ...checkError,
];

export const validateCoursesChartsSummary = [
    query('timeframe').optional().notEmpty().isIn(TIMEFRAME_TYPES).withMessage('timeframe must be ' + TIMEFRAME_TYPES.join(', ')),
    query('startDate').optional().notEmpty().matches(DATETIME_VALIDATOR).withMessage('startDate is not a date'),
    query('endDate').optional().notEmpty().matches(DATETIME_VALIDATOR).withMessage('endDate is not a date'),
    query('branch_id').optional().notEmpty().isInt().withMessage('branch_id is not a integer'),
    query('course_id').optional().notEmpty().isInt().withMessage('course_id is not a integer'),
    query('hide_deactivated_users').optional().isBoolean().withMessage('hide_deactivated_users is not a boolean'),
    ...checkError,
];

export const validateBranches = [
    query('branch_id').notEmpty().isInt({min: 0}).withMessage('branch_id is not an integer greater or equal than zero'),
    query('hide_deactivated_users').optional().isBoolean().withMessage('hide_deactivated_users is not a boolean'),
    ...checkError,
];

export const validateEnrollmentStatus = [
    query('status').optional().notEmpty().isArray().withMessage('status is an array')
        .custom((value) => {
            const invalidValues = value.filter((item) => !Object.values(ENROLLMENT_STATUSES_MAP).includes(item));
            if (invalidValues.length > 0) {
                throw new Error('status can contains just ' + Object.values(ENROLLMENT_STATUSES_MAP).join(', '));
            }
            return true;
        }),
    ...checkError,
];
