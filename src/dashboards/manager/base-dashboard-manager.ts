import httpContext from 'express-http-context';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import { Translations } from '../../services/hydra';
import { BaseResponse } from '../../models/base';
import { FieldTranslation } from '../constants/dashboard-types';

export abstract class BaseDashboardManager {
    session: SessionManager;
    logger: SessionLoggerService;
    translatableFields: string[];

    public constructor(session: SessionManager) {
        this.session = session;
        this.logger = httpContext.get('logger');
        this.translatableFields = [];
    }

    public async loadTranslations(): Promise<{ [key: string]: string }> {
        const hydra = this.session.getHydra();
        const toTranslate: Translations = {
            translations: {},
            lang_code: this.session.user.getLangCode()
        };

        if (this.translatableFields) {
            for (const field of this.translatableFields) {
                toTranslate.translations[field] = fieldTranslationsKey[field];
            }
        }
        const translations = await hydra.getTranslations(toTranslate);
        const translationsData = translations.data;

        Object.entries(translationsData).forEach(([key, value], index) => {
            let newValue = value;
            // check if the translation value is a duplicate only if is a selected field
            // (Is possible we have two columns contains the same translation value but
            // is not possible we have two different header column with same translation value)
            if (this.translatableFields.includes(key)) {
                let count = 1;
                while (this.existsDuplicate(translationsData, index, newValue)) {
                    newValue = `${value} ${count}`;
                    count = count + 1;
                }
            }
            translationsData[key] = newValue;
        });
        return translationsData;
    }

    /**
     * Check if in the object translation there is another previous field with same translation.
     * @param translation
     * @param indexValue
     * @param valueToCheck
     * @private
     */
    protected existsDuplicate(translation: object, indexValue, valueToCheck: string): boolean {
        for (const [index, [, value]] of Object.entries(Object.entries(translation))) {
            if (index >= indexValue) {
                return false;
            }

            if (value.toLowerCase() === valueToCheck.toLowerCase() && index !== indexValue) {
                return true;
            }
        }
        return false;
    }

    /**
     * Utils for Query
     */
    protected renderStringInQuerySelect(text: string): string {
        if (typeof text !== 'string') {
            throw(new Error('Invalid translation returned'));
        }
        return `"${text.replace(/"/g, '""')}"`;
    }

    protected renderStringInQueryCase(text: string): string {
        if (typeof text !== 'string') {
            throw(new Error('Invalid translation returned'));
        }
        return `'${text.replace(/'/g, "''")}'`;
    }
}

export const fieldTranslationsKey = {
    // Standard translations
    [FieldTranslation.YES]: {key: '_YES', module: 'standard'},
    [FieldTranslation.NO]: {key: '_NO', module: 'standard'},
    [FieldTranslation.NO_ANSWER]: {key: '_NO_ANSWER', module: 'standard'},
    [FieldTranslation.FIRST_NAME]: {key: 'First Name', module: 'standard'},
    [FieldTranslation.LAST_NAME]: {key: 'Last Name', module: 'standard'},

    // Privacy-policy dashboard translations
    [FieldTranslation.ACCEPTANCE_DATE]: {key: 'Answer Date', module: 'standard'},
    [FieldTranslation.COURSE_CODE]: {key: 'Code', module: 'standard'},
    [FieldTranslation.COURSE_NAME]: {key: 'Course Name', module: 'transcripts'},
    [FieldTranslation.COURSE_TYPE]: {key: 'Type', module: 'standard'},
    [FieldTranslation.DASHBOARDS_CURRENT_VERSION]: {key: 'current version', module: 'standard'},
    [FieldTranslation.DOMAIN]: {key: 'Domain', module: 'standard'},
    [FieldTranslation.EMAIL]: {key: 'Email', module: 'standard'},
    [FieldTranslation.LAST_LOGIN]: {key: 'Last Login Date', module: 'standard'},
    [FieldTranslation.POLICY_ACCEPTED]: {key: 'Acceptance Status', module: 'standard'},
    [FieldTranslation.POLICY_NAME]: {key: 'Privacy Policy Name', module: 'standard'},
    [FieldTranslation.SESSION_TIME]: { key: 'Session Time', module: 'report' },
    [FieldTranslation.TRACK_ID]: {key: 'Track ID', module: 'standard'},
    [FieldTranslation.USER_ID]: {key: 'User ID', module: 'standard'},
    [FieldTranslation.VERSION]: {key: 'Version', module: 'standard'},
    [FieldTranslation.VERSION_ID]: {key: 'Version ID', module: 'standard'},

    // Courses dashboard translations
    [FieldTranslation.COMPLETION_DATE]: {key: 'Completion date', module: 'standard'},
    [FieldTranslation.COURSEUSER_STATUS_COMPLETED]: {key: 'Completed', module: 'standard'},
    [FieldTranslation.COURSEUSER_STATUS_CONFIRMED]: {key: '_USER_STATUS_CONFIRMED', module: 'standard'},
    [FieldTranslation.COURSEUSER_STATUS_ENROLLED]: {key: 'Enrolled', module: 'standard'},
    [FieldTranslation.COURSEUSER_STATUS_IN_PROGRESS]: {key: '_USER_STATUS_BEGIN', module: 'standard'},
    [FieldTranslation.COURSEUSER_STATUS_NOT_STARTED]: {key: 'Not started', module: 'standard'},
    [FieldTranslation.COURSEUSER_STATUS_OVERBOOKING]: {key: '_USER_STATUS_OVERBOOKING', module: 'subscribe'},
    [FieldTranslation.COURSEUSER_STATUS_SUBSCRIBED]: {key: 'Subscribed', module: 'standard'},
    [FieldTranslation.COURSEUSER_STATUS_SUSPENDED]: {key: '_USER_STATUS_SUSPEND', module: 'standard'},
    [FieldTranslation.COURSEUSER_STATUS_WAITING_LIST]: {key: '_WAITING_USERS', module: 'standard'},
    [FieldTranslation.ENROLLMENT_DATE]: {key: 'Enrollment date', module: 'standard'},
    [FieldTranslation.HAS_ESIGNATURE_ENABLED]: {key: 'E-signature', module: 'apps'},
    [FieldTranslation.IDCOURSE]: {key: 'Id', module: 'standard'},
    [FieldTranslation.LAST_ACCESS]: {key: 'Last access date', module: 'standard'},
    [FieldTranslation.OTHER_COURSES]: {key: 'Other Courses', module: 'learn'},
    [FieldTranslation.SCORE]: {key: 'Score', module: 'standard'},
    [FieldTranslation.STATUS]: {key: 'Status', module: 'standard'},
    [FieldTranslation.TIME_IN_COURSE]: {key: 'Training Material Time', module: 'course'},
    [FieldTranslation.USERNAME]: {key: 'Username', module: 'standard'},

    // Branches dashboard translations
    [FieldTranslation.BRANCH_ID] : {key: 'Branch ID', module: 'standard'},
    [FieldTranslation.COURSE_TYPE_ELEARNING] : {key: 'E-Learning', module: 'standard'},
    [FieldTranslation.COURSE_TYPE_CLASSROOM] : {key: 'Classroom', module: 'standard'},
    [FieldTranslation.COURSE_TYPE_WEBINAR] : {key: 'Webinar', module: 'standard'},
    [FieldTranslation.CREDITS]: { key: '_CREDITS', module: 'standard' },
    [FieldTranslation.FULL_NAME]: {key: 'Fullname', module: 'standard'},
    [FieldTranslation.HAS_CHILDREN] : {key: 'Has Children', module: 'standard'},
    [FieldTranslation.OVERDUE]: {key: 'Overdue', module: 'standard'},
    [FieldTranslation.TITLE]: { key: '_TITLE', module: 'standard' },
    [FieldTranslation.TOTAL_USERS] : {key: 'Users', module: 'salesforce'},
};

export class BaseDashboardManagerResponse implements BaseResponse {
    success: boolean;
    data?: any;
    error?: string;
    errorCode?: number;
    constructor() {
        this.success = true;
    }
}

