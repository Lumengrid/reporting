import { FieldsList } from '../models/report-manager';
import { DateOptionsValueDescriptor } from '../models/custom-report';
import moment from 'moment-timezone';

export class Utils {

    public static sleep(ms: number) {
        return new Promise(resolve => {
            setTimeout(resolve, ms);
        });
    }

    public parseLegacyFilterDateRange(newReportDateFilter: DateOptionsValueDescriptor, legacyDateFromFilter: any, legacyDateToFilter: any): DateOptionsValueDescriptor {
        return {
            ...newReportDateFilter,
            any: false,
            type: 'range',
            days: 0,
            operator: 'range',
            from: legacyDateFromFilter,
            to: legacyDateToFilter,
        };
    }

    parseLegacyFilterDate(newReportDateFilter: DateOptionsValueDescriptor, legacyDateFilter: any): DateOptionsValueDescriptor {
        switch (legacyDateFilter.type) {
            case 'ndago':
                return this.parseLegacyNDaysAgo(newReportDateFilter, legacyDateFilter);
            case 'range':
                return this.parseLegacyRange(newReportDateFilter, legacyDateFilter);
            default:
                return newReportDateFilter;
        }
    }

    private parseLegacyNDaysAgo(newReportDateFilter: DateOptionsValueDescriptor, legacyDateFilter: any): DateOptionsValueDescriptor {
        switch (legacyDateFilter.data.combobox) {
            case '<':
                newReportDateFilter.operator = 'isAfter';
                newReportDateFilter.days = +legacyDateFilter.data.days_count;
                break;
            case '<=':
                newReportDateFilter.operator = 'isAfter';
                newReportDateFilter.days = +legacyDateFilter.data.days_count + 1;
                break;
            case '>':
                newReportDateFilter.operator = 'isBefore';
                newReportDateFilter.days = +legacyDateFilter.data.days_count;
                break;
            case '>=':
                newReportDateFilter.operator = 'isBefore';
                newReportDateFilter.days = (+legacyDateFilter.data.days_count > 0) ? +legacyDateFilter.data.days_count - 1 : 0;
                break;
            case '=':
                // new type of operator - we need to map it in the FE and in Query composition
                newReportDateFilter.operator = 'isEqual';
                newReportDateFilter.days = +legacyDateFilter.data.days_count;
                break;
            default:
                return newReportDateFilter;
        }
        newReportDateFilter.type = 'relative';
        newReportDateFilter.any = false;

        return newReportDateFilter;
    }

    private parseLegacyRange(newReportDateFilter: DateOptionsValueDescriptor, legacyDateFilter: any): DateOptionsValueDescriptor {
        return {
            ...newReportDateFilter,
            any: false,
            type: 'range',
            days: 0,
            operator: 'range',
            from: legacyDateFilter.data.from,
            to: legacyDateFilter.data.to,
        };
    }

    /**
     * Parse the learning objects types from legacy to aamon data model
     * @param legacyLoTypes {string[]} list of types for legacy
     */
    parseLearningObjectTypes(legacyLoTypes: string[]): { [key: string]: boolean } {
        const baseLearningObjectTypes = [
            'authoring',
            'video',
            'poll',
            'file',
            'tincan',
            'test',
            'htmlpage',
            'deliverable',
            'scormorg'
        ];

        if (legacyLoTypes.length === 0) {
            legacyLoTypes = baseLearningObjectTypes;
        }

        // map the legacy types to a map
        const legacyLoTypesMap: { [key: string]: boolean } = {};
        for (const legacyType of legacyLoTypes) {
            legacyLoTypesMap[legacyType] = true;
        }

        // parse the legacy types to aamon types
        const parsedTypes: { [key: string]: boolean } = {};
        for (const type of baseLearningObjectTypes) {
            parsedTypes[type] = legacyLoTypesMap[type] === true;
        }
        return parsedTypes;
    }

    /**
     * Remove https, and www from a string
     * @param url The url to purify
     */
    purifyUrl(url: string): string {
        if (url) {
            url = url.replace(/^(?:https?:\/\/)?(?:www\.)?/i, '').split('/')[0];
        }
        return url;
    }

    static stringToBoolean(str: string): boolean {
        switch (str.toLowerCase().trim()) {
            case 'true':
            case 'yes':
            case '1':
                return true;
            case 'false':
            case 'no':
            case '0':
                return false;
            default:
                return Boolean(str);
        }
    }

    // Used in Report Manager, generic method to retrieve selected fields
    getFieldsForManager(reportManagerFields: FieldsList[], translations: { [key: string]: string }) {
        const result = [];
        for (const field of reportManagerFields) {
            result.push({
                field,
                idLabel: field,
                mandatory: false,
                isAdditionalField: false,
                translation: translations[field]
            });
        }
        return result;
    }


    // Returns the object with the keys sorted
    public static orderObjectKeys(objectParam: {[key: string]: any }): {[key: string]: any} {
        return Object.keys(objectParam).sort().reduce(
            (obj, key) => {
                obj[key] = objectParam[key];
                return obj;
            }, {} as {[key: string]: any });
    }

    /**
     * Convert the datetime to the reference timezone and return in UTC
     * @param date {string} The Date (could have also the time)
     * @param time {string} The Time
     * @param timezone {string} The Timezone
     * @returns {moment.Moment} Return a DateTime Moment object with Timezone and UTC
     */
    getDateTimeInTimezoneAndUTC(date: string, time: string, timezone: string): moment.Moment {

        // Create a Moment Date object for the "date" and "time"
        const dateMoment = moment(date).format('YYYY-MM-DD');
        const dateTimeMoment = moment(`${dateMoment} ${time}`).format('YYYY-MM-DD HH:mm:ss');

        return moment.tz(dateTimeMoment, timezone).utc();
    }

    getMicroTime(): number {
        return (Date.now ? Date.now() : new Date().getTime()) / 1000;
    }

}
