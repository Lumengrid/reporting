import { ReportManagerInfo } from '../../models/report-manager';
import { DateOptionsFilter, InfoFilter, PatchField, ReportPatchInput } from '../interfaces/patch.interface';
import { DateOptionsValueDescriptor, SelectionInfo } from '../../models/custom-report';

export class ReportPatch {
    private static patchSimpleFields(newInfo: ReportManagerInfo, data: ReportPatchInput): void {
        const fields = [
            'loginRequired',
            'description',
            'title',
            'timezone',
            'fields',
            'conditions',
            'userAdditionalFieldsFilter',
            'loTypes',
        ];
        fields.map((field) => {
            if (data[field] !== undefined) {
                newInfo[field] = data[field];
            }
        });
    }

    private static patchSelectInfo(data: InfoFilter[]): SelectionInfo[] {
        return data as SelectionInfo[];
    }

    private static patchPlanning(newInfo: ReportManagerInfo, data: ReportPatchInput): void {
        if (data.planning === undefined) {
            return;
        }
        if (data.planning.active !== undefined) {
            newInfo.planning.active = data.planning.active;
        }
        if (data.planning.option === undefined) {
            return;
        }
        const fields = [
            'isPaused', 'recipients', 'every',
            'timeFrame', 'scheduleFrom', 'hostname',
            'subfolder', 'startHour', 'timezone'
        ];
        fields.map((field) => {
            if (data.planning.option[field] !== undefined) {
                newInfo.planning.option[field] = data.planning.option[field];
            }
        });
    }

    private static patchFilter(newInfo: ReportManagerInfo, data: ReportPatchInput, filterKey: string,
                        simpleFields: string[], infoFilterFields?: string[], dateFilterFields?: string[]): void {
        if (data[filterKey] === undefined) {
            return;
        }
        simpleFields.map((field) => {
            if (data[filterKey][field] !== undefined) {
                if (newInfo[filterKey] === undefined) {
                    newInfo[filterKey] = {};
                    newInfo[filterKey][field] = {};
                }
                if (newInfo[filterKey][field] === undefined) {
                    newInfo[filterKey][field] = {};
                }
                newInfo[filterKey][field] = data[filterKey][field];
            }
        });
        infoFilterFields.map((field) => {
            if (data[filterKey][field] !== undefined) {
                if (newInfo[filterKey] === undefined) {
                    newInfo[filterKey] = {};
                    newInfo[filterKey][field] = {};
                }
                if (newInfo[filterKey][field] === undefined) {
                    newInfo[filterKey][field] = {};
                }
                newInfo[filterKey][field] = this.patchSelectInfo(data[filterKey][field])
            }
        });
        dateFilterFields.map((field) => {
            if (newInfo[filterKey] === undefined) {
                newInfo[filterKey] = {
                    any: true,
                    days: 1,
                    type: '',
                    operator: '',
                    to: '',
                    from: ''
                };
            }
            newInfo[filterKey][field] = this.patchDateOptionsFilter(newInfo[filterKey][field], data[filterKey][field])
        });
    }

    private static patchDateOptionsFilter(newInfo: DateOptionsValueDescriptor, data: DateOptionsFilter): DateOptionsValueDescriptor {
        if (data === undefined) {
            return newInfo;
        }
        if (newInfo === undefined) {
            newInfo = {
                any: true,
                days: 1,
                type: '',
                operator: '',
                to: '',
                from: ''
            };
        }
        const fields = [
            'any',
            'operator',
            'type',
            'from',
            'to',
            'days'
        ];
        fields.map((field) => {
            if (data[field] !== undefined) {
                newInfo[field] = data[field];
            }
        });

        return newInfo
    }

    private static patchDateOptions(newInfo: ReportManagerInfo, data: ReportPatchInput): void {
        const fields = [
            'enrollmentDate',
            'completionDate',
            'surveyCompletionDate',
            'archivingDate',
            'courseExpirationDate',
            'issueDate',
            'creationDateOpts',
            'expirationDateOpts',
            'publishedDate',
            'contributionDate',
            'externalTrainingDate'
        ]
        fields.map((field) => {
            newInfo[field] = this.patchDateOptionsFilter(newInfo[field], data[field]);
        });
    }

    public static execute(reportInfo: ReportManagerInfo, patchInfo: ReportPatchInput): ReportManagerInfo {
        const newInfo = reportInfo;

        this.patchSimpleFields(newInfo, patchInfo);
        this.patchPlanning(newInfo, patchInfo);
        this.patchDateOptions(newInfo, patchInfo);
        const filters = [
            new PatchField('visibility', ['type'], ['users', 'groups', 'branches']),
            new PatchField('sortingOptions', ['selector', 'selectedField', 'orderBy']),
            new PatchField('users',
                ['all', 'hideDeactivated', 'showOnlyLearners', 'hideExpiredUsers', 'isUserAddFields'],
                ['users', 'groups', 'branches']),
            new PatchField('courses', ['all'], ['courses', 'categories', 'instructors', 'courseType']),
            new PatchField('surveys', ['all'], ['surveys']),
            new PatchField('learningPlans', ['all'], ['learningPlans']),
            new PatchField('badges', ['all'], ['badges']),
            new PatchField('assets', ['all'], ['assets', 'channels']),
            new PatchField('sessions', ['all'], ['sessions']),
            new PatchField('instructors', ['all'], ['instructors']),
            new PatchField('certifications', ['all', 'activeCertifications', 'expiredCertifications', 'archivedCertifications', 'conditions'],
                ['certifications'], ['certificationDate', 'certificationExpirationDate']),
            new PatchField('enrollment', [
                'completed', 'inProgress', 'notStarted',
                'waitingList', 'suspended', 'enrollmentsToConfirm',
                'subscribed', 'overbooking', 'enrollmentTypes'
            ]),
            new PatchField('sessionDates', ['conditions'], [], ['startDate', 'endDate']),
            new PatchField('externalTrainingStatusFilter', ['approved', 'waiting', 'rejected']),
            new PatchField('publishStatus', ['published', 'unpublished']),
            new PatchField('sessionAttendanceType', ['blended', 'fullOnsite', 'fullOnline', 'flexible'])
        ]
        filters.map((filter) => this.patchFilter(newInfo, patchInfo, filter.name, filter.simpleFields, filter.infoFilterFields, filter.dateFilterFields));

        return newInfo;
    }
}
