import { ReportId } from '../value_objects/ReportId';
import { ReportManagerInfo } from '../../models/report-manager';
import moment from 'moment';
import { ReportValidation } from '../components/ReportValidation';
import { ReportPatchInput } from '../interfaces/patch.interface';
import { ReportPatch } from '../components/ReportPatch';

export class Report {
    public constructor(
        private readonly id: ReportId,
        private info: ReportManagerInfo,
    ) {
    }

    public get Id(): ReportId {
        return this.id;
    }

    public get Info(): ReportManagerInfo {
        return this.info;
    }

    public retrieveReportManagerInfoData(data: ReportManagerInfo): ReportManagerInfo {
        ReportValidation.FIELDS_NOT_EDITABLE.map((field) => {
            if (data.hasOwnProperty(field) && data[field] !== undefined) {
                delete data[field];
            }
        });

        return {
            ...this.info,
            ...data
        };
    }

    public update(hostname: string,
                  subfolder: string,
                  userId: number,
                  userLevel: string,
                  isDatalakeV2Active: boolean,
                  isReportDownloadPermissionLinkEnable: boolean,
                  patch: boolean,
                  data: ReportManagerInfo | ReportPatchInput
    ): void {
        ReportValidation.validate(this.info, userLevel, isReportDownloadPermissionLinkEnable, patch, data);
        const newData = {
            ...data
        };
        const newInfo = patch
            ? ReportPatch.execute(this.info, newData as ReportPatchInput)
            : this.retrieveReportManagerInfoData(newData as ReportManagerInfo);

        ReportValidation.checkFilters(newInfo);
        ReportValidation.checkDateOptions(newInfo);
        ReportValidation.checkEnrollment(newInfo);
        ReportValidation.checkMandatoryFields(isDatalakeV2Active, newInfo);
        ReportValidation.checkMandatoryFieldsForSpecificReport(newInfo);

        newInfo.lastEdit = moment(new Date()).format('YYYY-MM-DD HH:mm:ss')
        newInfo.lastEditBy.idUser = userId;
        newInfo.lastEditBy.firstname = newInfo.lastEditBy.lastname = newInfo.lastEditBy.username = newInfo.lastEditBy.avatar = '';
        if (newInfo.planning.active && newInfo.planning.option) {
            newInfo.planning.option.hostname = hostname;
            newInfo.planning.option.subfolder = subfolder ?? '';
        }

        this.info = newInfo;
    }
}
