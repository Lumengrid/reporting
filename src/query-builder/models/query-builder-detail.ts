import { CustomReportTypeDetail, LastEditByDetails } from '../interfaces/query-builder.interface';
import { InformationReport } from '../../models/report-manager';

export const QUERY_BUILDER_ACTIVE = 1;

export class QueryBuilderDetail {
    id: string;
    name: string;
    platform: string;
    description: string;
    creationDate: string;
    authorId: string|number;
    lastEditBy: number;
    lastEditByDetails: LastEditByDetails;
    lastEditByDate: string;
    status: number | boolean; // TODO Remove boolean after db alignment
    sql?: string;
    json?: string;
    deleted?: boolean;
    relatedReports?: InformationReport[];

    constructor(data: CustomReportTypeDetail) {
        this.id = data.id;
        this.name = data.name;
        this.platform = data.platform;
        this.description = data.description;
        this.creationDate = data.creationDate;
        this.authorId = data.authorId;
        this.lastEditBy = data.lastEditBy;
        this.lastEditByDetails = data.lastEditByDetails;
        this.lastEditByDate = data.lastEditByDate;
        this.status = data.status;
        this.sql = data.sql;
        this.json = data.json;
        this.deleted = data.deleted;
        this.relatedReports = data.relatedReports;
    }
}
