import { SessionResponse } from '../hydra';

export enum UserLevels {
    GOD_ADMIN = 'super_admin',
    POWER_USER = 'power_user',
    USER = 'user'
}

class Permissions {
    public viewReport: boolean;
    public updateReport: boolean;
    public manager: boolean;
    public viewEcommerceTransaction: boolean;

    public constructor() {
        this.viewReport = this.updateReport = this.manager = false;
    }
}

export default class UserManager {
    private permissions: Permissions;
    private groups: number[];
    private branches: number[];
    private branchesWithParents: number[];

    private idUser: number;
    private username: string;
    private eMail: string;
    private erpAdmin: boolean;
    private level: string;
    private lang: string;
    private langCode: string;
    private timezone: string;

    public constructor(session?: SessionResponse) {
        if (session && session.data && session.data.user) {
            this.idUser = session.data.user.idUser;
            this.username = session.data.user.username;
            this.eMail = session.data.user.eMail;
            this.level = session.data.user.level;
            this.erpAdmin = session.data.user.erpAdmin;
            this.lang = session.data.user.lang;
            this.langCode = session.data.user.langCode;
            this.permissions = session.data.user.permissions;
            this.groups = session.data.user.groups;
            this.branches = session.data.user.branches;
            this.branchesWithParents = session.data.user.branchesWithParents;
            this.timezone = session.data.user.timezone;
        } else {
            this.permissions = new Permissions();
            this.erpAdmin = false;
            this.eMail = this.level = this.lang = this.langCode = this.timezone = '';
            this.idUser = 0;
            this.groups = this.branches = this.branchesWithParents = [];
        }
    }

    public getIdUser(): number {
        return this.idUser;
    }

    public getUsername(): string {
        return this.username;
    }

    public getEMail(): string {
        return this.eMail;
    }

    public getLang(): string {
        return this.lang;
    }

    public getLangCode(): string {
        return this.langCode;
    }

    public getLevel(): string {
        return this.level;
    }

    public canViewReport(): boolean {
        return this.permissions.viewReport;
    }

    public canUpdateReport(): boolean {
        return this.permissions.updateReport;
    }

    public canBeManager(): boolean {
        return this.permissions.manager;
    }

    public canViewEcommerceTransaction(): boolean {
        return this.permissions.viewEcommerceTransaction;
    }

    public isERPAdmin(): boolean {
        return this.erpAdmin;
    }

    public isGodAdmin(): boolean {
        return this.level === UserLevels.GOD_ADMIN;
    }

    public isPowerUser(): boolean {
        return this.level === UserLevels.POWER_USER;
    }

    public isUser(): boolean {
        return this.level === UserLevels.USER;
    }

    public getUserGroups(): number[] {
        return this.groups;
    }

    public getUserBranches(): number[] {
        return this.branches;
    }

    public getUserBranchesWithParents(): number[] {
        return this.branchesWithParents;
    }
    public getTimezone(): string {
        return this.timezone;
    }
}
