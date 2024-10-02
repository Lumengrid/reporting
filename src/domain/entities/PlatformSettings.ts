export interface PlatformSettingsDetails {
    readonly toggleDatalakeV3: boolean | null;
}

export class PlatformSettings {
    public constructor(
        private readonly platform: string,
        private readonly details: PlatformSettingsDetails,
    ) {
    }

    public get Details(): PlatformSettingsDetails {
        return this.details;
    }
}
