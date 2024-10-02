import moment from 'moment-timezone';
import { isOClockTime, isValidTimezone } from '../../../src/shared/customValidators';


describe('Custom validator', () => {
    it('should pass the check with a valid timezone', () => {
        const allTimezones = moment.tz.names();
        const randomTimezone = allTimezones[Math.floor(Math.random() * allTimezones.length)];

        expect(isValidTimezone(randomTimezone)).toBeTruthy();
    });

    it('shouldn\'t pass the check with a invalid timezone', () => {
        expect(isValidTimezone('randomTimezone')).toBeFalsy();
    });

    it('should pass the check with a valid time frame', () => {
        expect(isOClockTime('10:00')).toBeTruthy();
    });

    it('shouldn\'t pass the check with a invalid time frame', () => {
        expect(isOClockTime('10:01')).toBeFalsy();
    });
});
