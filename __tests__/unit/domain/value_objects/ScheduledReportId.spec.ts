import { ScheduledReportId } from '../../../../src/domain/value_objects/ScheduledReportId';
import { InvalidScheduledReportIdException } from '../../../../src/domain/exceptions/InvalidScheduledReportIdException';

describe('ScheduledReportId', () => {
  it.each([ // a list of non valid uuids
    ['12345'],
    ['d54e7723-0c33-41ba-92b7-e6e5fd95825'], // 1 byte shorter
    ['d54e77230c3341ba92b7e6e5fd958253'], // missing hyphens
    [123], // not even a string
    [NaN],
  ])('Cannot be built using wrong report ids', (uuid) => {
    // @ts-ignore
    expect(() => new ScheduledReportId(uuid, 'hydra.docebosaas.com')).toThrow(InvalidScheduledReportIdException);
  });

  it.each([ // a list of non valid domains
    [''],
    [123], // not even a string
    [NaN],
  ])('Cannot be built using wrong platform url', (platformUrl) => {
    // @ts-ignore
    expect(() => new ScheduledReportId('d54e7723-0c33-41ba-92b7-e6e5fd958253', platformUrl))
      .toThrow(InvalidScheduledReportIdException);
  });

  it('Can be built using proper uuid and platform', () => {
    const id = new ScheduledReportId('d54e7723-0c33-41ba-92b7-e6e5fd958253', 'someurl.com');

    expect(id.ReportId).toEqual('d54e7723-0c33-41ba-92b7-e6e5fd958253');
    expect(id.Platform).toEqual('someurl.com');
  });
});
