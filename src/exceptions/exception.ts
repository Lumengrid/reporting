/**
 * Generic exception class to extend with custom errors.
 * Can be used as a stand-alone exception.
 */
export class Exception extends Error {
  constructor(message?: string, protected readonly code?: number) {
    super(message);

    // This is needed to dynamycally set the error name
    // Otherwise it's always 'Error'
    this.name = this.constructor.name;
  }

  public getCode(): number | undefined {
    return this.code;
  }
}
