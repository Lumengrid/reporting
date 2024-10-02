/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: [
    "<rootDir>/src",
    "<rootDir>/__tests__",
  ],
  testMatch: [ "**/__tests__/**/*.spec.ts", "**/src/**/*.spec.ts" ],
  testPathIgnorePatterns: ["/node_modules/"]
};