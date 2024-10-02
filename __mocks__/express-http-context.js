module.exports = {
    get: jest.fn().mockReturnValue({
        debug: jest.fn(() => {}),
        error: jest.fn(() => {}),
    })
};
