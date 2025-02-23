"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
function isAxiosError(error) {
    return (typeof error === 'object' &&
        error !== null &&
        'response' in error &&
        typeof error.response === 'object');
}
