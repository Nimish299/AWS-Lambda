import processLdRequestWorkflow from './processLdRequest.js';


/**
 * Lambda function handler that executes the LaunchDarkly request processing workflow.
 * 
 * @returns {Promise<Object>} - A promise that resolves to an object with statusCode and body properties.
 * @throws {Error} - Throws an error if `processLdRequestWorkflow` fails, which is caught and logged.
 */
export const handler = async () => {
    try {
        await processLdRequestWorkflow();
        return { statusCode: 200, body: JSON.stringify('Success') };
    } catch (error) {
        console.error('Lambda execution failed:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Internal Server Error' }),
        };
    }
};
handler()