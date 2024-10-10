const { processLdRequestWorkflow } = require('./processLdRequest.js'),
 { sendSlackMessage } = require('../services/slackService.js'),


  /**
   * Lambda function handler that executes the LaunchDarkly request processing workflow.
   *
   * @returns {Promise<Object>} - A promise that resolves to an object with statusCode and body properties.
   * @throws {Error} - Throws an error if `processLdRequestWorkflow` fails, which is caught and logged.
   */
  handler = async () => {
    try {
      await processLdRequestWorkflow();

      return { statusCode: 200, body: JSON.stringify('Success') };
    }
    catch (error) {
      await sendSlackMessage(`<@trial-engineers> Status: Failed - 500 Internal Server Error: ${error.message}`);

      return {
        statusCode: 500,
        body: JSON.stringify({ message: 'Internal Server Error' })
      };
    }
  };

module.exports = { handler };

// For testing purposes
if (require.main === module) {
    handler().then(console.log).catch(console.error);
}
