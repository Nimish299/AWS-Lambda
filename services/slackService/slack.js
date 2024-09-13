
const SLACK_WEBHOOK = 'https://hooks.slack.com/services/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';

/**
 * Sends a message to a Slack channel using a webhook.
 *
 * This function posts a message to a specified Slack webhook URL. It constructs a payload with the given message
 * and sends it as a JSON object in the request body.
 *
 * @param {string} message - The message text to send to the Slack channel.
 * @returns {Promise<void>} - A promise that resolves when the request completes.
 * @throws {Error} - Throws an error if the fetch request fails.
 */
async function sendSlackMessage (message) {
  const payload = message.blocks ? message : { text: message },
    response = await fetch(`${SLACK_WEBHOOK}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

  if (!response.ok) {
    throw new Error(`HTTP error! Status: ${response.status}`);
  }
}
module.exports = { sendSlackMessage };
