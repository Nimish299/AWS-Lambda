
import fetch from 'node-fetch';

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
export async function sendSlackMessage(message) {
    const payload = { text: message, };
    const response = await fetch('https://hooks.slack.com/services/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', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    });
  }