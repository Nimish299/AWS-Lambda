const { sendSlackMessage } = require('./slackService.js');

/**
 * Fetches data from a specified API endpoint.
 *
 * This function performs a GET request to the given API URL, including an authorization header.
 * If the request fails, it logs an error and optionally sends a Slack message.
 *
 * @param {string} API - The URL of the API endpoint to fetch data from.
 * @param {string} ldAccessToken - The authorization key for accessing the API.
 * @param {Function} sendSlackMessage - Function to send error messages to Slack.
 * @returns {Promise<Object|null>} - The JSON data from the API if successful, or null if there was an error.
 */
async function fetchDataFromApi (API, ldAccessToken) {
  try {
    let resp = await fetch(API,
      {
        method: 'GET',
        headers: {
          'Authorization': ldAccessToken
        }
      });

    if (!resp.ok) {
      const errorText = await resp?.text();
      // Send error message to Slack and log the error

      sendSlackMessage(`GET request failed with status ${resp.status}: ${errorText}`);
      return { isError: true, errorMessage: `Status ${resp.status}: ${errorText}` };
    }

    const data = await resp.json();

    return { isError: false, data: data };
  }
  catch (error) {
  // Send error message to Slack and log the error
    sendSlackMessage(`Error fetching data from API: ${error.message}`);

    return { isError: true, errorMessage: error.message };
  }
}

/**
 * Sends a PATCH request to a specified API endpoint.
 *
 * This function sends data to the API using a PATCH method. It includes authorization and content-type headers,
 * and sends a JSON payload containing patch operations. If the request fails,
 * it logs an error and optionally sends a Slack message.
 *
 * @param {string} API - The URL of the API endpoint to send data to.
 * @param {string} ldAccessToken - The authorization key for accessing the API.
 * @param {Array<Object>} patchOperation - The patch operations to include in the request body.
 * @param {Function} sendSlackMessage - Function to send error messages to Slack.
 * @returns {Promise<Object>} - The JSON data from the API response.
 */
async function sendDataToApi (API, ldAccessToken, patchOperation) {
  try {
    let resp = await fetch(API,
      {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': ldAccessToken
        },
        body: JSON.stringify(patchOperation)
      });

    if (!resp.ok) {
      const errorText = await resp.text();

      sendSlackMessage(`PATCH request failed with status ${resp.status}: ${errorText}`);

      return { isError: true, errorMessage: `Status ${resp.status}: ${errorText}` };
    }

    // return resp;
    return { isError: false, data: resp };
  }
  catch (error) {
    sendSlackMessage(`Error sending PATCH request: ${error.message}`);

    return { isError: true, errorMessage: error.message };
  }
}

module.exports = { sendDataToApi, fetchDataFromApi };
