
/**
 * Fetches data from a specified API endpoint.
 *
 * This function performs a GET request to the given API URL, including an authorization header.
 * If the request fails, it logs an error and optionally sends a Slack message.
 *
 * @param {string} API - The URL of the API endpoint to fetch data from.
 * @param {string} apiKey - The authorization key for accessing the API.
 * @param {Function} sendSlackMessage - Function to send error messages to Slack.
 * @returns {Promise<Object|null>} - The JSON data from the API if successful, or null if there was an error.
 */
const fetchDataFromApi = async (API, apiKey, sendSlackMessage) => {
    try {
      let resp = await fetch(`${API}`,
        {
          method: 'GET',
          headers: {
            'Authorization': apiKey
          }
        });

      if (!resp.ok) {
        const errorText = await resp.text();
        // Send error message to Slack and log the error

        await sendSlackMessage(`GET request failed with status ${resp.status}: ${errorText}`);

        return null;
      }

      const data = await resp.json();

      return data;
    }
    catch (error) {
    // Send error message to Slack and log the error
      await sendSlackMessage(`Error fetching data from API: ${error.message}`);

      return null;
    }
  },

  /**
   * Sends a PATCH request to a specified API endpoint.
   *
   * This function sends data to the API using a PATCH method. It includes authorization and content-type headers,
   * and sends a JSON payload containing patch operations. If the request fails,
   * it logs an error and optionally sends a Slack message.
   *
   * @param {string} API - The URL of the API endpoint to send data to.
   * @param {string} apiKey - The authorization key for accessing the API.
   * @param {Array<Object>} patchOperation - The patch operations to include in the request body.
   * @param {Function} sendSlackMessage - Function to send error messages to Slack.
   * @returns {Promise<Object>} - The JSON data from the API response.
   */

  sendDataToApi = async (API, apiKey, patchOperation, sendSlackMessage) => {
    let resp = await fetch(`${API}`,
      {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': apiKey
        },
        body: JSON.stringify({
          patch: [
            ...patchOperation]
        })
      });

    if (!resp.ok) {
      const errorText = await resp.text();

      await sendSlackMessage(`PATCH request failed with status ${resp.status}: ${errorText}`);
    }
    const data = await resp.json();

    return data;
  };

module.exports = { sendDataToApi, fetchDataFromApi };
