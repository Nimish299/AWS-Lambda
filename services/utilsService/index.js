const _ = require('lodash');
const zlib = require('zlib');
const { Readable } = require('stream');
const csv = require('csv-parser');
const { sendSlackMessage } = require('../slackService');

/**
 * Formats a success message for domain updates in LaunchDarkly.
 *
 * @param {string} lastUpdatedDate - The last updated date for domains.
 * @param {number} numberOfDomains - The total number of domains added.
 * @returns {Object} - A Slack message object containing the success message.
 */
function formatSuccessMessage (lastUpdatedDate, numberOfDomains) {
  return {
    'text': 'Domain Update Summary',
    'blocks': [
      {
        'type': 'section',
        'text': {
          'type': 'mrkdwn',
          'text': '*LaunchDarkly Domain Update - Success*'
        }
      },
      {
        'type': 'section',
        'block_id': 'section123',
        'fields': [
          {
            'type': 'mrkdwn',
            'text': `*Total Domains Added:* ${numberOfDomains}`
          },
          {
            'type': 'mrkdwn',
            'text': `*Last Updated On:* ${lastUpdatedDate}`
          }
        ]
      }
    ]
  };
}

/**
 * Formats a date and time string from given parts and a numeric time part.
 *
 * @param {Array} parts - An array containing parts of the date.
 * @param {string} numericPart - A string representing the numeric time (e.g., 'HHMMSS').
 * @returns {string} - A formatted date-time string.
 */
function formatDateTimeString (parts, numericPart) {
  // Optionally, you can replace lodash's _.get with simple indexing since the indices are known
  const year = _.get(parts, '[4]'),
    month = _.get(parts, '[5]'),
    day = _.get(parts, '[6]'),

    hour = numericPart.substring(0, 2),
    minute = numericPart.substring(2, 4),
    second = numericPart.substring(4, 6);

  // Return the formatted date-time string in ISO 8601 format
  return `${year}-${month}-${day}T${hour}:${minute}:${second}Z`;
}


/**
 * Unzips and parses CSV data from a gzipped file.
 *
 * @param {Buffer} gzippedData - The gzipped data buffer to be decompressed and parsed.
 * @param {boolean} hasHeaders - Indicates if the CSV file contains headers.
 * @param {number} columnIndex - The index of the column to extract from each row (e.g., 0 for the first column).
 * @returns {Promise<Array>} - A promise that resolves to an array of data from the specified column.
 */
async function unzipAndParseCSV (gzippedData, hasHeaders = false, columnIndex = 0) {
  try {
    // Create a gunzip stream to decompress the gzipped data
    const gunzip = zlib.createGunzip(),

      // Create a readable stream from the gzipped data and pipe it through the gunzip
      csvStream = Readable.from(gzippedData).pipe(gunzip),

      // Initialize an array to store parsed data
      parsedData = [],

      // Create a CSV parser and process each row
      parser = csv({ headers: hasHeaders });

    for await (const row of csvStream.pipe(parser)) {
      // Extract the value from the specified column index
      const columnValue = row[columnIndex];

      parsedData.push(columnValue);
    }

    // Return the parsed data
    return parsedData;
  }
  catch (error) {
    await sendSlackMessage(`<@trial-engineers >Error during unzipping or parsing CSV data:: ${error}`);

    return Promise.reject(error);
  }
}

module.exports = { formatDateTimeString, formatSuccessMessage, unzipAndParseCSV };
