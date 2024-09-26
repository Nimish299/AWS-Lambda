const _ = require('lodash');
const zlib = require('zlib');
const { Readable } = require('stream');
const csv = require('csv-parser');
const { sendSlackMessage } = require('../slackService');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');

dayjs.extend(utc);

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

    hour = numericPart?.substring(0, 2),
    minute = numericPart?.substring(2, 4),
    second = numericPart?.substring(4, 6);

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
    const gunzip = zlib?.createGunzip(),

      // Create a readable stream from the gzipped data and pipe it through the gunzip
      csvStream = Readable?.from(gzippedData)?.pipe(gunzip),

      // Initialize an array to store parsed data
      parsedData = [],

      // Create a CSV parser and process each row
      parser = csv({ headers: hasHeaders });

    for await (const row of csvStream?.pipe(parser)) {
      // Extract the value from the specified column index
      const columnValue = row[columnIndex];

      parsedData?.push(columnValue);
    }

    // Return the parsed data
    return parsedData;
  }
  catch (error) {
    await sendSlackMessage(`<@trial-engineers >Error during unzipping or parsing CSV data:: ${error}`);

    return Promise.reject(error);
  }
}


/**
 * Updates the last update date based on the rule descriptions in `segmentData`.
 *
 * The function searches through the `description` field of each rule in `segmentData`.
 * It tries to parse the description as a date. If a valid date is found, it is used as the last update date.
 * If no valid date is found in any rules, a fallback date is used.
 *
 * @param {Object} segmentData - The segment data containing rules with descriptions.
 *
 * @returns {Date} - The formatted date found in the description, or a fallback date if none is valid.
 */
function setLastUpdateDateFromSegment (segmentData) {
  let lastUpdateDate;

  const validRule = segmentData?.rules?.find((rule) => {
    if (rule?.description) {
      const parsedDate = dayjs(rule?.description).utc();

      return parsedDate?.isValid();
    }

    return false;
  });

  if (validRule) {
    lastUpdateDate = dayjs(validRule?.description)?.utc()?.toDate();
  }
  else {
  /* eslint-disable no-process-env */

    const fallbackDate = process.env.FALLBACK_DATE || '2024-01-01';

    lastUpdateDate = new Date(fallbackDate);
    let formatlastdate = new Date(lastUpdateDate)?.toDateString();

    // eslint-disable-next-line max-len
    sendSlackMessage(`Fallback to default date as no valid date found in rules. Using fallback date: ${formatlastdate}`);
  }

  return lastUpdateDate;
}

/**
 * Sorts an array of S3 manifest URLs based on the embedded date and time in the path.
 * The manifest URLs are assumed to have the format:
 * 'pqa_trials/YYYY/MM/DD/HHMMSS0000/manifest'
 *
 * @param {string[]} manifestUrls - The array of manifest URLs to be sorted.
 * @returns {string[]} The sorted array of manifest URLs.
 */
function sortS3ManifestUrlsByTimestamp (manifestUrls) {
  if (!Array.isArray(manifestUrls) || manifestUrls.length === 0) {
    // Return an empty array if input is invalid or empty
    return [];
  }

  // Sort the manifest URLs based on their extracted timestamp
  return _.sortBy(manifestUrls, (url) => {
    const [, year, month, day, time] = url.split('/');

    return `${year}${month}${day}${time}`;
  });
}


/**
 * Fetches all dates from the last update date to the current date.
 *
 * This function generates an array of date objects starting from the
 * `lastUpdatedDate` up to the current date.
 *
 * @param {Date} lastUpdatedDate - The last update date as a Date object.
 * @returns {Promise<Date[]>} - A promise that resolves to an array of Date objects representing each day
 * from the `lastUpdatedDate` to the current date (inclusive).
 */
function getDatesFromLastUpdateToCurrent (lastUpdatedDate) {
  const currentDate = new Date(),
    results = [];

  // lastUpdatedDate is set by setDescription, which ensures a valid date or fallback
  // so no need to check tempDate
  let currentIteratingDate = new Date(lastUpdatedDate);

  // Normalize currentDate to the start of the current day (00:00:00)
  currentIteratingDate.setUTCHours(0, 0, 0, 0); // Start at midnight of the last update date
  currentDate.setUTCHours(0, 0, 0, 0); // Start at midnight of the current date

  // Iterate through each day from lastUpdatedDate to the current date (inclusive)
  // eslint-disable-next-line no-unmodified-loop-condition
  while (currentIteratingDate <= currentDate) {
    results.push(new Date(currentIteratingDate));
    currentIteratingDate?.setUTCDate(currentIteratingDate?.getUTCDate() + 1);
  }

  return results;
}


/**
 * Convert a stream to a string.
 *
 * This function reads data from a stream, collects chunks of data, and concatenates them into a single string.
 *
 * @param {stream.Readable} stream - The stream to be converted to a string.
 * @returns {Promise<string>} - A promise that resolves with the string representation of the stream.
 */
function streamToString (stream) {
  const chunks = [];

  return new Promise((resolve, reject) => {
    // Collect data chunks from the stream
    stream.on('data', (chunk) => { return chunks.push(chunk); });

    // Resolve the promise with the concatenated string when the stream ends
    stream.on('end', () => { return resolve(Buffer.concat(chunks).toString('utf-8')); });

    // Reject the promise if an error occurs
    stream.on('error', reject);
  });
}
module.exports = { formatDateTimeString,
  formatSuccessMessage,
  unzipAndParseCSV,
  setLastUpdateDateFromSegment,
  streamToString,
  getDatesFromLastUpdateToCurrent,
  sortS3ManifestUrlsByTimestamp
};

