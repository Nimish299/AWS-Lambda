const { fetchDataFromApi, sendDataToApi } = require('../services/ldService/ld.js');
const { ListObjects, GetObject } = require('../services/s3Service/s3.js');
const { sendSlackMessage } = require('../services/slackService/slack.js');
const _ = require('lodash');
const csv = require('csv-parser');
const { Readable } = require('stream');
const zlib = require('zlib'),

  /* eslint-disable no-process-env */

  BUCKET_NAME = process.env.BUCKET_NAME,
  Limit = process.env.LIMIT,
  projectKey = process.env.PROJECT_KEY,
  environmentKey = process.env.ENVIRONMENT_KEY,
  segmentKey = process.env.SEGMENT_KEY,
  ldAccessToken = process.env.API_KEY,
  baseUrl = process.env.LD_BASE_URL;

let lastUpdatedDate,
  allManifestUrls = [];

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

  const validRule = segmentData.rules.find((rule) => {
    if (rule?.description) {
      const longDateFormat = rule.description.replace(' at', '') + ' UTC',
        parsedDate = new Date(longDateFormat);

      return !isNaN(parsedDate);
    }

    return false;
  });

  if (validRule) {
    const ruleDescription = validRule.description.replace(' at', '') + ' UTC';

    lastUpdateDate = new Date(ruleDescription);
  }
  else {
    const fallbackDate = process.env.FALLBACK_DATE || '2024-09-03';

    lastUpdateDate = new Date(fallbackDate);
  }

  if (!validRule) {
    // eslint-disable-next-line max-len
    sendSlackMessage(`Fallback to default date as no valid date found in rules. Using fallback date: ${lastUpdateDate}`);
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

  // Helper function to extract the timestamp from the manifest URL
  const extractTimestamp = (url) => {
    const urlParts = url.split('/'),
      year = urlParts[1],
      month = urlParts[2],
      day = urlParts[3],
      // HHMMSS0000
      time = urlParts[4];

    // Combine the year, month, day, and time to create a comparable timestamp
    return `${year}${month}${day}${time}`;
  };

  // Sort the manifest URLs based on their extracted timestamp
  return manifestUrls.sort((a, b) => {
    const timestampA = extractTimestamp(a),
      timestampB = extractTimestamp(b);

    return timestampA.localeCompare(timestampB);
  });
}


/**
 * Fetches all dates from the last update date to the current date.
 *
 * This function generates an array of date objects starting from the
 * `lastUpdatedDate` up to the current date.
 *
 * @returns {Promise<Date[]>} - A promise that resolves to an array of Date objects representing each day
 * from the `lastUpdatedDate` to the current date (inclusive).
 */
function getDatesFromLastUpdateToCurrent () {
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
    currentIteratingDate.setUTCDate(currentIteratingDate.getUTCDate() + 1);
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


/**
 * Processes the manifest files for a given day.
 *
 * This function retrieves the list of objects from S3 for a specified folder path,
 * filters out manifest files that are not updated yet based on the last updated time,
 * and adds their URLs to the `allManifestUrls` array.
 *
 * @param {Date|string} day - The day to process, provided as a Date object or ISO date string.
 * @returns {Promise<void>} A promise that resolves when the processing is complete.
 */
async function processDailyManifests (day) {
  // Convert the input day to a Date object if it's a string
  const date = new Date(day),

    // Extracting Dates and preparing folder path...
    dayDate = date.toISOString().split('T')[0],
    lastDate = new Date(lastUpdatedDate).toISOString().split('T')[0],
    year = date.getUTCFullYear(),
    month = String(date.getUTCMonth() + 1).padStart(2, '0'),
    dateStr = String(date.getUTCDate()).padStart(2, '0'),
    folderPath = `pqa_trials/${year}/${month}/${dateStr}/`;

  let lastUpdatedTime,
    manifest_url = [];


  /**
            * Pointing the cursor to the last updated time.
            * Adding a condition to start from the next time after the last update.
            */
  if (dayDate === lastDate) {
    const hours = String(lastUpdatedDate.getUTCHours()).padStart(2, '0'),
      minutes = String(lastUpdatedDate.getUTCMinutes()).padStart(2, '0'),
      seconds = String(lastUpdatedDate.getUTCSeconds()).padStart(2, '0');

    lastUpdatedTime = `${hours}${minutes}${seconds}0000`;
  }
  else {
    lastUpdatedTime = '0000000000';
  }

  try {
    // Retrieve the list of objects from S3
    const response = await ListObjects(folderPath),

      // Filter the list of objects to include only manifest files that are updated
      filteredContents = _.filter(response.Contents, (item) => {
        const parts = item.Key.split('/');

        if (parts.length > 4 && parts[4] && parts[5] === 'manifest') {
          const numericPart = parts[4];

          return numericPart > lastUpdatedTime;
        }

        return false;
      });

    // Collect the URLs of the filtered manifest files
    filteredContents.forEach((item) => {
      manifest_url.push(item.Key);
    });

    // Add collected URLs to the global array
    allManifestUrls.push(...manifest_url);
  }
  catch (error) {
    // Send error message if list retrieval fails
    await sendSlackMessage(`<@nimish.agrawal>Error processing List at ${folderPath}: ${error}`);
    throw error;
  }
}

/**
 * Handles the workflow for processing a LaunchDarkly (LD) request.
 *
 * This asynchronous function manages the steps required to process a request, which include:
 * - Fetching data from LD and S3
 * - Updating LD with the processed data
 *
 */
async function processLdRequestWorkflow () {
  // Fetch Segment Data  using LD API
  const API = `${baseUrl}/segments/${projectKey}/${environmentKey}/${segmentKey}`,

    // Fetching Data from LD
    segmentData = await fetchDataFromApi(API, ldAccessToken, sendSlackMessage);

  // Extract the last updated date from the description (i.e., rule name)
  // lastUpdatedDate is set by setDescription, which ensures a valid date or fallback
  lastUpdatedDate = setLastUpdateDateFromSegment(segmentData);
  let domains = [];

  try {
    // Extract all days between the last updated date and today
    const daysToProcess = getDatesFromLastUpdateToCurrent();
    let allFileUrls = [],
      lastFilePath,
      manifestPromises,
      // Array to store promises for each day's processing
      dayProcessingPromises = [];

    // Extracting All the URN of data file from Manifest  from Last updated date to current date
    for (const day of daysToProcess) {
      // For each day, push a promise returned by processDailyManifests(day) to the dayProcessingPromises array
      // This ensures all days are processed concurrently
      dayProcessingPromises.push(processDailyManifests(day));
    }
    // Wait for all promises to resolve
    await Promise.all(dayProcessingPromises);

    // `allManifestUrls` now contains all manifest URLs collected from all days
    // Sort the URLs in ascending order
    // Need latest  Manifest URL  to update last update date
    allManifestUrls = sortS3ManifestUrlsByTimestamp(allManifestUrls);

    // Extract All File url using Manifest File
    manifestPromises = allManifestUrls.map(async (manifestPath) => {
      try {
        // Fetch the manifest file from the given path
        const data = await GetObject(manifestPath),
          // Convert the readable stream  to a string
          manifestContent = await streamToString(data.Body),
          manifestJson = JSON.parse(manifestContent),
          File_urls = manifestJson.entries.map((entry) => { return entry.url; });

        return File_urls;
      }
      catch (error) {
        await sendSlackMessage(`<@nimish.agrawal>Error processing manifest at ${manifestPath}: ${error}`);

        throw error;
      }
    });
    // Create an array of promises where each promise handles the extraction of file URLs from a manifest
    allFileUrls = await Promise.all(manifestPromises);
    allFileUrls = allFileUrls.flat();
    try {
      let domainPromises = allFileUrls.map(async (filePath) => {
        try {
          // Remove the S3 bucket prefix from the file path
          filePath = filePath.replace(`s3://${BUCKET_NAME}/`, '');

          // Fetch the object from S3
          const data = await GetObject(filePath),

            // Create a gunzip stream to decompress the data
            gunzip = zlib.createGunzip(),

            // Create a readable stream from the object body and pipe it through the gunzip
            csvStream = Readable.from(data.Body).pipe(gunzip),

            parsedData = [],
            // Create a CSV parser to handle the incoming data
            parser = csv({ headers: false });

          // Process each row from the CSV stream and extracting domains
          for await (const row of csvStream.pipe(parser)) {
            const domain = row[0];

            parsedData.push(domain);
          }

          return parsedData;
        }
        catch (error) {
          if (error.name === 'NoSuchKey') {
            await sendSlackMessage(`<@nimish.agrawal>${filePath}, skipping... `);

            return [];
          }
        }
      });

      // Create an array of promises where each promise handles the extraction of Domain from file URLs
      domains = await Promise.all(domainPromises);
      domains = domains.flat();
      domains = _.uniq(_.compact(domains));
    }
    catch (error) {
      await sendSlackMessage(`<@nimish.agrawal>Error processing file: ${error}`);

      // Return an empty array in case of error
      return []; // Return an empty array in case of error
    }


    // No Need to update if there are no domain
    if (domains.length === 0) {
      let formattedLastUpdate = lastUpdatedDate.toLocaleString('en-US', {
        month: 'long',
        day: 'numeric',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: true,
        timeZone: 'UTC'
      });

      await sendSlackMessage(`domains is empty, So not updating on ${formattedLastUpdate}`);

      return;
    }


    // update the lastUpdatedDate
    lastFilePath = _.last(allFileUrls);

    if (lastFilePath) {
      // Take the Time part from URL
      const parts = lastFilePath.split('/'),
        numericPart = parts[7];

      if (numericPart && numericPart.length === 10) {
        const year = parts[4],
          month = parts[5],
          day = parts[6],
          hour = numericPart.substring(0, 2),
          minute = numericPart.substring(2, 4),
          second = numericPart.substring(4, 6),
          dateTimeString = `${year}-${month}-${day}T${hour}:${minute}:${second}Z`,

          parsedDate = new Date(dateTimeString),
          formattedDate = parsedDate.toLocaleString('en-US', {
            month: 'long',
            day: 'numeric',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: true,
            timeZone: 'UTC'
          });

        lastUpdatedDate = formattedDate;
      }
      else {
        await sendSlackMessage(`<@nimish.agrawal>'Invalid numeric part format in': ${lastFilePath}`);
      }
    }
  }
  catch (error) {
    await sendSlackMessage(`<@nimish.agrawal>Error processing data From S3: ${error}`);
    throw error;
  }

  try {
    // Just to make sure it run if we get any new domain
    if (domains.length > 0) {
      let patchOperation = [],
        updatedEmailDomains = [...domains],
        initialOperationType = 'add',
        resp;

      // Check if there is some rule exist or not
      if (segmentData.rules && segmentData.rules.length > 0) {
        const existingEmailDomains = segmentData.rules[0]?.clauses[0]?.values || [];

        // Merge old data with new data as we will replace First rule till limit
        updatedEmailDomains = [...existingEmailDomains, ...updatedEmailDomains];
        initialOperationType = 'replace';
      }
      // Check if the number of updated email domains exceeds the limit
      if (updatedEmailDomains.length > Limit) {
        // Divide the email domains into chunks of size `Limit`
        for (let count = 0; count < updatedEmailDomains.length; count += Limit) {
          let emailDomainsChunk = updatedEmailDomains.slice(count, count + Limit);

          // Json to be send on patchOperation
          patchOperation.push({
            // Use 'replace' for the first chunk, 'add' for subsequent chunks
            op: count === 0 ? initialOperationType : 'add',
            path: '/rules/0',
            value: {
              clauses: [
                {
                  contextKind: 'user',
                  attribute: 'emailDomain',
                  op: 'in',
                  values: emailDomainsChunk,
                  negate: false
                }
              ],
              description: lastUpdatedDate


            }
          });
        }
      }
      else {
        patchOperation.push({
          op: initialOperationType,
          path: '/rules/0',
          value: {
            clauses: [
              {
                contextKind: 'user',
                attribute: 'emailDomain',
                op: 'in',
                values: updatedEmailDomains,
                negate: false
              }
            ],
            description: lastUpdatedDate

          }
        });
      }
      // Send the patch operation to the API and log the response
      resp = await sendDataToApi(API, ldAccessToken, patchOperation, sendSlackMessage);
      const numberOfDomains = domains.length,

        successMessage = {
          'text': 'Domain Update Notification',
          'blocks': [
            {
              'type': 'section',
              'text': {
                'type': 'mrkdwn',
                'text': '*Successfully updated the domains in LaunchDarkly:*'
              }
            },
            {
              'type': 'section',
              'block_id': 'section123',
              'fields': [
                {
                  'type': 'mrkdwn',
                  'text': `*Total number of domains added:* ${numberOfDomains}`
                },
                {
                  'type': 'mrkdwn',
                  'text': `*Last updated:* ${lastUpdatedDate}`
                }
              ]
            }
          ]
        };

      if (resp.ok) {
        await sendSlackMessage(successMessage);
      }
    }
  }
  catch (error) {
    sendSlackMessage(`<@nimish.agrawal>Error Patching dates: ${error}`);
    throw error;
  }
}

module.exports = { processLdRequestWorkflow };
