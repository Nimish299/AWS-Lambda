const { fetchDataFromApi, sendDataToApi } = require('../services/ldService/ld.js');
const { ListObjects, GetObject } = require('../services/s3Service/s3.js');
const { sendSlackMessage } = require('../services/slackService/slack.js');
const _ = require('lodash');
const csv = require('csv-parser');
const { Readable } = require('stream');
const zlib = require('zlib'),

  BUCKET_NAME = process.env.BUCKET_NAME,
  Limit = process.env.LIMIT,
  projectKey = process.env.PROJECT_KEY,
  environmentKey = process.env.ENVIRONMENT_KEY,
  segmentKey = process.env.SEGMENT_KEY,
  apiKey = process.env.API_KEY,
  baseUrl = process.env.LD_BASE_URL;

let last_update_Date;

/**
 * Updates the last update date based on the rule descriptions in `segmentData`.
 *
 * The function searches through the `description` field of each rule in `segmentData`.
 * It tries to parse the description as a date. If a valid date is found, it is used as the last update date.
 * If no valid date is found in any rule, a fallback date is used.
 *
 * @param {Object} segmentData - The segment data containing rules with descriptions.
 *
 * @returns {Date} - The formatted date found in the description, or a fallback date if none is valid.
 */
function setDescription (segmentData) {
  let formattedDate;

  // Check the first rule's description to see if the date format is correct.
  // If it's invalid, check the descriptions in all other rules.
  // If no valid date format is found in any rule, default to a fallback date.
  // default to a fallback date

  const length = segmentData?.rules?.length;

  if (length) {
    for (let i = 0; i < segmentData.rules.length; i++) {
      let description = segmentData.rules[i]?.description;

      if (description) {
        // Remove 'at' and append 'UTC' for proper date formatting
        let longDateFormat = description.replace(' at', '') + ' UTC';
        const parsedDate = new Date(longDateFormat);

        if (!isNaN(parsedDate)) {
          // Valid date found, break the loop
          formattedDate = parsedDate;
          break;
        }
      }
    }
  }

  // If no valid date was found, set a fallback date
  if (!formattedDate) {
    formattedDate = new Date('2024-09-03');

    // eslint-disable-next-line max-len
    sendSlackMessage(`<@nimish.agrawal> Fallback to default date as no valid date found in rules. Using fallback date: ${formattedDate}`);
  }

  return formattedDate;
}

/**
 * Fetches all dates from the last update date to the current date.
 *
 * This function generates an array of date objects starting from the
 * `last_update_Date` up to the current date.
 *
 * @returns {Promise<Date[]>} - A promise that resolves to an array of Date objects representing each day
 * from the `last_update_Date` to the current date (inclusive).
 */
function fetchDates () {
  const currentDate = new Date(),
    Results = [],
    // last_update_Date is set by setDescription, which ensures a valid date or fallback
    // so no need to check tempDate
    tempDate = new Date(last_update_Date),

    // Normalize the time part of the currentDate to start of the day
    endOfCurrentDate = new Date(currentDate);

  endOfCurrentDate.setUTCHours(0, 0, 0, 0);
  endOfCurrentDate.setUTCDate(endOfCurrentDate.getUTCDate() + 1);

  while (tempDate < endOfCurrentDate) {
    Results.push(new Date(tempDate));
    tempDate.setUTCDate(tempDate.getUTCDate() + 1);
  }

  return Results;
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
    segmentData = await fetchDataFromApi(API, apiKey, sendSlackMessage);

  // Extract the last updated date from the description (i.e., rule name)
  // last_update_Date is set by setDescription, which ensures a valid date or fallback
  last_update_Date = setDescription(segmentData);
  let Domains = [];

  try {
    // Extract all days between the last updated date and today
    const days = fetchDates();
    let all_Manifest_url = [],
      All_File_urls = [],
      lastFilePath;
    // Extracting All the URN of data file from Manifest  from Last updated date to current date

    for (const day of days) {
      // Extracting Dates For Condition on Fetching Manifest file
      // As it come in format YYYY-MM-DDTHH:mm:ss.sssZ so split it Using T

      // Day date
      const dayDate = new Date(day).toISOString().split('T')[0],
        // last Updated Date
        lastDate = new Date(last_update_Date).toISOString().split('T')[0],
        // Folder structure in S3 is  pqa_trials/strftime('%Y/%m/%d/%H%M%S0000')
        year = day.getUTCFullYear(),
        // Month is zero-based, so add 1
        month = String(day.getUTCMonth() + 1).padStart(2, '0'),
        date = String(day.getUTCDate()).padStart(2, '0'),
        // Construct the folder path based on the date components
        folderPath = `pqa_trials/${year}/${month}/${date}/`;

      /**
     * Pointing the cursor to the last updated time.
     * Adding a condition to start from the next time after the last update.
     * If the lambda function runs twice a day, this ensures it continues from the correct time.
     */
      let Last_updated_Time, response,
        Manifest_url = [],
        filteredContents = [];

      if (dayDate === lastDate) {
        // Extract hours, minutes, and seconds from the last updated date
        const hours = String(last_update_Date.getUTCHours()).padStart(2, '0'),
          minutes = String(last_update_Date.getUTCMinutes()).padStart(2, '0'),
          seconds = String(last_update_Date.getUTCSeconds()).padStart(2, '0');

        Last_updated_Time = `${hours}${minutes}${seconds}0000`;
      }
      else {
        Last_updated_Time = 0;
      }

      try {
        // List of  Objects on S3 on given folder Path
        response = await ListObjects(folderPath);
      }
      catch (error) {
        sendSlackMessage(`<@nimish.agrawal>Error processing List at ${folderPath}: ${error}`);
        throw error;
      }

      /**
       *
       * Filter all the manifest file  which are not updated yet
       *   code filters the list of S3 objects to include only those
       * manifest files that have a timestamp greater than the last updated time.
       */
      filteredContents = _.filter(response.Contents, (item) => {
        const parts = item.Key.split('/');

        if (parts.length > 4 && parts[4] && parts[5] === 'manifest') {
          const numericPart = parts[4];

          return numericPart > Last_updated_Time;
        }

        return false;
      });

      // Store all Url file


      // Filter all key from Manifest File
      filteredContents.forEach((item) => {
        // Add each manifest file's key (URL) to the Manifest_url array
        Manifest_url.push(item.Key);
      });
      // Store all Url in all_Manifest_url
      all_Manifest_url.push(...Manifest_url);
    }
    // Extract All File url using Manifest File

    // Create an array of promises where each promise handles the extraction of file URLs from a manifest
    All_File_urls = await Promise.all(all_Manifest_url.map(async (manifestPath) => {
      try {
        // Fetch the manifest file from the given path
        const data = await GetObject(manifestPath),
          // Convert the readable stream  to a string
          manifestContent = await streamToString(data.Body),
          // Parse the manifest content
          manifestJson = JSON.parse(manifestContent),
          File_urls = manifestJson.entries.map((entry) => { return entry.url; });

        // Return the extracted URLs
        return File_urls;
      }
      catch (error) {
        await sendSlackMessage(`<@nimish.agrawal>Error processing manifest at ${manifestPath}: ${error}`);

        throw error;
      }
    }));
    All_File_urls = All_File_urls.flat();
    try {
      // Create an array of promises where each promise handles the extraction of Domain from file URLs
      Domains = await Promise.all(All_File_urls.map(async (filePath) => {
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

          // Process each row from the CSV stream and extracting Domains
          for await (const row of csvStream.pipe(parser)) {
            const domain = row[0];

            parsedData.push(domain);
          }

          return parsedData;
        }
        catch (error) {
          if (error.name === 'NoSuchKey') {
            sendSlackMessage(`<@nimish.agrawal>${filePath}, skipping... `);

            return [];
          }
        }
      }));
      Domains = Domains.flat();
    }
    catch (error) {
      await sendSlackMessage(`<@nimish.agrawal>Error processing file: ${error}`);

      // Return an empty array in case of error
      return []; // Return an empty array in case of error
    }


    // No Need to update if there are no domain
    if (Domains.length === 0) {
      last_update_Date = last_update_Date.toLocaleString('en-US', {
        month: 'long',
        day: 'numeric',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: true,
        timeZone: 'UTC'
      });
      await sendSlackMessage(`Domains is empty, So not updating on ${last_update_Date}`);

      return;
    }


    // update the last_update_Date
    lastFilePath = _.last(All_File_urls);

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

        // eslint-disable-next-line require-atomic-updates
        last_update_Date = formattedDate;
      }
      else {
        sendSlackMessage(`<@nimish.agrawal>'Invalid numeric part format in': ${lastFilePath}`);
      }
    }
  }
  catch (error) {
    await sendSlackMessage(`<@nimish.agrawal>Error processing data From S3: ${error}`);
    throw error;
  }

  try {
    // Just to make sure it run if we get any new domain
    if (Domains.length > 0) {
      let patchOperation = [],
        updatedEmailDomains = [...Domains],
        operationType = 'add';

      // Check if there is some rule exist or not
      if (segmentData.rules && segmentData.rules.length > 0) {
        const existingEmailDomains = segmentData.rules[0]?.clauses[0]?.values || [];

        // Merge old data with new data as we will replace First rule till limit
        updatedEmailDomains = [...existingEmailDomains, ...updatedEmailDomains];
        operationType = 'replace';
      }
      // Check if the number of updated email domains exceeds the limit
      if (updatedEmailDomains.length > Limit) {
        // Divide the email domains into chunks of size `Limit`
        for (let count = 0; count < updatedEmailDomains.length; count += Limit) {
          let emailDomainsChunk = updatedEmailDomains.slice(count, count + Limit);

          // Json to be send on patchOperation
          patchOperation.push({
            // Use 'replace' for the first chunk, 'add' for subsequent chunks
            op: count === 0 ? operationType : 'add',
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
              description: last_update_Date


            }
          });
        }
      }
      else {
        patchOperation.push({
          op: operationType,
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
            description: last_update_Date

          }
        });
      }
      // API URL for patching domains in LaunchDarkly (LD)

      // Send the patch operation to the API and log the response
      await sendDataToApi(API, apiKey, patchOperation, sendSlackMessage);
      const numberOfDomains = Domains.length,

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
                  'text': `*Last updated:* ${last_update_Date}`
                }
              ]
            }
          ]
        };

      await sendSlackMessage(successMessage);
    }
  }
  catch (error) {
    await sendSlackMessage(`<@nimish.agrawal>Error Patching dates: ${error}`);
    throw error;
  }
}

// export default processLdRequestWorkflow;
module.exports = { processLdRequestWorkflow };
