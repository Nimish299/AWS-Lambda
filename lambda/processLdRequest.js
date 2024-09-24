const { fetchDataFromApi, sendDataToApi } = require('../services/ldService/index.js');
const { ListObjects, GetObject } = require('../services/s3Service/index.js');
const { sendSlackMessage } = require('../services/slackService/index.js');
const { formatDateTimeString,
  formatSuccessMessage, unzipAndParseCSV,
  setLastUpdateDateFromSegment, streamToString,
  getDatesFromLastUpdateToCurrent, sortS3ManifestUrlsByTimestamp } =
   require('../services/utilsService');
const _ = require('lodash');
const dayjs = require('dayjs'),


  /* eslint-disable no-process-env */

  BUCKET_NAME = process.env.BUCKET_NAME,
  Limit = process.env.LIMIT,
  projectKey = process.env.PROJECT_KEY,
  environmentKey = process.env.ENVIRONMENT_KEY,
  segmentKey = process.env.SEGMENT_KEY,
  ldAccessToken = process.env.API_KEY,
  baseUrl = process.env.LD_BASE_URL;

/**
 * Processes the manifest files for a given day.
 *
 * This function retrieves the list of objects from S3 for a specified folder path,
 * filters out manifest files that are not updated yet based on the last updated time,
 * and adds their URLs to the `allManifestUrls` array.
 *
 * @param {Date|string} day - The day to process, provided as a Date object or ISO date string.
 * @param {Date} lastUpdatedDate - The last update date as a Date object.
 * @returns {Promise<void>} A promise that resolves when the processing is complete.
 */
async function processDailyManifests (day, lastUpdatedDate) {
  // Convert the input day to a Date object if it's a string
  const date = dayjs(day),
    // Extracting Dates and preparing folder path...
    dayDate = date.utc().format('YYYY-MM-DD'),
    lastDate = dayjs(lastUpdatedDate).utc().format('YYYY-MM-DD'),
    year = date.utc().format('YYYY'),
    month = date.utc().format('MM'),
    dateStr = date.utc().format('DD'),
    folderPath = `pqa_trials/${year}/${month}/${dateStr}/`;

  let lastUpdatedTime,
    manifest_url = [];


  /**
  * Pointing the cursor to the last updated time.
  * Adding a condition to start from the next time after the last update.
  */
  if (dayDate === lastDate) {
    lastUpdatedTime = dayjs(lastUpdatedDate).utc().format('HHmmss') + '0000';
  }
  else {
    lastUpdatedTime = '0000000000';
  }

  try {
    let response, filteredContents;

    // Retrieve the list of objects from S3
    response = await ListObjects(folderPath);


    if (response?.isError) {
      return Promise.reject(response.isError);
    }
    // Filter the list of objects to include only manifest files that are updated
    filteredContents = _.filter(response.data.Contents, (item) => {
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

    return manifest_url;
  }
  catch (error) {
    // Send error message if list retrieval fails
    await sendSlackMessage(`<@trial-engineers >Error processing List at ${folderPath}: ${error}`);

    return Promise.reject(error);
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
  const API = `${baseUrl}/segments/${projectKey}/${environmentKey}/${segmentKey}`;

  // Fetching Data from LD
  let segmentData = await fetchDataFromApi(API, ldAccessToken, sendSlackMessage),
    domains = [],
    lastUpdatedDate;

  if (segmentData.isError) {
    return Promise.reject(segmentData.errorMessage);
  }
  segmentData = segmentData.data;

  // Extract the last updated date from the description (i.e., rule name)
  // lastUpdatedDate is set by setDescription, which ensures a valid date or fallback
  lastUpdatedDate = setLastUpdateDateFromSegment(segmentData);

  try {
    // Extract all days between the last updated date and today
    const daysToProcess = getDatesFromLastUpdateToCurrent(lastUpdatedDate);
    let allFileUrls = [],
      lastFilePath,
      manifestPromises,
      // Array to store promises for each day's processing
      dayProcessingPromises = [],
      allManifestUrls = [];

    // Extracting All the URN of data file from Manifest  from Last updated date to current date
    for (const day of daysToProcess) {
      // For each day, push a promise returned by processDailyManifests(day) to the dayProcessingPromises array
      // This ensures all days are processed concurrently
      dayProcessingPromises.push(processDailyManifests(day, lastUpdatedDate));
    }
    // Wait for all promises to resolve
    allManifestUrls = await Promise.all(dayProcessingPromises);
    allManifestUrls = allManifestUrls.flat();

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
        await sendSlackMessage(`<@trial-engineers >Error processing manifest at ${manifestPath}: ${error}`);

        return Promise.reject(error);
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
          const data = await GetObject(filePath);

          // Check if the object exists and data.Body is defined
          if (!data || !data.Body) {
            await sendSlackMessage(`${filePath} is empty or missing. Skipping...`);

            return [];
          }
          // Unzip and parse the CSV data, extracting the first column (domains)
          // No headers, extract column 0
          let parsedDomains = await unzipAndParseCSV(data.Body, false, 0);

          return parsedDomains;
        }
        catch (error) {
          if (error.name === 'NoSuchKey') {
            await sendSlackMessage(`<@trial-engineers >${filePath}, skipping... `);

            return [];
          }
        }
      });

      // Create an array of promises where each promise handles the extraction of Domain from file URLs
      domains = await Promise.all(domainPromises);
      domains = domains.flat();
      // Remove Duplicates and  null values
      domains = _.uniq(_.compact(domains));
    }
    catch (error) {
      await sendSlackMessage(`<@trial-engineers >Error processing file: ${error}`);

      // Return an empty array in case of error
      return []; // Return an empty array in case of error
    }


    // No Need to update if there are no domain
    if (domains.length === 0) {
      let formattedLastUpdate = dayjs(lastUpdatedDate)
          .utc()
          .format('MMMM D, YYYY, h:mm:ss A [UTC]'),

        successMessage = await formatSuccessMessage(formattedLastUpdate, 0);

      await sendSlackMessage(successMessage);

      return;
    }


    // update the lastUpdatedDate
    lastFilePath = _.last(allFileUrls);

    if (lastFilePath) {
      // Take the Time part from URL using lodash _.get for safe array access
      const parts = lastFilePath.split('/'),
        numericPart = _.get(parts, '[7]');

      if (numericPart && numericPart.length === 10) {
        const dateTimeString = formatDateTimeString(parts, numericPart),

          // Parse and format using Day.js
          formattedDate = dayjs(dateTimeString)
            .utc()
            .format('MMMM D, YYYY, h:mm:ss A [UTC]');

        lastUpdatedDate = formattedDate;
      }
      else {
        await sendSlackMessage(`<@trial-engineers > 'Invalid numeric part format in': ${lastFilePath}`);
      }
    }
  }
  catch (error) {
    await sendSlackMessage(`<@trial-engineers >Error processing data From S3: ${error}`);

    return Promise.reject(error);
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
      if (resp.isError) {
        return;
      }
      const numberOfDomains = domains.length,

        successMessage = await formatSuccessMessage(lastUpdatedDate, numberOfDomains);

      if (resp?.data?.ok) {
        await sendSlackMessage(successMessage);
      }
    }
  }
  catch (error) {
    sendSlackMessage(`<@trial-engineers >Error Patching dates: ${error}`);

    return Promise.reject(error);
  }
}

module.exports = { processLdRequestWorkflow };
