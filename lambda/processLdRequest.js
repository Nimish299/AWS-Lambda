import { fetchDataFromApi, sendDataToApi } from '../services/ldService/ld.js';
import { ListObjects, GetObject } from '../services/s3Service/s3.js';
import { sendSlackMessage } from '../services/slackService/slack.js';

import _ from "lodash";
import csv from 'csv-parser';
import { Readable } from 'stream';
import zlib from 'zlib';
let last_update_Date


const BUCKET_NAME = process.env.BUCKET_NAME;
const Limit = process.env.LIMIT;
const projectKey = process.env.PROJECT_KEY;
const environmentKey = process.env.ENVIRONMENT_KEY;
const segmentKey = process.env.SEGMENT_KEY;
const apiKey = process.env.API_KEY;
const baseUrl = process.env.LD_BASE_URL;

function setDescription(segmentData) {
    let formattedDate;

    if (segmentData.rules[0]?.description) {
        let longDateFormat = segmentData.rules[0]?.description;

        // Remove 'at' from the description and append 'UTC' for time zone
        // Correct the date format From rule name 
        longDateFormat = longDateFormat.replace(' at', '') + ' UTC';

        const parsedDate = new Date(longDateFormat);

        if (!isNaN(parsedDate)) {
            formattedDate = parsedDate;
        } else {
           sendSlackMessage(`Invalid date format;${longDateFormat}`);

        }
    } else {
        formattedDate = new Date('2024-09-03');
    }
    return formattedDate;
}
function fetchDates() {
    try {
        const currentDate = new Date();
        const Results = [];
        const tempDate = new Date(last_update_Date);

        // Normalize the time part of the currentDate to start of the day
        const endOfCurrentDate = new Date(currentDate);
        endOfCurrentDate.setUTCHours(0, 0, 0, 0);
        endOfCurrentDate.setUTCDate(endOfCurrentDate.getUTCDate() + 1);

        while (tempDate < endOfCurrentDate) {
            Results.push(new Date(tempDate));
            tempDate.setUTCDate(tempDate.getUTCDate() + 1);
        }

        return Results;
    } catch (error) {
        sendSlackMessage(`Error fetching dates: ${error}`);
        throw error;
    }
}

/**
 * Convert a stream to a string.
 * 
 * This function reads data from a stream, collects chunks of data, and concatenates them into a single string.
 * 
 * @param {stream.Readable} stream - The stream to be converted to a string.
 * @returns {Promise<string>} - A promise that resolves with the string representation of the stream.
 */
async function streamToString(stream) {
    const chunks = [];

    return new Promise((resolve, reject) => {
        // Collect data chunks from the stream
        stream.on('data', chunk => chunks.push(chunk));

        // Resolve the promise with the concatenated string when the stream ends
        stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));

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
const processLdRequestWorkflow = async () => {
    //Fetch Segment Data  using LD API 
    const API = `${baseUrl}/segments/${projectKey}/${environmentKey}/${segmentKey}`;

    //Fetching Data from LD
    const segmentData = await fetchDataFromApi(API, apiKey, sendSlackMessage);

    // Extract the last updated date from the description (i.e., rule name)
    last_update_Date = setDescription(segmentData)
    let Domains = [];
    try {
        // Extract all days between the last updated date and today
        const days = fetchDates();
        let all_Manifest_url = [];
        // Extracting All the URN of data file from Manifest  from Last updated date to current date
        for (const day of days) {
            //Extracting Dates For Condition on Fetching Manifest file 
            // As it come in format YYYY-MM-DDTHH:mm:ss.sssZ so split it Using T

            //Day date 
            const dayDate = new Date(day).toISOString().split('T')[0];
            //last Updated Date
            const lastDate = new Date(last_update_Date).toISOString().split('T')[0];
            //Current Date
            const todayDate = new Date().toISOString().split('T')[0];

            // Folder structure in S3 is  pqa_trials/strftime('%Y/%m/%d/%H%M%S0000')
            const year = day.getUTCFullYear();
            // Month is zero-based, so add 1
            const month = String(day.getUTCMonth() + 1).padStart(2, '0');
            const date = String(day.getUTCDate()).padStart(2, '0');

            // Construct the folder path based on the date components
            const folderPath = `pqa_trials/${year}/${month}/${date}/`;

            /** 
            * Pointing the cursor to the last updated time.
            * Adding a condition to start from the next time after the last update.
            * If the lambda function runs twice a day, this ensures it continues from the correct time.
            */
            let Last_updated_Time;
            if (dayDate === lastDate ) {
                // Extract hours, minutes, and seconds from the last updated date
                const hours = String(last_update_Date.getUTCHours()).padStart(2, '0');
                const minutes = String(last_update_Date.getUTCMinutes()).padStart(2, '0');
                const seconds = String(last_update_Date.getUTCSeconds()).padStart(2, '0');
                Last_updated_Time = `${hours}${minutes}${seconds}0000`;

            }
            else {
                Last_updated_Time = 0;
            }
            let response;
            try {
                //List of  Objects on S3 on given folder Path
                response = await ListObjects(folderPath);
            }
            catch (error) {
                await sendSlackMessage(`Error processing List at ${folderPath}: ${error}`);
                throw error;
            }
            /**
             * 
             * Filter all the manifest file  which are not updated yet
             *   code filters the list of S3 objects to include only those
             * manifest files that have a timestamp greater than the last updated time.
             */
            const filteredContents = _.filter(response.Contents, (item) => {
                const parts = item.Key.split('/');
                if (parts.length > 4 && parts[4] && parts[5] === 'manifest') {
                    const numericPart = parts[4];

                    return numericPart > Last_updated_Time;
                }
                return false;
            });

            //Store all Url file
            let Manifest_url = [];

            //Filter all key from Manifest File
            filteredContents.forEach(item => {
                // Add each manifest file's key (URL) to the Manifest_url array
                Manifest_url.push(item.Key)
            });
            //Store all Url in all_Manifest_url
            all_Manifest_url.push(...Manifest_url)
        }
        //Extract All File url using Manifest File
        let All_File_urls = [];
        // Create an array of promises where each promise handles the extraction of file URLs from a manifest
        All_File_urls = await Promise.all(
            all_Manifest_url.map(async (manifestPath) => {
                try {
                    // Fetch the manifest file from the given path
                    const data = await GetObject(manifestPath);
                    // Convert the readable stream  to a string
                    const manifestContent = await streamToString(data.Body);
                    // Parse the manifest content 
                    const manifestJson = JSON.parse(manifestContent);
                    const File_urls = manifestJson.entries.map(entry => entry.url);
                    // Return the extracted URLs
                    return File_urls;
                } catch (error) {
                    await sendSlackMessage(`Error processing manifest at ${manifestPath}: ${error}`);

                    throw error;
                }
            })
        );
        All_File_urls = All_File_urls.flat();
        try {
            // Create an array of promises where each promise handles the extraction of Domain from file URLs 
            Domains = await Promise.all
                (
                    All_File_urls.map(async (filePath) => {
                        try {
                            // Remove the S3 bucket prefix from the file path
                            filePath = filePath.replace(`s3://${BUCKET_NAME}/`, '');

                            // Fetch the object from S3 
                            const data = await GetObject(filePath);

                            // Create a gunzip stream to decompress the data 
                            const gunzip = zlib.createGunzip();

                            // Create a readable stream from the object body and pipe it through the gunzip
                            const csvStream = Readable.from(data.Body).pipe(gunzip);

                            const parsedData = [];
                            // Create a CSV parser to handle the incoming data
                            const parser = csv({ headers: false });

                            // Process each row from the CSV stream and extracting Domains
                            for await (const row of csvStream.pipe(parser)) {
                                const domain = row[0];
                                parsedData.push(domain);
                            }
                            return parsedData;
                        }
                        catch (error) {
                            if (error.name === 'NoSuchKey') {
                                await sendSlackMessage(`${filePath}, skipping... ${error}`);

                                return [];
                            }
                        }
                    }));
            Domains = Domains.flat();
        }
        catch (error) {
            await sendSlackMessage(`Error processing file: ${error}`);

            // Return an empty array in case of error
            return []; // Return an empty array in case of error

        }

        //No Need to update if there are no domain
        if (Domains.length === 0) {
            await sendSlackMessage(`Domains is empty, So not updating on ${last_update_Date}`);
            return;
        }

        //update the last_update_Date 
        let lastFilePath = _.last(All_File_urls);
        if (lastFilePath) {
            //Take the Time part from URL
            const parts = lastFilePath.split('/');
            const numericPart = parts[7];

            if (numericPart && numericPart.length === 10) {
                const year = parts[4];
                const month = parts[5];
                const day = parts[6]
                const hour = numericPart.substring(0, 2);
                const minute = numericPart.substring(2, 4);
                const second = numericPart.substring(4, 6);
                const dateTimeString = `${year}-${month}-${day}T${hour}:${minute}:${second}Z`;

                //updating last  update Date
                last_update_Date = new Date(dateTimeString);

                //Making more visible
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
            } else {
                await sendSlackMessage(`'Invalid numeric part format in': ${lastFilePath}`);
            }
        }
    } catch (error) {
        await sendSlackMessage(`Error processing data:: ${error}`);
        
    }
    try {
        let patchOperation = [];
        let updatedEmailDomains = [...Domains];
        let operationType = 'add';
        //Check if there is some rule exist or not 
        if (segmentData.rules && segmentData.rules.length > 0) {
            const existingEmailDomains = segmentData.rules[0]?.clauses[0]?.values || [];
            //Merge old data with new data as we will replace First rule till limit 
            updatedEmailDomains = [...existingEmailDomains, ...updatedEmailDomains];
            operationType = 'replace';
        }
        // Check if the number of updated email domains exceeds the limit
        if (updatedEmailDomains.length > Limit) {
            //Divide the email domains into chunks of size `Limit`
            for (let count = 0; count < updatedEmailDomains.length; count += Limit) {
                let emailDomainsChunk = updatedEmailDomains.slice(count, count + Limit);
                //Json to be send on patchOperation
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
                                negate: false,
                            },
                        ],
                        description: last_update_Date


                    },
                });
            }
        } else {
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
                            negate: false,
                        },
                    ],
                    description: last_update_Date

                },
            });
        }
        // API URL for patching domains in LaunchDarkly (LD)

        // Send the patch operation to the API and log the response
        await sendDataToApi(API, apiKey, patchOperation, sendSlackMessage);
        const numberOfDomains = Domains.length; 

        const successMessage = {
            "text": "Domain Update Notification",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Successfully updated the domains in LaunchDarkly:*"
                    }
                },
                {
                    "type": "section",
                    "block_id": "section123",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": `*Total number of domains added:* ${numberOfDomains}`
                        },
                        {
                            "type": "mrkdwn",
                            "text": `*Last updated:* ${last_update_Date}`
                        }
                    ]
                }
            ]
        };
        await sendSlackMessage(successMessage);

    }
    catch (error) {

        await sendSlackMessage(`Error Patching dates: ${error}`);

    }
};

export default processLdRequestWorkflow;
