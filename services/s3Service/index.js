const dotenv = require('dotenv');

dotenv.config();
/* eslint-disable no-process-env */

const { S3Client, GetObjectCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3'),
  s3 = new S3Client({ region: process.env.AWS_REGION }),
  BUCKET_NAME = process.env.BUCKET_NAME;

/**
 * Lists all objects in the specified S3 folder path.
 *
 * @param {string} folderPath - The path to the folder in S3.
 * @returns {Promise<Object>} - A promise that resolves to the response from the S3 list objects command.
 * @throws {Error} - Throws an error if the S3 request fails.
 */
async function ListObjects (folderPath) {
  let result;

  try {
    const command1 = new ListObjectsV2Command({
        Bucket: BUCKET_NAME,
        Prefix: folderPath
      }),

      response = await s3.send(command1);

    result = {
      isError: false,
      data: response
    };
  }
  catch (error) {
    console.error('Error fetching objects:', error);
    result = {
      isError: true,
      errorMessage: error.message
    };
  }

  return result;
}


/**
 * Retrieves an object from S3 by its key.
 *
 * @param {string} key - The key of the object in S3.
 * @returns {Promise<Object|Array>} - A promise that resolves to the S3 object data
 * @throws {Error} - Throws an error if the S3 request fails
 */
async function GetObject (key) {
  try {
    const params = {
        Bucket: BUCKET_NAME,
        Key: key
      },
      command = new GetObjectCommand(params),
      data = await s3.send(command);

    return data;
  }
  catch (error) {
    if (error.name === 'NoSuchKey') {
      return [];
    }

    return Promise.reject(error);
  }
}

module.exports = { GetObject, ListObjects };
