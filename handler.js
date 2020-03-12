'use strict';
const fetch = require('node-fetch');
const AWS = require('aws-sdk');
const S3= new AWS.S3();
const { tagEvent } = require('./serverless_sdk');

module.exports.upload = async (event, context, callback) => {
  tagEvent('custom-tag', 'Initiating upload...', { custom: { tag: 'data' } });

  fetch(event.postUrl)
    .then((response) => {
      if (response.ok) {
        return response;
      }
      return Promise.reject(new Error(
            `Failed to fetch ${response.url}: ${response.status} ${response.statusText}`));
    })
    .then(response => (
      S3.putObject({
        Bucket: process.env.BUCKET,
        Key: event.postName,
        Body: response,
      }).promise()
    ))
    .then(v => callback(null, v), callback);
}
