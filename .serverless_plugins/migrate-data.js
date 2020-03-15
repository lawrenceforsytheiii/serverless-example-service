const aws = require('aws-sdk');

const getLambda = serverless => {
  aws.config.update({
    region: serverless.service.provider.region,
    apiVersions: {
      lambda: '2015-03-31'
    }
  });

  return new aws.Lambda();
};

const getLambdaFunc = (serverless, options) => {
  console.log(`options: ${JSON.stringify(options)}`);
  console.log(`function: ${JSON.stringify(serverless.service.functions)}`);
  const func = serverless.service.functions[options.l].name;
  return func;
};

const invokeLambda = (serverless, options) => new Promise((resolve, reject) => {
  const lambda = getLambda(serverless);
  const payload = {
    postUrl: options.postUrl,
    postName: options.postName
  };

  const params = {
    FunctionName: getLambdaFunc(serverless, options),
    Payload: JSON.stringify(payload)
  }

  lambda.invoke(params, (error, result) => {
    if (error) {
      serverless.cli.log(`Error invoking lambda function! ${JSON.stringify(error)}`);
      return reject(error);
    }
    serverless.cli.log(`Invoked lambda and created ${params.postName} object in S3`);
    return resolve(result);
  });
});

const getS3 = () => {
  aws.config.apiVersions.update({
    s3: '2006-03-01'
  });

  return new aws.S3();
};

const getS3Bucket = (serverless, options) => {
  const bucket = serverless.service.resources.Resources[options.b].Properties.TableName;
  return bucket;
};

//TODO: Expand to list and retrieve multiple objects
const getS3Object = (serverless, options) => new Promise((resolve, reject) => {
  const s3 = getS3(serverless);

  const params = {
    Bucket: getS3Bucket(serverless, options),
    Key: options.postName
  };

  s3.getObject(params, (err, data) => {
    if (err) {
      serverless.cli.log(`Error on getting S3 object! ${JSON.stringify(err)}`);
      return reject(err);
    }

    // Move list, s3Data var declaration, and promise resolution outside of
    // getObjects when looping through bucket for getting multiple objects.
    let s3ObjArr = [];
    s3ObjArr.push(JSON.parse(data.Body.toString()));
    serverless.variables.s3Data = s3ObjArr;
    serverless.cli.log(`Got initial data from ${params.Bucket}`);
    return resolve(s3ObjArr);
  });
});

const getDynamoDB = () => {
  aws.config.apiVersions.update({
    dynamodb: '2012-08-10'
  });

  return new aws.DynamoDB();
};

const getDynamoTableName = (serverless, options) => {
  const table = serverless.service.resources.Resources[options.t].Properties.TableName;
  return table;
};

const getDynamoPutPromise = (dynamodb, params, serverless) => new Promise((resolve, reject) => {
  dynamodb.putItem(params, (error) => {
    if (error) {
      return reject(error);
    }
    serverless.cli.log(`Uploaded: ${JSON.stringify(params)}`);
    return resolve();
  });
});

const migrateData = (serverless, options) => new Promise((resolve, reject) => {
  const dynamodb = getDynamoDB(serverless);
  const tableName = getDynamoTableName(serverless, options);
  const itemUploads = [];

  serverless.variables.s3Data.forEach(data => {
    const params = {
      TableName: tableName,
      Item: data
    };
    itemUploads.push(getDynamoPutPromise(dynamodb, params, serverless));
  });

  Promise.all(itemUploads).then(() => {
    serverless.cli.log(`Items created in ${tableName} successfully!`);
    resolve();
  }).catch(error => {
    serverless.cli.log(`DynamoDB item creation failed: ${JSON.stringify(error)}`);
    reject(error);
  });
});

class MigrateDataPlugin {
  constructor(serverless, options) {
    this.commands = {
      'migrate-data': {
        lifecycleEvents: [
          'invokeLambda',
          'getS3Object',
          'migrateData'
        ],
        usage: 'Triggers lambda to put object in S3 bucket and copy that data over to a DynamoDB table.',
        options: {
          lambda: {
            usage: 'Specify the name of your lambda function',
            required: true,
            shortcut: 'l'
          },
          bucket: {
            usage: 'Specify the name of your S3 bucket',
            required: true,
            shortcut: 'b'
          },
          table: {
            usage: 'Specify the name of your DynamoDB table',
            required: true,
            shortcut: 't'
          },
          postUrl: {
            usage: 'Specify the url to fetch the post object',
            required: true,
            shortcut: 'u'
          },
          postName: {
            usage: 'Specify the name of the post and S3 object',
            required: true,
            shortcut: 'n'
          }
        }
      }
    };

    this.hooks = {
      'migrate-data:invokeLambda': invokeLambda.bind(null, serverless, options),
      'migrate-data:getS3Object': getS3Object.bind(null, serverless, options),
      'migrate-data:migrateData': migrateData.bind(null, serverless, options)
    };
  };
};

module.exports = MigrateDataPlugin;

