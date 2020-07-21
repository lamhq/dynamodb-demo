const AWS = require('aws-sdk');
const fs = require('fs');

AWS.config.update({
  region: 'us-west-2',
  endpoint: 'http://localhost:8000',
});

function createTable() {
  const dynamodb = new AWS.DynamoDB();

  const params = {
    TableName: 'Movies',
    KeySchema: [
      { AttributeName: 'year', KeyType: 'HASH' }, // Partition key
      { AttributeName: 'title', KeyType: 'RANGE' }, // Sort key
    ],
    AttributeDefinitions: [
      { AttributeName: 'year', AttributeType: 'N' },
      { AttributeName: 'title', AttributeType: 'S' },
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 10,
      WriteCapacityUnits: 10,
    },
  };

  dynamodb.createTable(params, (err, data) => {
    if (err) {
      console.error('Unable to create table. Error JSON:', JSON.stringify(err, null, 2));
    } else {
      console.log('Created table. Table description JSON:', JSON.stringify(data, null, 2));
    }
  });
}

function importData() {
  const docClient = new AWS.DynamoDB.DocumentClient();
  console.log('Importing movies into DynamoDB. Please wait.');

  const allMovies = JSON.parse(fs.readFileSync('moviedata.json', 'utf8'));
  allMovies.forEach((movie) => {
    const params = {
      TableName: 'Movies',
      Item: {
        year: movie.year, title: movie.title, info: movie.info,
      },
    };
    docClient.put(params, (err, data) => {
      if (err) {
        console.error('Unable to add movie', movie.title, '. Error JSON:', JSON.stringify(err, null, 2));
      } else {
        console.log('PutItem succeeded:', movie.title);
      }
    });
  });
}

function read() {
  const docClient = new AWS.DynamoDB.DocumentClient();
  const table = 'Movies';
  const year = 2004;
  const title = 'Little Black Book';

  const params = {
    TableName: table,
    Key: {
      year,
      title,
    },
  };

  docClient.get(params, (err, data) => {
    if (err) {
      console.error('Unable to read item. Error JSON:', JSON.stringify(err, null, 2));
    } else {
      console.log('GetItem succeeded:', JSON.stringify(data, null, 2));
    }
  });
}

function query() {
  const docClient = new AWS.DynamoDB.DocumentClient();
  console.log('Querying for movies from 2004.');
  const params = {
    TableName: 'Movies',
    KeyConditionExpression: '#yr = :yyyy',
    ExpressionAttributeNames: { '#yr': 'year' },
    ExpressionAttributeValues: { ':yyyy': 2004 },
  };
  docClient.query(params, (err, data) => {
    if (err) {
      console.error('Unable to query. Error:', JSON.stringify(err, null, 2));
    } else {
      console.log('Query succeeded.');
      data.Items.forEach((item) => {
        console.log(' -', `${item.year}: ${item.title}`);
      });
    }
  });
}

let lastEvaluatedKey = null;

function paginatedQuery() {
  const docClient = new AWS.DynamoDB.DocumentClient();
  console.log('Querying for movies from 2004.');
  const params = {
    TableName: 'Movies',
    KeyConditionExpression: '#yr = :yyyy',
    ExpressionAttributeNames: { '#yr': 'year' },
    ExpressionAttributeValues: { ':yyyy': 2004 },
    Limit: 3,
  };
  if (lastEvaluatedKey) {
    params.ExclusiveStartKey = lastEvaluatedKey;
  }

  docClient.query(params, (err, data) => {
    if (err) {
      console.error('Unable to query. Error:', JSON.stringify(err, null, 2));
    } else {
      console.log('Query succeeded.');
      data.Items.forEach((item) => {
        // console.log(' -', `${item.year}: ${item.title}`);
        console.log(item);
      });
      lastEvaluatedKey = data.LastEvaluatedKey;
      setTimeout(paginatedQuery, 2000);
    }
  });
}

paginatedQuery();
