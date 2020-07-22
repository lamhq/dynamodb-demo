/* eslint-disable no-console */
const AWS = require('aws-sdk');
const fs = require('fs');
const faker = require('faker');

AWS.config.update({
  region: 'us-west-2',
  endpoint: 'http://localhost:8000',
});
const dynamodb = new AWS.DynamoDB();
const docClient = new AWS.DynamoDB.DocumentClient();

function createTable() {
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

  return new Promise((rs, rj) => {
    dynamodb.createTable(params, (err, data) => {
      if (err) {
        console.error('Unable to create table. Error JSON:', JSON.stringify(err, null, 2));
        rj(err);
      } else {
        console.log('Created table. Table description JSON:', JSON.stringify(data, null, 2));
        rs();
      }
    });
  });
}

function createIndex() {
  const params = {
    TableName: 'Movies',
    AttributeDefinitions: [
      // The index key attributes can consist of any top-level attribute
      // AttributeType must be String, Number, or Binary
      { AttributeName: 'isActive', AttributeType: 'N' },

      /* you can optionally specify a sort key */
      // { AttributeName: 'releaseDate', AttributeType: 'S' },
    ],
    GlobalSecondaryIndexUpdates: [
      {
        Create: {
          IndexName: 'IsActive',
          KeySchema: [
            { AttributeName: 'isActive', KeyType: 'HASH' },
            // { AttributeName: 'releaseDate', KeyType: 'RANGE' },
          ],
          Projection: {
            // you can project all attributes into a global secondary index.
            // This gives you maximum flexibility.
            // However, your storage cost would increase, or even double.
            ProjectionType: 'ALL',
          },
          ProvisionedThroughput: { ReadCapacityUnits: 9999, WriteCapacityUnits: 9999 },
        },
      },
    ],
  };
  return new Promise((rs, rj) => {
    dynamodb.updateTable(params, (err, data) => {
      if (err) {
        console.error('Unable to update table. Error JSON:', JSON.stringify(err, null,
          2));
        rj(err);
      } else {
        console.log('Updated table. Table description JSON:', JSON.stringify(data,
          null, 2));
        rs();
      }
    });
  });
}

function deleteTable() {
  const params = {
    TableName: 'Movies',
  };

  return new Promise((rs, rj) => {
    dynamodb.deleteTable(params, (err, data) => {
      if (err) {
        console.error('Unable to delete table. Error JSON:', JSON.stringify(err, null, 2));
        rj(err);
      } else {
        console.log('Deleted table. Table description JSON:', JSON.stringify(data, null, 2));
        rs();
      }
    });
  });
}

function importData() {
  console.log('Importing movies into DynamoDB. Please wait.');
  const allMovies = JSON.parse(fs.readFileSync('moviedata.json', 'utf8'));
  return Promise.all(allMovies.map((movie) => {
    const params = {
      TableName: 'Movies',
      Item: {
        year: movie.year,
        title: movie.title,
        isActive: faker.helpers.randomize([1, 0]),
        releaseDate: movie.info.release_date,
        info: movie.info,
      },
    };
    return new Promise((rs, rj) => {
      docClient.put(params, (err, data) => {
        if (err) {
          console.error('Unable to add movie', movie.title, '. Error JSON:', JSON.stringify(err, null, 2));
          rj(err);
        } else {
          console.log('PutItem succeeded:', movie.title);
          rs(data);
        }
      });
    });
  }));
}

function read() {
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
  console.log('Querying for movies from 2004.');
  const params = {
    TableName: 'Movies',
    KeyConditionExpression: '#yr = :yyyy',
    ExpressionAttributeNames: { '#yr': 'year' },
    ExpressionAttributeValues: { ':yyyy': 2004 },
  };
  return new Promise((rs, rj) => {
    docClient.query(params, (err, data) => {
      if (err) {
        console.error('Unable to query. Error:', JSON.stringify(err, null, 2));
        rj(err);
      } else {
        console.log('Query succeeded.');
        data.Items.forEach((item) => {
          console.log(' -', `${item.year}: ${item.title}`);
        });
        rs(data);
      }
    });
  });
}

function queryOnGSI() {
  console.log('Querying for active movies.');
  const params = {
    TableName: 'Movies',
    IndexName: 'IsActive',
    KeyConditionExpression: 'isActive = :val',
    ExpressionAttributeValues: {
      ':val': 1,
    },
    Limit: 30,
  };
  return new Promise((rs, rj) => {
    docClient.query(params, (err, data) => {
      if (err) {
        console.error('Unable to query. Error:', JSON.stringify(err, null, 2));
        rj(err);
      } else {
        console.log('Query succeeded.');
        data.Items.forEach((item) => {
          console.log(item);
        });
        rs(data);
      }
    });
  });
}

let lastEvaluatedKey = null;

function paginatedQuery() {
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

async function run() {
  /* Please setup and run DynamoDB in your local machine before running this script */
  // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html

  // await deleteTable();
  await createTable();
  await importData();
  await createIndex();
  await query();
  await queryOnGSI();
}

run();
