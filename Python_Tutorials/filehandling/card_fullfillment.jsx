// See https://github.com/dialogflow/dialogflow-fulfillment-nodejs
// for Dialogflow fulfillment library docs, samples, and to report issues
'use strict';

const functions = require('firebase-functions');
const { WebhookClient } = require('dialogflow-fulfillment');
const { Card, Suggestion } = require('dialogflow-fulfillment');
const mysql = require('mysql');
process.env.DEBUG = 'dialogflow:debug'; // enables lib debugging statements
const Datastore = require('@google-cloud/datastore');
const datastore = new Datastore({
    projectId: 'thematic-mapper-259808'
});
const crypto = require('crypto-js');
function user_regis(agent) {
    var user_name = agent.parameters.user_name;
    var password = agent.parameters.password;
    var email = agent.parameters.email;
    var pan = agent.parameters.pan;
    const taskKey = datastore.key('user_info');
    const entity = {
        key: taskKey,
        data: {
            user_name: user_name,
            password: password,
            email: email,
            pan: pan,
            inserted_date: new Date().toLocaleString()
        }
    };
    return datastore.save(entity).then(() => {
        console.log(`Saved ${entity.key.name}: ${entity.data.user_name}`);
        agent.add(`your detail has been saved`);

    });
}

exports.dialogflowFirebaseFulfillment = functions.https.onRequest((request, response) => {
    const agent = new WebhookClient({ request, response });
    console.log('Dialogflow Request headers: ' + JSON.stringify(request.headers));
    console.log('Dialogflow Request body: ' + JSON.stringify(request.body));

    function welcome(agent) {
        agent.add(`Welcome to Axis Bank! \r\n Please provide username and password for verify`);
    }
    function connectToDatabase() {
        const connection = mysql.createConnection({
            host: '34.66.87.184',
            user: 'root',
            password: 'Ftrack3!',
            database: 'test_db'
        });
        return new Promise((resolve, reject) => {
            connection.connect();
            resolve(connection);
        });
    }
    function queryDatabase(connection) {
        return new Promise((resolve, reject) => {
            connection.query('select * from dee_users', (error, results, fields) => {
                resolve(results);
            });
        });
    }
    function queryDatabase1(connection, param) {
        return new Promise((resolve, reject) => {
            connection.query('select * from dee_users where email = ?', [param], (error, results, fields) => {
                resolve(results);
            });
        });
    }
    function mysqlgetinfo(agent) {
        const user_email = agent.parameters.email;
        return connectToDatabase()
            .then(connection => {
                return queryDatabase(connection)
                    .then(result => {
                        console.log(result);
                        result.map(user => {
                            if (user_email === user.email) {
                                agent.add(`First Name: ${user.first_name} and last name is ${user.last_name}`);
                            }
                        });
                        //  agent.add(`First Name: ${result[0].first_name} and last name is ${result[0].last_name}` );
                        connection.end();
                    });
            });


    }
    // function insertquery(connection) {
    //     return new Promise((reoslve, reject) => {
    //         connection.query('insert into dee_')
    //     })
    // }
    function mysqlregis(agent) {
        const first_name = agent.parameters.first_name;
        const last_name = agent.parameters.last_name;
        const email = agent.parameters.email;
        const password = agent.parameters.password;
        const encrypt_password = String(crypto.MD5(password));
        const status = 1;
        return connectToDatabase()
            .then(connection => {
                return queryDatabase1(connection, email)
                    .then(result => {
                        result.map(user => {
                            if (user.email === email) {
                                agent.add(`User( ${user.first_name}  ${user.last_name}) already associated with the given email ${user.email}.`);
                            }
                            else {
                                connection.query(`insert into users (first_name,last_name,email,password,status) values (?, ?, ?, ?,?)`, [first_name, last_name, email, encrypt_password, status], (error, results, fields) => {
                                    if (error) {
                                        return console.error(error.message);
                                    }
                                    agent.add(`Thank you for Registering MR. ${first_name}  ${last_name}.`);
                                });
                            }
                        });
                        connection.end();
                    });
                // //console.log(`insert into dee_users values (5,?, ?, ?, ?)`, [first_name, last_name, email, password]);
                // connection.query(`insert into users (first_name,last_name,email,password,status) values (?, ?, ?, ?,?)`, [first_name, last_name, email, encrypt_password, status], (error, results, fields) => {
                //     if (error) {
                //         return console.error(error.message);
                //     }
                // });
                // return queryDatabase1(connection, email)
                //     .then(result => {
                //         console.log(result);
                //         result.map(user => {
                //             agent.add(`Thank you for Registering MR. ${user.first_name}  ${user.last_name}.`);

                //         });
                //         //  agent.add(`First Name: ${result[0].first_name} and last name is ${result[0].last_name}` );
                //         connection.end();
                //     });
                // //connection.end();
                //agent.add(`First Name: ${first_name} and last name is ${last_name}`);

            });


    }

    function fallback(agent) {
        agent.add(`I didn't understand`);
        agent.add(`I'm sorry, can you try again?`);
    }
    const { dialogflow, SignIn } = require('actions-on-google');
    const app = dialogflow({
        // REPLACE THE PLACEHOLDER WITH THE CLIENT_ID OF YOUR ACTIONS PROJECT
        clientId: "86ddfe62ffff4839856e0f0801b74e37",
    });
    // Intent that starts the account linking flow.
    app.intent('Start Signin', (conv) => {
        conv.ask(new SignIn('To get your account details'));
    });
    let intentMap = new Map();
    intentMap.set('Default Welcome Intent', welcome);
    intentMap.set('Default Fallback Intent', fallback);
    intentMap.set('mysql.getdata', mysqlgetinfo);
    intentMap.set('mysql.registration', mysqlregis);
    //intentMap.set('user.regis', user_regis);
    // intentMap.set('your intent name here', googleAssistantHandler);
    agent.handleRequest(intentMap);
});
