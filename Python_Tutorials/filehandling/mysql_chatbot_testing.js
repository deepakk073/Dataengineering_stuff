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
            connection.query('select * from users', (error, results, fields) => {
                resolve(results);
            });
        });
    }
    function queryDatabase1(connection, param) {
        return new Promise((resolve, reject) => {
            connection.query('select * from users where email = ?', [param], (error, results, fields) => {
                resolve(results);
            });
        });
    }
    function loginhandler(agent) {
        const user_email = agent.parameters.email;
        const password = agent.parameters.password;
        const encrypt_password = String(crypto.MD5(password));
        return connectToDatabase()
            .then(connection => {
                return queryDatabase1(connection, user_email)
                    .then(result => {
                        console.log('deepak');
                        console.log(result);
                        result.map(user => {
                            console.log(`deep1 ${user.email}  ,  ${user.password}`);
                            if (user_email === user.email) {
                                if (encrypt_password === user.password) {
                                    agent.add(`Welcome to Make My Trip. First Name: ${user.first_name} and last name is ${user.last_name}`);
                                }
                                else {
                                    agent.add(`Please provide the correct password`);
                                }
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
    function insertuserdata(connection, first_name, last_name, email, encrypt_password, status) {
        return new Promise((resolve, reject) => {
            connection.query(`insert into users (first_name,last_name,email,password,status) values (?, ?, ?, ?,?)`, [first_name, last_name, email, encrypt_password, status], (error, results, fields) => {
                if (error) {
                    return console.error(error.message);
                }
                const message1 = `Thank you for Registering MR. ${first_name}  ${last_name}.`;
                resolve(message1);

            });
        });
    }
    function mysqlregis(agent) {
        const first_name = agent.parameters.first_name;
        const last_name = agent.parameters.last_name;
        const email = agent.parameters.email;
        const password = agent.parameters.password;
        const encrypt_password = String(crypto.MD5(password));
        const status = 1;
        var message = '';
        return connectToDatabase()
            .then(connection => {

                console.log('deep1');
                return queryDatabase1(connection, email)
                    .then(result => {
                        var result_email = '';
                        var result_fname = '';
                        var result_lname = ''; P

                        //console.log(result.email);
                        console.log('deep2');
                        try {
                            console.log('try block');
                            result_email = result[0].email;
                            result_fname = result[0].first_name;
                            result_lname = result[0].last_name;
                        }
                        catch (e) {
                            console.log('catch block');
                            console.log(e.message);
                            result_email = '';
                            result_fname = '';
                            result_lname = '';
                        }

                        console.log(`deepak - ${result_email} ${result_fname} ${result_lname} `);
                        if (result_email === email) {
                            console.log('deep3');
                            message = ` User( ${result_fname}  ${result_lname}) already associated with the given email ${result_email}.`;
                            console.log(message);
                            agent.add(message);
                            //return message;
                        }
                        else {
                            console.log('deep4');
                            return insertuserdata(connection, first_name, last_name, email, encrypt_password, status)
                                .then(message1 => {
                                    console.log(message1);
                                    agent.add(message1);
                                });


                            ///return message;
                        }


                        agent.add(message);
                        connection.end();
                    });


            });

    }
    function getfleightdetails(connection, source, destination) {
        return new Promise((resolve, reject) => {
            connection.query('select * from fleight_details where lower(source) = ? and lower(destination) = ? ', [source, destination], (error, results, fields) => {
                if (error) {
                    return console.error(error.message);
                }
                resolve(results);
            });

        });
    }

    function fleight_booking(agent) {
        const source = lower(agent.parameters.source);
        const destination = lower(agent.parameters.destination);
        const tdate = agent.parameters.date;
        return connectToDatabase()
            .then(connection => {
                console.log(' In fleight booking function')
                return getfleightdetails(connection, source, destination)
                    .then(result => {
                        console.log('getting the sql data of fleight booking');
                        result.map(user => {
                            console.log((`fleight_no ${user.fleightno} source ${user.source} destination ${user.destination} date ${tdate} time ${user.travel_time}`))
                            agent.add(`fleight_no ${user.fleightno} source ${user.source} destination ${user.destination} date ${tdate} time ${user.travel_time}`)
                        })
                    });

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
    intentMap.set('loginhandler', loginhandler);
    intentMap.set('mysql.registration', mysqlregis);
    intentMap.set('fleight.booking', fleight_booking);
    //intentMap.set('user.regis', user_regis);
    // intentMap.set('your intent name here', googleAssistantHandler);
    agent.handleRequest(intentMap);
});
