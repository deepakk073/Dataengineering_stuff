// See https://github.com/dialogflow/dialogflow-fulfillment-nodejs
// for Dialogflow fulfillment library docs, samples, and to report issues
'use strict';

const functions = require('firebase-functions');
const { WebhookClient } = require('dialogflow-fulfillment');
const { Card, Suggestion } = require('dialogflow-fulfillment');
const admin = require('firebase-admin');
// Your web app's Firebase configuration
const firebaseConfig = {
    apiKey: "AIzaSyA7qI44AX-tpcSWGOPL2BTNkHpMcETB9eI",
    authDomain: "ma-c4m.firebaseapp.com",
    databaseURL: "https://ma-c4m.firebaseio.com",
    projectId: "ma-c4m",
    storageBucket: "ma-c4m.appspot.com",
    messagingSenderId: "25680144148",
    appId: "1:25680144148:web:8f557a89bc9df68cc6c401"
};
admin.initializeApp(firebaseConfig);
var database = admin.database();

process.env.DEBUG = 'dialogflow:debug'; // enables lib debugging statements

exports.dialogflowFirebaseFulfillment = functions.https.onRequest((request, response) => {
    const agent = new WebhookClient({ request, response });
    function takeUserDetails(agent) {
        return database.ref('users')
            .push({
                phoneNumber: agent.parameters.phonenumber,
                email: agent.parameters.email
            })
            .then(() => {
                agent.add('Thanks for sharing the details, Please provide us the flight booking details');
            });
    }
    //console.log('Dialogflow Request headers: ' + JSON.stringify(request.headers));
    ///console.log('Dialogflow Request body: ' + JSON.stringify(request.body));

    function welcome(agent) {
        agent.add(`I can help you with registering to our Flight ticketing system, let's start by knowing your email and phone number`);
    }
    function userverification(agent) {
        return database.ref('users')
            .get()
            .then((snapshot) => {
                snapshot.forEach((doc) => {
                    console.log(doc.id, '=>', doc.data());
                });
            })
            .catch((err) => {
                console.log('Error getting documents', err)
            })
    }
    function fallback(agent) {
        agent.add(`Please try again`);
        agent.add(`I'm sorry, can you try again?`);
    }
    let intentMap = new Map();
    intentMap.set('user.regis', takeUserDetails);
    intentMap.set('Default Fallback Intent', fallback);
    intentMap.set('Default Welcome Intent', welcome);
    // intentMap.set('your intent name here', googleAssistantHandler);
    agent.handleRequest(intentMap);
});
