/**
 * Install K6 from here: https://k6.io/docs/getting-started/installation/
 * Ref: https://betterprogramming.pub/load-testing-websockets-with-k6-feb99bf75798
 */

import ws from 'k6/ws';
import {check} from 'k6';
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";

const debug = true;
const peak_vus = debug ? 10 : 100;
const total_time_millis = (debug ? 34 : 120) * 1000;
const ramp_up_down_time = debug ? '5s' : '30s';
const sustained_load_time = debug ? '15s' : '60s';

export const options = {
    stages: [
        {duration: ramp_up_down_time, target: peak_vus},
        {duration: sustained_load_time, target: peak_vus},
        {duration: ramp_up_down_time, target: 0},
    ],
};

const debugLog = debug ? console.log : () => {};

export default function () {
    //const url = 'ws://echo.websocket.org'; // public websocket server for quick test
    const url = 'ws://localhost:8080/';    // local websocket server

    let username = 'k6-' + uuidv4();
    const res = ws.connect(url, null, (socket) => {
        socket.on('open', function open() {
            debugLog('connected');
            socket.send("Identify:" + username);
        });

        socket.on('message', function message(data) {
            debugLog('Message received: ', data);
            if (data.indexOf('IdentifiedAs:') === 0) {
                let username = data.substr('IdentifiedAs:'.length);
                debugLog('Identified as ' + username);

                socket.setInterval(function interval() {
                    let broadcastMessage = 'Sent at ' + new Date() + ', R=' + Math.floor(Math.random() * 100);
                    socket.send(broadcastMessage);
                    debugLog('Message broadcasted: ', broadcastMessage);
                }, 1000);

            } else {
                check(data, {'data is correct': (r) => r && r.indexOf("Sent at") !== -1});
            }
        });

        socket.on('close', () => debugLog('disconnected'));

        // socket.setTimeout(function () {
        //     debugLog(total_time_millis + ' seconds passed, closing the socket');
        //     socket.close();
        // }, total_time_millis);
    });

    check(res, {'status is 101': (r) => r && r.status === 101});

}