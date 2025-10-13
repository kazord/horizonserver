const WebSocket = require('ws');
const { expect } = require('chai');

let ws;
let ws2;
let step = 0;
let messages = [];

describe('WebSocket server 127.0.0.1:7040', function () {
    this.timeout(5000);


    after(function (done) {
        if (ws) {
            ws.close();
        }
        if (ws2) {
            ws2.close();
        }
        done();
    });

    it('open, wait no messages, send init, then receive 3 messages', function (done) {
        messages = [];
        ws = new WebSocket('ws://127.0.0.1:7040');

        let finished = false;
        let t1, t2;

        const cleanup = () => {
            clearTimeout(t1);
            clearTimeout(t2);
            if (ws) {
                ws.removeAllListeners('message');
                ws.removeAllListeners('error');
                ws.removeAllListeners('close');
            }
        };

        const fail = (err) => {
            if (finished) return;
            finished = true;
            cleanup();
            done(err);
        };

        ws.on('error', fail);
        ws.on('close', () => {
            if (!finished) fail(new Error('Connection closed prematurely'));
        });

        ws.on('message', (data) => {
            messages.push(data.toString());
        });

        ws.once('open', () => {
            // wait 1s to ensure no messages are received before init
            t1 = setTimeout(() => {
                try {
                    expect(messages).to.have.lengthOf(0);
                } catch (err) {
                    return fail(err);
                }

                // send init
                ws.send(JSON.stringify({
                    namespace: "player",
                    event: "init",
                    data: { login: "ddurieux", password: "pass" }
                }));

                // wait 1s to collect the 3 messages
                t2 = setTimeout(() => {
                    try {
                        expect(messages).to.have.lengthOf(3);
                        // check first message is zone enter channel 0
                        const msg1 = JSON.parse(messages[0]);
                        checkMessagePlayerChannel0(msg1);

                        const msg2 = JSON.parse(messages[1]);
                        checkMessagePlayerChannel1(msg2);

                        const msg3 = JSON.parse(messages[2]);
                        checkMessagePlayerChannel2(msg3, "ddurieux");

                        finished = true;
                        cleanup();
                        done();
                    } catch (err) {
                        fail(err);
                    }
                }, 1000);
            }, 1000);
        });
    });


    function checkMessagePlayerChannel0(msg) {
        // {
        //     "channel": 0,
        //     "object_id": "bcd4ee4e-91bd-4645-bbed-70d569be2141",
        //     "object_type": "GorcPlayer",
        //     "player_id": "5213cfa2-5ad4-4055-9af9-4118936c225a",
        //     "timestamp": 1760225547,
        //     "type": "gorc_zone_enter",
        //     "zone_data": {
        //         "health": 100,
        //         "position": {
        //             "x": 0,
        //             "y": 0,
        //             "z": 0
        //         },
        //         "velocity": {
        //             "x": 0,
        //             "y": 0,
        //             "z": 0
        //         },
        //         "name: ""
        //     }
        // }
        expect(msg).to.have.all.keys(
            'channel',
            'object_id',
            'object_type',
            'player_id',
            'timestamp',
            'type',
            'zone_data'
        );
        expect(msg).to.have.property('channel', 0);
        expect(msg).to.have.property('object_id');
        expect(msg).to.have.property('object_type', 'GorcPlayer');
        expect(msg).to.have.property('player_id');
        expect(msg).to.have.property('timestamp');
        expect(msg).to.have.property('type', 'gorc_zone_enter');
        expect(msg).to.have.nested.property('zone_data').that.has.all.keys(
            'health',
            'position',
            'velocity',
        );
        expect(msg).to.have.nested.property('zone_data.health', 100);
        expect(msg).to.have.nested.property('zone_data.position.x', 0);
        expect(msg).to.have.nested.property('zone_data.position.y', 0);
        expect(msg).to.have.nested.property('zone_data.position.z', 0);
        expect(msg).to.have.nested.property('zone_data.velocity.x', 0);
        expect(msg).to.have.nested.property('zone_data.velocity.y', 0);
        expect(msg).to.have.nested.property('zone_data.velocity.z', 0);
    }

    function checkMessagePlayerChannel1(msg) {
        // {
        //     "channel": 1,
        //     "object_id": "bcd4ee4e-91bd-4645-bbed-70d569be2141",
        //     "object_type": "GorcPlayer",
        //     "player_id": "5213cfa2-5ad4-4055-9af9-4118936c225a",
        //     "timestamp": 1760225547,
        //     "type": "gorc_zone_enter",
        //     "zone_data": {
        //         "level": 1,
        //         "movement_state": "idle"
        //     }
        // }
        expect(msg).to.have.all.keys(
            'channel',
            'object_id',
            'object_type',
            'player_id',
            'timestamp',
            'type',
            'zone_data'
        );
        expect(msg).to.have.property('channel', 1);
        expect(msg).to.have.property('object_id');
        expect(msg).to.have.property('object_type', 'GorcPlayer');
        expect(msg).to.have.property('player_id');
        expect(msg).to.have.property('timestamp');
        expect(msg).to.have.property('type', 'gorc_zone_enter');
        expect(msg).to.have.nested.property('zone_data').that.has.all.keys(
            'level',
            'movement_state'
        );
        expect(msg).to.have.nested.property('zone_data.level', 1);
        expect(msg).to.have.nested.property('zone_data.movement_state', 'idle');
    }

    function checkMessagePlayerChannel2(msg, pseudo) {
        // {
        //     "channel": 2,
        //     "object_id": "bcd4ee4e-91bd-4645-bbed-70d569be2141",
        //     "object_type": "GorcPlayer",
        //     "player_id": "5213cfa2-5ad4-4055-9af9-4118936c225a",
        //     "timestamp": 1760225547,
        //     "type": "gorc_zone_enter",
        //     "zone_data": {
        //         "chat_bubble": null,
        //         "name": "Player_5213cfa2-5ad4-4055-9af9-4118936c225a"
        //     }
        // }
        expect(msg).to.have.all.keys(
            'channel',
            'object_id',
            'object_type',
            'player_id',
            'timestamp',
            'type',
            'zone_data'
        );
        expect(msg).to.have.property('channel', 2);
        expect(msg).to.have.property('object_id');
        expect(msg).to.have.property('object_type', 'GorcPlayer');
        expect(msg).to.have.property('player_id');
        expect(msg).to.have.property('timestamp');
        expect(msg).to.have.property('type', 'gorc_zone_enter');
        expect(msg).to.have.nested.property('zone_data').that.has.all.keys(
            'chat_bubble',
            'name'
        );
        expect(msg).to.have.nested.property('zone_data.chat_bubble', null);
        expect(msg).to.have.nested.property('zone_data.name', pseudo);
    }
});
