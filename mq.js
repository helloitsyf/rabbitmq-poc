var mq = require("amqplib");
var _ = require("lodash");

var dlx = "dlx";

var q = "tasks";
var rq = "retry-tasks";
var kill = "kill";

const failureSettings = {
  tasks: {
    allowFailure: true,
    maxAllowedRetries: 3,
  },
  //   tasks: {
  //     allowFailure: false,
  //   },
};

const a = async () => {
  const conn = await mq.connect({
    protocol: "amqp",
    hostname: "localhost",
    port: 5672,
    username: "guest",
    password: "guest",
    locale: "en_US",
    frameMax: 0,
    heartbeat: 0,
  });

  const ch = await conn.createChannel();

  await ch.assertExchange(dlx);

  await ch.deleteQueue(q);
  await ch.deleteQueue(rq);
  await ch.deleteQueue(kill);

  await ch.assertQueue(q, {
    durable: true,
    deadLetterExchange: dlx,
    deadLetterRoutingKey: rq,
  });
  await ch.assertQueue(rq, {
    durable: true,
    deadLetterExchange: "",
    deadLetterRoutingKey: q,
    messageTtl: 2000,
  });
  await ch.bindQueue(rq, dlx, rq);
  await ch.assertQueue(kill, {
    durable: true,
  });

  await ch.sendToQueue(q, Buffer.from("something to do"));

  ch.consume(q, async function (msg) {
    if (msg !== null) {
      //... do whatever you want

      const deathProperties = _.get(msg, 'properties.headers["x-death"]', [
        { count: 0 },
      ]);
      const [{ count = 0 }] = deathProperties;
      console.log({ count });

      const isAllowedToFail = _.get(
        failureSettings,
        `${q}.allowFailure`,
        false
      );
      const maxAllowedRetries = _.get(
        failureSettings,
        `${q}.maxAllowedRetries`,
        -1
      );
      if (isAllowedToFail && count > maxAllowedRetries) {
        console.log("transferring...");
        const messageBuffer = _.get(msg, "content", null);
        await ch.publish("", kill, messageBuffer);
        await ch.ack(msg);
        console.log("transferred");
      } else {
        await ch.nack(msg, false, false);
      }
    }
  });

  ch.consume(kill, function (msg) {
    if (msg !== null) {
      console.log(msg);
      console.log(msg.content.toString());
      ch.ack(msg);
      //   ch.nack(msg, false, true);
    }
  });
};

a();
