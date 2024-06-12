// parseTZSP.js

const dgram = require("dgram");
const server = dgram.createSocket("udp4");
const dns2 = require("dns2");
const { Packet } = dns2;
const mysql = require("./mysql");
const dbPool = mysql.createPool(
  new URL("mysql://tzsp_server:tzsp_server@192.168.88.21/tzsp_server")
);

function getValueInAnswer(answer) {
  if (answer.type === 1) {
    return answer.address;
  }
  if (answer.type === 5) {
    return answer.domain;
  }
  return "";
}

async function upsert(dbSql, sTable, mWhere, mData = {}) {
  dbSql.table(sTable);
  dbSql.where(mWhere);
  const mAdd = {};
  for (const k in mData) {
    mAdd[k] = mData[k];
  }
  for (const k in mWhere) {
    mAdd[k] = mWhere[k];
  }
  const oRes = await dbSql.add(mAdd);
  if (oRes.insertId) {
    dbSql.where({ id: oRes.insertId });
    if (Object.keys(mData).length) {
      await dbSql.save(mData);
    }
  }
}

// 处理收到的数据包
server.on("message", async (msg, rinfo) => {
  //console.log(`服务器收到来自 ${rinfo.address}:${rinfo.port} 的消息`);

  // 解析TZSP数据包
  const oRes = parseTZSP(msg);
  for (const answer of oRes.dns.answers) {
    console.log(oRes.ipv4.sourceAddr, answer);
    const value = getValueInAnswer(answer);
    if (value) {
      try {
        const dbConn = await mysql.getConnection(dbPool);
        await upsert(dbConn.sql(), "dns_records", {
          server: oRes.ipv4.sourceAddr,
          name: answer.name,
          type: answer.type,
          value,
        });
        dbConn.release();
      } catch (e) {}
    }
  }
});

// 处理服务器启动事件
server.on("listening", () => {
  const address = server.address();
  console.log(`服务器正在监听 ${address.address}:${address.port}`);
});

// 处理错误事件
server.on("error", (err) => {
  console.error("服务器发生错误:", err);
  server.close();
});

// 启动服务器
const PORT = 37008;
const HOST = "0.0.0.0";
server.bind(PORT, HOST);

function bufferToIpv4(buffer) {
  // 确保 buffer 的长度是 4
  if (buffer.length !== 4) {
    throw new Error("Invalid IPv4 buffer length");
  }

  // 将每个字节转换为对应的十进制数，并用点号连接起来
  return buffer[0] + "." + buffer[1] + "." + buffer[2] + "." + buffer[3];
}

function parseTZSP(buffer) {
  const oRes = {};
  let offset = 0;
  oRes.header = parseHeader(buffer.slice(offset, offset + 5));
  offset += 5;
  oRes.ethernet = parseEthernet(buffer.slice(offset, offset + 14));
  offset += 14;
  if (oRes.ethernet.type === "VLAN") {
    offset += 4;
  }
  oRes.ipv4 = parseIPv4(buffer.slice(offset, offset + 20));
  offset += 20;
  oRes.udp = parseUDP(buffer.slice(offset, offset + 8));
  offset += 8;
  oRes.dns = Packet.parse(buffer.slice(offset));
  return oRes;
}

function parseHeader(buffer) {
  const oRes = {
    version: buffer.readUInt8(0),
    type: buffer.readUInt8(1),
    encapsulatedProtocol: buffer.readUInt16BE(2),
  };
  return oRes;
}

function parseEthernet(buffer) {
  const oRes = {};
  let offset = 0;
  oRes.destination = buffer
    .slice(offset, offset + 6)
    .toString("hex")
    .match(/.{1,2}/g)
    .join(":");
  offset += 6;
  oRes.source = buffer
    .slice(offset, offset + 6)
    .toString("hex")
    .match(/.{1,2}/g)
    .join(":");
  offset += 6;
  oRes.type = buffer.readUInt16BE(offset);
  if (oRes.type === 2048) {
    oRes.type = "IPv4";
  }
  if (oRes.type === 33024) {
    oRes.type = "VLAN";
  }
  return oRes;
}

function parseIPv4(buffer) {
  const oRes = {};
  let offset = 0;
  offset += 2;
  oRes.totalLength = buffer.readUInt16BE(offset, offset + 2);
  offset += 2;
  oRes.indentification = buffer.readUInt16BE(offset, offset + 2);
  offset += 2;
  oRes.fragmentOffset = buffer.readUInt16BE(offset, offset + 2);
  offset += 2;
  oRes.TTL = buffer.readUInt8(offset);
  offset += 1;
  oRes.protocol = buffer.readUInt8(offset);
  offset += 1;
  oRes.headerChecksum = buffer.readUInt16BE(offset, offset + 2);
  offset += 2;
  oRes.sourceAddr = bufferToIpv4(buffer.slice(offset, offset + 4));
  offset += 4;
  oRes.destAddr = bufferToIpv4(buffer.slice(offset, offset + 4));
  offset += 4;
  return oRes;
}

function parseUDP(buffer) {
  const oRes = {};
  let offset = 0;
  oRes.sourcePort = buffer.readUInt16BE(offset, offset + 2);
  offset += 2;
  oRes.destPort = buffer.readUInt16BE(offset, offset + 2);
  offset += 2;
  oRes.length = buffer.readUInt16BE(offset, offset + 2);
  offset += 2;
  oRes.checkSum = buffer.readUInt16BE(offset, offset + 2);
  offset += 2;
  return oRes;
}
