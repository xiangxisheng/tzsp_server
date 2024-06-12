const mysql = require("mysql");
module.exports.open = function (ctx, global) {
  ctx.db_conns = [];
  ctx.db = async function (sDbName) {
    // 调用这个函数后就从数据库连接池里取出了个新连接
    if (!global.db_pool.hasOwnProperty(sDbName)) {
      throw new Error(`not find sDbName=${sDbName} in $G.db`);
    }
    const conn = await module.exports.getConnection(global.db_pool[sDbName]);
    ctx.db_conns.push(conn);
    return conn;
  };
};
module.exports.close = function (ctx) {
  for (const k in ctx.db_conns) {
    const db_conn = ctx.db_conns[k];
    db_conn.release();
  }
};
module.exports.createPool = function (oURL) {
  const aPath = oURL.pathname.split("/");
  const config = {};
  config.host = oURL.hostname;
  if (oURL.port) {
    config.port = oURL.port;
  }
  config.user = oURL.username;
  config.password = process.env.PASSWORD;
  if (oURL.password) {
    config.password = oURL.password;
  }
  config.database = aPath[1];
  config.connectionLimit = 100;
  return mysql.createPool(config);
};
module.exports.getConnection = async function (oDbPool) {
  const getConnection = async function () {
    return new Promise((resolve, reject) => {
      oDbPool.getConnection(function (err, connection) {
        if (err) {
          return reject(err);
        }
        resolve(connection);
      });
    });
  };
  const oConn = await getConnection();
  const oRet = {};
  oRet.begin = async function () {
    return await oConn.beginTransaction();
  };
  oRet.rollback = async function () {
    return await oConn.rollback();
  };
  oRet.commit = async function () {
    return await oConn.commit();
  };
  oRet.release = function () {
    oConn.release();
  };
  oRet.query = async function (sql, values) {
    // 执行查询语句
    return new Promise((resolve, reject) => {
      //console.log("query:", sql, values);
      oConn.query(sql, values, (err, rows) => {
        if (err) {
          return reject(err);
        }
        resolve(rows);
      });
    });
  };
  oRet.queryByPdoStyle = function (sSql, mWhere) {
    // 将PHP版的预编译转换成Node.js版本
    const reg = /\:[A-Za-z][0-9_A-Za-z]*/g;
    const newSql = sSql.replace(reg, "?");
    const aList = sSql.match(reg);
    const aValues = [];
    for (const k in aList) {
      const sFieldName = aList[k].substr(1);
      if (!mWhere.hasOwnProperty(sFieldName)) {
        throw new Error(`not found sFieldName=${sFieldName} in mWhere`);
      }
      aValues.push(mWhere[sFieldName]);
    }
    return oRet.query(newSql, aValues);
  };
  oRet.sql = function () {
    //取得sql实例
    const mData = {};
    mData.sField = "*";
    const oSql = {};
    oSql.table = (sTableName) => {
      mData.sTableName = sTableName;
      return oSql;
    };
    oSql.field = (sField) => {
      mData.sField = sField;
      return oSql;
    };
    oSql.where = (mWhere, sWhere) => {
      mData.mWhere = mWhere;
      if (sWhere) {
        mData.sWhere = sWhere;
      } else {
        // 如果没有提供sWhere就需要自动生成
        const aWhere = [];
        for (const k in mWhere) {
          aWhere.push(`${k}=:${k}`);
        }
        if (aWhere.length > 0) {
          mData.sWhere = "(" + aWhere.join(")AND(") + ")";
        }
      }
      return oSql;
    };
    oSql.lock = () => {
      mData.isLock = true;
      return oSql;
    };
    oSql.select = () => {
      const aSql = [];
      aSql.push(`SELECT ${mData.sField} FROM ${mData.sTableName}`);
      if (mData.sWhere) {
        aSql.push(`WHERE ${mData.sWhere}`);
      }
      if (mData.limit) {
        aSql.push(`LIMIT ${mData.limit}`);
      }
      if (mData.isLock) {
        aSql.push(`FOR UPDATE`);
      }
      const sSql = aSql.join(" ");
      return oRet.queryByPdoStyle(sSql, mData.mWhere);
    };
    oSql.find = async () => {
      mData.limit = 1;
      const aRows = await oSql.select();
      for (const k in aRows) {
        return aRows[k];
      }
    };
    const getData = (mData) => {
      const getVal = (k, v, mParam) => {
        if (v === null) {
          return "NULL";
        }
        if (v.hasOwnProperty("sql")) {
          return v.sql;
        }
        mParam[k] = v;
        return ":" + k;
      };
      const mParam = {};
      const aSql = [];
      var i = 0;
      for (const k in mData) {
        i++;
        const sFieldName = /^[a-z]+$/i.test(k) ? "`" + k + "`" : k;
        const kk = "data" + i;
        const sVal = getVal(kk, mData[k], mParam);
        aSql.push(`${sFieldName}=${sVal}`);
      }
      return [mParam, aSql.join(",")];
    };
    const mValHebing = function (mVal1, mVal2) {
      const mRet = {};
      for (const k in mVal1) {
        mRet[k] = mVal1[k];
      }
      for (const k in mVal2) {
        mRet[k] = mVal2[k];
      }
      return mRet;
    };
    oSql.add = (mValue) => {
      const aSql = [];
      aSql.push(`INSERT IGNORE ${mData.sTableName}`);
      const d = getData(mValue);
      aSql.push("SET " + d[1]);
      const sSql = aSql.join(" ");
      return oRet.queryByPdoStyle(sSql, d[0]);
    };
    oSql.save = (mValue) => {
      const aSql = [];
      aSql.push(`UPDATE ${mData.sTableName}`);
      const d = getData(mValue);
      aSql.push("SET " + d[1]);
      if (mData.sWhere) {
        aSql.push(` WHERE ${mData.sWhere}`);
      }
      const sSql = aSql.join(" ");
      return oRet.queryByPdoStyle(sSql, mValHebing(mData.mWhere, d[0]));
    };
    return oSql;
  };
  return oRet;
};
