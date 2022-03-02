const app = require('express')()
    , http = require('http').Server(app)
    , bodyParser = require('body-parser')
    , Cyjs = require('crypto-js')
    , oracledb = require('oracledb')
    , { Client } = require('pg')
    , QueryStream = require('pg-query-stream')
    , fs = require('fs-extra')
    , readline = require('readline');
;

const
    sanitizeString = function (value) {
        value = value.toString().replace(/'/g, '\'\'');
        return value;
    }

app.use(bodyParser.json({ limit: '1mb' }));
app.use(bodyParser.urlencoded({ limit: '1mb', extended: true, parameterLimit: 10000 }));

oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
oracledb.autoCommit = true;

fs.ensureDirSync(__dirname + '/tmp');

async function setNLS(oracleConn) {
    await oracleConn.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = ',.'");
    await oracleConn.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'DD/MM/RRRR HH24:MI:SSXFF'");
    await oracleConn.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/RRRR'");
    await oracleConn.execute("ALTER SESSION SET NLS_TERRITORY = 'BRAZIL'");
    await oracleConn.execute("ALTER SESSION SET NLS_LANGUAGE = 'BRAZILIAN PORTUGUESE'");
    await oracleConn.execute("ALTER SESSION SET NLS_DATE_LANGUAGE = 'BRAZILIAN PORTUGUESE'");
}

async function processLineByLine(req) {
    let db;
    try {
        const stream = fs.createReadStream(req.dataloadpath);
        const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
        db = new Client(Cyjs.AES.decrypt(req.body.dwc, '$H!T').toString(Cyjs.enc.Utf8));
        db.connect()
        let i = 0, inserts = [];
        const keys = req.body.keys;
        for await (const data of rl) {
            if (i == 0 && req.body.presql)
                await db.query(req.body.presql);
            if (i == 10000) {
                await db.query(inserts.join(';'));
                i = 0; inserts = [];
            }
            const d = JSON.parse(data);
            const values = [];
            for (let i = 0; i < keys.length; i++) {
                if (d[keys[i]] == null) {
                    values.push('null')
                } else {
                    values.push(`'${sanitizeString(d[keys[i]])}'`);
                }
            }
            inserts.push(`insert into ${req.body.table} (${keys.map((k) => { return `"${k}"`; })}) values (${values.join(',')})`);
            i++;
        }
        if (inserts.length > 0) {
            await db.query(inserts.join(';'));
            inserts = [];
        }
        stream.close();
        db.end(); db = null;
        fs.unlink(req.dataloadpath);
        req.res.end();
    } catch (err) {
        console.error(err);
        if (db) {
            try {
                await db.end();
            } catch (error) {
            }
        } db = null;
    }
}

app.post('/postgresql', async (req, res) => {
    let postgreConn;
    try {
        postgreConn = new Client(JSON.parse(req.body.dbc));
        await postgreConn.connect();
        const result = await postgreConn.query(req.body.q);
        res.json({ success: true, data: result.rows });
    } catch (err) {
        res.json({ success: false, msg: err.message });
        if (postgreConn) {
            try {
                await postgreConn.end();
            } catch (error) {
            }
        }
        postgreConn = null;
    }
});

app.post('/postgresql/etl', async (req, res) => {
    let postgreConn;
    try {
        postgreConn = new Client(JSON.parse(req.body.dbc));
        await postgreConn.connect();
        const stream = postgreConn.query(new QueryStream(req.body.q));
        req.dataloadpath = `${__dirname}/tmp/${process.hrtime()[1]}.txt`;
        const writer = fs.createWriteStream(req.dataloadpath);
        stream.on('error', function (err) {
            console.error(err)
            res.status(500).end(); writer.end();
        });
        stream.on('data', function (data) {
            writer.write(JSON.stringify(data) + '\n');
        });
        stream.on('end', async () => {
            writer.end();
            postgreConn.end(); postgreConn = null;
            processLineByLine(req);
        });
    } catch (err) {
        res.json({ success: false, msg: err.message });
        if (postgreConn) {
            try {
                await postgreConn.end();
            } catch (error) {
            }
        } postgreConn = null;
    }
});

app.post('/oracle', async (req, res) => {
    let oracleConn;
    try {
        oracledb.fetchAsBuffer = [ oracledb.BLOB ];
        oracleConn = await oracledb.getConnection(JSON.parse(req.body.dbc));
        await setNLS(oracleConn);
        const result = await oracleConn.execute(req.body.q);
        res.json({ success: true, data: result.rows });
    } catch (err) {
        res.json({ success: false, msg: err.message });
        if (oracleConn) {
            try {
                await oracleConn.close();
            } catch (error) {
            }
        }
        oracleConn = null;
    }
});

app.post('/oracle/etl', async (req, res) => {
    let oracleConn;
    try {
        oracleConn = await oracledb.getConnection(JSON.parse(req.body.dbc));
        await setNLS(oracleConn);
        const stream = await oracleConn.queryStream(req.body.q);
        req.dataloadpath = `${__dirname}/tmp/${process.hrtime()[1]}.txt`;
        const writer = fs.createWriteStream(req.dataloadpath);
        stream.on('error', function (err) {
            console.error(err)
            res.status(500).end();
        });
        stream.on('data', function (data) {
            writer.write(JSON.stringify(data) + '\n');
        });
        stream.on('end', async () => {
            writer.end();
            oracleConn.close(); oracleConn = null;
            processLineByLine(req);
        });
    } catch (err) {
        res.json({ success: false, msg: err.message });
        if (oracleConn) {
            try {
                await oracleConn.close();
            } catch (error) {
            }
        }
        oracleConn = null;
    }
});

http.listen(process.env.NODE_PORT, function () {
    console.log('Server UP', 'PID ' + process.pid);
});