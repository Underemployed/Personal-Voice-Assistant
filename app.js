const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const fs = require('fs');
const async = require('async');

const timeframes = ['2015-05'];
const limit = 5000;
const concurrency = 4; 

async function processBatch(connection, last_unix, test_done) {
    const df = await connection.all(`SELECT * FROM parent_reply WHERE unix > ${last_unix} AND parent IS NOT NULL AND score > 2 ORDER BY unix ASC LIMIT ${limit}`);
    if (df.length === 0) return null;

    last_unix = df[df.length - 1].unix;
    const parentData = df.map(row => row.parent).join('\n') + '\n';
    const commentData = df.map(row => row.comment).join('\n') + '\n';

    if (!test_done) {
        fs.appendFileSync('test.from', parentData, 'utf8');
        fs.appendFileSync('test.to', commentData, 'utf8');
        test_done = true;
    } else {
        fs.appendFileSync('train.from', parentData, 'utf8');
        fs.appendFileSync('train.to', commentData, 'utf8');
    }

    fs.writeFileSync('last_unix.txt', last_unix.toString());

    return { last_unix, test_done };
}

async function processData() {
    for (const timeframe of timeframes) {
        const connection = await open({
            filename: `${timeframe}.db`,
            driver: sqlite3.Database
        });

        let last_unix = 0;
        let test_done = false;

        try {
            const lastUnixData = fs.readFileSync('last_unix.txt', 'utf8');
            last_unix = parseInt(lastUnixData);
        } catch (error) {
            console.log('Error reading last_unix.txt:', error);
        }

        let counter = 0;
        let tasks = [];

        while (true) {
            tasks.push(async () => {
                const result = await processBatch(connection, last_unix, test_done);
                if (result === null) return false;
                last_unix = result.last_unix;
                test_done = result.test_done;
                counter++;
                if (counter % 20 === 0) {
                    console.log(counter * limit, "rows completed so far");
                }
                return true;
            });

            if (tasks.length >= concurrency) {
                const results = await async.parallel(tasks);
                tasks = [];
                if (results.every(result => result === false)) break;
            }
        }

        // Process any remaining tasks
        if (tasks.length > 0) {
            await async.parallel(tasks);
        }

        await connection.close();
        console.log(`Finished processing timeframe: ${timeframe}`);
    }
}

processData().catch(console.error);