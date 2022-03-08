import http from 'http';
import fs from 'fs';
import async from 'async';
import {parse} from 'csv-parse';
import { VKAPI } from 'vkontakte-api';
import mysql from 'mysql';

const VK_ACCESS_TOKEN = ''

const DB_CONNECTION = {
    host     : '',
    user     : '',
    password : '',
    port     : 0,
    database: ''
}
const TABLE_NAME = 'parsed_orcs'

const TEXT_FILE = './test.txt'
const JSON_FILE = './test_db.json'

const INDEX_START_FROM = 0

// map of headers columns
const CSV_CONNECTION_INDEXES = {
    FULL_NAME: 2, // ФИО
    DATE_BIRTH: 8, // Дата рождения
    CATEGORY_COOPERATOR: 5, // Табельный номер
    NAME_COOPERATOR: 6, // Наименование сотрудника
    PASSPORT_SERIES: 10, // Серия пасспорта
    PASSPORT_DEF: 11, // -    
    PASSPORT_ID_UM: 12, // Ид. номер    
    PASSPORT_ISSUED: 13, // Кем выдан    
    PASSPORT_DATA: 14, // Дата выдачи   
}

const connection = mysql.createConnection(DB_CONNECTION);

  connection.connect(function(err) {
    if (err) {
      console.error('Database connection failed: ' + err.stack);
      return;
    }
    console.log('Connected to database.');
  });

  connection.query(`CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
      id INT PRIMARY KEY AUTO_INCREMENT, 
      full_name VARCHAR(255), 
      date_birth VARCHAR(255), 
      vk_link VARCHAR(255), 
      vk_avatar VARCHAR(255), 
      category_cooperator VARCHAR(255), 
      name_cooperator VARCHAR(255), 
      passport_series VARCHAR(255), 
      passport_def VARCHAR(255), 
      passport_id_um VARCHAR(255), 
      passport_issued VARCHAR(255), 
      passport_data VARCHAR(255)
      )`, function (error) {
    if (error) throw error;
    // connected!
    console.log('Table created')
  });

const api = new VKAPI({
    rps: 20,
    accessToken: VK_ACCESS_TOKEN,
    lang: 'en',
  });

var inputFile='/Users/vzhukova/Downloads/orcs.csv'; 
var parser = parse({delimiter: ','}, function (err, data) {
    console.log('HEADERS: ', data[0])
    func1(data)
  });
  fs.createReadStream(inputFile).pipe(parser);

const requestListener = function (req, res) {
  res.writeHead(200);
  res.end('Hello, World!');
}

const server = http.createServer(requestListener);
server.listen(8080);
console.log('server is listen in 8080 port')

var searchInVk = async (fullPerson, name, surname, day, month, year, numTry = 1) => {
    const full_name = `${name} ${surname}`
    return new Promise(async (resolve, reject) => {
        try {
            var dataSearch = await api.users.search({
                q: full_name, 
                birth_day: day, 
                birth_month: month,
                birth_year: year,
                global: 1,
            })
    
            if(dataSearch.items.length === 0) {
                console.log('not found')
                resolve()
                return
            }

            var userData = await api.users.get({
                user_ids: [dataSearch.items[0].id],
                fields: [
                    "nickname",
                    "bdate",
                    "photo_id",
                    "relation",
                    "relatives",
                    "can_post",
                    "can_see_all_posts",
                    "can_send_friend_request",
                    "can_write_private_message",
                    "career",
                    "city",
                    "common_count",
                    "connections",
                    "contacts",
                    "country",
                    "education",
                    "exports",
                    "followers_count",
                    "has_mobile",
                    "has_photo",
                    "interests",
                    "last_seen",
                    "lists",
                    "military",
                    "occupation",
                    "online",
                    "personal",
                    "photo_100",
                    "photo_200",
                    "photo_200_orig",
                    "photo_400_orig",
                ]
            })
            console.log('FOUND!', userData)
            userData.forEach(user => {
                const vk_link = `https://vk.com/id${user?.id}`
                const vk_avatar = user?.photo_400_orig

                // Text file write
                fs.appendFileSync(TEXT_FILE, `${full_name} ${vk_link} ${vk_avatar} ${user?.bdate} ${user?.city?.title}(${user?.country?.title}) last_seen: ${ new Date(user?.last_seen?.time * 1000).toLocaleDateString()} \n`)

                // Json file write
                const jsonData = {
                    full_name: fullPerson[CSV_CONNECTION_INDEXES.FULL_NAME],
                    date_birth: fullPerson[CSV_CONNECTION_INDEXES.DATE_BIRTH],
                    vk_link: vk_link,   
                    vk_avatar: vk_avatar,    
                    category_cooperator: fullPerson[CSV_CONNECTION_INDEXES.CATEGORY_COOPERATOR],
                    name_cooperator: fullPerson[CSV_CONNECTION_INDEXES.NAME_COOPERATOR],
                    passport_series: fullPerson[CSV_CONNECTION_INDEXES.PASSPORT_SERIES],    
                    passport_def: fullPerson[CSV_CONNECTION_INDEXES.PASSPORT_DEF],    
                    passport_id_um: fullPerson[CSV_CONNECTION_INDEXES.PASSPORT_ID_UM],    
                    passport_issued: fullPerson[CSV_CONNECTION_INDEXES.PASSPORT_ISSUED],    
                    passport_data: fullPerson[CSV_CONNECTION_INDEXES.PASSPORT_DATA],    
                }
                fs.readFile(JSON_FILE, function(err, json) {
                    console.log('!!', json)
                    var array =  (json && JSON.parse(json)) || [];
                    array.push(jsonData);
                    fs.writeFile(JSON_FILE, JSON.stringify(array), function(err) {
                        if (err) {
                            console.log(err);
                            return;
                        }
                        console.log("The file was saved!");
                    });
                });

                // DB file write
                connection.query(`INSERT INTO ${TABLE_NAME} SET ?`, jsonData, function (error, results, fields) {
                    if (error) throw error;
                    console.log(results.insertId);
                    });
            })
            resolve(userData)
        } catch(error) {
            console.log('!!!!', error, 'json: ', JSON.stringify(error))
            if (error?.errorInfo?.error_code === 6 || error?.code === 'ETIMEDOUT') {
                if(numTry === 10) {
                    console.log('rejected', numTry)
                    reject(error)
                    connection.end();
                }
                console.log('timeout started')
                setTimeout(() => {
                    searchInVk(fullPerson, name, surname, day, month, year, numTry + 1)
                    .then(resolve)
                    .catch(reject)
                }, 1000 * 60);
            }
            console.log('rejected')
            reject(error)
        }
    })
}

var func1 = async (data, i = INDEX_START_FROM) => {
    if(!data || !data[i]) return
    var fullPerson = data[i]
    var birthDate = (fullPerson[CSV_CONNECTION_INDEXES.DATE_BIRTH])?.split('/')
    var fullName = fullPerson[CSV_CONNECTION_INDEXES.FULL_NAME].split(' ')
    console.log(fullPerson[2], fullPerson[8])
    var res = await searchInVk(fullPerson, fullName[1], fullName[0], birthDate[0], birthDate[1], birthDate[2])
    func1(data, i + 1)
}