import { Store } from 'express-session';
import mysql, { Connection, Pool,  RowDataPacket } from 'mysql2/promise';
import timers from 'node:timers';
import { Logger } from 'winston';

import { ConnectionOptions } from 'mysql2';
import winston from 'winston/lib/winston/config';

type MySQLStoreOptions = {
    clearExpired: boolean,
    checkExpirationInterval: number,
    expiration: number,
    createDatabaseTable: boolean,
    endConnectionOnClose: boolean,
    disableTouch: boolean,
    charset: string,
    schema: {
        tableName: string,
        columnNames: {
            session_id: string,
            expires: string,
            data: string,
        },
    },
}


export default class MySQLStore extends Store {
    state = 'UNINITIALIZED';
    connection: Connection | Pool
    logger: Logger
    defaultOptions = {
        // Whether or not to automatically check for and clear expired sessions:
        clearExpired: true,
        // How frequently expired sessions will be cleared; milliseconds:
        checkExpirationInterval: 900000,
        // The maximum age of a valid session; milliseconds:
        expiration: 86400000 * 60,
        // Whether or not to create the sessions database table, if one does not already exist:
        createDatabaseTable: true,
        // Whether or not to end the database connection when the store is closed:
        endConnectionOnClose: true,
        // Whether or not to disable touch:
        disableTouch: false,
        charset: 'utf8mb4_bin',
        schema: {
            tableName: 'sessions',
            columnNames: {
                session_id: 'session_id',
                expires: 'expires',
                data: 'data',
            },
        },
    };

    #options: {
        // Whether or not to automatically check for and clear expired sessions:
        clearExpired: boolean,
        // How frequently expired sessions will be cleared; milliseconds:
        checkExpirationInterval: number,
        // The maximum age of a valid session; milliseconds:
        expiration: number,
        // Whether or not to create the sessions database table, if one does not already exist:
        createDatabaseTable: boolean,
        // Whether or not to end the database connection when the store is closed:
        endConnectionOnClose: boolean,
        // Whether or not to disable touch:
        disableTouch: boolean,
        charset: string,
        schema: {
            tableName: string,
            columnNames: {
                session_id: string,
                expires: string,
                data: string,
            },
        },
    } = this.defaultOptions;

    onReadyPromises: Array<any> = [];
    #expirationInterval:any

    constructor(options?: MySQLStoreOptions, connection?: Connection | Pool, logger?: Logger) {
        super();
        this.state = 'INITIALIZING';
        if(!connection) {  throw new Error('MySQLStore requires a mysql connection'); }
        this.connection = connection;
        this.logger = logger || new Logger();
        options && (this.options = options )
        if (!this.connection) {
            this.connection = this.createPool(this.options);
        }
        this.onReadyPromises = [];
        Promise.resolve().then(() => {
            if (this.options.createDatabaseTable) {
                return this.createDatabaseTable();
            }
        }).then(() => {
            this.state = 'INITIALIZED';
            if (this.options.clearExpired) {
                this.setExpirationInterval();
            }
        }).then(() => {
            this.resolveReadyPromises();
        }).catch(error => {
            this.rejectReadyPromises(error);
        });
    }

    async onReady() {
        if (this.state === 'INITIALIZED') return Promise.resolve();
        return new Promise((resolve, reject) => {
            this.onReadyPromises.push({ resolve, reject });
        });
    };

    resolveReadyPromises() {
        this.onReadyPromises.forEach(promise => promise.resolve());
        this.onReadyPromises = [];
    };

    async rejectReadyPromises (error: any) {
        this.onReadyPromises.forEach(promise => promise.reject(error));
        this.onReadyPromises = [];
    };

    prepareOptionsForMySQL2(options: ConnectionOptions) {
        return {...options}
    };

    createPool(options: Partial<MySQLStoreOptions> | mysql.PoolOptions) {
        const mysqlOptions = this.prepareOptionsForMySQL2(options);
        return mysql.createPool(mysqlOptions);
    };
    
    public get options() {
        return this.#options;
    }

    public set options(options: MySQLStoreOptions) {
        this.#options = {
            ...this.defaultOptions,
            ...options,
        }
        this.#options.endConnectionOnClose = !this.connection,

            this.#options.schema = {
                ...this.defaultOptions.schema,
                ...options?.schema
            };
        this.#options.schema.columnNames = {
            ...this.defaultOptions.schema.columnNames,
            ...options?.schema?.columnNames
        };
        this.validateOptions();
    };

    validateOptions() {
        let { options, defaultOptions } = this;
        const allowedColumnNames = Object.keys(defaultOptions.schema.columnNames);
        const columnNames = Object.keys(options.schema.columnNames);
        columnNames.forEach(function (userDefinedColumnName) {
            if (!allowedColumnNames.includes(userDefinedColumnName)) {
                throw new Error('Unknown column specified ("' + userDefinedColumnName + '"). Only the following columns are configurable: "session_id", "expires", "data". Please review the documentation to understand how to correctly use this option.');
            }
        });
    };

    async createDatabaseTable() {
        const schema = [
            "CREATE TABLE IF NOT EXISTS `sessions`",
            "(`session_id` varchar(128) COLLATE utf8mb4_bin NOT NULL,",
            "`expires` int(11) unsigned NOT NULL,",
            "`data` mediumtext COLLATE utf8mb4_bin,",
            "PRIMARY KEY (\`session_id\`)",
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
        ].join(' ').replace(/`[^`]+`/g, '??');
        let {tableName, columnNames} = this.options.schema;
        
        const params = [
            tableName,
            columnNames.session_id,
            columnNames.expires,
            columnNames.data,
            columnNames.session_id,
        ];
        try {
            let result = await this.query(schema, params);
            this.logger.info('Sessions database table is properly configured');
            return result;
        } catch (error) {
            this.logger.error('Failed to create or validate sessions database table.');
            this.logger.error(error);
            throw error;
        }
    };

    async get(session_id:String, callback: Function) {
        this.logger.info(`Getting session: ${session_id}`);
        // LIMIT not needed here because the WHERE clause is searching by the table's primary key.
        const sql = 'SELECT ?? AS data, ?? as expires FROM ?? WHERE ?? = ?';
        const params = [
            this.options.schema.columnNames.data,
            this.options.schema.columnNames.expires,
            this.options.schema.tableName,
            this.options.schema.columnNames.session_id,
            session_id,
        ];
        try {
            const result = await this.query(sql, params) as [(RowDataPacket & {'session_id':any, data:any}[]), any];
            const [rows] = result;
            
            if (!!rows) {
                this.logger.info(`Session (${session_id}) not found`);
                return callback(null);// not found
            }
            // Check the expires time.
            const now = Math.round(Date.now() / 1000);
            const row = rows[0] as RowDataPacket;
            if (row.expires < now) {
                this.logger.info(`Session (${session_id}) expired`);
                return callback(null);// expired
            }
            let data = row.data;
            if (typeof data === 'string') {
                try { data = JSON.parse(data); } catch (error) {
                    this.logger.error(`Failed to parse data for session (${session_id})`);
                    this.logger.error(error);
                    throw error;
                }
            }
            return callback(null,data)
        } catch (error) {
            this.logger.error(`Failed to get session: ${session_id}`);
            this.logger.error(error);
            return callback(error)        
        }

    };

    async set(session_id:any, data:any, callback: any) {
        this.logger.info(`Setting session: ${session_id}`);
        let expires;
        if (data.cookie) {
            if (data.cookie.expires) {
                expires = data.cookie.expires;
            } else if (data.cookie._expires) {
                expires = data.cookie._expires;
            }
        }
        if (!expires) {
            expires = Date.now() + this.options.expiration;
        }
        if (!(expires instanceof Date)) {
            expires = new Date(expires);
        }
        // Use whole seconds here; not milliseconds.
        expires = Math.round(expires.getTime() / 1000);
        data = JSON.stringify(data);
        const sql = 'INSERT INTO ?? (??, ??, ??) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE ?? = VALUES(??), ?? = VALUES(??)';
        const params = [
            this.options.schema.tableName,
            this.options.schema.columnNames.session_id,
            this.options.schema.columnNames.expires,
            this.options.schema.columnNames.data,
            session_id,
            expires,
            data,
            this.options.schema.columnNames.expires,
            this.options.schema.columnNames.expires,
            this.options.schema.columnNames.data,
            this.options.schema.columnNames.data,
        ];
        try {
            await this.query(sql, params)
            return callback();

        } catch (error) {
            this.logger.error('Failed to insert session data.');
            this.logger.error(error);
            return callback(error)
        }
    };

    async touch(session_id:any, data:any, callback: any) {
        if (this.options.disableTouch) return;// noop
        this.logger.info(`Touching session: ${session_id}`);
        let expires;
        if (data.cookie) {
            if (data.cookie.expires) {
                expires = data.cookie.expires;
            } else if (data.cookie._expires) {
                expires = data.cookie._expires;
            }
        }
        if (!expires) {
            expires = Date.now() + this.options.expiration;
        }
        if (!(expires instanceof Date)) {
            expires = new Date(expires);
        }
        // Use whole seconds here; not milliseconds.
        expires = Math.round(expires.getTime() / 1000);
        // LIMIT not needed here because the WHERE clause is searching by the table's primary key.
        const sql = 'UPDATE ?? SET ?? = ? WHERE ?? = ?';
        const params = [
            this.options.schema.tableName,
            this.options.schema.columnNames.expires,
            expires,
            this.options.schema.columnNames.session_id,
            session_id,
        ];
        try {
            await this.query(sql, params)
            return callback()
        } catch (error) {
            this.logger.error(`Failed to touch session (${session_id})`);
            this.logger.error(error);
            return callback(error)
        }
    };

    async destroy(session_id:string, callback?: Function) {
        try{
            this.logger.info(`Destroying session: ${session_id}`);
            // LIMIT not needed here because the WHERE clause is searching by the table's primary key.
            const sql = 'DELETE FROM ?? WHERE ?? = ?';
            const params = [
                this.options.schema.tableName,
                this.options.schema.columnNames.session_id,
                session_id,
            ];
            await this.query(sql, params)
        } catch (error) {
            this.logger.error(`Failed to destroy session (${session_id})`);
            this.logger.error(error);
            if(typeof callback === 'function') return callback(error)
        }
        this.logger.info(`Destroyed session: ${session_id}`);
        if(typeof callback === 'function') return callback()
        return
    };

    async length(callback: Function) {
        this.logger.info('Getting number of sessions');
        const sql = 'SELECT COUNT(*) FROM ?? WHERE ?? >= ?';
        const params = [
            this.options.schema.tableName,
            this.options.schema.columnNames.expires,
            Math.round(Date.now() / 1000),
        ];
        try{
            const result = await this.query(sql, params) as [(RowDataPacket & {'session_id':any, data:any}[]), any];
            const [rows] = result;
            return callback(null,rows[0] && rows[0]['COUNT(*)'] || 0)
        }catch(error){
            this.logger.error('Failed to get number of sessions.');
            this.logger.error(error);
            return callback(error)
        }
    };

    async all(callback: Function) {
        const sql = 'SELECT * FROM ?? WHERE ?? >= ?';
        const params = [
            this.options.schema.tableName,
            this.options.schema.columnNames.expires,
            Math.round(Date.now() / 1000),
        ];
        try{
            const result = await this.query(sql, params) as [(RowDataPacket & {'session_id':any, data:any}[]), any];
            const [rows] = result ;
            let sessions:any = {};
            rows.forEach((row)=> {
                const session_id = row.session_id;
                let data = row.data;
                if (typeof data === 'string') {
                    try { data = JSON.parse(data); } catch (error) {
                        this.logger.error('Failed to parse data for session (' + session_id + ')');
                        this.logger.error(error);
                        return null;
                    }
                }
                sessions[session_id] = data;
            });
            return callback(null,sessions)
        } catch (error) {
            return callback(error)
        }
    };

    clear(callback?:(error:unknown)=>void) {
        try{
            this.logger.info('Clearing all sessions');
            const sql = 'DELETE FROM ??';
            const params = [
                this.options.schema.tableName,
            ];
             this.query(sql, params)
        }catch(error) {
            if(typeof callback === 'function') return callback(error)   
        };
    };

    clearExpiredSessions = async(options = this.options)=>{
        this.logger.info('Clearing expired sessions');
        const sql = 'DELETE FROM ?? WHERE ?? < ?';
        
        const params = [
            options.schema.tableName,
            options.schema.columnNames.expires,
            Math.round(Date.now() / 1000),
        ];
        try {
            let result = await this.query(sql, params)
            return result;
        } catch (error) {
            this.logger.error('Failed to clear expired sessions.');
            this.logger.error(error);
            throw error;
        }

    };

    async query(sql: string, params: any) {
        try {
            const result = await this.connection.query(sql, params)
            return result;
        } catch (error) {
            this.logger.error('Failed to query database.');
        }
    };

    setExpirationInterval(interval = this.options.checkExpirationInterval) {
        this.logger.info('Setting expiration interval to', interval + 'ms');
        this.clearExpirationInterval();
        this.clearExpiredSessions();
        this.#expirationInterval = setInterval(this.clearExpiredSessions, interval, this.options);
    };

    clearExpirationInterval() {
        this.logger.info('Clearing expiration interval');
        timers.clearInterval(this.#expirationInterval);
        this.#expirationInterval = null;
    };

    async close() {
        return Promise.resolve().then(() => {
            this.logger.info('Closing session store');
            this.clearExpirationInterval();
            if (this.state === 'INITIALIZED' && this.connection && this.options.endConnectionOnClose) {
                this.state = 'CLOSING';
                return this.connection.end().finally(() => {
                    this.state = 'CLOSED';
                });
            }
        });
    };

    async promiseAllSeries(promiseFactories: any[]) {
        let result = Promise.resolve();
        promiseFactories.forEach(promiseFactory => {
            result = result.then(promiseFactory);
        });
        return result;
    };
}

