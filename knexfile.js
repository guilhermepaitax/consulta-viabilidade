module.exports = {
  development: {
    client: 'pg',
    connection: {
      host: '192.168.171.32',
      port: '5432',
      user: "dicgp",
      password: "geofloripa2019",
      database: "geo_fpolis"
    }
  },
  production: {
    client: 'postgresql',
    connection: {
      database: 'my_db',
      user:     'username',
      password: 'password'
    },
    pool: {
      min: 2,
      max: 10
    },
    migrations: {
      tableName: 'knex_migrations'
    }
  }

};
