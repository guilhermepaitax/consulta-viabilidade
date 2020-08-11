module.exports = {

  development: {
    client: 'pg',
    connection: {
      host: '192.168.2.22',
      port: '5432',
      user: "postgres",
      password: "b6PWZ32PQ9WzWZks",
      database: "geo_fpolis"
    }
  },
  // development: {
  //   client: 'pg',
  //   connection: {
  //     host: '192.168.171.32',
  //     port: '5432',
  //     user: "postgres",
  //     password: "b6PWZ32PQ9WzWZks",
  //     database: "geo_fpolis"
  //   }
  // },
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
