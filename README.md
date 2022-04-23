# Appository Sync

Appository Sync is an open-source developer platform for building relatively simple applications. It provides a [Socket.IO](https://socket.io/) based data storing backend with notification of changes that makes it easier to make JavaScript web and mobile apps which supposed to share their states. Data is stored on disk as plain JSON files.

## Setup

You need to provide the TOKEN_SECRET environment variable that will be used for API authorization tokens verification. Easies way it to create *.env* file in the root directory of the project (see.env.example for reference).

To get authorization token for a particular bucket (i.e. namespace for application data in the server storage) run this command:

    $ npm run token your_bucket_name

Token that will be printed can be used in your application.

## Running

Server can be started manually:

    $ npm start

Or with use of PM2 manager:

    $ pm2 start ecosystem.json

## License

This repository is available under the [GNU General Public License](./LICENSE).
