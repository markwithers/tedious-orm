var logger = require('log_manager').getLogger();
var tedious = require('tedious');

module.exports.createConnection = function (config) {
    var con = tedious.Connection;
    var connection = new con(config);

    return connection;
};

module.exports.connect = function (connection, callback) {
    connection.on('connect', function (err) {
        if (err) {
            console.log(err);
            logger.error(err);
        } else {
            callback();
        }
    });
};

module.exports.getRequest = function (connection, sql, callback) {
    var req = tedious.Request;
    var request = new req(sql, function (err) {
        if (err) {
            console.log(err);
            logger.error(err);
        }

        if (callback) {
            callback(request.rows);
        }

        connection.close();
    });
    request.rows = [];
    registerRowFiller(request);

    return request;
};

var getRow = function (columns) {
    var row = {};
    columns.forEach(function (column) {
        if (column.isNull) {
            row[column.metadata.colName] = null;
        } else {
            row[column.metadata.colName] = column.value;
        }
    });
    return row;
};

var registerRowFiller = function (request) {
    request.on('row', function (columns) {
        var row = getRow(columns);
        request.rows.push(row);
    });
};

module.exports.getRow = getRow;
module.exports.registerRowFiller = registerRowFiller;