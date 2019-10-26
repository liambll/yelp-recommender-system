/*
 * 
 *  Web Server Implementation using nodejs and expressjs
 * 
 */

//export modules
var
        _express = require('express'),
        _bodyParser = require('body-parser'),
        _path = require('path'),
        _ejs = require('ejs'),
        _async = require('async'),
        _cassandra = require('cassandra-driver');

//cluster contact points and keyspace
var _client = new _cassandra.Client({contactPoints: ['199.60.17.136', '199.60.17.173'], keyspace: 'bigbang'});
//gateway domain name and port
var _listenDevice = 'gateway.sfucloud.ca';
const _listenPort = 8000;

var _app = _express();
var _dirName = '/home/rmathiya/nodejs/node-v6.9.1-linux-x64/bin/views';
var _dirDemo = '/home/rmathiya/nodejs/node-v6.9.1-linux-x64/bin/demo';
var _errorMsgContentType = 'text/plain;charset=utf-8';
var _userId = '';
var _password = '';
var _firstName = '';
var _recommendationData, _busniessCheckinData;
var _busniessIds;
var _errorMsg = "";
//lock to control asynchronous flow
var lock = 2;

//connect to cassandra database
_client.connect(function (err, result) {
    console.log('cassandra connected');
});

_app.use(_bodyParser.urlencoded({extended: true}));

_app.use(_express.static(_dirName));

_app.use(_express.static(_dirDemo));

_app.engine('html', _ejs.renderFile);

_app.set('view engine', 'ejs');

//load user login page
_app.get('/', function (request, response) {
    response.sendFile('index.html');
});

//load userinfo page
_app.get('/userInfo', function (request, response) {

    _errorMsg = "";

    //query to get the top 20 restaurants per each state for a given user
    const _recommendationQuery = "Select business_id , business_name as name, business_review_count as review_count, business_state as state,\
				  business_full_address as full_address,  business_open as open, business_city as city, business_stars as stars,\
                                  business_latitude as latitude, business_longitude as longitude from recommendations where  user_id='" + _userId + "' ALLOW FILTERING";

    console.log("_recommendationQuery------------->", _recommendationQuery);
    _client.execute(_recommendationQuery, function (error, result) {
        if (error)
        {
            _errorMsg = "Failed to retrieve recommendations from the database!!!";
        } else
        {
            _recommendationData = result.rows;
            if (typeof _recommendationData === 'undefined' || _recommendationData.length === 0)
            {
                _errorMsg = "User information is not available!!!";
            }
        }
        lock -= 1;
        if (lock === 0)
        {
            lock = 2;
            sendResponse();
        }
    });

    //query to get the checkin details of the top 20 recommended restaurants per state for a given user
    const _businessCheckinQuery = "SELECT business_id, day, hour, checkin FROM recommendations_checkin where user_id='" + _userId + "' ALLOW FILTERING";

    console.log("_businessCheckinQuery------------->", _businessCheckinQuery);

    _client.execute(_businessCheckinQuery, function (error, result) {
        if (error)
        {
            _errorMsg = "Failed to retrieve checkin details of the restaurants from the database!!!";
        } else
        {
            // console.log("_busniessCheckinData------------->", _busniessCheckinData);
            _busniessCheckinData = result.rows;
            if (typeof _busniessCheckinData === 'undefined' || _busniessCheckinData.length === 0)
            {
                _errorMsg = "Business checkin information is not available!!!";
            }
        }
        lock -= 1;
        if (lock === 0)
        {
            lock = 2;
            sendResponse();
        }
    });

    //callback
    function sendResponse()
    {
        if (_errorMsg !== "")
        {
            response.setHeader('Content-Type', _errorMsgContentType);
            response.send(_errorMsg);
        } else
        {
            response.render("userInfo", {firstname: _firstName, recommendationDetails: _recommendationData, busniessCheckinDetails: _busniessCheckinData});
        }
    }
});

//gets called if the user clicks on submit button in the login page 
_app.post('/', function (request, response) {

    //get the userid and the password
    _userId = request.body.userid;
    _password = request.body.pwd;
    _errorMsg = "";

    if (_userId !== '')
    {
        const _userLoginQuery = "select name, password from yelp_user where user_id='" + _userId + "'";

        _client.execute(_userLoginQuery, function (error, result) {
            if (error)
            {
                _errorMsg = "Failed to retrieve user information from the database!!!";
                sendResponse();
            } else
            {
                //validate userid
                var data = result.rows;
                if (typeof data === 'undefined' || data.length === 0)
                {
                    _errorMsg = "Invalid User ID!!!";
                } else
                {
                    //validate password
                    pwd = data[0].password;
                    _firstName = data[0].name;
                    console.log(_firstName);
                    console.log(_password);
                    console.log(pwd);
                    if (_password !== pwd)
                    {
                        _errorMsg = "Incorrect Password!!!";
                    }
                }
                sendResponse();
            }
        });
    } else
    {
        _errorMsg = "Invalid User Id";
        sendResponse();
    }

    //callback
    function sendResponse()
    {
        if (_errorMsg !== "")
        {
            response.setHeader('Content-Type', _errorMsgContentType);
            response.send(_errorMsg);
        } else
        {
            //redirect to userinfo page
            response.redirect('/userInfo');
        }
    }
});

_app.listen(_listenPort, _listenDevice, function () {
    console.log("Server listening on: http://gateway.sfucloud.ca:%s", _listenPort);
});