"use strict";

const express = require("express");
const pgp = require("pg-promise")();

// Constants
const PORT = 8080;
const HOST = "0.0.0.0";
const credentials = {
  host: "postgres",
  port: 5432,
  database: "root",
  user: "root",
  password: "root",
};
// App
const app = express();

const db = pgp(credentials);

app.get("/", (req, res) => {
  res.send("Hello World");
});

//sites
app.get("/sites/coord_search", (req, res) => {
  var lon = parseInt(req.query.lon);
  var lat = parseInt(req.query.lat);
  var lon_threshold = parseInt(req.query.lon_threshold);
  var lat_threshold = parseInt(req.query.lat_threshold);
  var site_response = {};
  db.any(
    "SELECT * FROM aea_sites WHERE longitude >= $1 AND longitude <= $2 AND latitude >= $3 AND latitude <= $4",
    [
      lon - lon_threshold,
      lon + lon_threshold,
      lat - lat_threshold,
      lat + lat_threshold,
    ]
  )
    .then((user) => {
      site_response = user;
      res.setHeader("Content-Type", "application/json");
      res.json({ sites: site_response });
    })
    .catch((error) => {
      console.log(error);
      res.send("error");
    });
});

app.get("/sites/name_search", (req, res) => {
  var name = req.query.name;
  db.any("SELECT * FROM aea_sites WHERE site_name LIKE $1", ["%" + name + "%"])
    .then((user) => {
      var name_response = user;
      res.setHeader("Content-Type", "application/json");
      res.send(name_response);
    })
    .catch((error) => {
      console.log(error);
      res.send("error");
    });
});

//wind speed
app.get("/wind_speed", (req, res) => {
  var site = req.query.site;
  var month = req.query.month;
  var hour = req.query.hour;
  db.any(
    "SELECT * FROM aea_sites WHERE site_name = $1 AND month = $2 AND hour = $3",
    [site, month, hour]
  )
    .then((user) => {
      var wind_response = user;
      res.setHeader("Content-Type", "application/json");
      res.send(wind_response);
    })
    .catch((error) => {
      console.log(error);
      res.send("error");
    });
});

app.listen(PORT, HOST);
console.log(`Running on http://${HOST}:${PORT}`);
