const express = require("express");
const bodyParser = require("body-parser");
const app = express();
const axios = require("axios");

require("dotenv").config();

// parse application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: false }));

// parse application/json
app.use(bodyParser.json());

app.set("view engine", "ejs"); //setting view
app.get("/", function (req, res) {
  res.render("pages/index");
});
app.post("/api/search", function (req, res) {
  var lat = req.body.Latitude;
  var lon = req.body.Longitude;
  var height = req.body.HubHeight;
  var wind_surface = req.body.WindSurface;
  var start_date = req.body.start;
  var end_date = req.body.end;

  // NASA POWER
  var formatted_start_date =
    start_date.slice(0, 4) + start_date.slice(5, 7) + start_date.slice(8, 10);
  var formatted_end_date =
    end_date.slice(0, 4) + end_date.slice(5, 7) + end_date.slice(8, 10);
  if (height < 6) {
    // Sets keyword based on where the height is closest to
    var param = "WS2M";
  } else {
    var param = "WS10M";
  }
  var nasa = axios.get(
    `https://power.larc.nasa.gov/api/temporal/hourly/point?community=RE&parameters=${param},WSC&latitude=${lat}&longitude=${lon}&start=${formatted_start_date}&end=${formatted_end_date}&format=JSON&wind-elevation=${height}&wind-surface=${wind_surface}`
  );

  // Wind Toolkit
  function yearRange(start_year, end_year) {
    var years = [];
    while (start_year <= end_year) {
      years.push(start_year++);
    }
    return years;
  }
  function spacedList(array) {
    var spaced_string = "";
    for (var i = 0; i < array.length; i++) {
      spaced_string += array[i] + " ";
    }
    return spaced_string.trim();
  }
  var begin_year = new Date(start_date).getFullYear();
  var end_year = new Date(end_date).getFullYear();
  var heights = [10, 40, 60, 80, 100, 120, 140, 160, 200];
  var closest = heights.reduce(function (prev, curr) {
    return Math.abs(curr - height) < Math.abs(prev - height) ? curr : prev;
  });
  console.log(spacedList(yearRange(begin_year, end_year)));

  var wind_key = process.env.WIND_TOOLKIT_API_KEY;
  var email = process.env.EMAIL;
  var wind_toolkit = axios.get(
    `https://developer.nrel.gov/api/wind-toolkit/v2/wtk-download.json?api_key=${wind_key}&wkt=POINT(${lat} ${lon})&attributes=windspeed_${closest}m,winddirection_${closest}m&names=${spacedList(
      yearRange(begin_year, end_year)
    )}&email=${email}`
  );
  axios
    .all([nasa, wind_toolkit])
    .then(
      axios.spread((...responses) => {
        var nasa_data = responses[0].data.properties.parameter.WSC;
        res.render("pages/results", { NASA: nasa_data });
      })
    )
    .catch((error) => {
      console.log(error);
      console.log(error.response.data);
    });
});
app.listen(3000, function () {
  console.log("Server started on port 3000");
});
